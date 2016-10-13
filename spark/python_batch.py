'''
spark-submit --master spark://172.31.1.231:7077 --driver-memory 13g \
    --executor-memory 13g --packages com.databricks:spark-avro_2.10:2.0.1 \
    python_batch.py
'''
from __future__ import print_function
import re
import json
import heapq
from itertools import islice, cycle, permutations, izip  # python 2.7
from collections import Counter
from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark.sql import SQLContext
from elasticsearch import Elasticsearch
from riak import RiakClient
import sys
sys.path.insert(0, '../redis')
from RedisConfig import RedisConfig

# Get configuration parameters for any machine in the cluster via Redis.
cfg = RedisConfig()

# Save configuration parameters as global variables that will be broadcast
# via Spark to the worker nodes (RedisConfig isn't serializable via pickle)
LANGUAGE                = "python"
SPARK_IP                = cfg.SPARK_IP
SPARK_PORT              = int(cfg.SPARK_PORT)
SPARK_APP_NAME          = cfg.SPARK_APP_NAME
AWS_ACCESS_KEY_ID       = cfg.AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY   = cfg.AWS_SECRET_ACCESS_KEY
S3_BUCKET               = cfg.S3_BUCKET
S3_CONTENTS_FILE_BLOB   = cfg.S3_CONTENTS_FILE_BLOB % (LANGUAGE, LANGUAGE)
S3_FILES_FILE_BLOB      = cfg.S3_FILES_FILE_BLOB % (LANGUAGE, LANGUAGE)
ES_IP                   = cfg.ES_IP
ES_PORT                 = int(cfg.ES_PORT)
RIAK_IPS                = cfg.RIAK_IPS.split(',')
RIAK_PORT               = int(cfg.RIAK_PORT)
K                       = int(cfg.K)
N                       = int(cfg.N)
B                       = int(cfg.B)
#STOPWORDS               = set(cfg.STOPWORDS.split(','))

###############################################################################
# Setup context
###############################################################################
conf = SparkConf() \
    .setMaster("spark://%s:%d" %
        (SPARK_IP, SPARK_PORT)) \
    .setAppName(SPARK_APP_NAME)
sc = SparkContext(conf=conf)
sc_sql = SQLContext(sc)

###############################################################################
# Load contents and files
###############################################################################
# Load contents table
df_contents = sc_sql.read.format("com.databricks.spark.avro").load(
    "s3a://%s:%s@%s/%s" %
    (AWS_ACCESS_KEY_ID,
     AWS_SECRET_ACCESS_KEY,
     S3_BUCKET,
     S3_CONTENTS_FILE_BLOB))
df_contents.registerTempTable("contents")

def extract_dependency(tup):
    '''
    Extract names of dependencies
    Output: (id, match_obj)
    '''
    _id, line = tup
    prog = re.compile("""^(?:import|from)[\t ]+([^\. ]+)""")
    obj = prog.match(line)
    if obj:
        return (_id, obj.group(1))
    return (_id, obj)

# (id, dep)
df_contents2 = sc_sql.createDataFrame(
    sc_sql.sql("""
        SELECT id, SPLIT(content, '\\n') as lines
        FROM contents
        """) \
        .flatMap(lambda row: \
            [(row.id, line) for line in row.lines] if row.lines else []) \
        .map(extract_dependency) \
        .filter(lambda tup: True if tup[1] else False) \
        .flatMap(lambda tup: [(tup[0], x.strip(' '))
            for x in tup[1].split(',')]),
    ['id', 'dep'])

# Load files table
df_files = sc_sql.read.format("com.databricks.spark.avro").load(
    "s3a://%s:%s@%s/%s" %
    (AWS_ACCESS_KEY_ID,
     AWS_SECRET_ACCESS_KEY,
     S3_BUCKET,
     S3_FILES_FILE_BLOB))
df_files.registerTempTable("files")

# (id, dep_repo, path)
df_files2 = sc_sql.sql("""
    SELECT id, repo_name as repo, path
    FROM files
    """)

###############################################################################
# For each repo:
# 1. gather all unique file/folder names
# 2. gather all unique dependency names
# 3. remove dependencies with names equal to any file/folder
#    name within the same repo (i.e. "intra-deps")
###############################################################################
# (dep_repo, path, dep)
tmp1 = df_files2.join(df_contents2, on='id')
tmp1.persist(StorageLevel.DISK_ONLY)

def seqOp1(z, x):
    z.update(x)
    return z

def combOp1(z1, z2):
    if len(z1) > len(z2):
        z1.update(z2)
        return z1
    z2.update(z1)
    return z2

def seqOp2(z, x):
    z.add(x)
    return z

def combOp2(z1, z2):
    if len(z1) > len(z2):
        z1.update(z2)
        return z1
    z2.update(z1)
    return z2

def remove_intra_deps1(tup):
    '''
    Version 1 (used for input as an Elasticsearch query)

    Remove dependencies with names equal to any file/folder in the same repo.
    Then, transform the (repo, dep_set) record representation to
    [(repo, dep) for dep in dep_set], which is accordingly flattened (by
    flatMap).
    '''
    repo = tup[0]
    dep_set, name_set = tup[1]
    dep_set.difference_update(name_set)
    return [(repo, dep) for dep in dep_set]

# (repo, name_set)
tmp2 = tmp1.select('repo', 'path') \
    .map(lambda row: (row.repo, row.path[:-2].split('/'))) \
    .aggregateByKey(set(), seqOp1, combOp1)

# (repo, dep_set)
tmp3 = tmp1.select('repo', 'dep') \
    .rdd \
    .aggregateByKey(set(), seqOp2, combOp2)

# (repo, (dep_set, name_set))
tmp4 = tmp3.join(tmp2)
tmp4.persist(StorageLevel.DISK_ONLY)

def minibatch(iterable, size):
    lst = []
    for i, x in enumerate(iterable):
        if i > 0 and i % size == 0:
            yield lst
            lst = []
        lst.append(x)
    yield lst

###############################################################################
# Query Elasticsearch for dependency to repo matching
###############################################################################
# (repo, filtered_dep)
rdd_es_input = tmp4.flatMap(remove_intra_deps1)

def _match_repos(tups, ES_IP, ES_PORT, LANGUAGE):
    '''
    Mini-batch the tups in the partition, use them to bulk search
    ElasticSearch, then concatenate the responses.

    The resulting RDD looks like this:
    (dst_repo, src_repo)
      dst_repo: repo that is depended on
      src_repo: the src_repo that depends on dst_repo
    '''
    results = []
    # NOTE: timeout to 3600 seconds
    es = Elasticsearch([{'host' : ES_IP, 'port': ES_PORT}],
        timeout=3600, filter_path=['hits.hits._*'])
    # TODO: refactor query's schema in case future index/query structure changes
    query = {
        "query" : {
            "function_score": {
                "query": {
                    "constant_score" : { 
                        "filter" : {
                            "term" : { 
                                "repo_name" : None
                            }
                        }
                    }
                },
                "functions": [
                    {
                        "field_value_factor": {
                            "field": "stars",
                            "missing": 1
                        }
                    },
                    {
                        "field_value_factor": {
                            "field": "fork",
                            "missing": 1
                        }
                    }
                ]
            }
        },
        "from": 0,
        "size": 1
    }
    for mb in minibatch(tups, 10):
        body_elems = []
        # Build the mini-batch to be sent to es.msearch
        for tup in mb:
            dep = tup[1]
            query['query']['function_score']['query']['constant_score'] \
                ['filter']['term']['repo_name'] = dep
            body_elems.append('{}')
            body_elems.append(json.dumps(query))
        body = '\n'.join(body_elems)
        response_obj = es.msearch(body=body, index='github',
            doc_type=LANGUAGE)
        responses = response_obj['responses']
        for tup, response in izip(mb, responses):
            hits = response['hits']['hits']
            # Filter out responses with zero hits
            if len(hits) != 0:
                _source = hits[0]['_source']
                dst_repo = "%s/%s" % (_source['user'], _source['repo_name'])
                src_repo = tup[0]
                # Filter out forks
                if src_repo != dst_repo:
                    results.append((dst_repo, src_repo))
    return results

def match_repos(ES_IP, ES_PORT, LANGUAGE):
    return lambda tups: _match_repos(tups, ES_IP, ES_PORT, LANGUAGE)

def seqOp3(z, x):
    z.append(x)
    return z

def combOp3(z1, z2):
    if len(z1) > len(z2):
        z1.extend(z2)
        return z1
    z2.extend(z1)
    return z2

# (dst_repo, src_repo)
rdd_query_output = rdd_es_input.mapPartitions(match_repos(ES_IP, ES_PORT,
    LANGUAGE))

# (dst_repo, src_repos, len(src_repos))
rdd_repo_graph = rdd_query_output.aggregateByKey([], seqOp3, combOp3) \
    .map(lambda tup: (tup[0], tup[1], len(tup[1])))
tmp1.unpersist()
rdd_repo_graph.persist(StorageLevel.DISK_ONLY)

###############################################################################
# Obtain the top K depended-on repos
###############################################################################
top_repos = rdd_repo_graph.top(K, key=lambda tup: tup[-1])

# # Briefly visualize the top 10 repos
# for tup in islice(top_repos, 0, 10):
#     dst_repo, src_repos, count = tup
#     print("%s: %d" % (dst_repo, count))
#     for src_repo in islice(src_repos, 0, 10):
#         print("  %s" % src_repo)

###############################################################################
# Send graph edges to Riak
###############################################################################
# Send the rest of the repo adjacency lists to Riak
def _write_neighbors(tups, RIAK_IPS, RIAK_PORT, LANGUAGE):
    clients = [RiakClient(host=ip, protocol='pbc',
                          pb_port=RIAK_PORT)
              for ip in RIAK_IPS]
    buckets = [client.bucket('%s/neighbors' % LANGUAGE)
        for client in clients]
    for tup, bucket in izip(tups, cycle(buckets)):
        repo, adj_repos, _ = tup
        _ = bucket.new(repo, adj_repos).store()

def write_neighbors(RIAK_IPS, RIAK_PORT, LANGUAGE):
    return lambda tups: _write_neighbors(tups, RIAK_IPS, RIAK_PORT, LANGUAGE)

rdd_repo_graph.foreachPartition(write_neighbors(RIAK_IPS, RIAK_PORT, LANGUAGE))

###############################################################################
# Build the floral structure and send to Riak
###############################################################################
def _write_flowers(repos, RIAK_IPS, RIAK_PORT, N, LANGUAGE):
    '''
    We could optimize this function via memoization. If part of a flower
    already exists on the Riak cluster, then we just re-use that instead of
    calculating it again. However, in the worst-case scenario, since Riak isn't
    in strong consistency mode by default, we have no guarantee that the
    memoized view is up-to-date/consistent.

    Riak does have a strong consistency mode, but at the moment it isn't ready
    for production.
    '''
    clients = [RiakClient(host=ip, protocol='pbc', pb_port=RIAK_PORT)
        for ip in RIAK_IPS]
    neighbors = [client.bucket('%s/neighbors' % LANGUAGE) \
        for client in clients]
    flowers = [client.bucket('%s/flowers' % LANGUAGE) \
        for client in clients]

    for repo0, neighbor, flower in izip(repos,
                                        cycle(neighbors),
                                        cycle(flowers)):
        repo_set = set()
        nodes = []
        links = []
        repos1 = neighbor.get(repo0).data
        repos1 = repos1 if repos1 is not None else []
        degree0 = len(repos1)
        if repo0 not in repo_set:
            repo_set.add(repo0)
            nodes.append([repo0, degree0])

        # Take up to N
        for i in range(len(repos1)):
            repos2 = neighbor.get(repos1[i]).data
            repos2 = repos2 if repos2 is not None else []
            repos1[i] = [repos1[i], len(repos2)]
        top_repos1 = heapq.nlargest(N, repos1, lambda tup: tup[1])

        # 1st order
        for x in top_repos1:
            repo1, degree1 = x
            if repo1 not in repo_set:
                repo_set.add(repo1)
                nodes.append([repo1, degree1])
            links.append([repo0, repo1])

            # Take up to N
            repos2 = neighbor.get(repo1).data
            repos2 = repos2 if repos2 is not None else []
            for j in range(len(repos2)):
                repos3 = neighbor.get(repos2[j]).data
                repos3 = repos3 if repos3 is not None else []
                repos2[j] = [repos2[j], len(repos3)]
            top_repos2 = heapq.nlargest(N, repos2, lambda tup: tup[1])

            # 2nd order
            for y in top_repos2:
                repo2, degree2 = y
                if repo2 not in repo_set:
                    repo_set.add(repo2)
                    nodes.append([repo2, degree2])
                links.append([repo1, repo2])

        flower.new(repo0, [nodes, links]).store()

def write_flowers(RIAK_IPS, RIAK_PORT, N, LANGUAGE):
    return lambda repos: _write_flowers(repos, RIAK_IPS,
        RIAK_PORT, N, LANGUAGE)

# (dst_repo)
rdd_repo = rdd_repo_graph.map(lambda tup: tup[0])
rdd_repo.foreachPartition(write_flowers(RIAK_IPS, RIAK_PORT, N, LANGUAGE))

# Send top K flowers to Riak
def write_top_flowers(RIAK_IPS, RIAK_PORT, LANGUAGE):
    clients = [RiakClient(host=ip, protocol='pbc', pb_port=RIAK_PORT)
        for ip in RIAK_IPS]
    flowers = [client.bucket('%s/flowers' % LANGUAGE) \
        for client in clients]
    top_flowers = [client.bucket('%s/top_flowers' % LANGUAGE) \
        for client in clients]

    for k, tup, flower, top_flower in izip(range(len(top_repos)),
                                          top_repos,
                                          cycle(flowers),
                                          cycle(top_flowers)):
        dst_repo = tup[0]
        top_flower.new('%d' % k, flower.get(dst_repo).data).store()

    top_flowers[0].new('K', len(top_repos)).store()

write_top_flowers(RIAK_IPS, RIAK_PORT, LANGUAGE)
rdd_repo_graph.unpersist()

###############################################################################
# Now let's find co-occurrences!
###############################################################################
def remove_intra_deps2(tup):
    '''
    Version 2 (used for calculating intra-repo dependency co-occurrences)

    Remove dependencies with names equal to any file/folder in the same repo.
    Then, transform from (repo, dep_set) record format to
    (dep_list) format, dropping the repo column in the process.
    '''
    repo = tup[0]
    dep_set, name_set = tup[1]
    dep_set.difference_update(name_set)
    return list(dep_set)

def _filter_stop_words(dep_lists, ES_IP, ES_PORT, LANGUAGE):
    '''
    Filter each dep_list for stop words by keeping the dep in dep_list iff
    dep gives zero matches from Elasticsearch.
    '''
    results = []
    # NOTE: timeout to 3600 seconds
    es = Elasticsearch([{'host' : ES_IP, 'port': ES_PORT}],
        timeout=3600, filter_path=['hits.hits._*'])
    query = {
        "query": {
            "constant_score": {
                "filter": {
                    "term": {
                        "repo_name": None
                    }
                }
            }
        },
        "from": 0,
        "size": 1
    }
    for dep_list in dep_lists:
        x = []
        if len(dep_list) == 0:
            continue
        if len(dep_list) >= 15:
            # mini-batch to not overload ES
            for mb in minibatch(dep_list, 10):
                body_elems = []
                for dep in mb:
                    query['query']['constant_score']['filter']['term'] \
                        ['repo_name'] = dep
                    body_elems.append('{}')
                    body_elems.append(json.dumps(query))
                body = '\n'.join(body_elems)
                response_obj = es.msearch(body=body, index='github',
                    doc_type=LANGUAGE)
                responses = response_obj['responses']
                for dep, response in izip(mb, responses):
                    hits = response['hits']['hits']
                    if len(hits) != 0:
                        if all(ord(c) < 128 for c in dep):
                            x.append(dep)
                        x.append(dep)
        else:
            body_elems = []
            for dep in dep_list:
                query['query']['constant_score']['filter']['term'] \
                    ['repo_name'] = dep
                body_elems.append('{}')
                body_elems.append(json.dumps(query))
            body = '\n'.join(body_elems)
            response_obj = es.msearch(body=body, index='github',
                doc_type=LANGUAGE)
            responses = response_obj['responses']
            for dep, response in izip(dep_list, responses):
                hits = response['hits']['hits']
                if len(hits) != 0:
                    if all(ord(c) < 128 for c in dep):
                        x.append(dep)
                    x.append(dep)
        if len(x) != 0:
            x = list(set(x))
            results.append(x)

    return results

def filter_stop_words(ES_IP, ES_PORT, LANGUAGE):
    return lambda dep_lists: _filter_stop_words(dep_lists, ES_IP, ES_PORT,
        LANGUAGE)

# (dep1, dep2)
rdd_dep_pairs = tmp4.map(remove_intra_deps2) \
    .mapPartitions(filter_stop_words(ES_IP, ES_PORT, LANGUAGE)) \
    .flatMap(lambda dep_list: list(permutations(dep_list, r=2)))
# rdd_dep_pairs = tmp4.map(remove_intra_deps2) \
#     .mapPartitions(filter_stop_words2(STOPWORDS)) \
#     .flatMap(lambda dep_list: list(permutations(dep_list, r=2)))

def seqOp4(z, x):
    z[x] += 1
    return z

def combOp4(z1, z2):
    if len(z1) > len(z2):
        z1 += z2
        return z1
    z2 += z1
    return z2

# (dep1, counter)
rdd_cooccurs = rdd_dep_pairs.aggregateByKey(Counter(), seqOp4, combOp4)

def _write_cooccurs(tups, RIAK_IPS, RIAK_PORT, B, LANGUAGE):
    results = []
    clients = [RiakClient(host=ip, protocol='pbc', pb_port=RIAK_PORT)
        for ip in RIAK_IPS]
    buckets = [client.bucket('%s/cooccurs' % LANGUAGE) \
        for client in clients]
    for (dep, counter), bucket in izip(tups, cycle(buckets)):
        top_items = heapq.nlargest(B, counter.iteritems(),
            key=lambda tup: tup[1])  # WARNING: Python 2.7
        bucket.new(dep, top_items).store()
        results.append((dep, Counter(dict(top_items))))
    return results

def write_cooccurs(RIAK_IPS, RIAK_PORT, B, LANGUAGE):
    return lambda tups: _write_cooccurs(tups, RIAK_IPS, RIAK_PORT, B, LANGUAGE)

# Send to Riak
rdd_cooccurs2 = rdd_cooccurs.mapPartitions(
    write_cooccurs(RIAK_IPS, RIAK_PORT, B, LANGUAGE))

class TopMinHeapSet:
    def __init__(self, max_size):
        self.heap = []
        self.exists = set()
        self.max_size = max_size
        self.size = 0

    def process(self, depA, depB, count):
        dep1, dep2 = min(depA, depB), max(depA, depB)
        if (dep1, dep2) in self.exists:
            return
        if self.size < self.max_size:
            self.size += 1
            heapq.heappush(self.heap, (count, dep1, dep2))
            self.exists.add((dep1, dep2))
        elif self.heap[0][0] < count:
            popped = heapq.heappushpop(self.heap, (count, dep1, dep2))
            self.exists.remove((popped[1], popped[2]))
            self.exists.add((dep1, dep2))

    def show(self):
        return [(dep1, dep2, count) for (count, dep1, dep2) in self.heap]
        
    def show_sorted(self):
        return sorted([(dep1, dep2, count)
                      for (count, dep1, dep2) in self.heap],
                      key=lambda tup: tup[2],
                      reverse=True)

def _keep_top_cooccurs(tups, B):
    top_min_heap_set = TopMinHeapSet(B)
    for i, tup in enumerate(tups):
        depA, counter = tup
        for depB, count in counter.iteritems():  # WARNING: Python 2.7
            top_min_heap_set.process(depA, depB, count)
    return top_min_heap_set.show()

def keep_top_cooccurs(B):
    return lambda tups: _keep_top_cooccurs(tups, B)

# For each partition, keep the top B co-occurring dependencies
rdd_top_cooccurs = rdd_cooccurs2.mapPartitions(keep_top_cooccurs(B))

# Then collect and merge sort to keep top B
# (too lazy to implement partial sort via quickselect)
local_top_cooccurs = rdd_local_top_cooccurs.collect()
top_min_heap_set = TopMinHeapSet(B)
for x in local_top_cooccurs:
    top_min_heap_set.process(*x)

top_cooccurs = top_min_heap_set.show_sorted()

# Store top B co-occurrences into Riak
def write_top_cooccurs(RIAK_IPS, RIAK_PORT, LANGUAGE):
    clients = [RiakClient(host=ip, protocol='pbc', pb_port=RIAK_PORT)
        for ip in RIAK_IPS]
    buckets = [client.bucket('%s/top_cooccurs' % LANGUAGE) \
        for client in clients]
    for b, (dep1, dep2, count), bucket in izip(range(len(top_cooccurs)),
                                              top_cooccurs,
                                              cycle(buckets)):
        bucket.new('%d' % b, [dep1, dep2, count]).store()
    buckets[0].new('B', len(top_cooccurs)).store()

write_top_cooccurs(RIAK_IPS, RIAK_PORT, LANGUAGE)
