'''
spark-submit --master spark://172.31.1.231:7077 --driver-memory 13g \
    --executor-memory 13g --packages com.databricks:spark-avro_2.10:2.0.1 \
    --py-files pyfiles.zip python_batch.py
'''
from __future__ import print_function
import re
import json
import heapq
from itertools import islice, cycle
from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark.sql import *
from elasticsearch import Elasticsearch
from riak import RiakClient
from docopt import docopt
import sys
sys.path.insert(0, '../redis')
from RedisConfig import RedisConfig

cfg = RedisConfig()

LANGUAGE = "python"

###############################################################################
# Setup context
###############################################################################
conf = SparkConf() \
    .setMaster("spark://%s:%d" %
        (cfg.SPARK_IP, cfg.SPARK_PORT)) \
    .setAppName(cfg.SPARK_APP_NAME)
sc = SparkContext(conf=conf)
sc_sql = SQLContext(sc)

###############################################################################
# Load contents and files
###############################################################################
# Load contents table
df_contents = sc_sql.read.format("com.databricks.spark.avro").load(
    "s3a://%s:%s@%s/%s" %
    (cfg.AWS_ACCESS_KEY_ID,
     cfg.AWS_SECRET_ACCESS_KEY,
     cfg.S3_BUCKET,
     cfg.S3_CONTENTS_FILE_BLOB % (LANGUAGE, LANGUAGE)))
df_contents.registerTempTable("contents")

# Load files table
df_files = sc_sql.read.format("com.databricks.spark.avro").load(
    "s3a://%s:%s@%s/%s" %
    (cfg.AWS_ACCESS_KEY_ID,
     cfg.AWS_SECRET_ACCESS_KEY,
     cfg.S3_BUCKET,
     cfg.S3_FILES_FILE_BLOB % (LANGUAGE, LANGUAGE)))
df_files.registerTempTable("files")

# Extract names of dependencies
def extract_dependency(tup):
    _id, line = tup
    prog = re.compile("""^(?:import|from)[\t ]+([^\. ]+)""")
    obj = prog.match(line)
    if obj:
        return (_id, obj.group(1))
    return (_id, obj)

rdd_contents = sc_sql.sql("""
    SELECT id, SPLIT(content, '\\n') as lines
    FROM contents
    """) \
    .flatMap(lambda row: \
        [(row.id, line) for line in row.lines] if row.lines else []) \
    .map(extract_dependency) \
    .filter(lambda tup: True if tup[1] else False)

# (id, dep)
df_contents = sc_sql.createDataFrame(rdd_contents, ['id', 'dep'])

# (id, dep_repo, path)
df_files = sc_sql.sql("""
    SELECT id, repo_name as repo, path
    FROM files
    """)

###############################################################################
# 1. For each repo, gather all unique file/folder names
# 2. For each repo, gather all unique dependency names
# 3. For each repo, remove dependencies with names equal to any file/folder
#    name within the same repo (i.e. "intra-deps")
###############################################################################
# (dep_repo, path, dep)
tmp1 = df_files.join(df_contents, on='id')
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

def remove_intra_deps(tup):
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

# (repo, filtered_dep)
rdd_query_input = tmp3.join(tmp2).flatMap(remove_intra_deps)

###############################################################################
# Query Elasticsearch for dependency to repo matching
###############################################################################
def minibatch(iterable, size):
    lst = []
    for i, x in enumerate(iterable):
        if i > 0 and i % size == 0:
            yield lst
            lst = []
        lst.append(x)
    yield lst

def match_repos(tups):
    """
    Mini-batch the tups in the partition, use them to bulk search
    ElasticSearch, then concatenate the responses.

    The resulting RDD looks like this:
    (dst_repo, src_repo)
      dst_repo: repo that is depended on
      src_repo: the src_repo that depends on dst_repo
    """
    results = []
    # NOTE: timeout to 3600 seconds
    es = Elasticsearch([{'host' : cfg.ES_IP, 'port': cfg.ES_PORT}],
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
        response_obj = es.msearch(body=body, index=['github'],
            doc_type=[LANGUAGE])
        responses = response_obj['responses']
        for tup, response in zip(mb, responses):
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
rdd_query_output = rdd_query_input.mapPartitions(match_repos)

###############################################################################
# Obtain the top K depended-on repos
###############################################################################
# (dst_repo, src_repos, len(src_repos))
rdd_repo_graph = rdd_query_output.aggregateByKey([], seqOp3, combOp3) \
    .map(lambda tup: (tup[0], tup[1], len(tup[1])))
tmp1.unpersist()
rdd_repo_graph.persist(StorageLevel.DISK_ONLY)

top_K_repos = rdd_repo_graph.top(cfg.K, key=lambda tup: tup[-1])

# Briefly visualize the top 10 repos
for tup in islice(top_K_repos, 0, 10):
    dst_repo, src_repos, count = tup
    print("%s: %d" % (dst_repo, count))
    for src_repo in islice(src_repos, 0, 10):
        print("  %s" % src_repo)

###############################################################################
# Send graph edges to Riak
###############################################################################
# Send the rest of the repo adjacency lists to Riak
def write_neighbors_to_riak(tups):
    clients = [RiakClient(host=ip, protocol='pbc',
                          pb_port=cfg.RIAK_PORT)
               for ip in cfg.RIAK_IPS.split(',')]
    buckets = [client.bucket('%s/neighbors' % LANGUAGE)
        for client in clients]
    for tup, bucket in zip(tups, cycle(buckets)):
        repo, adj_repos, _ = tup
        _ = bucket.new(repo, adj_repos).store()

rdd_repo_graph.foreachPartition(write_neighbors_to_riak)

###############################################################################
# Build the floral structure and send to Riak
###############################################################################
def write_flower_to_riak(repos):
    """
    We could optimize this function via memoization. If part of a flower
    already exists on the Riak cluster, then we just re-use that instead of
    calculating it again. However, in the worst-case scenario, since Riak isn't
    in strong consistency mode by default, we have no guarantee that the
    memoized view is up-to-date.

    Riak does have a strong consistency mode, but at the moment it isn't ready
    for production.
    """
    clients = [RiakClient(host=ip, protocol='pbc', pb_port=cfg.RIAK_PORT)
        for ip in cfg.RIAK_IPS.split(',')]
    neighborss = [client.bucket('%s/neighbors' % LANGUAGE) \
        for client in clients]
    flowers = [client.bucket('%s/flowers' % LANGUAGE) \
        for client in clients]

    N = cfg.N

    for repo0, neighbor, flower in zip(repos,
                                       cycle(neighborss),
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

# (dst_repo)
rdd_repo = rdd_repo_graph.map(lambda tup: tup[0])
rdd_repo.foreachPartition(write_flower_to_riak)

def cache_top_K():
    clients = [RiakClient(host=ip, protocol='pbc', pb_port=cfg.RIAK_PORT)
        for ip in cfg.RIAK_IPS.split(',')]
    flowers = [client.bucket('%s/flowers' % LANGUAGE) \
        for client in clients]
    top_flowers = [client.bucket('%s/top_flowers' % LANGUAGE) \
        for client in clients]

    for k, tup, flower, top_flower in zip(range(cfg.K),
                                          top_K_repos,
                                          cycle(flowers),
                                          cycle(top_flowers)):
        dst_repo = tup[0]
        top_flower.new('%d' % k, flower.get(dst_repo).data).store()

    top_flowers[0].new('K', cfg.K).store()

cache_top_K()
