from __future__ import print_function
import re
import json
from itertools import islice, cycle
from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark.sql import *
from elasticsearch import Elasticsearch
from riak import RiakClient
import config as cfg

###############################################################################
# Setup context
###############################################################################
conf = SparkConf() \
    .setMaster("spark://%s:%d" %
        (cfg.SPARK_IP, cfg.SPARK_PORT)) \
    .setAppName(cfg.SPARK_APP_NAME)
sc = SparkContext(conf)
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
     cfg.S3_CONTENTS_FILE_BLOB))
df_contents.registerTempTable("contents")

# Load files table
df_files = sc_sql.read.format("com.databricks.spark.avro").load(
    "s3a://%s:%s@%s/%s" %
    (cfg.AWS_ACCESS_KEY_ID,
     cfg.AWS_SECRET_ACCESS_KEY,
     cfg.S3_BUCKET,
     cfg.S3_FILES_FILE_BLOB))
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
    # query = {
    #     "query" : {
    #         "constant_score" : { 
    #             "filter" : {
    #                 "term" : { 
    #                     "repo_name" : None
    #                 }
    #             }
    #         }
    #     },
    #     "from": 0,
    #     "size": 1
    # }
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
            #query['query']['constant_score']['filter']['term'] \
            #    ['repo_name'] = dep
            query['query']['function_score']['query']['constant_score'] \
                ['filter']['term']['repo_name'] = dep
            body_elems.append('{}')
            body_elems.append(json.dumps(query))
        body = '\n'.join(body_elems)
        response_obj = es.msearch(body=body, index=['github'],
            doc_type=['repos'])
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
top_K_repos = rdd_repo_graph.top(cfg.K, key=lambda tup: tup[-1])

# Briefly visualize the top 10 repos
for tup in islice(top_K_repos, 0, 10):
    dst_repo, src_repos, count = tup
    print("%s: %d" % (dst_repo, count))
    for src_repo in islice(src_repos, 0, 10):
        print("  %s" % src_repo)

# Send the top K repos to Riak round-robin
riak_clients = [RiakClient(host=ip, protocol='pbc', pb_port=cfg.RIAK_PORT)
    for ip in cfg.RIAK_IPS]
riak_buckets_top = [riak_client.bucket('top_repo_adj_graph')
    for riak_client in riak_clients]
riak_buckets_top[0].new('K', cfg.K).store()

for k, tup, riak_bucket in zip(range(len(top_K_repos)),
                               top_K_repos,
                               cycle(riak_buckets_top)):
    _ = riak_bucket.new('%d' % k, [tup[0], tup[1]]).store()

###############################################################################
# Send graph edges to Riak
###############################################################################
# Send the rest of the repo adjacency lists to Riak
def write_to_riak(tups):
    riak_clients = [RiakClient(host=ip, protocol='pbc',
                               pb_port=cfg.RIAK_PORT)
                    for ip in cfg.RIAK_IPS]
    riak_buckets = [riak_client.bucket('repo_adj_graph')
        for riak_client in riak_clients]
    for tup, riak_bucket in zip(tups, cycle(riak_buckets)):
        repo, adj_repos, _ = tup
        _ = riak_bucket.new(repo, adj_repos).store()

rdd_repo_graph.foreachPartition(write_to_riak)
