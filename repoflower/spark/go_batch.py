'''
spark-submit --master spark://172.31.1.231:7077 --driver-memory 13g \
    --executor-memory 13g --packages com.databricks:spark-avro_2.10:2.0.1 \
    go_batch.py
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
import sys
sys.path.insert(0, '../redis')
from RedisConfig import RedisConfig

cfg = RedisConfig()

LANGUAGE = "go"
RIAK_IPS = cfg.RIAK_IPS.split(',')
RIAK_PORT = int(cfg.RIAK_PORT)
K = int(cfg.K)
N = int(cfg.N)

###############################################################################
# Setup context
###############################################################################
conf = SparkConf() \
    .setMaster("spark://%s:%s" %
        (cfg.SPARK_IP, cfg.SPARK_PORT)) \
    .setAppName(cfg.SPARK_APP_NAME)
sc = SparkContext(conf=conf)
sc_sql = SQLContext(sc)

###############################################################################
# Load dependencies
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
def helper(tup):
    """
    Go over each line of the content (source file) and extract all
    dependencies. We're going to perform a shortcut: instead of querying
    Elasticsearch, let's check if the imported package/module starts with
    "github.com/".
    """
    _id, content = tup
    # Split the content by carriage return and newline, then flatten
    a = [x.split('\n') for x in content.split('\r')]
    b = []
    for lst in a:
        b.extend(lst)
    c = [x for x in b if x != '']
    # Try to find all imports
    try:
        d = []
        prog1 = re.compile(r'\s*import\s*\(\s*(.*)')
        prog2 = re.compile(r'\s*import\s*([^(]*)')
        single_flag = False
        counter = 0
        for line in c:
            match = prog1.match(line)
            if match is not None:
                counter += 1
                s = match.group(1).split('//')[0].rstrip()
                if s != '':
                    d.append(s)
                break
            else:
                match = prog2.match(line)
                if match is not None:
                    single_flag = True
                    s = match.group(1).split('//')[0].rstrip()
                    if s != '':
                        d.append(s)
                    break
            counter += 1
        if not single_flag:
            for i in range(counter, len(c)):
                line = c[i].split('//')[0].rstrip()
                if line == '':
                    continue
                if line[-1] == ')':
                    if line[:-1] != '':
                        d.append(line[:-1])
                    break
                d.append(line)
        prog3 = re.compile(r'.*\"(.*)\".*')
        e = [prog3.search(x) for x in d]
        f = [x.group(1) for x in e if x is not None]
        f = ['/'.join(gh_dep[11:].split('/')[:2])
             for gh_dep in f if dep[:11] == 'github.com/']
        f = [x for x in f if x != '']
    except:
        f = []
    deps = (_id, f)
    # (id, deps)
    return deps

rdd_deps = sc_sql.sql("""
    SELECT id, content
    FROM contents
    """) \
    .rdd \
    .filter(lambda tup: tup[1] is not None) \
    .map(helper) \
    .filter(lambda tup: len(tup[1]) != 0)

# (id, deps)
df_deps = sc_sql.createDataFrame(rdd_deps, ['id', 'deps'])

# (id, repo)
df_files = sc_sql.sql("""
    SELECT id, repo_name as repo
    FROM files
    """)

def reversal(row):
    if row.deps is None:
        return []
    return [(dep, row.repo) for dep in row.deps]

# (deps, repo) -> (dep, repo)
rdd_matches = df_files.join(df_deps, on='id', how='outer') \
    .select('deps', 'repo') \
    .flatMap(reversal)
    #.flatMap(lambda row: [(dep, row.repo) for dep in row.deps])

###############################################################################
# Reverse schema
###############################################################################
def seqOp(z, x):
    z.add(x)
    return z

def combOp(z1, z2):
    if len(z1) > len(z2):
        z1.update(z2)
        return z1
    z2.update(z1)
    return z2

rdd_neighbors = rdd_matches.aggregateByKey(set(), seqOp, combOp) \
    .map(lambda tup: (tup[0], list(tup[1])))

###############################################################################
# Send to Riak
###############################################################################
def write_neighbors_to_riak(tups):
    clients = [RiakClient(host=ip, protocol='pbc',
                          pb_port=RIAK_PORT)
               for ip in RIAK_IPS]
    buckets = [client.bucket('%s/neighbors' % LANGUAGE)
        for client in clients]
    for tup, bucket in zip(tups, cycle(buckets)):
        repo, adj_repos = tup
        _ = bucket.new(repo, adj_repos).store()

rdd_neighbors.foreachPartition(write_neighbors_to_riak)

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
    clients = [RiakClient(host=ip, protocol='pbc', pb_port=RIAK_PORT)
        for ip in RIAK_IPS]
    neighborss = [client.bucket('%s/neighbors' % LANGUAGE) \
        for client in clients]
    flowers = [client.bucket('%s/flowers' % LANGUAGE) \
        for client in clients]
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

top_K_repos = rdd_neighbors.top(K, key=lambda tup: len(tup[1]))

# (dst_repo)
rdd_repo = rdd_neighbors.map(lambda tup: tup[0])
rdd_repo.foreachPartition(write_flower_to_riak)

def cache_top_K():
    clients = [RiakClient(host=ip, protocol='pbc', pb_port=RIAK_PORT)
        for ip in RIAK_IPS]
    flowers = [client.bucket('%s/flowers' % LANGUAGE) \
        for client in clients]
    top_flowers = [client.bucket('%s/top_flowers' % LANGUAGE) \
        for client in clients]
    for k, tup, flower, top_flower in zip(range(K),
                                          top_K_repos,
                                          cycle(flowers),
                                          cycle(top_flowers)):
        dst_repo = tup[0]
        top_flower.new('%d' % k, flower.get(dst_repo).data).store()
    top_flowers[0].new('K', K).store()

cache_top_K()
