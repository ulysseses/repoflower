from __future__ import print_function
import heapq
import json
from riak import RiakClient
from flask import Flask, jsonify

# single threaded blocking... for now
client = RiakClient(host="172.31.1.229", pb_port=8087, protocol="pbc")
bucket = client.bucket("repo_adj_graph")
bucket_top = client.bucket("top_repo_adj_graph")

# Get at most K * M * N nodes and (K * M) + (K * M * N) links
K = 5
M = 5
N = 5
repo_set = set()
_nodes = []
_links = []

# def keygen():
#     for keys in bucket_top.stream_keys():
#         for key in key:
#             if key == 'K':
#                 continue
#             yield key

# 0th order
for k in range(K):
    repo0, repos1 = bucket_top.get('%d' % k).data
    degree0 = len(repos1)
    if repo0 not in repo_set:
        repo_set.add(repo0)
        _nodes.append((repo0, degree0))

    # Take up to M
    for i in range(len(repos1)):
        repos2 = bucket.get(repos1[i]).data
        repos2 = repos2 if repos2 is not None else []
        repos1[i] = (repos1[i], len(repos2))
    top_repos1 = heapq.nlargest(M, repos1, lambda tup: tup[1])

    # 1st order
    for x in top_repos1:
        repo1, degree1 = x
        if repo1 not in repo_set:
            repo_set.add(repo1)
            _nodes.append((repo1, degree1))
        _links.append((repo0, repo1))

        # Take up to N
        repos2 = bucket.get(repo1).data
        repos2 = repos2 if repos2 is not None else []
        for j in range(len(repos2)):
            repos3 = bucket.get(repos2[j]).data
            repos3 = repos3 if repos3 is not None else []
            repos2[j] = (repos2[j], len(repos3))
        top_repos2 = heapq.nlargest(N, repos2, lambda tup: tup[1])

        # 2nd order
        for y in top_repos2:
            repo2, degree2 = y
            if repo2 not in repo_set:
                repo_set.add(repo2)
                _nodes.append((repo2, degree2))
            _links.append((repo1, repo2))

del repo_set

nodes = [{"id": repo, "group": degree} for (repo, degree) in _nodes]
del _nodes
links = [{"source": src_repo, "target": dst_repo} for \
    (src_repo, dst_repo) in _links]
del _links

with open('static/topK.json', 'w') as f:
    app = Flask(__name__)
    with app.test_request_context():
        raw = jsonify({"nodes": nodes, "links": links}).data
        f.write(raw)
