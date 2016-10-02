from __future__ import print_function
import heapq
import json
from riak import RiakClient
from flask import Flask, jsonify

# single threaded blocking... for now
client = RiakClient(host="172.31.1.229", pb_port=8087, protocol="pbc")
bucket = client.bucket("python/top_flowers")

node_set = set()
link_set = set()

def keygen():
    for keys in bucket.stream_keys():
        for key in keys:
            if key == 'K':
                continue
            yield key

for k in keygen():
    # out = bucket.get(k).data
    # print('k: %s, out: %s' % (k, out))
    nodes, links = bucket.get(k).data
    nodes = [tuple(lst) for lst in nodes]
    links = [tuple(lst) for lst in links]
    node_set.update(nodes)
    link_set.update(links)

nodes, links = list(node_set), list(link_set)
del node_set
del link_set

nodes = [{"id": repo, "group": degree} for (repo, degree) in nodes]
links = [{"source": src_repo, "target": dst_repo} for \
    (src_repo, dst_repo) in links]

with open('static/python_topK.json', 'w') as f:
    app = Flask(__name__)
    with app.test_request_context():
        raw = jsonify({"nodes": nodes, "links": links}).data
        f.write(raw)
