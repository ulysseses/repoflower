'''
Usage: refresh.py <language>
'''
from __future__ import print_function
import heapq
import json
from riak import RiakClient
from flask import Flask, jsonify
from docopt import docopt
import sys
sys.path.insert(0, '../redis')
from RedisConfig import RedisConfig

arguments = docopt(__doc__)
language = arguments['<language>']

cfg = RedisConfig()

# single threaded blocking... for now
client = RiakClient(host=cfg.RIAK_IPS.split(',')[0],
    pb_port=cfg.RIAK_PORT, protocol="pbc")
bucket = client.bucket("%s/top_flowers" % language)

node_set = set()
link_set = set()

def keygen():
    for keys in bucket.stream_keys():
        for key in keys:
            if key == 'K':
                continue
            yield key

for k in keygen():
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

with open('static/%s_top_K.json' % language, 'w') as f:
    app = Flask(__name__)
    with app.test_request_context():
        raw = jsonify({"nodes": nodes, "links": links}).data
        f.write(raw)
