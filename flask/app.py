from flask import Flask, render_template, jsonify
from riak import RiakClient
import sys
sys.path.insert(0, '../redis')
from RedisConfig import RedisConfig

cfg = RedisConfig()
riak_ip = cfg.RIAK_IPS.split(',')[1]
riak_port = cfg.RIAK_PORT
del cfg

app = Flask(__name__)

@app.route('/')
def hello_python():
    return render_template('python.html')

@app.route('/go')
def hello_go():
    return render_template('go.html')

@app.route('/flowers/<lang>/<user>/<repo_name>')
def get_flower(lang, user, repo_name):
    client = RiakClient(host=riak_ip, pb_port=riak_port, protocol="pbc")
    bucket = client.bucket("%s/flowers" % lang)

    repo = "%s/%s" % (user, repo_name)
    graph = bucket.get(repo).data
    if graph is None:
        abort(404)

    _nodes, _links = graph
    nodes = [{"id": repo, "group": degree} for (repo, degree) in _nodes]
    del _nodes
    links = [{"source": repo_a, "target": repo_b} for \
        (repo_a, repo_b) in _links]
    del _links

    return jsonify({"nodes": nodes, "links": links})

@app.route('/cooccurs/<lang>/<dep>')
def get_cooccurs(lang, dep):
    client = RiakClient(host=riak_ip, pb_port=riak_port, protocol='pbc')
    bucket = client.bucket('%s/cooccurs' % lang)
    return jsonify({"cooccurs": bucket.get(dep).data})
    


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)
