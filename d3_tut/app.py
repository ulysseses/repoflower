from flask import Flask, render_template, jsonify
from riak import RiakClient


app = Flask(__name__)

@app.route('/')
def hello():
    return render_template('index2.html')

@app.route('/repos/<user>/<repo_name>')
def get_flower(user, repo_name):
    client = RiakClient(host="172.31.1.229", pb_port=8087, protocol="pbc")
    bucket = client.bucket("flower")

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


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
