from riak import RiakClient
import config as cfg

client = RiakClient(host=cfg.RIAK_IPS[0], pb_port=cfg.RIAK_PORT,
	protocol='pbc')
bucket = client.bucket('repo_adj_graph')
bucket_top = client.bucket('top_repo_adj_graph')

for keys in bucket.stream_keys():
	for key in keys:
		bucket.delete(key)

for keys in bucket_top.stream_keys():
	for key in keys:
		bucket_top.delete(key)
