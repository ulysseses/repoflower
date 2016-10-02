from riak import RiakClient
import python_config as cfg

prefixes = ['python']
suffixes = ['neighbors', 'flowers', 'top_flowers']

client = RiakClient(host=cfg.RIAK_IPS[0], pb_port=cfg.RIAK_PORT,
	protocol='pbc')
for bucket in client.get_buckets():
	for keys in bucket.stream_keys():
		for key in keys:
			bucket.delete(key)
