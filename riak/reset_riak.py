from riak import RiakClient
import sys
sys.path.insert(0, '../redis')
from RedisConfig import RedisConfig

cfg = RedisConfig()

client = RiakClient(host=cfg.RIAK_IPS.split(',')[0], pb_port=int(cfg.RIAK_PORT),
	protocol='pbc')
for bucket in client.get_buckets():
	for keys in bucket.stream_keys():
		for key in keys:
			bucket.delete(key)
