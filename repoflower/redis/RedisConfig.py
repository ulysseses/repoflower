import redis

class RedisConfig:
	def __init__(self, host="172.31.1.234", port=6379, db=0):
		self.r = redis.StrictRedis(host=host, port=port, db=db)

	def __getattr__(self, attr):
		return self.r.get(attr)

	def __setattr__(self, k, v):
		self.r.set(k, v)
