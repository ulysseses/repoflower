'''
Reset the Elasticsearch cluster.
'''
import sys
sys.path.insert(0, '../redis')
from RedisConfig import RedisConfig

from elasticsearch import Elasticsearch
import json

cfg = RedisConfig()

es = Elasticsearch([{'host': cfg.ES_IP, 'port': cfg.ES_PORT}], timeout=120)

if es.indices.exists('github'):
    print("deleting '%s' index..." % 'github')
    res = es.indices.delete(index='github')
    print("response: '%s'" % res)

print("creating '%s' index..." % 'github')
config = {
    "settings": {
        "analysis": {
            "analyzer": {
                "my_analyzer": {
                    "type": "pattern",
                    "pattern": " ",
                    "lowercase": False,
                    "stopwords": cfg.STOPWORDS.split(',')
                }
            }
        }
    },
    "mappings": {
        "go": {
            "properties": {
                "repo_name": {
                    "type": "string",
                    "index": "not_analyzed"
                }
            }
        },
        "python": {
            "properties": {
                "repo_name": {
                    "type": "string",
                    "analyzer": "my_analyzer"
                }
            }
        }
    }
}
res = es.indices.create(index='github', body=json.dumps(config))
print("response: '%s'" % res)