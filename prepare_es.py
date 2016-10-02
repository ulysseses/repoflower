import requests
from elasticsearch import Elasticsearch
import json
import csv
from itertools import islice
from stopwords import STOPWORDS

es = Elasticsearch([{'host': 'localhost', 'port': 9200}], timeout=120)
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
                    "stopwords": STOPWORDS
                }
            }
        }
    },
    "mappings": {
        "go": {
            "properties": {
                "repo_name": {
                    "type": "string",
                    #"index": "not_analyzed"
                    "analyzer": "my_analyzer"
                }
            }
        },
        "python": {
            "properties": {
                "repo_name": {
                    "type": "string",
                    #"index": "not_analyzed"
                    "analyzer": "my_analyzer"
                }
            }
        }
    }
}
res = es.indices.create(index='github', body=json.dumps(config))
print("response: '%s'" % res)