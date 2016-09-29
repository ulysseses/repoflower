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
        "repos": {
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

with open('python_repos2.csv', 'r') as f:
    bulk_data = []
    reader = csv.reader(f, delimiter=',')
    for _id, row in enumerate(islice(reader, 1, None)):
        user, repo_name, num_bytes, stars, fork = row
        # invert fork to enable custom query scoring
        fork = 1 if fork == 0 else 0
        # es.index(index='github', doc_type='repos', id=_id, body={
        #     'user': user, 'repo_name': repo_name
        # })
        data_dict = {
            'user': user,
            'repo_name': repo_name,
            'stars': stars,
            'fork': fork
        }
        op_dict = {
            'index': {
                '_index': 'github',
                '_type': 'repos',
                '_id': _id
            }
        }
        bulk_data.append(op_dict)
        bulk_data.append(data_dict)

print("bulk indexing...")
res = es.bulk(index='github', body=bulk_data, refresh=False)
es.indices.flush(index='github')

print("es.count():")
print(es.count(index="github", doc_type="repos"))
