"""
Usage: index_python_es.py <filename>
"""
from docopt import docopt
import requests
from elasticsearch import Elasticsearch
import json
import csv
from itertools import islice
from stopwords import STOPWORDS

arguments = docopt(__doc__)
filename = arguments['<filename>']

es = Elasticsearch([{'host': 'localhost', 'port': 9200}], timeout=120)

with open(filename, 'r') as f:
    bulk_data = []
    reader = csv.reader(f, delimiter=',')
    for _id, row in enumerate(islice(reader, 1, None)):
        user, repo_name, num_bytes, stars, fork = row
        stars = int(stars)
        fork = int(fork)
        # invert fork to enable custom query scoring
        fork = 1 if fork == 0 else 0
        data_dict = {
            'user': user,
            'repo_name': repo_name,
            'stars': stars,
            'fork': fork
        }
        op_dict = {
            'index': {
                '_index': 'github',
                '_type': 'python',
                '_id': _id
            }
        }
        bulk_data.append(op_dict)
        bulk_data.append(data_dict)

print("bulk indexing...")
res = es.bulk(index='github', body=bulk_data, refresh=False)
es.indices.flush(index='github')

print("es.count():")
print(es.count(index="github", doc_type="python"))
