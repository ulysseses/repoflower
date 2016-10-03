'''
Usage: index_es.py <language> <filename>
'''
from docopt import docopt
from elasticsearch import Elasticsearch
import csv
from itertools import islice
from ..redis import RedisConfig

cfg = RedisConfig()

arguments = docopt(__doc__)
language = arguments['<language>'].lower()
filename = arguments['<filename>']

if language not in {'python', 'go'}:
    raise Exception("only python and go are currently accepted")

es = Elasticsearch([{'host': cfg.ES_IP, 'port': cfg.ES_PORT}], timeout=120)

with open(filename, 'r') as f:
    bulk_data = []
    reader = csv.reader(f, delimiter=',')
    for _id, row in enumerate(islice(reader, 1, None)):
        user, repo_name, num_bytes = row[:3]
        data_dict = {
            'user': user,
            'repo_name': repo_name
        }
        op_dict = {
            'index': {
                '_index': 'github',
                '_type': language,
                '_id': _id
            }
        }
        if language == 'python':
            stars, fork = row[3:]
            stars = int(stars)
            fork = int(fork)
            # invert fork to enable custom query scoring
            fork = 1 if fork == 0 else 0
            data_dict['stars'] = stars
            data_dict['fork'] = fork
        bulk_data.append(op_dict)
        bulk_data.append(data_dict)

print("bulk indexing...")
res = es.bulk(index='github', body=bulk_data, refresh=False)
es.indices.flush(index='github')

print("es.count():")
print(es.count(index="github", doc_type=language))
