# for python 2.7
from __future__ import print_function
from requests.auth import HTTPBasicAuth
from urllib2 import urlopen, Request
import requests
from itertools import islice, cycle
import glob
import csv
import json
import time
import os
from tqdm import tqdm

GITHUB_API_TOKENS = []
with open('tokens.txt', 'r') as f:
    for line in f:
        GITHUB_API_TOKENS.append(line.rstrip().split()[1])
TOKEN_CYCLE = cycle(GITHUB_API_TOKENS)

def call_github(user, repo, token):
    url = "https://api.github.com/repos/%s/%s" % (user, repo)
    req = Request(url)
    req.add_header('Authorization', 'token %s' % token)
    res = urlopen(req, timeout=5)
    jsondata = json.loads(res.read())
    return jsondata


in_filename = glob.glob('python_repos_*.csv')[0]
out_filename = in_filename[:-4] + '_out.csv'
print('in_filename: %s' % in_filename)
print('out_filename: %s' % out_filename)
with open(in_filename, 'r') as f:
    reader = csv.reader(f, delimiter=',')
    keywords = [row for row in islice(reader, 1, None)]

with open(out_filename, 'w') as f:
    f.write('user,repo_name,num_bytes,stars,fork\n')
    for i, row in tqdm(enumerate(keywords)):
        if i % 1000 == 0 and i > 0:
            f.flush()
        user, repo, num_bytes = row
        while True:
            token = next(TOKEN_CYCLE)
            try:
                jsondata = call_github(user, repo, token)
                stars = jsondata['stargazers_count']
                fork = 1 if jsondata['fork'] else 0
                f.write('%s,%s,%s,%s,%s\n' % \
                    (user, repo, num_bytes, stars, fork))
                break
            except requests.exceptions.Timeout as e:
                print(e)
                time.sleep(5)
            except requests.exceptions.HTTPError as e:
                print(e)
                print("%s/%s" % (user, repo))
                f.write('%s,%s,%s,%s,%s\n' % \
                    (user, repo, num_bytes, 0, 0))
                break
            except Exception as e:
                print(e)
                print("%s/%s" % (user, repo))
                break
