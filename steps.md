## Select only the .go files

```
SELECT *
FROM [bigquery-public-data:github_repos.files]
WHERE RIGHT(path, 3) = '.go'
```

## Select the content of the .go files

```
SELECT *
FROM [bigquery-public-data:github_repos.contents]
WHERE id IN (SELECT id FROM gofiles.files)
```

## Select only Go repos

```
SELECT
  LEFT(repo_name, INSTR(repo_name, '/') - 1) AS user,
  RIGHT(repo_name, LENGTH(repo_name) - INSTR(repo_name, '/')) AS repo_name,
  language.bytes AS num_bytes
FROM [bigquery-public-data:github_repos.languages]
WHERE language.name = 'Go'
```

## Extract dependencies

* Split content by line
* left of line should be "import "

```Python
snippet1 = \
'''
package main

import (  // derp
	"fmt"
	"io/ioutil"  // implements some I/O utility

	m "math"
	"net/http"
)

// whats up yo
func main() {
	fmt.Println("hello")
}
'''

snippet2 = \
'''
package blah

import "nothing"

func main() {
	//...
}
'''

snippet3 = \
'''
import tensorflow as tf

if __name__ == '__main__':
	print('hello world!')
'''

def extract_deps(snippet):
	# Split the snippet by carriage return and newline and flatten
	a = [x.split('\n') for x in snippet.split('\r')]
	b = []
	for lst in a:
		b.extend(lst)
	c = [x for x in b if x != '']

	# Find all imports
	try:
		d = []
		prog1 = re.compile(r'\s*import\s*\(\s*(.*)')
		prog2 = re.compile(r'\s*import\s*([^(]*)')
		single_flag = False
		counter = 0
		for line in c:
			match = prog1.match(line)
			if match is not None:
				counter += 1
				s = match.group(1).split('//')[0].rstrip()
				if s != '':
					d.append(s)
				break
			else:
				match = prog2.match(line)
				if match is not None:
					single_flag = True
					s = match.group(1).split('//')[0].rstrip()
					if s != '':
						d.append(s)
					break
			counter += 1

		if not single_flag:
			for i in range(counter, len(c)):
				line = c[i].split('//')[0].rstrip()
				if line == '':
					continue
				if line[-1] == ')':
					if line[:-1] != '':
						d.append(line[:-1])
					break
				d.append(line)

		prog3 = re.compile(r'.*\"(.*)\".*')
		e = [prog3.search(x) for x in d]
		f = [x.group(1) for x in e if x is not None]

		return f
	except:
		return []

print(snippet1)
print(extract_deps(snippet1))
print()
print(snippet2)
print(extract_deps(snippet2))
print()
print(snippet3)
print(extract_deps(snippet3))
```

## Filter for GitHub dependencies

```Python

```