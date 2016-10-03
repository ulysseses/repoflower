'''
Input: Spark temp table called "contents"
Output: DataFrame with schema as ['id', 'dep']
'''
def _helper(row):
    _id, content = row.id, row.content
    # Split the content by carriage return and newline, then flatten
    a = [x.split('\n') for x in content.split('\r')]
    b = []
    for lst in a:
        b.extend(lst)
    c = [x for x in b if x != '']

    # Try to find all imports
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

        return [(_id, x) for x in f]
    except:
        return []


def extract_deps(sc_sql):
    rdd_contents = sc_sql.sql("""
        SELECT id, content
        FROM contents
        """) \
        .flatMap(_helper)

    df_contents = sc_sql.createDataFrame(rdd_contents, ['id', 'dep'])
    return df_contents
