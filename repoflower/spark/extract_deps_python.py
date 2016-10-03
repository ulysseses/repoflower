'''
Input: Spark temp table called "contents"
Output: DataFrame with schema as ['id', 'dep']
'''
def _helper(tup):
    _id, line = tup
    prog = re.compile("""^(?:import|from)[\t ]+([^\. ]+)""")
    obj = prog.match(line)
    if obj:
        return (_id, obj.group(1))
    return (_id, obj)

def extract_deps(sc_sql):
    rdd_contents = sc_sql.sql("""
        SELECT id, SPLIT(content, '\\n') as lines
        FROM contents
        """) \
        .flatMap(lambda row: \
            [(row.id, line) for line in row.lines] if row.lines else []) \
        .map(_helper)
        .filter(lambda tup: True if tup[1] else False)

    df_contents = sc_sql.createDataFrame(rdd_contents, ['id', 'dep'])
    return df_contents
