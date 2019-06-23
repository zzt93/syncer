import json

import sys
from datetime import datetime


def extract_timestamp(f):
    try:
        t1 = datetime.strptime(f, '%Y-%m-%d %H:%M:%S')
        return t1
    except ValueError:
        try:
            t1 = datetime.strptime(f, '%Y-%m-%d %H:%M:%S.%f')
            return t1
        except ValueError:
            return None


def compareTimestamp(f, s):
    t1 = extract_timestamp(f)
    t2 = extract_timestamp(s)
    return t1 is not None and t1 == t2


def compare(es='es', mysql='mysql'):
    with open(es, 'r') as myfile:
        es_json = json.loads(myfile.read())
    with open(mysql, 'r') as myfile:
        mysql_lines = myfile.readlines()

    es_map = {}
    for h in es_json['hits']['hits']:
        # print(h)
        es_map[h['_id']] = h['_source']

    mysql_map = {}
    if len(mysql_lines) >= 1:
        col = mysql_lines[0].strip().split('\t')
        for i in range(1, len(mysql_lines)):
            row = mysql_lines[i].strip().split('\t')
            row_map = {}
            for i in range(len(row)):
                row_map[col[i]] = row[i]
            mysql_map[row[0]] = row_map

    for key, obj in es_map.iteritems():
        if key in mysql_map:
            m = mysql_map[key]
            for k, v in obj.iteritems():
                if k in m and m[k] == str(v):
                    continue
                elif k in m:
                    if compareTimestamp(m[k], str(v)):
                        continue
                    print('Fail to match {}: "{}" vs "{}"'.format(k, v, m[k]))
                else:
                    print('No {} in {}'.format(k, m))
        else:
            print('Fail to find match {}'.format(key))


if __name__ == '__main__':
    paths = sys.argv[1:]
    compare(*paths)
