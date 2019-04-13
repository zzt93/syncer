import json

import sys


def compare(es='es', mysql='mysql'):
    with open(es, 'r') as myfile:
        es_json = json.loads(myfile.read())
    with open(mysql, 'r') as myfile:
        mysql_lines = myfile.readlines()

    es_map = {}
    for h in es_json['hits']['hits']:
        # print(h)
        es_map[h['_id']] = h['_source']

    col = mysql_lines[1].strip().split('\t')
    mysql_map = {}
    for i in range(2, len(mysql_lines)):
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
                else:
                    print('Fail to match {}={} in {}'.format(k, v, m))
                    break
        else:
            print('Fail to find match {}'.format(key))


if __name__ == '__main__':
    paths=sys.argv[1:-1]
    compare(*paths)
