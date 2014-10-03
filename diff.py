#!/usr/bin/env python2
# -*- coding: utf-8 -*-
'''Проверяет документы в двух различных ElasticSearch документы на соответcтвие'''

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import argparse
import os
import tempfile
import json
import codecs
import util

util.conf_io()

argparser = argparse.ArgumentParser(description='Shows the difference between the indices ElasticSearch.')
argparser.add_argument("--host-es1", dest='host_es1', help='Host name first ElasticSearch, default localhost', default='localhost')
argparser.add_argument("--port-es1", dest='port_es1', help='Number port first ElasticSearch, default 9200', type=int, default=9200)
argparser.add_argument("--host-es2", dest='host_es2', help='Host name second ElasticSearch, default localhost', default='localhost')
argparser.add_argument("--port-es2", dest='port_es2', help='Number port second ElasticSearch, default 9200', type=int, default=9200)
argparser.add_argument("-g", "--group-code", dest='group_code', help='Group code for query', type=int)
argparser.add_argument("-d", "--doc-type", dest='doc_type', help='Type documents for comparison')
argparser.add_argument("-m", "--meld", dest='meld', help="Use meld for show difference", action='store_true')
argparser.add_argument("-f", "--first", dest='first', help="Show only the first N rows of differences, default 10", type=int, default=10)
argparser.add_argument("index1", metavar='index1', nargs=1, help='Names first index ElasticSearch')
argparser.add_argument("index2", metavar='index2', nargs=1, help='Names second index ElasticSearch')
args = argparser.parse_args()

es1 = Elasticsearch([{'host': args.host_es1, 'port': args.port_es1}])
es2 = Elasticsearch([{'host': args.host_es2, 'port': args.port_es2}])

if args.group_code:
    query = {'query': {'prefix': { '_id': '%d:' % args.group_code }}}
else:
    query = {'query': {'match_all': {}}}

if args.doc_type:
    documents = scan(es2, query=query, index=args.index2[0], doc_type=args.doc_type, fields='')
else:
    documents = scan(es2, query=query, index=args.index2[0], fields='')

es2_ids = set()
for hit in documents:
    id2=hit['_id']
    type2=hit['_type']
    index2=hit['_index']
    es2_ids.add((index2, type2, id2))

change = False

if args.doc_type:
    documents = scan(es1, query=query, index=args.index1[0], doc_type=args.doc_type)
else:
    documents = scan(es1, query=query, index=args.index1[0])
difference = 0
for hit in documents:
    id1=hit['_id']
    type1=hit['_type']
    index1=hit['_index']
    doc1=hit['_source']
    if (index1, type1, id1) in es2_ids:
        hit2=es2.get(index=index1, doc_type=type1, id=id1)
        doc2=hit2['_source']
        if doc1 != doc2:
            if args.meld:
                print u'Различия index=%s, doc_type=%s, id=%s' % (index1, type1, id1)
                if difference < args.first:
                    name_tmp1 = tempfile.mktemp()
                    name_tmp2 = tempfile.mktemp()
                    with codecs.open(name_tmp1, 'w', encoding='utf-8') as tmp1:
                        with codecs.open(name_tmp2, 'w', encoding='utf-8') as tmp2:
                            json.dump(doc1, tmp1, ensure_ascii=False, sort_keys=True, indent=4, separators=(',', ': '))
                            tmp1.close()
                            json.dump(doc2, tmp2, ensure_ascii=False, sort_keys=True, indent=4, separators=(',', ': '))
                            tmp2.close()
                            os.system('meld %s -L "[%s:%d] %s" %s -L "[%s:%d] %s"' % (name_tmp1, args.host_es1, args.port_es1, id1, name_tmp2, args.host_es2, args.port_es2, id1))
                            os.unlink(name_tmp1)
                            os.unlink(name_tmp2)
            difference += 1
            change = True
        es2_ids.discard((index1, type1, id1))
    else:
        print u'Во втором не найден документ index=%s, doc_type=%s, id=%s' % (index1, type1, id1)
        change = True

for (index2, type2, id2) in es2_ids:
    print u'В первом не найден документ index=%s, doc_type=%s, id=%s' % (index2, type2, id2)
    change = True

if not change:
    print u'Различий не найдено'

