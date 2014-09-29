#!/usr/bin/env python2
# -*- coding: utf-8 -*-
'''Проверяет документы в двух различных ElasticSearch документы на соответcтвие'''

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import argparse
import util

util.conf_io()

argparser = argparse.ArgumentParser(description='Shows the difference between the indices ElasticSearch.')
argparser.add_argument("--host-es1", dest='host_es1', help='Host name first ElasticSearch, default localhost', default='localhost')
argparser.add_argument("--port-es1", dest='port_es1', help='Number port first ElasticSearch, default 9200', type=int, default=9200)
argparser.add_argument("--host-es2", dest='host_es2', help='Host name second ElasticSearch, default localhost', default='localhost')
argparser.add_argument("--port-es2", dest='port_es2', help='Number port second ElasticSearch, default 9200', type=int, default=9200)
argparser.add_argument("--group-code", dest='group_code', help='Group code for query, default 45000000', type=int, default=45000000)
argparser.add_argument("--doc-type", dest='doc_type', help='Type documents for comparison')
argparser.add_argument("index1", metavar='index1', nargs=1, help='Names first index ElasticSearch')
argparser.add_argument("index2", metavar='index2', nargs=1, help='Names second index ElasticSearch')
args = argparser.parse_args()

es1 = Elasticsearch([{'host': args.host_es1, 'port': args.port_es1}])
es2 = Elasticsearch([{'host': args.host_es2, 'port': args.port_es2}])

query = {'query': {'prefix': { '_id': '%d:' % args.group_code }}}

es2_ids = util.tree()

def present(index, doc_type, _id):
    if index in es2_ids:
        if doc_type in es2_ids[index]:
            if _id in es2_ids[index][doc_type]:
                return True
    return False

if args.doc_type:
    documents = scan(es2, query, '10m', index=args.index2[0], doc_type=args.doc_type, fields='')
else:
    documents = scan(es2, query, '10m', index=args.index2[0], fields='')

for hit in documents:
    id2=hit['_id']
    type2=hit['_type']
    index2=hit['_index']
    es2_ids[index2][type2][id2] =''

change = False

if args.doc_type:
    documents = scan(es1, query, '10m', index=args.index1[0], doc_type=args.doc_type)
else:
    documents = scan(es1, query, '10m', index=args.index1[0])

for hit in documents:
    id1=hit['_id']
    type1=hit['_type']
    index1=hit['_index']
    doc1=hit['_source']
    if present(index1, type1, id1):
        hit2=es2.get(index=index1, doc_type=type1, id=id1)
        doc2=hit2['_source']
        if doc1 != doc2:
            print u'Различия в index=%s, doc_type=%s, id=%s' % (index1, type1, id1)
            for key1 in doc1:
                if key1 not in doc2:
                    print u'Во втором нет ключа %s' % key1
                else:
                    if doc1[key1] != doc2[key1]:
                        print u'Различия doc1[%s]=%s doc2[%s]=%s' % (key1, repr(doc1[key1]), key1, repr(doc2[key1]))
                    del doc2[key1]
            for key2 in doc2:
                print u'В первом нет ключа %s' % key2
            change=True
        del es2_ids[index1][type1][id1]
    else:
        print u'Во втором не найден документ index=%s, doc_type=%s, id=%s' % (index1, type1, id1)
        change = True

for index2 in es2_ids:
    for type2 in es2_ids[index2]:
        for id2 in es2_ids[index2][type2]:
            print u'В первом не найден документ index=%s, doc_type=%s, id=%s' % (index2, type2, id2)
            change = True

if not change:
    print u'Различий не найдено'

