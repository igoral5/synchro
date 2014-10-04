#!/usr/bin/env python2
# -*- coding: utf-8 -*-
'''Приводит в соответствие два различных индекса ElasticSearch'''

import argparse
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import json
import codecs
import os
import logging
import util

util.conf_io()

argparser = argparse.ArgumentParser(description='Leads to the same state two different indices ElasticSearch')
argparser.add_argument("--host-in", dest='host_source', help='Host name source ElasticSearch, default localhost', default='localhost')
argparser.add_argument("--port-in", dest='port_source', help='Number port source ElasticSearch, default 9200', type=int, default=9200)
argparser.add_argument("--host-out", dest='host_dest', help='Host name destination ElasticSearch, default localhost', default='localhost')
argparser.add_argument("--port-out", dest='port_dest', help='Number port destination ElasticSearch, default 9200', type=int, default=9200)
argparser.add_argument("--query-sour", dest='query_source', help="Query for source EalsticSearfch")
argparser.add_argument("--query-dest", dest='query_destination', help="Query for destination EalsticSearfch")
argparser.add_argument("-g", "--group-code", dest='group_code', help='Group code for query, work only without key --query-sour and  --query-dest', type=int)
argparser.add_argument("-d", "--doc-type", dest='doc_type', help='Type documents for query')
argparser.add_argument("-o", "--only", dest='only', help="Only create file for bulk interfaces ElasticSearch, without load", action='store_true')
argparser.add_argument("source", metavar='source', nargs=1, help='Names source index ElasticSearch')
argparser.add_argument("dest", metavar='dest', nargs=1, help='Names destination index ElasticSearch')
args = argparser.parse_args()

logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(name)s %(message)s', datefmt="%Y-%m-%d %H:%M:%S %Z")
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.setLevel(logging.DEBUG)

es_source = Elasticsearch([{'host': args.host_source, 'port': args.port_source}])
es_dest = Elasticsearch([{'host': args.host_dest, 'port': args.port_dest}])

if args.query_source:
    query_source = json.loads(args.query_source, encoding='utf-8')
else:
    if args.group_code:
        query_source = {'query': {'prefix': { '_id': '%d:' % args.group_code }}}
    else:
        query_source = {'query': {'match_all': {}}}

if args.query_destination:
    query_destination = json.loads(args.query_destination, encoding='utf-8')
else:
    if args.query_source:
        query_destination = query_source
    else:
        if args.group_code:
            query_destination = {'query': {'prefix': { '_id': '%d:' % args.group_code }}}
        else:
            query_destination = {'query': {'match_all': {}}}

if args.doc_type:
    documents = scan(es_dest, query=query_destination, index=args.dest[0], doc_type=args.doc_type, fields='')
else:
    documents = scan(es_dest, query=query_destination, index=args.dest[0], fields='')

dest_ids = set()
try:
    for hit in documents:
        dest_id=hit['_id']
        dest_type=hit['_type']
        dest_ids.add((dest_type, dest_id))
except Exception as e:
    logger.error(e)


if args.doc_type:
    documents = scan(es_source, query=query_source, index=args.source[0], doc_type=args.doc_type)
else:
    documents = scan(es_source, query=query_source, index=args.source[0])
name_file = os.path.splitext(os.path.basename(__file__))[0] + '.json'
with codecs.open(name_file, 'w', encoding='utf-8') as file_descriptor:
    for hit in documents:
        source_id = hit['_id']
        source_type = hit['_type']
        source_doc = hit['_source']
        if (source_type, source_id) in dest_ids:
            dest_doc = es_dest.get(args.dest[0], source_id, source_type)['_source']
            if source_doc != dest_doc:
                meta = {'index': {'_index': args.dest[0], '_type': source_type, '_id': source_id}}
                util.save_json_to_file(file_descriptor, meta, source_doc)
            dest_ids.discard((source_type, source_id))
        else:
            meta = {'index': {'_index': args.dest[0], '_type': source_type, '_id': source_id}}
            util.save_json_to_file(file_descriptor, meta, source_doc)
    for (dest_type, dest_id) in dest_ids:
        meta = {'delete': {'_index': args.dest[0], '_type': dest_type, '_id': dest_id}}
        util.save_json_to_file(file_descriptor, meta)
statinfo = os.stat(name_file)
if statinfo.st_size > 0:
    if not args.only:
        ret = os.system('curl -S -XPOST "http://%s:%d/_bulk" --data-binary @%s > /dev/null 2>&1' % (args.host_dest, args.port_dest, name_file))
        if ret == 0:
            logger.info(u'Индексы синхронизированы')
        else:
            logger.error(u'Ошибка загрузки данных в ElasticSearch')
    else:
        logger.info(u'Сформирован файл %s' % name_file)
else:
    logger.info(u'Индексы идентичны')
