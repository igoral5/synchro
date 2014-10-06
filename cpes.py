#!/usr/bin/env python2
# -*- coding: utf-8 -*-
'''Приводит в соответствие два различных индекса ElasticSearch'''

import argparse
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import json
import codecs
import os
import sys
import re
import logging
import util

util.conf_io()

argparser = argparse.ArgumentParser(description='Leads to the same state two different indices ElasticSearch')
argparser.add_argument("source", metavar='source', nargs=1, help='URL source ElasticSearch in format [http://]name_host.com[:9200]/name_index[/doc_type]')
argparser.add_argument("destination", metavar='destination', nargs=1, help='URL destination ElasticSearch in format [http://]name_host.com[:9200]/name_index[/doc_type]')
argparser.add_argument("--query-sour", dest='query_sour', help="Query for source ElasticSearch")
argparser.add_argument("--query-dest", dest='query_dest', help="Query for destination ElasticSearch")
argparser.add_argument("-g", "--group-code", dest='group_code', help='Group code for query, work only without key --query-sour and  --query-dest', type=int)
argparser.add_argument("-o", "--only", dest='only', help="Only create file for bulk interfaces ElasticSearch, without load", action='store_true')
args = argparser.parse_args()

class TranslateName(object):
    def __init__(self, source, destination):
        source_split = source.split(',')
        destination_split = destination.split(',')
        if self.template(source_split) or self.template(destination_split):
            self.not_translate = True
        else:
            if len(source_split) == len(destination_split):
                self.convert = {}
                for i1, i2 in zip(source_split, destination_split):
                    self.convert[i1] = i2
                self.not_translate = False
            else:
                self.not_translate = True
    
    def trans(self, source_name):
        if self.not_translate:
            return source_name
        else:
            try:
                return self.convert[source_name]
            except:
                return source_name
    
    def template(self, name_index):
        for s in name_index:
            if s[0] == '_':
                return True
            if s.find('*') != -1:
                return True
            if s.find('?') != -1:
                return True
        return False

logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(name)s %(message)s', datefmt="%Y-%m-%d %H:%M:%S %Z")
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.setLevel(logging.DEBUG)
logger_elasticsearch = logging.getLogger('elasticsearch')
logger_elasticsearch.addHandler(ch)

regexp = re.compile(u'^(http://)?([\w\.-]+)(:(\d+))?/([\w\*\.\?,-]+)(/([\w\*\.\?,-]+))?/?$', re.IGNORECASE | re.UNICODE)

res = regexp.match(args.source[0])
if res:
    args.source_host = res.group(2)
    args.source_index = res.group(5)
    args.source_doc = res.group(7)
    if res.group(4):
        args.source_port = int(res.group(4))
    else:
        args.source_port = 9200
else:
    logger.error(u'Неверный формат источника ElasticSearch')
    sys.exit(1)
res = regexp.match(args.destination[0])
if res:
    args.destination_host = res.group(2)
    args.destination_index = res.group(5)
    args.destination_doc = res.group(7)
    if res.group(4):
        args.destination_port = int(res.group(4))
    else:
        args.destination_port = 9200
else:
    logger.error(u'Неверный формат получателя ElasticSearch')
    sys.exit(1)

es_source = Elasticsearch([{'host': args.source_host, 'port': args.source_port}])
es_dest = Elasticsearch([{'host': args.destination_host, 'port': args.destination_port}])

if args.query_sour:
    try:
        query_source = json.loads(args.query_sour, encoding='utf-8')
    except:
        logger.error(u'Неверный формат запроса источника')
        sys.exit(1)
else:
    if args.group_code:
        query_source = {'query': {'prefix': { '_id': '%d:' % args.group_code }}}
    else:
        query_source = {'query': {'match_all': {}}}

if args.query_dest:
    try:
        query_destination = json.loads(args.query_dest, encoding='utf-8')
    except:
        logger.error(u'Неверный формат запроса получателя')
        sys.exit(1)
else:
    if args.query_sour:
        query_destination = query_source
    else:
        if args.group_code:
            query_destination = {'query': {'prefix': { '_id': '%d:' % args.group_code }}}
        else:
            query_destination = {'query': {'match_all': {}}}

if args.destination_doc:
    documents = scan(es_dest, query=query_destination, index=args.destination_index, doc_type=args.destination_doc, fields='')
else:
    documents = scan(es_dest, query=query_destination, index=args.destination_index, fields='')

dest_ids = set()
try:
    for hit in documents:
        index2 = hit['_index']
        id2=hit['_id']
        type2=hit['_type']
        dest_ids.add((index2, type2, id2))
except Exception as e:
    logger.error(e)

translate = TranslateName(args.source_index, args.destination_index)

if args.source_doc:
    documents = scan(es_source, query=query_source, index=args.source_index, doc_type=args.source_doc)
else:
    documents = scan(es_source, query=query_source, index=args.source_index)

name_file = os.path.splitext(os.path.basename(__file__))[0] + '.json'

with codecs.open(name_file, 'w', encoding='utf-8') as file_descriptor:
    for hit in documents:
        source_index = hit['_index']
        destination_index = translate.trans(source_index)
        source_id = hit['_id']
        source_type = hit['_type']
        source_doc = hit['_source']
        if (destination_index, source_type, source_id) in dest_ids:
            dest_doc = es_dest.get(destination_index, source_id, source_type)['_source']
            if source_doc != dest_doc:
                meta = {'index': {'_index': destination_index, '_type': source_type, '_id': source_id}}
                util.save_json_to_file(file_descriptor, meta, source_doc)
            dest_ids.discard((destination_index, source_type, source_id))
        else:
            meta = {'index': {'_index': destination_index, '_type': source_type, '_id': source_id}}
            util.save_json_to_file(file_descriptor, meta, source_doc)
    for (dest_index, dest_type, dest_id) in dest_ids:
        meta = {'delete': {'_index': dest_index, '_type': dest_type, '_id': dest_id}}
        util.save_json_to_file(file_descriptor, meta)
statinfo = os.stat(name_file)
if statinfo.st_size > 0:
    if not args.only:
        ret = os.system('curl -S -XPOST "http://%s:%d/_bulk" --data-binary @%s > /dev/null 2>&1' % (args.destination_host, args.destination_port, name_file))
        if ret == 0:
            logger.info(u'Индексы синхронизированы')
        else:
            logger.error(u'Ошибка загрузки данных в ElasticSearch')
    else:
        logger.info(u'Сформирован файл %s' % name_file)
else:
    logger.info(u'Индексы идентичны')
