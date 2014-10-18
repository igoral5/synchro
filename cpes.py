#!/usr/bin/env python2
# -*- coding: utf-8 -*-
'''Приводит в соответствие два различных индекса ElasticSearch'''

import argparse
from elasticsearch import helpers
import os
import logging
import util

util.conf_io()

argparser = argparse.ArgumentParser(description='Leads to the same state two different indices ElasticSearch')
argparser.add_argument("source", metavar='source', nargs=1, help='URL source ElasticSearch in format [http://]name_host.com[:9200]/name_index[/doc_type]')
argparser.add_argument("destination", metavar='destination', nargs=1, help='URL destination ElasticSearch in format [http://]name_host.com[:9200]/name_index[/doc_type]')
argparser.add_argument("--query-sour", dest='query_sour', help="Query for source ElasticSearch")
argparser.add_argument("--query-dest", dest='query_dest', help="Query for destination ElasticSearch")
argparser.add_argument("-g", "--group-code", dest='group_code', help='Group code for query, work only without key --query-sour and --query-dest', type=int)
args = argparser.parse_args()

logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(name)s %(message)s', datefmt="%Y-%m-%d %H:%M:%S %Z")
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.setLevel(logging.DEBUG)
logger_elasticsearch = logging.getLogger('elasticsearch')
logger_elasticsearch.addHandler(ch)

util.parse_args(args, logger)

def gen():
    dest_ids = set()
    try:
        for hit in args.documents_destination:
            index2 = hit['_index']
            id2=hit['_id']
            type2=hit['_type']
            dest_ids.add((index2, type2, id2))
    except Exception as e:
        logger.error(e)
    for hit in args.documents_source:
        source_index = hit['_index']
        destination_index = args.translate.trans(source_index)
        source_id = hit['_id']
        source_type = hit['_type']
        source_doc = hit['_source']
        if (destination_index, source_type, source_id) in dest_ids:
            dest_doc = args.es_dest.get(destination_index, source_id, source_type)['_source']
            if source_doc != dest_doc:
                yield {
                    '_index': destination_index,
                    '_type': source_type,
                    '_id': source_id,
                    '_source': source_doc
                }
            dest_ids.discard((destination_index, source_type, source_id))
        else:
            yield {
                '_index': destination_index,
                '_type': source_type,
                '_id': source_id,
                '_source': source_doc
            }
    for (dest_index, dest_type, dest_id) in dest_ids:
        yield {
            '_op_type': 'delete',
            '_index': dest_index,
            '_type': dest_type,
            '_id': dest_id
        }

def main():
    actions = gen()
    success, failed = helpers.bulk(args.es_dest, actions, True)
    if success + failed != 0: 
        if failed == 0:
            logger.info(u'Индексы синхронизированы, добавлено, изменено, удалено %d записей', success)
        else:
            logger.error(u'Ошибка загрузки данных в ElasticSearch, ошибок %d, успешно %d', failed, success)
    else:
        logger.info(u'Индексы идентичны')

if __name__ == '__main__':
    main()

