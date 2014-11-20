#!/usr/bin/env python2
# -*- coding: utf-8 -*-
'''Копирует выборку в файл'''

import argparse
import os
import logging
import codecs
import util


util.conf_io()

argparser = argparse.ArgumentParser(description='Copy data ElasticSearch in file')
argparser.add_argument("source", metavar='source', nargs=1, help='URL source ElasticSearch in format [http://]name_host.com[:9200]/name_index[/doc_type]')
argparser.add_argument("--query-sour", dest='query_sour', help="Query for source ElasticSearch")
argparser.add_argument("-g", "--group-code", dest='group_code', help='Group code for query, work only without key --query-sour', type=int)
argparser.add_argument("name_file", metavar='name_file', nargs=1, help="Name file for saved data ElasticSearch")
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

def main():
    with codecs.open(args.name_file[0], "w", encoding='utf-8') as file_descriptor:
        for hit in args.documents_source:
            meta = {'index': {'_index': hit['_index'], '_type': hit['_type'], '_id': hit['_id']}}
            body = hit['_source']
            util.save_json_to_file(file_descriptor, meta, body)

if __name__ == '__main__':
    main()


