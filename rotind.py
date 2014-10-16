#!/usr/bin/env python2
# -*- coding: utf-8 -*-
'''Скрипт удаляет старые индексы из ElasticSearch'''

from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
import argparse
import re
import datetime

argparser = argparse.ArgumentParser(description='Delete old indexes is ElasticSearch.')
argparser.add_argument('-H', '--host', dest='host', help='Hostname ElasticSearch, default localhost', default='localhost')
argparser.add_argument('-p', '--port', dest='port', help='Number port ElasticSearch, default 9200', type=int, default=9200)
argparser.add_argument('-o', '--old', dest='old', help='Delete indexes older N days, default N=30', type=int, default=30)
args = argparser.parse_args()

def main():
    es_client = Elasticsearch([{'host': args.host, 'port': args.port}])
    es_index = IndicesClient(es_client)
    list_indexes = [index for index in es_index.status()['indices']]
    regexp = re.compile(u'(\d{4})\.(\d{2})\.(\d{2})', re.IGNORECASE | re.UNICODE )
    current_date = datetime.date.today()
    for index in list_indexes:
        res = regexp.search(index)
        if res:
            date_indx = datetime.date(year=int(res.group(1)), month=int(res.group(2)), day=int(res.group(3)))
            if (current_date - date_indx).days > args.old:
                es_index.delete(index)

if __name__ == '__main__':
    main()
