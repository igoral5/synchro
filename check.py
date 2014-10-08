#!/usr/bin/env python2
# -*- coding: utf-8 -*-
'''Скрипт выводит состояние телеметрии'''

import redis
import argparse
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import collections
import logging
import util

util.conf_io()

conf = util.Configuration('synchro.conf')

parser = argparse.ArgumentParser(description='Check telemetry and routes information.')
parser.add_argument("--host-redis", dest='host_redis', help='Host name redis, default localhost', default='localhost')
parser.add_argument("--port-redis", dest='port_redis', help='Number port redis, default 6379', type=int, default=6379)
parser.add_argument("--db-redis", dest='db_redis', help='Number database redis, default 0', type=int, default=0)
parser.add_argument("--host-es", dest='host_es', help='Host name ElasticSearch, default localhost', default='localhost')
parser.add_argument("--port-es", dest='port_es', help='Number port ElasticSearch, default 9200', type=int, default=9200)
parser.add_argument("--only", dest='region', help="Make a report, only the specified region", choices=conf.sections())
parser.add_argument("--list", dest='full_report', help='Out full list directions', action='store_true')
args = parser.parse_args()

logger = logging.getLogger('elasticsearch')
logger.addHandler(logging.NullHandler())

redis_client = redis.StrictRedis( host = args.host_redis, port = args.port_redis, db = args.db_redis )
es_client = Elasticsearch([{'host': args.host_es, 'port': args.port_es}])

def check_telemetry(group_code):
    key = 'telemetry:tn:%d*' % group_code
    return len(redis_client.keys(key))

def check_prediction(group_code):
    key = 'prediction:tn:route:%d*' % group_code
    return len(redis_client.keys(key))

def sort_routes(item):
    name = item['_source']['name'].replace('*', '')
    complex_id_split = item['_id'].split(':')
    mr_id = int(complex_id_split[1])
    direction = int(complex_id_split[2])
    return (name, mr_id, direction)

def list_routes(group_code, route_count_transport):
    sum_count = 0
    query = {'query': {'prefix': {'_id': '%d:' % group_code }}, '_source': {'include': ['name', 'direction']}}
    for hit in sorted(scan(es_client, query=query, index='region_*', doc_type='route'), key = sort_routes):
        complex_id = hit['_id']
        complex_id_split = complex_id.split(':')
        mr_id = int(complex_id_split[1])
        direction = int(complex_id_split[2])
        if route_count_transport[(group_code, mr_id, direction)] == 0:
            count_str = u''
        else:
            count_str = u'ТС:%3d' % route_count_transport[(group_code, mr_id, direction)]
        print u'%8s %-80s %8d/%1s' % (hit['_source']['name'], hit['_source']['direction'], mr_id, chr(direction + ord('A'))), count_str
        sum_count += route_count_transport[(group_code, mr_id, direction)]
    print u'\nИтого транспортных средств на загруженных направлениях: %d' % sum_count

def main():
    if args.region:
        conf.set_section(args.region)
        regions = [{'name': conf.get('name'), 'group_code': conf.getint('group-code')}]
        name_index = conf.get('name-index')
        query_telemetry = 'telemetry:tn:%d:*' % conf.getint('group-code')
    else:
        regions =[]
        for region in conf.sections():
            regions.append({'name': conf.conf.get(region, 'name'), 'group_code': conf.conf.getint(region, 'group-code')})
        name_index = 'region_*'
        query_telemetry = 'telemetry:tn:*'
    route_count_transport = collections.defaultdict(int)
    for key in redis_client.keys(query_telemetry):
        key_split = key.split(':')
        group_code = int(key_split[2])
        mr_id = int(key_split[3])
        direction = int(key_split[4])
        route_count_transport[(group_code, mr_id, direction)] += 1
    group_codes = {}
    query = {'query': {'match_all': {}}, '_source': {'include': ['region']}}
    for hit in scan(es_client, query=query, index=name_index, doc_type='route'):
        complex_id = hit['_id']
        group_code = int(complex_id.split(':')[0])
        region = hit['_source']['region']
        if group_code not in group_codes:
            group_codes[group_code] = {'region': region, 'count': 0}
        group_codes[group_code]['count'] += 1
    print u'Количество направлений по регионам\n'
    count = 0
    for group_code in group_codes:
        print u'%8d %-20s %6d' % (group_code, group_codes[group_code]['region'], group_codes[group_code]['count'])
        count += group_codes[group_code]['count']
    print u'\nИтого направлений: %d' % count
    for item in regions:
        print u'\n%s\n' % item['name']
        print u'Количество транспортных средств на маршрутах: %d' % check_telemetry(item['group_code'])
        print u'Количество прогнозов прибытия транспортных средств: %d' % check_prediction(item['group_code'])
        if args.full_report:
            print u'\nСписок направлений по %s:\n' % item['name']
            list_routes(item['group_code'], route_count_transport)
            print u'\n'

if __name__ == '__main__':
    main()


