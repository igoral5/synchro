#!/usr/bin/env python2
# -*- coding: utf-8 -*-
'''Скрипт выводит отчет о текущем состоянии маршрутов и телеметрии'''

import redis
import argparse
import collections
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from elasticsearch.helpers import scan
import util
import const

util.conf_io()

parser = argparse.ArgumentParser(description='Report on the state routes and telemetry.')
parser.add_argument("--host-redis", dest='host_redis', help='Host name redis, default localhost', default='localhost')
parser.add_argument("--port-redis", dest='port_redis', help='Number port redis, default 6379', type=int, default=6379)
parser.add_argument("--db-redis", dest='db_redis', help='Number database redis, default 0', type=int, default=0)
parser.add_argument("--host-es", dest='host_es', help='Host name ElasticSearch, default localhost', default='localhost')
parser.add_argument("--port-es", dest='port_es', help='Number port ElasticSearch, default 9200', type=int, default=9200)
parser.add_argument("--only", dest='region', help="Make a report, only the specified region", choices=const.name_urls)
args = parser.parse_args()

route_count = util.tree()

redis_client = redis.StrictRedis( host = args.host_redis, port = args.port_redis, db = args.db_redis )
es_client = Elasticsearch([{'host': args.host_es, 'port': args.port_es}])
es_index = IndicesClient(es_client)
list_indexes = [index for index in es_index.status()['indices']]

if args.region:
    regions = [{'name': args.region, 'index': const.name_index_es[args.region], 'group_code': const.group_codes[args.region]}]
    query_telemetry = 'telemetry:tn:%d:*' % const.group_codes[args.region]
    query_prediction = 'prediction:tn:route:%d:*' % const.group_codes[args.region]
else:
    regions =[]
    for region in const.name_urls:
        regions.append({'name': region, 'index': const.name_index_es[region], 'group_code': const.group_codes[region]})
    query_telemetry = 'telemetry:tn:*'
    query_prediction = 'prediction:tn:route:*'

exists_telemetry = set()

for key in redis_client.keys(query_telemetry):
    key_split = key.split(':')
    group_code  = int(key_split[-4])
    mr_id = int(key_split[-3])
    direction = int(key_split[-2])
    exists_telemetry.add((group_code, mr_id, direction))

exists_prediction = set()

for key in redis_client.keys(query_prediction):
    key_split = key.split(':')
    group_code = int(key_split[-6])
    mr_id = int(key_split[-5])
    direction = int(key_split[-4])
    exists_prediction.add((group_code, mr_id, direction))

for item in regions:
    if item['index'] in list_indexes:
        region = item['name']
        query = {'query': {'prefix': {'_id': '%d:' % item['group_code']}}, '_source': {'include': ['name', 'direction']}}
        exists = False
        for hit in scan(es_client, query=query, index=item['index'], doc_type='route'):
            complex_id = hit['_id']
            complex_id_split = complex_id.split(':')
            group_code = int(complex_id_split[0])
            mr_id = int(complex_id_split[1])
            direction = int(complex_id_split[2])
            route = hit['_source']
            key = 'report_info:tn:routes:%s:%d:%d:%d' % (region, group_code, mr_id, direction)
            if redis_client.exists(key):
                if region not in route_count:
                    route_count[region] = collections.defaultdict(int)
                exists = True
                value = [bool(int(item)) for item in redis_client.lrange(key, 0, -1)]
                if value[0]:
                    route_count[region]['loaded'] += 1
                if value[0] and value[1] and value[2]:
                    route_count[region]['validate'] += 1
                if value[0] and not value[1]:
                    route_count[region]['schedule'] += 1
                    if 'schedule_list' not in route_count[region]:
                        route_count[region]['schedule_list'] = {}
                    route_count[region]['schedule_list'][(route['name'].replace('*', ''), mr_id, direction)] = u'%8s %-100s %d/%d/%d' % (route['name'], route['direction'], group_code, mr_id, direction)
                if value[0] and not value[2]:
                    route_count[region]['geometry'] += 1
                    if 'geometry_list' not in route_count[region]:
                        route_count[region]['geometry_list'] = {}
                    route_count[region]['geometry_list'][(route['name'].replace('*', ''), mr_id, direction)] = u'%8s %-100s %d/%d/%d' % (route['name'], route['direction'], group_code, mr_id, direction)
                if value[3]:
                    route_count[region]['generate_schedule'] += 1
                if (group_code, mr_id, direction) not in exists_telemetry:
                    route_count[region]['telemetry'] += 1
                if (group_code, mr_id, direction) not in exists_prediction:
                    route_count[region]['prediction'] += 1
        if exists:
            route_count[region]['all'] = len(redis_client.keys('report_info:tn:routes:%s:*' % region))
        
remark = []
for region in route_count:
    print const.name_region[region]
    print
    print u'Всего направлений                              %5d' % route_count[region]['all']
    print u'  Из них загружено в базу                      %5d' % route_count[region]['loaded']
    print u'  Прошли валидацию                             %5d' % route_count[region]['validate']
    print u'  Имеют ошибки расписания                      %5d' % route_count[region]['schedule'],
    if 'schedule_list' in route_count[region]:
        print u'[%d]' % (len(remark) + 1)
        remark.append(route_count[region]['schedule_list'])
    else:
        print
    print u'  Имеют ошибки геометрии                       %5d' % route_count[region]['geometry'],
    if 'geometry_list' in route_count[region]:
        print u'[%d]' % (len(remark) + 1)
        remark.append(route_count[region]['geometry_list'])
    else:
        print
    print u'  Сгенерировано расписаний                     %5d' % route_count[region]['generate_schedule']
    print u'  Не имеют транспортных средств на направлении %5d' % route_count[region]['telemetry']
    print u'  Не имеют прогнозов прибытия                  %5d' % route_count[region]['prediction']
    print

for i, item in enumerate(remark):
    print i + 1
    print
    for key in sorted(item.keys()):
        print item[key]
    print


