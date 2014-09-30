#!/usr/bin/env python2
# -*- coding: utf-8 -*-
'''Скрипт выводит отчет о текущем состоянии маршрутов и телеметрии'''

import redis
import argparse
import collections
from elasticsearch import Elasticsearch
import util
import const

util.conf_io()

parser = argparse.ArgumentParser(description='Report on the state routes and telemetry.')
parser.add_argument("--host-redis", dest='host_redis', help='Host name redis, default localhost', default='localhost')
parser.add_argument("--port-redis", dest='port_redis', help='Number port redis, default 6379', type=int, default=6379)
parser.add_argument("--db-redis", dest='db_redis', help='Number database redis, default 0', type=int, default=0)
parser.add_argument("--host-es", dest='host_es', help='Host name ElasticSearch, default localhost', default='localhost')
parser.add_argument("--port-es", dest='port_es', help='Number port ElasticSearch, default 9200', type=int, default=9200)
parser.add_argument("--only", dest='region', help="Make a report, only the specified region", choices=['moscow', 'podolsk', 'krasnoyarsk', 'sochi'])
args = parser.parse_args()

redis_client = redis.StrictRedis( host = args.host_redis, port = args.port_redis, db = args.db_redis )
es_client = Elasticsearch([{'host': args.host_es, 'port': args.port_es}])

def get_info_route(group_code, mr_id, direction):
    complex_id = '%d:%d:%d' % (group_code, mr_id, direction)
    res = es_client.search(index='region_*', doc_type='route', body={'query': {'match': { '_id': complex_id}}, '_source': {'include': ['name', 'direction']}})
    if res['hits']['total'] > 0:
        return {'name': res['hits']['hits'][0]['_source']['name'], 'dir_name': res['hits']['hits'][0]['_source']['direction'] }
    return {'name': u'Неизвестно', 'dir_name': u'Неизвестное наименование направления'}

if args.region:
    query_telemetry = 'telemetry:tn:%d:*' % const.group_codes[args.region]
    query_prediction = 'prediction:tn:route:%d:*' % const.group_codes[args.region]
    query_report = 'report_info:tn:routes:%s:*' % args.region
else:
    query_telemetry = 'telemetry:tn:*'
    query_prediction = 'prediction:tn:route:*'
    query_report = 'report_info:tn:routes:*'

exists_telemetry = set()

for key in redis_client.keys(query_telemetry):
    key_split = key.split(':')
    group_code  = int(key_split[2])
    mr_id = int(key_split[3])
    direction = int(key_split[4])
    exists_telemetry.add((group_code, mr_id, direction))

exists_prediction = set()

for key in redis_client.keys(query_prediction):
    key_split = key.split(':')
    group_code = int(key_split[-3])
    mr_id = int(key_split[-2])
    direction = int(key_split[-1])
    exists_prediction.add((group_code, mr_id, direction))

route_count = util.tree()

for key in redis_client.keys(query_report):
    value = [bool(int(item)) for item in redis_client.lrange(key, 0, -1)]
    key_split = key.split(':')
    region = key_split[3]
    group_code = int(key_split[-3])
    mr_id = int(key_split[-2])
    direction = int(key_split[-1])
    if region not in route_count:
        route_count[region] = collections.defaultdict(int)
    route_count[region]['all'] += 1
    if value[0]:
        route_count[region]['loaded'] += 1
    if value[0] and value[1] and value[2]:
        route_count[region]['validate'] += 1
    if value[0] and not value[1]:
        route_count[region]['schedule'] += 1
        if 'schedule_list' not in route_count[region]:
            route_count[region]['schedule_list'] = set()
        route_count[region]['schedule_list'].add((group_code, mr_id, direction))
    if value[0] and not value[2]:
        route_count[region]['geometry'] += 1
        if 'geometry_list' not in route_count[region]:
            route_count[region]['geometry_list'] = set()
        route_count[region]['geometry_list'].add((group_code, mr_id, direction))
    if value[3]:
        route_count[region]['generate_schedule'] += 1
    if (group_code, mr_id, direction) not in exists_telemetry:
        route_count[region]['telemetry'] += 1
    if (group_code, mr_id, direction) not in exists_prediction:
        route_count[region]['prediction'] += 1

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
    for (group_code, mr_id, direction) in item:
        value = get_info_route(group_code, mr_id, direction)
        print u'%8s %-80s' % (value['name'], value['dir_name'])
    print


