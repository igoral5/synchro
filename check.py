#!/usr/bin/env python2
# -*- coding: utf-8 -*-
'''Скрипт выводит состояние телеметрии'''

import redis
import argparse
from elasticsearch import Elasticsearch
# from elasticsearch.helpers import scan
import util

util.conf_io()

route_count_transport = util.tree()

parser = argparse.ArgumentParser(description='Check telemetry and routes information.')
parser.add_argument("--host-redis", dest='host_redis', help='Host name redis, default localhost', default='localhost')
parser.add_argument("--port-redis", dest='port_redis', help='Number port redis, default 6379', type=int, default=6379)
parser.add_argument("--db-redis", dest='db_redis', help='Number database redis, default 0', type=int, default=0)
parser.add_argument("--host-es", dest='host_es', help='Host name ElasticSearch, default localhost', default='localhost')
parser.add_argument("--port-es", dest='port_es', help='Number port ElasticSearch, default 9200', type=int, default=9200)
args = parser.parse_args()

redis_client = redis.StrictRedis( host = args.host_redis, port = args.port_redis, db = args.db_redis )
es_client = Elasticsearch([{'host': args.host_es, 'port': args.port_es}])

def check_telemetry(code):
    key = 'telemetry:tn:%d*' % code
    return len(redis_client.keys(key))

def check_prediction(code):
    key = 'prediction:tn:route:%d*' % code
    return len(redis_client.keys(key))

# def check_routes(group):
#     return collection.find({'group':group}).count()

def list_routes(group):
    sum_count = 0
    prefix = '%d:' % group
    res = es_client.search(index='region_*', doc_type='route', search_type='scan', scroll='10m', body={'query': {'prefix': { 'id': prefix }}})
    scroll_id = res['_scroll_id']
    docs = es_client.scroll(scroll='10m', scroll_id=scroll_id)
    while len(docs['hits']['hits']) > 0:
        for doc in docs['hits']['hits']:
            complex_id = doc['_id']
            complex_id_split = complex_id.split(':')
            mr_id = int(complex_id_split[1])
            direction = int(complex_id_split[2])
            count = 0
            if group in route_count_transport:
                if mr_id in route_count_transport[group]:
                    if direction in route_count_transport[group][mr_id]:
                        count = len(route_count_transport[group][mr_id][direction])
            print u'%8s %-80s mr_id=%6d, направление=%1s, ТС:%3d' % (doc['_source']['ru']['name'], doc['_source']['ru']['direction'], mr_id, chr(direction + ord('A')), count)
            sum_count = sum_count + count
        scroll_id = docs['_scroll_id']
        docs = es_client.scroll(scroll='10m', scroll_id=scroll_id)
    print u'\nИтого транспортных средств на загруженных направлениях: %d' % sum_count

def check_route_telemetry():
    global route_count_transport
    for key in redis_client.keys('telemetry:tn:*'):
        key_split = key.split(':')
        group = int(key_split[2])
        mr_id = int(key_split[3])
        direction = int(key_split[4])
        uniqueid = int(key_split[5])
        route_count_transport[group][mr_id][direction][uniqueid] = ''

def print_list_routes():
    group_codes = {}
    res = es_client.search(index='region_*', doc_type='route,suburban', q='*', search_type='scan', scroll='10m')
    scroll_id = res['_scroll_id']
    docs = es_client.scroll(scroll='10m', scroll_id=scroll_id)
    while len(docs['hits']['hits']) > 0:
        for doc in docs['hits']['hits']:
            complex_id = doc['_id']
            group_code = int(complex_id.split(':')[0])
            region = doc['_source']['ru']['region']
            if group_code not in group_codes:
                group_codes[group_code] = {'region': region, 'count': 0}
            group_codes[group_code]['count'] += 1
        scroll_id = docs['_scroll_id']
        docs = es_client.scroll(scroll='10m', scroll_id=scroll_id)
    print u'Количество направлений по регионам\n'
    # sum = 0
    for group_code in group_codes:
        print u'%8d %-20s %6d' % (group_code, group_codes[group_code]['region'], group_codes[group_code]['count'])
        sum += group_codes[group_code]['count']
    print u'\nИтого направлений: %d' % sum

check_route_telemetry()
print_list_routes()
print u'\nСочи\n'
print u'Количество транспортных средств на маршрутах:', check_telemetry(3000000)
print u'Количество прогнозов прибытия транспортных средств:', check_prediction(3000000)
print u'\nСписок направлений по Сочи:\n'
list_routes(3000000)
print u'\nМосква\n'
print u'Количество транспортных средств на маршрутах:', check_telemetry(45000000)
print u'Количество прогнозов прибытия транспортных средств:', check_prediction(45000000)
print u'\nПодольск, московская область\n'
print u'Количество транспортных средств на маршрутах:', check_telemetry(46246000)
print u'Количество прогнозов прибытия транспортных средств:', check_prediction(46246000)
print u'\nСписок файлов маршрутов по Подольску:\n'
list_routes(46246000)
print u'\nКрасноярский край\n'
print u'Количество транспортных средств на маршрутах:', check_telemetry(4000000)
print u'Количество прогнозов прибытия транспортных средств:', check_prediction(4000000)
print u'\nСписок файлов маршрутов по Красноярскому краю:\n'
list_routes(4000000)
print u'\n'


