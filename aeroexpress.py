#!/usr/bin/env python2
# -*- coding: utf-8 -*-
'''Скрипт создает маршруты аэроэкспресса'''

import time
import datetime
import argparse
import os
import sys
import codecs
import locale
from elasticsearch import Elasticsearch
import logging
import parsers
import util
import const

if sys.stdout.encoding is None:
    sys.stdout = codecs.getwriter(locale.getpreferredencoding())(sys.stdout)
if sys.stderr.encoding is None:
    sys.stderr = codecs.getwriter(locale.getpreferredencoding())(sys.stderr)
locale.setlocale(locale.LC_ALL, '')

parser = argparse.ArgumentParser(description='Add routes aeroexpress in ElasticSearch.')
parser.add_argument("--host", dest='host', help='Hostname site TransNavigation, default asip.office.transnavi.ru', default='asip.office.transnavi.ru')
parser.add_argument("--url", dest='url', help='Path to script in site TransNavigation, default /podolsk', default='/podolsk')
parser.add_argument("--user", dest='user', help='User name for site TransNavigation, default asipguest', default='asipguest')
parser.add_argument("--passwd", dest='passwd', help='Password for site TransNavigation, default asipguest', default='asipguest')
parser.add_argument("--try", dest='num_try', help='Number of attempts to obtain data from site TransNavigation, default 3, if 0 the number of attempts is infinitely', type=int, default=3)
parser.add_argument("--host-es", dest='host_es', help='Host name ElasticSearch, default localhost', default='localhost')
parser.add_argument("--port-es", dest='port_es', help='Number port ElasticSearch, default 9200', type=int, default=9200)
parser.add_argument("--only", dest='only_create', help="Only create file, without loading into ElasticSearch", action='store_true')
args = parser.parse_args()

logger = logging.getLogger(u'metro-%s' % args.url[1:])
formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(name)s %(message)s', datefmt="%Y-%m-%d %H:%M:%S %Z")
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.setLevel(logging.DEBUG)

class SynchroStations:
    '''Синхронизация остановок транспорта'''
    def __init__(self):
        pass
    
    def synchro(self):
        self.process_stops()
    
    def process_stops(self):
        handler = parsers.StopsXMLParser()
        util.http_request('/getStops.php', handler, args, logger)
        self.stations = handler.stations

    def set_tags(self, st_id, transport):
        if st_id in self.stations:
            self.stations[st_id]['tags'].add(transport)
    
    def get_station(self, st_id):
        if st_id in self.stations:
            return self.stations[st_id]
        else:
            return None
    
    def form_file(self, file_descriptor):
        for st_id in self.stations:
            station_tn = self.get_json(st_id)
            if station_tn == None:
                continue
            complex_id = station_tn['id']
            meta = {'index': {'_index': const.name_index_es[args.url[1:]], '_type': 'station', '_id': complex_id}}
            util.save_json_to_file(file_descriptor, meta, station_tn)

    def get_json(self, st_id):
        if len(self.stations[st_id]['tags']) == 0:
            return None
        _id = u'%d:%d' % (group_code, st_id)
        tags = []
        for transport in sorted(self.stations[st_id]['tags']):
            tags.append(transport)
        station = {
            'id': _id,
            'location': [
                self.stations[st_id]['location']['long'],
                self.stations[st_id]['location']['lat']
            ],
            'tags': tags,
            'name': self.stations[st_id]['name'],
            'region': const.name_region[args.url[1:]]
        }
        return station
    
class SynchroRoutes:
    '''Синхронизация маршрутов'''
    def __init__(self, stations):
        self.stations = stations
 
    def synchro(self, file_descriptor):
        self.process_marshes()
        self.process_marshvariants()
        for mr_id in self.marshes:
            self.route = util.tree()
            for mv_id in self.marsh_variants[mr_id]:
                self.process_racecard(mv_id)
                self.process_racecoord(mv_id)
            self.process_schedule(mr_id)
            self.form_file(mr_id, file_descriptor)
        self.stations.form_file(file_descriptor)
    
    def process_schedule(self, mr_id):
        if mr_id == 7344: # Белорусский Вокзал - Шереметьево
            self.route['schedule'][0] = {'mask': 127, 'schedule': [
                "5:30 6:00 6:30 7:00 7:30 8:00 8:20 8:40 9:00 9:30 10:00 10:30 11:00 11:30 12:00 12:30 13:30 14:00 14:30 15:00 15:30 16:00 16:30 17:00 17:20 17:40 18:00 18:30 19:00 19:30 20:00 20:30 21:00 21:30 22:00 22:30 23:00 23:30 0:00 0:30",
                "6:05 6:35 7:05 7:35 8:05 8:35 9:02 9:18 9:35 10:05 10:35 11:08 11:35 12:05 12:35 13:05 14:05 14:35 15:05 15:35 16:05 16:35 17:05 17:35 17:55 18:22 18:37 19:05 19:38 20:05 20:35 21:05 21:35 22:05 22:35 23:05 23:35 0:05 0:35 1:05"]}
            self.route['schedule'][1] = {'mask': 127, 'schedule': [
                "5:00 6:00 6:30 7:00 7:30 8:00 8:30 9:00 9:20 9:40 10:00 10:30 11:00 11:30 12:30 13:00 13:30 14:00 14:30 15:00 15:30 16:00 16:30 17:00 17:30 18:00 18:20 18:40 19:00 19:30 20:00 20:30 21:00 21:30 22:00 22:30 23:00 23:30 0:00 0:30",
                "5:35 6:35 7:05 7:35 8:05 8:45 9:08 9:40 10:05 10:20 10:36 11:05 11:35 12:05 13:05 13:35 14:05 14:35 15:05 15:35 16:05 16:35 17:05 17:45 18:05 18:35 19:05 19:17 19:35 20:05 20:35 21:05 21:35 22:05 22:35 23:05 23:35 0:05 0:35 1:05"]}
        elif mr_id == 7336: # Киевский вокзал - Аэропорт Внуково
            self.route['schedule'][0] = {'mask':127, 'schedule': [
                "6:00 7:00 8:00 9:00 10:00 11:00 13:00 14:00 15:00 15:30 16:00 17:00 17:30 18:00 19:00 19:30 20:00 21:00 22:00 23:00 0:00",
                "6:38 7:36 8:36 9:35 10:35 11:35 13:39 14:36 15:34 16:06 16:34 17:37 18:05 18:35 19:34 20:05 20:35 21:35 22:35 23:35 0:40"]}
            self.route['schedule'][1] = {'mask':127, 'schedule': [
                "6:00 7:00 8:00 9:00 10:00 11:00 12:00 13:00 14:00 15:00 16:00 16:30 17:00 18:00 18:30 19:00 20:00 21:00 22:00 23:00 0:00",
                "6:37 7:40 8:40 9:41 10:40 11:38 12:38 13:36 14:38 15:36 16:39 17:06 17:36 18:36 19:06 19:36 20:39 21:36 22:39 23:36 0:34"]}
        elif mr_id == 7310: # Павелецкий вокзал - Аэропорт Домодедово
            self.route['schedule'][0] = {'mask': 127, 'schedule': [
                "6:00 6:30 7:00 7:30 8:00 8:30 9:00 9:30 10:00 10:30 11:00 11:30 12:00 13:00 13:30 14:00 14:30 15:00 15:30 16:00 16:30 17:00 17:30 18:00 18:30 19:00 19:30 20:00 20:30 21:00 21:30 22:00 22:30 23:00 23:30 0:00 0:30",
                "6:46 7:16 7:47 8:16 8:47 9:16 9:47 10:16 10:46 11:16 11:46 12:16 12:43 13:46 14:19 14:46 15:16 15:46 16:16 16:46 17:16 17:46 18:16 18:46 19:17 19:47 20:16 20:48 21:16 21:46 22:16 22:46 23:16 23:46 0:16 0:43 1:13"]}
            self.route['schedule'][1] = {'mask':127, 'schedule': [
                "6:00 6:30 7:00 7:30 8:00 8:30 9:00 9:30 10:00 10:30 11:00 11:30 12:00 13:00 13:30 14:00 14:30 15:00 15:30 16:00 16:30 17:00 17:30 18:00 18:30 19:00 19:30 20:00 20:30 21:00 21:30 22:00 22:30 23:00 23:30 0:00", 
                "6:43 7:15 7:46 8:14 8:44 9:14 9:43 10:16 10:43 11:13 11:43 12:13 12:47 13:43 14:15 14:43 15:16 15:43 16:14 16:43 17:15 17:47 18:15 18:46 19:15 19:46 20:16 20:45 21:18 21:43 22:16 22:47 23:13 23:43 0:13 0:43"]}

    def form_file(self, mr_id, file_descriptor):
        logger.info(u'Обработка маршрута %s %s mr_id=%d', self.marshes[mr_id]['name'], self.marshes[mr_id]['description'], mr_id)
        for direction in self.route['race_card']:
            self.form_route(mr_id, direction, file_descriptor)
    
    def process_marshes(self):
        handler = parsers.MarshesXMLParser(set([7]), logger=logger)
        util.http_request('/getMarshes.php', handler, args, logger=logger)
        self.marshes = handler.marshes
    
    def process_marshvariants(self):
        handler = parsers.MarshVariantsXMLParser(current_time)
        util.http_request('/getMarshVariants.php', handler, args, logger)
        self.marsh_variants = handler.marsh_variants
    
    def process_racecard(self, mv_id):
        handler = parsers.RaceCardXMLParser()
        util.http_request('/getRaceCard.php?mv_id=%d' % mv_id, handler, args, logger)
        self.route['race_card'] = handler.race_card
     
    def process_racecoord(self, mv_id):
        handler = parsers.RaceCoordXMLParser()
        util.http_request('/getRaceCoord.php?mv_id=%d' % mv_id, handler, args, logger)
        self.route['race_coord'] = handler.race_coord
    
    def create_route(self, mr_id, direction):
        _id = '%d:%d:%d' % (group_code, mr_id, direction)
        name = self.marshes[mr_id]['name']
        name_direction = self.get_name_direction(mr_id, direction)
        stations = []
        for item in self.route['race_card'][direction]:
            st_id = item['st_id']
            self.stations.set_tags(st_id, self.marshes[mr_id]['transport'])
            id_station = '%d:%d' % (group_code, st_id)
            stations.append({ 'id': id_station, 'distance': item['distance'] * 1000 })
        
        route = {
            'id': _id,
            'ru': {
                'name': '',
                'direction': name_direction,
                'region': name_region[args.url[1:]],
                'suggest_route': {
                    'input': [
                        name
                    ],
                    'output': name,
                    'payload': {
                        'id': id,
                        'type': 'route'
                    }
                }
            },
            'tr': {
                'name': translit('', language_code='ru', reversed=True),
                'direction': translit(name_direction, language_code='ru', reversed=True),
                'region': translit(name_region[args.url[1:]], language_code='ru', reversed=True),
                'suggest_route': {
                    'input': [
                        translit(name, language_code='ru', reversed=True)
                    ],
                    'output': translit(name, language_code='ru', reversed=True),
                    'payload': {
                        'id': id,
                        'type': 'route'
                    }
                }
            },
            'transport': self.marshes[mr_id]['transport'],
            'stations': stations,
            'valid': True
        }
        return route
    
    def get_name_direction(self, mr_id, direction):
        firststation_id = self.route['race_card'][direction][0]['st_id']
        laststation_id = self.route['race_card'][direction][-1]['st_id']
        name_direction = u'%s - %s' % (self.stations.get_station(firststation_id)['name'], self.stations.get_station(laststation_id)['name'])
        return name_direction
    
    def create_geometry(self, mr_id, direction):
        id = '%d:%d:%d' % (group_code, mr_id, direction)
        points = []
        for point in self.route['race_coord'][direction]:
            points.append(point['coord']['long'])
            points.append(point['coord']['lat'])
        geometry = {
            'route': id,
            'points': points
        }
        return geometry

    def create_schedules(self, mr_id, direction):
        id_route = '%d:%d:%d' % (group_code, mr_id, direction)
        schedules = []
        for index_st, item in enumerate(self.route['schedule'][direction]['schedule']):
            st_id = self.route['race_card'][direction][index_st]['st_id']
            id_station = '%d:%d' % (group_code, st_id)
            datas_json = []
            mask = self.route['schedule'][direction]['mask']
            weekday = []
            running_one = 1
            for i in xrange(7):
                if mask & running_one:
                    weekday.append(week_day_en[i])
                running_one = running_one << 1
            datas_json.append({'time': item, 'weekday': weekday})
            schedule = {
                'route': id_route,
                'station': id_station,
                'station_idx': index_st,
                'data': datas_json
            }
            schedules.append(schedule)
        return schedules
    
    def form_route(self, mr_id, direction, file_descriptor):
        route = self.create_route(mr_id, direction)
        complex_id = route['id']
        meta = {'index': {'_index': name_index_es[args.url[1:]], '_type': 'route', '_id': complex_id}}
        save_json_to_file(meta, file_descriptor)
        save_json_to_file(route, file_descriptor)
    
    def form_geometry(self, mr_id, direction, file_descriptor):
        geometry = self.create_geometry(mr_id, direction)
        complex_id = geometry['route']
        meta = {'index': {'_index':  name_index_es[args.url[1:]], '_type': 'geometry', '_id': complex_id}}
        save_json_to_file(meta, file_descriptor)
        save_json_to_file(geometry, file_descriptor)
    
    def form_schedule(self, mr_id, direction, file_descriptor):
        schedules = self.create_schedules(mr_id, direction)
        for schedule in schedules:
            complex_id = '%s@%d' % (schedule['route'], schedule['station_idx'])
            meta = {'index': {'_index': name_index_es[args.url[1:]], '_type': 'schedule', '_id': complex_id}}
            save_json_to_file(meta, file_descriptor)
            save_json_to_file(schedule, file_descriptor)

def delete_old_doc(index, doc_type, prefix, file_descriptor):
    if doc_type == 'geometry' or doc_type == 'schedule':
        query = {'query': {'prefix': { 'route': prefix }}}
    elif doc_type == 'route' or doc_type == 'station':
        query = {'query': {'prefix': { 'id': prefix }}}
    try:
        res = es_client.search(index=index, doc_type=doc_type, search_type='scan', scroll='10m', fields='', body=query)
    except:
        return
    scroll_id = res['_scroll_id']
    docs = es_client.scroll(scroll='10m', scroll_id=scroll_id)
    while len(docs['hits']['hits']) > 0:
        for doc in docs['hits']['hits']:
            meta = {'delete': {'_index': index, '_type': doc_type, '_id': doc['_id']}}
            save_json_to_file(meta, file_descriptor)
        scroll_id = docs['_scroll_id']
        docs = es_client.scroll(scroll='10m', scroll_id=scroll_id)

mongo_client = MongoClient(args.host_mongo, args.port_mongo)
mongo_db = mongo_client[args.db_mongo]
redis_client = redis.StrictRedis( host = args.host_redis, port = args.port_redis, db = args.db_redis )
if not args.only_create:
    es_client = Elasticsearch([{'host': args.host_es, 'port': args.port_es}])
current_day = (datetime.date.today() - datetime.date.fromtimestamp(0)).days
current_time = time.time()
total_geocoding = 0
cache_geocoding = 0
google_geocoding = 0
redis_key_limit = 'limit_geocoding:google:%d' % current_day
try:
    n_limit = int(redis_client.get(redis_key_limit))
except:
    n_limit = 0
over_query_limit = 0
next_time_geocoding = 0

group_code = 8000

name_file = 'aeroexpress.json'
f = codecs.open(name_file, "w", encoding="utf-8")
if not args.only_create:
    delete_old_doc('region_moskva', 'route', '%d:' % group_code, f)
    delete_old_doc('region_moskva', 'geometry', '%d:' % group_code, f)
    delete_old_doc('region_moskva', 'schedule', '%d:' % group_code, f)
    delete_old_doc('region_moskva', 'station', '%d:' % group_code, f)

synchro_stations = SynchroStations()
synchro_stations.synchro()
synchro_routes = SynchroRoutes(synchro_stations)
synchro_routes.synchro(f)

f.close()
if not args.only_create:
    os.system('curl -S -XPOST "http://%s:%d/_bulk" --data-binary @%s' % (args.host_es, args.port_es, name_file))
