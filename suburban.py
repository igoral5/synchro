#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Скрипт создает маршруты электричек,
   текущие xml файлы получены из
   station_15_09.xml из http://script.moscowmap.ru/export_rasp/station.php
   export_rasp_15_09.xml из http://script.moscowmap.ru/export_rasp/'''

import argparse
import os
import sys
import codecs
import locale
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import logging
import parsers
import util
import shapely.wkt
import shapely.geometry
import json


if sys.stdout.encoding is None:
    sys.stdout = codecs.getwriter(locale.getpreferredencoding())(sys.stdout)
if sys.stderr.encoding is None:
    sys.stderr = codecs.getwriter(locale.getpreferredencoding())(sys.stderr)
locale.setlocale(locale.LC_ALL, '')

parser = argparse.ArgumentParser(description='Add routes suburban train in ElasticSearch.')
parser.add_argument("--host", dest='host', help='Hostname site MosMap, default script.moscowmap.ru', default='script.moscowmap.ru')
parser.add_argument("--url", dest='url', help='Path to script in site MosMap, default /export_rasp', default='/export_rasp')
parser.add_argument("--try", dest='num_try', help='Number of attempts to obtain data from site MosMap, default 3, if 0 the number of attempts is infinitely', type=int, default=3)
parser.add_argument("--host-es", dest='host_es', help='Host name ElasticSearch, default localhost', default='localhost')
parser.add_argument("--port-es", dest='port_es', help='Number port ElasticSearch, default 9200', type=int, default=9200)
parser.add_argument("--only", dest='only_create', help="Only create file, without loading into ElasticSearch", action='store_true')
args = parser.parse_args()

logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(name)s %(message)s', datefmt="%Y-%m-%d %H:%M:%S %Z")
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.setLevel(logging.DEBUG)

name_index = 'region_moskva'
region_ru = u'Москва'
    
class Stations:
    '''Обработка остановок электричек'''
    def __init__(self, f_wkt):
        self.stations = {}
        self.es_location = {}
        self.mos_region = shapely.wkt.load(f_wkt)

    def load(self):
        handler = parsers.StopsSubXMLParser()
        util.http_request('/station.php', handler, args, logger)
        for st_id in handler.stations:
            station = handler.stations[st_id]
            station['in_moscow_region'] = self.in_moscow_region(station['location'])
            self.stations[st_id] = station
        self.load_old()

    def set_tags(self, st_id, transport):
        if st_id in self.stations:
            self.stations[st_id]['tags'].add(transport)
    
    def get_station(self, st_id):
        if st_id in self.stations:
            return self.stations[st_id]
        else:
            return None
    
    def get_json(self, st_id):
        if len(self.stations[st_id]['tags']) == 0:
            return None
        _id = u'%d:%d' % (group_code, st_id)
        tags = []
        for transport in sorted(self.stations[st_id]['tags']):
            tags.append(transport)
        if _id in self.es_location:
            location = self.es_location[_id]
        else:
            location = self.stations[st_id]
        station = {
            'id': _id,
            'location': [
                location['location']['long'],
                location['location']['lat']
            ],
            'tags': tags,
            'name': u'%s (ж/д)' % self.stations[st_id]['name'],
            'region': region_ru
        }
        return station
    
    def save(self, file_descriptor):
        for st_id in self.stations:
            station = self.get_json(st_id)
            if station == None:
                continue
            meta = {'index': {'_index': name_index, '_type': 'station', '_id': station['id']}}
            util.save_json_to_file(file_descriptor, meta, station)
    
    def in_moscow_region(self, location):
        point = shapely.geometry.Point(location['long'], location['lat'])
        return self.mos_region.contains(point)
    
    def load_old(self):
        query = {'query': {'prefix': { '_id': '%s:' % group_code }}}
        try:
            for station in scan(es_client, query, '10m', index=name_index, doc_type='station'):
                self.es_location[station['_id']] = {'location': {'long': station['_source']['location'][0], 'lat': station['_source']['location'][1] }}
        except:
            pass


class UrbanRoutes:
    '''Обработка маршрутов электричек'''
    def __init__(self, stations, f_express):
        self.stations = stations
        j_express = json.load(f_express)
        self.express = []
        for item in j_express:
            self.express.append(item['st_id'])
        self.es_geometry = {}
    
    def load(self):
        handler = parsers.MarshesSubXMLParser(logger)
        util.http_request('/', handler, args, logger)
        self.marshes = handler.marshes
        self.load_old()
    
    def load_old(self):
        query = {'query': {'prefix': {'_id': '%d:' % group_code}}}
        try:
            res1 = es_client.count(index=name_index, doc_type='geometry', body=query)
            res2 = es_client.count(index=name_index, doc_type='route', body=query)
            if res1['count'] > res2['count']:
                for geometry in scan(es_client, query, '10m', index=name_index, doc_type='geometry'):
                    self.es_geometry[geometry['_id']] = geometry['_source']['points']
            else:
                for route in scan(es_client, query, '10m', index=name_index, doc_type='route'):
                    self.es_geometry[route['_id']] = route['_source']['geometry']
        except:
            pass
    
    def save(self, file_descriptor):
        for mr_id in self.marshes:
            if self.check_in_area(mr_id) and not self.is_aeroexspress(mr_id):
                if len(self.marshes[mr_id]['stations']) > 1:
                    route = self.get_json_route(mr_id)
                    meta = {'index': {'_index': name_index, '_type': 'route', '_id': route['id']}}
                    util.save_json_to_file(file_descriptor, meta, route)
                else:
                    logger.info(u'На маршруте %s %s id=%d, менее 2 остановок', self.marshes[mr_id]['name'], self.marshes[mr_id]['direction'], mr_id)
    
    def check_in_area(self, mr_id):
        for item in self.marshes[mr_id]['stations']:
            st_id = item['id']
            station = self.stations.get_station(st_id)
            if station and station['in_moscow_region']:
                return True
        return False
    
    def is_aeroexspress(self, mr_id):
        ''' Возвращает True, если это аэроэкспресс '''
        stations = []
        for item in self.marshes[mr_id]['stations']:
            stations.append(item['id'])
        return stations in self.express
    
    def get_json_route(self, mr_id):
        complex_id = '%d:%d:%d' % (group_code, mr_id, 0)
        stations_tmp = []
        for item in self.marshes[mr_id]['stations']:
            stations_tmp.append({"id": u'%d:%d' % (group_code, item['id']), 'time': item['time']})
            self.stations.set_tags(item['id'], u'suburban')
        if complex_id in self.es_geometry:
            geometry = self.es_geometry[complex_id]
        else:
            geometry = self.get_json_geometry(mr_id)
        if self.marshes[mr_id]['assignment']:
            assignment = self.marshes[mr_id]['assignment']
        else:
            assignment = u''
        route = {
            'id': complex_id,
            'transport': u'suburban',
            'region': region_ru,
            'name': '',
            'direction': self.marshes[mr_id]['direction'],
            'assignment': assignment,
            'stations': stations_tmp,
            'schedule': self.marshes[mr_id]['days'], 
            'geometry': geometry,
            'valid': True,
        }
        return route
    
    def get_json_geometry(self, mr_id):
        points = []
        for item in self.marshes[mr_id]['stations']:
            st_id = item['id']
            points.append(self.stations.get_station(st_id)['location']['long'])
            points.append(self.stations.get_station(st_id)['location']['lat'])
        return points

es_client = Elasticsearch([{'host': args.host_es, 'port': args.port_es}])
group_code = 8001
f_wkt = codecs.open('mos-area.wkt', "r", encoding='utf-8')
stations = Stations(f_wkt)
f_wkt.close()
stations.load()
f_express = codecs.open('aeroexpress_id.json', 'r', encoding='utf-8')
routes = UrbanRoutes(stations, f_express)
f_express.close()
routes.load()
name_file = 'suburban.json'
f = codecs.open(name_file, "w", encoding="utf-8")
if not args.only_create:
    util.delete_old_doc(es_client, name_index, 'suburban', group_code, f)
    util.delete_old_doc(es_client, name_index, 'geometry', group_code, f)
    util.delete_old_doc(es_client, name_index, 'station', group_code, f)
    util.delete_old_doc(es_client, name_index, 'route', group_code, f)
routes.save(f)
stations.save(f)
f.close()
if not args.only_create:
    os.system('curl -S -XPOST "http://%s:%d/_bulk" --data-binary @%s' % (args.host_es, args.port_es, name_file))
