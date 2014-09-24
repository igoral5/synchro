#!/usr/bin/env python2
# -*- coding: utf-8 -*-
'''Скрипт создает маршруты метро'''

import time
import datetime
import argparse
import os
import sys
import codecs
import locale
from elasticsearch import Elasticsearch
import json
import logging
import util
import parsers
import const

if sys.stdout.encoding is None:
    sys.stdout = codecs.getwriter(locale.getpreferredencoding())(sys.stdout)
if sys.stderr.encoding is None:
    sys.stderr = codecs.getwriter(locale.getpreferredencoding())(sys.stderr)
locale.setlocale(locale.LC_ALL, '')

argparser = argparse.ArgumentParser(description='Add routes metro in ElasticSearch.')
argparser.add_argument("--host", dest='host', help='Hostname site TransNavigation, default asip.office.transnavi.ru', default='asip.office.transnavi.ru')
argparser.add_argument("--url", dest='url', help='Path to script in site TransNavigation, default /podolsk', default='/podolsk')
argparser.add_argument("--user", dest='user', help='User name for site TransNavigation, default asipguest', default='asipguest')
argparser.add_argument("--passwd", dest='passwd', help='Password for site TransNavigation, default asipguest', default='asipguest')
argparser.add_argument("--try", dest='num_try', help='Number of attempts to obtain data from site TransNavigation, default 3, if 0 the number of attempts is infinitely', type=int, default=3)
argparser.add_argument("--host-es", dest='host_es', help='Host name ElasticSearch, default localhost', default='localhost')
argparser.add_argument("--port-es", dest='port_es', help='Number port ElasticSearch, default 9200', type=int, default=9200)
argparser.add_argument("--only", dest='only_create', help='Only create file, without loading into ElasticSearch', action='store_true')
argparser.add_argument("json_metro", metavar='json_metro', nargs=1, help='Names file with description routes metro')
args = argparser.parse_args()

logger = logging.getLogger(u'metro-%s' % args.url[1:])
formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(name)s %(message)s', datefmt="%Y-%m-%d %H:%M:%S %Z")
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.setLevel(logging.DEBUG)


class SynchroStations:
    '''Синхронизация остановок транспорта'''
    def __init__(self, json_metro):
        self.json = json_metro
        self.transfer = util.tree()
        for item in json_metro['stations']:
            if 'transitions' in item:
                for trans in item['transitions']:
                    self.transfer[item['id']][trans['to']] = trans['minutes'] * 60

    def synchro(self):
        self.process_stops()
    
    def process_stops(self):
        handler = parsers.StopsXMLParser()
        util.http_request('/getStops.php', handler, args, logger=logger)
        self.stations = handler.stations
        for item in self.json['add_stations']:
            self.stations[item['id']] = {'name': item['name'], 'location': {'lat': item['lat'], 'long': item['long'] }, 'tags': set() }
    
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
            'entrance': [
                { 
                    'type': 'inout', 
                    'time': 180,
                    'location': [
                        self.stations[st_id]['location']['long'],
                        self.stations[st_id]['location']['lat']
                    ]
                }
            ],
            'name': self.stations[st_id]['name'],
            'region': const.name_region[args.url[1:]]
        }
        if st_id in self.transfer:
            transfer = []
            for to in self.transfer[st_id]:
                transfer.append({'to': '%d:%d' % (group_code, to), 'time': self.transfer[st_id][to]})
            station['transfer'] = transfer
        return station
    
class SynchroRoutes:
    '''Синхронизация маршрутов'''
    def __init__(self, stations, json_metro):
        self.stations = stations
        self.marsh_variants = util.tree()
        self.time = util.tree()
        self.json = json_metro
        self.marshes_json = {}
        for line in json_metro['lines']:
            self.marshes_json[line['id']] = { 'name': line['ru']['name'], 'color': line['Color'], 'subtype': line['subtype']}
            for item in line['stations']:
                self.time[line['id']][item['from']][item['to']] = item['minutes'] * 60

    def synchro(self, file_descriptor):
        self.process_marshes()
        self.process_marshvariants()
        for mr_id in self.marshes:
            self.route = util.tree()
            for mv_id in self.marsh_variants[mr_id]:
                self.process_racecard(mr_id, mv_id)
                self.process_racecoord(mv_id)
            self.form_file(mr_id, file_descriptor)
        self.stations.form_file(file_descriptor)
    
    def form_file(self, mr_id, file_descriptor):
        logger.info(u'Обработка маршрута %s %s mr_id=%d' % (self.marshes[mr_id]['name'], self.marshes[mr_id]['description'], mr_id))
        for direction in self.route['race_card']:
            self.form_route(mr_id, direction, file_descriptor)
    
    def process_marshes(self):
        handler = parsers.MarshesXMLParser(set([6]), logger=logger)
        util.http_request('/getMarshes.php', handler, args, logger=logger)
        self.marshes = handler.marshes
    
    def process_marshvariants(self):
        handler = parsers.MarshVariantsXMLParser(current_time)
        util.http_request('/getMarshVariants.php', handler, args, logger=logger)
        for mr_id in handler.marsh_variants:
            mv_startdate = sorted(handler.marsh_variants[mr_id].keys())[-1]
            mv_id = sorted(handler.marsh_variants[mr_id][mv_startdate].keys())[-1]
            self.marsh_variants[mr_id][mv_id] = handler.marsh_variants[mr_id][mv_startdate][mv_id]
    
    def process_racecard(self, mr_id, mv_id):
        handler = parsers.RaceCardXMLParser()
        util.http_request('/getRaceCard.php?mv_id=%d' % mv_id, handler, args, logger=logger)
        for direction in handler.race_card:
            self.route['race_card'][direction] = [handler.race_card[direction][k] for k in sorted(handler.race_card[direction].keys())]
        for item in self.json['add_racecard']:
            if mr_id == item['mr_id']:
                self.route['race_card'][item['direction']].insert(item['index'], item['item'])
        for item in self.json['change_racecard']:
            if mr_id == item['mr_id']:
                self.route['race_card'][item['direction']][item['index']]['st_id'] = item['st_id']
    
    def process_racecoord(self, mv_id):
        handler = parsers.RaceCoordXMLParser()
        util.http_request('/getRaceCoord.php?mv_id=%d' % mv_id, handler, args, logger=logger)
        for direction in handler.race_coord:
            self.route['race_coord'][direction] = [handler.race_coord[direction][k] for k in sorted(handler.race_coord[direction].keys())]
    
    def create_route(self, mr_id, direction):
        _id = '%d:%d:%d' % (group_code, mr_id, direction)
        name = self.marshes[mr_id]['name']
        geometry = self.create_geometry(mr_id, direction)
        stations = []
        for index_station, item in enumerate(self.route['race_card'][direction]):
            st_id = item['st_id']
            self.stations.set_tags(st_id, self.marshes[mr_id]['transport'])
            id_station = '%d:%d' % (group_code, st_id)
            if index_station + 1 < len(self.route['race_card'][direction]):
                next_st_id = self.route['race_card'][direction][index_station + 1]['st_id']
                station_time = self.time[mr_id][st_id][next_st_id]
            else:
                station_time = 0
            stations.append({ 'id': id_station, 'distance': item['distance'] * 1000, 'time': station_time })
        route = {
            'id': _id,
            'name': name,
            'direction': self.marshes_json[mr_id]['name'],
            'region': const.name_region[args.url[1:]],
            'transport': self.marshes[mr_id]['transport'],
            'stations': stations,
            'valid': True,
            'color': self.marshes_json[mr_id]['color'],
            'subtype': self.marshes_json[mr_id]['subtype'],
            'geometry': geometry
        }
        return route
    
    def create_geometry(self, mr_id, direction):
        points = []
        for point in self.route['race_coord'][direction]:
            points.append(point['coord']['long'])
            points.append(point['coord']['lat'])
        return points

    def form_route(self, mr_id, direction, file_descriptor):
        route = self.create_route(mr_id, direction)
        complex_id = route['id']
        meta = {'index': {'_index': const.name_index_es[args.url[1:]], '_type': 'route', '_id': complex_id}}
        util.save_json_to_file(file_descriptor, meta, route)

if not args.only_create:
    es_client = Elasticsearch([{'host': args.host_es, 'port': args.port_es}])
current_time = time.time()

group_code = 8002

name_file = 'metro.json'
f = codecs.open(name_file, "w", encoding="utf-8")
if not args.only_create:
    util.delete_old_doc(es_client, 'region_moskva', 'route', group_code, f)
    util.delete_old_doc(es_client, 'region_moskva', 'geometry', group_code, f)
    util.delete_old_doc(es_client, 'region_moskva', 'station', group_code, f)
file_metro = codecs.open(args.json_metro[0], 'r', encoding='utf-8')
metro_json = json.load(file_metro)
file_metro.close()
synchro_stations = SynchroStations(metro_json)
synchro_stations.synchro()
synchro_routes = SynchroRoutes(synchro_stations, metro_json)
synchro_routes.synchro(f)

f.close()
if not args.only_create:
    os.system('curl -S -XPOST "http://%s:%d/_bulk" --data-binary @%s' % (args.host_es, args.port_es, name_file))
