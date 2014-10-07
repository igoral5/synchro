#!/usr/bin/env python2
# -*- coding: utf-8 -*-
'''Скрипт создает маршруты метро'''

import time
import argparse
import os
import codecs
from elasticsearch import Elasticsearch
import json
import logging
import util
import parsers

util.conf_io()

conf = util.Configuration('synchro.conf')
conf.set_section('moscow')

argparser = argparse.ArgumentParser(description='Add routes metro in ElasticSearch.')
argparser.add_argument("--only", dest='only_create', help='Only create file, without loading into ElasticSearch', action='store_true')
argparser.add_argument("json_metro", metavar='json_metro', nargs=1, help='Names file with description routes metro')
args = argparser.parse_args()

logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(name)s %(message)s', datefmt="%Y-%m-%d %H:%M:%S %Z")
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)
logger_elasticsearch = logging.getLogger('elasticsearch')
logger_elasticsearch.addHandler(ch)
logger.setLevel(logging.DEBUG)
logger_elasticsearch.setLevel(logging.DEBUG)

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
        util.http_request('/getStops.php', handler, conf, logger=logger)
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
            meta = {'index': {'_index': conf.get('name-index'), '_type': 'station', '_id': complex_id}}
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
            'region': conf.get('name')
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
        util.http_request('/getMarshes.php', handler, conf, logger=logger)
        self.marshes = handler.marshes
    
    def process_marshvariants(self):
        handler = parsers.MarshVariantsXMLParser(current_time)
        util.http_request('/getMarshVariants.php', handler, conf, logger=logger)
        self.marsh_variants = handler.marsh_variants
    
    def process_racecard(self, mr_id, mv_id):
        handler = parsers.RaceCardXMLParser()
        util.http_request('/getRaceCard.php?mv_id=%d' % mv_id, handler, conf, logger=logger)
        self.route['race_card'] = handler.race_card
        for item in self.json['add_racecard']:
            if mr_id == item['mr_id']:
                self.route['race_card'][item['direction']].insert(item['index'], item['item'])
        for item in self.json['change_racecard']:
            if mr_id == item['mr_id']:
                self.route['race_card'][item['direction']][item['index']]['st_id'] = item['st_id']
    
    def process_racecoord(self, mv_id):
        handler = parsers.RaceCoordXMLParser()
        util.http_request('/getRaceCoord.php?mv_id=%d' % mv_id, handler, conf, logger=logger)
        self.route['race_coord'] = handler.race_coord
    
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
            stations.append({ 'id': id_station, 'distance': item['distance'] * 1000, 'next': station_time })
        route = {
            'id': _id,
            'name': name,
            'direction': self.marshes_json[mr_id]['name'],
            'region': conf.get('name'),
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
        meta = {'index': {'_index': conf.get('name-index'), '_type': 'route', '_id': complex_id}}
        util.save_json_to_file(file_descriptor, meta, route)

current_time = time.time()
group_code = 8002
name_file = 'metro.json'
f = codecs.open(name_file, "w", encoding="utf-8")
if not args.only_create:
    es_client = Elasticsearch([{'host': conf.get('host-es'), 'port': conf.getint('port-es')}])
    util.delete_old_doc(es_client, conf.get('name-index'), 'route', group_code, f)
    util.delete_old_doc(es_client, conf.get('name-index'), 'geometry', group_code, f)
    util.delete_old_doc(es_client, conf.get('name-index'), 'station', group_code, f)
file_metro = codecs.open(args.json_metro[0], 'r', encoding='utf-8')
metro_json = json.load(file_metro)
file_metro.close()
synchro_stations = SynchroStations(metro_json)
synchro_stations.synchro()
synchro_routes = SynchroRoutes(synchro_stations, metro_json)
synchro_routes.synchro(f)
f.close()
if not args.only_create:
    os.system('curl -S -XPOST "http://%s:%d/_bulk" --data-binary @%s' % (conf.get('host-es'), conf.getint('port-es'), name_file))
