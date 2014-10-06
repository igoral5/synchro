#!/usr/bin/env python2
# -*- coding: utf-8 -*-
'''Скрипт создает маршруты аэроэкспресса'''

import time
import argparse
import os
import codecs
import json
from elasticsearch import Elasticsearch
import logging
import parsers
import util
import const

util.conf_io()

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
    def __init__(self, stations, json_schedule):
        self.stations = stations
        self.json_schedule = util.tree()
        for item in json_schedule:
            self.json_schedule[item['mr_id']][item['direction']] = {'mask': item['mask'], 'schedule': item['schedule']}
 
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
        if mr_id in self.json_schedule:
            for direction in self.json_schedule[mr_id]:
                self.route['schedule'][direction] = self.json_schedule[mr_id][direction]

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
        # name = self.marshes[mr_id]['name']
        name_direction = self.get_name_direction(mr_id, direction)
        schedules = self.create_schedules(mr_id, direction)
        stations = []
        for index_st, item in enumerate(self.route['race_card'][direction]):
            st_id = item['st_id']
            self.stations.set_tags(st_id, self.marshes[mr_id]['transport'])
            id_station = '%d:%d' % (group_code, st_id)
            stations.append({ 'id': id_station, 'distance': item['distance'] * 1000, 'schedule': schedules[index_st] })
        route = {
            'id': _id,
            'name': '',
            'direction': name_direction,
            'region': const.name_region[args.url[1:]],
            'transport': self.marshes[mr_id]['transport'],
            'stations': stations,
            'geometry': self.create_geometry(mr_id, direction),
            'valid': True
        }
        return route
    
    def get_name_direction(self, mr_id, direction):
        firststation_id = self.route['race_card'][direction][0]['st_id']
        laststation_id = self.route['race_card'][direction][-1]['st_id']
        name_direction = u'%s - %s' % (self.stations.get_station(firststation_id)['name'], self.stations.get_station(laststation_id)['name'])
        return name_direction
    
    def create_geometry(self, mr_id, direction):
        points = []
        for point in self.route['race_coord'][direction]:
            points.append(point['coord']['long'])
            points.append(point['coord']['lat'])
        return points

    def create_schedules(self, mr_id, direction):
        schedules = []
        for item in self.route['schedule'][direction]['schedule']:
            datas_json = []
            mask = self.route['schedule'][direction]['mask']
            weekday = []
            running_one = 1
            for i in xrange(7):
                if mask & running_one:
                    weekday.append(const.week_day_en[i])
                running_one = running_one << 1
            datas_json.append({'time': item, 'weekday': weekday})
            schedules.append(datas_json)
        return schedules
    
    def form_route(self, mr_id, direction, file_descriptor):
        route = self.create_route(mr_id, direction)
        complex_id = route['id']
        meta = {'index': {'_index': const.name_index_es[args.url[1:]], '_type': 'route', '_id': complex_id}}
        util.save_json_to_file(file_descriptor, meta, route)

group_code = 8000
current_time = time.time()
name_file = 'aeroexpress.json'
f = codecs.open(name_file, "w", encoding="utf-8")
if not args.only_create:
    es_client = Elasticsearch([{'host': args.host_es, 'port': args.port_es}])
    util.delete_old_doc(es_client, const.name_index_es[args.url[1:]], 'route', group_code, f)
    util.delete_old_doc(es_client, const.name_index_es[args.url[1:]], 'geometry', group_code, f)
    util.delete_old_doc(es_client, const.name_index_es[args.url[1:]], 'schedule', group_code, f)
    util.delete_old_doc(es_client, const.name_index_es[args.url[1:]], 'station', group_code, f)
f_json = codecs.open('aeroexpress_schedule.json', "r", encoding="utf-8")
synchro_stations = SynchroStations()
synchro_stations.synchro()
synchro_routes = SynchroRoutes(synchro_stations, json.load(f_json))
synchro_routes.synchro(f)
f_json.close()
f.close()
if not args.only_create:
    os.system('curl -S -XPOST "http://%s:%d/_bulk" --data-binary @%s' % (args.host_es, args.port_es, name_file))
