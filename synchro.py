#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Скрипт сихронизирует данные сайта ТрансНавигации c ElasticSearch, скачка выполняется в несколько потоков'''

import argparse
import base64
import sys
import os
import signal
import codecs
import locale
import httplib
import gzip
import collections
import threading
import time
import socket
import logging
import unicodecsv
import xml.parsers.expat
import json
import datetime
import tzlocal
import cStringIO
import redis
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from shapely.geometry import LineString
from shapely.geometry import Point
from shapely.ops import transform
from functools import partial
import pyproj
import glob

if sys.stdout.encoding is None:
    sys.stdout = codecs.getwriter(locale.getpreferredencoding())(sys.stdout)
if sys.stderr.encoding is None:
    sys.stderr = codecs.getwriter(locale.getpreferredencoding())(sys.stderr)
locale.setlocale(locale.LC_ALL, '')

parser = argparse.ArgumentParser(description='Synchro ElasticSearch use data TransNavigation.')
parser.add_argument("--host", dest='host', help='Hostname site TransNavigation, default asip.office.transnavi.ru', default='asip.office.transnavi.ru')
parser.add_argument("--url", dest='url', help='Path to script in site TransNavigation, default /podolsk', default='/podolsk')
parser.add_argument("--user", dest='user', help='User name for site TransNavigation, default asipguest', default='asipguest')
parser.add_argument("--passwd", dest='passwd', help='Password for site TransNavigation, default asipguest', default='asipguest')
parser.add_argument("--try", dest='num_try', help='Number of attempts to obtain data from site TransNavigation, default 3, if 0 the number of attempts is infinitely', type=int, default=3)
parser.add_argument("--thread", dest='max_thread', help="Maximum threads for downloading data, default 10", type=int, default=10)
parser.add_argument("--timeout", dest='timeout', help="Connection timeout for http connection, default 30 s", type=int, default=30)
parser.add_argument("--format", dest='format', help="Format of the information received, xml or csv, default xml", choices=['xml', 'csv'], default='xml')
parser.add_argument("--host-redis", dest='host_redis', help='Host name redis, default localhost', default='localhost')
parser.add_argument("--port-redis", dest='port_redis', help='Number port redis, default 6379', type=int, default=6379)
parser.add_argument("--db-redis", dest='db_redis', help='Number database redis, default 0', type=int, default=0)
parser.add_argument("--host-es", dest='host_es', help='Host name ElasticSearch, default localhost', default='localhost')
parser.add_argument("--port-es", dest='port_es', help='Number port ElasticSearch, default 9200', type=int, default=9200)
parser.add_argument("--log-txt", dest="txt_log", help="Name file text log")
parser.add_argument("--log-json", dest="json_log", help="Name file json log")
parser.add_argument("--correct", dest='correct_transport_type', help='Correct transport type 2->1, 3->1', action='store_true')
parser.add_argument("--distance", dest='distance', help="The maximum allowable distance from the stop to the geometry of the route, default 20 meters", type=int, default=20)
parser.add_argument("--create", dest='create_schedule', help='Create the missing schedule', action='store_true')
parser.add_argument("--only", dest='only', help='Create file for bulk interface ElasticSearch, wthout load', action='store_true')
args = parser.parse_args()

#Константа, словарь преобразования url в наименование региона
name_region = {'moscow': u'Москва', 'podolsk': u'Подольск', 'sochi': u'Сочи', 'krasnoyarsk': u'Красноярск'}

#Константа, справочник типов транспорта
transport_type = {0: u'unknown', 1: u'bus', 2: u'trolleybus', 3: u'tram', 4: u'taxi' }

#Константа, наименование дней недели
week_day = [u'понедельник', u'вторник', u'среда', u'четверг', u'пятница', u'суббота', u'воскресенье']

#Константа, наименование дней недели in english
week_day_en = [u'mon', u'tue', u'wed', u'thr', u'fri', u'sat', u'sun']

#Константа, словарь преобразования url в имя индекса ElasticSearch
name_index_es = {'moscow': u'region_moskva', 'podolsk': u'region_podolsk', 'sochi': u'region_sochi', 'krasnoyarsk': u'region_krasnoyarsk' }

#Константа, словарь преобразования url в груповой код
group_codes = {'podolsk': 46246000, 'sochi': 3000000, 'krasnoyarsk': 4000000, 'moscow': 45000000 }

#Константа, словарь преобразования url в имя проекции
name_proj = {'podolsk': 'EPSG:32637', 'sochi': 'EPSG:32637', 'krasnoyarsk': 'EPSG:32646', 'moscow': 'EPSG:32637'}


class JSONFormatter(logging.Formatter):
    '''Класс форматера для записи в формате JSON'''
    def __init__(self, fmt=None, datefmt=None):
        super(JSONFormatter, self).__init__(fmt=fmt, datefmt=datefmt)
    
    def format(self, record):
        json_obj = {
            '@timestamp': datetime.datetime.fromtimestamp(record.created, tzlocal.get_localzone()).replace(microsecond=0).isoformat(),
            '@version': 1,
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage()
        }
        if record.exc_info:
            json_obj['exception_class'] = record.funcName
            json_obj['exception_message'] = repr(record.exc_info[1])
            json_obj['stacktrace'] = self.formatException(record.exc_info)
        if hasattr(record, 'marker'):
            json_obj['marker'] = record.marker
        return json.dumps(json_obj, ensure_ascii=False)

SUBINFO = 15
logging.addLevelName(SUBINFO, 'SUBINFO')

def subinfo(self, message, *args, **kws):
    self.log(SUBINFO, message, *args, **kws) 

logging.Logger.subinfo = subinfo
logger = logging.getLogger(u'synchro-%s' % args.url[1:])
formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(name)s %(message)s', datefmt="%Y-%m-%d %H:%M:%S %Z")
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
logger.addHandler(ch)
if args.txt_log:
    fn = logging.FileHandler(args.txt_log, encoding='utf-8')
    fn.setLevel(logging.DEBUG)
    fn.setFormatter(formatter)
    logger.addHandler(fn)
if args.json_log:
    jh = logging.FileHandler(args.json_log, encoding='utf-8')
    jh.setLevel(logging.DEBUG)
    jformatter = JSONFormatter()
    jh.setFormatter(jformatter)
    logger.addHandler(jh)
logger.setLevel(logging.DEBUG)


def tree():
    return collections.defaultdict(tree)

def http_request(request, handler):
    '''Выполняет HTTP запрос'''
    n = 1
    while True:
        try:
            try:
                auth = base64.encodestring('%s:%s' % (args.user, args.passwd)).replace('\n', '')
                webservice = httplib.HTTP(args.host)
                webservice.putrequest('GET', args.url + request)
                webservice.putheader('Host', args.host)
                webservice.putheader('Authorization', 'Basic %s' % auth)
                webservice.putheader('Accept-Encoding', 'gzip, deflate')
                webservice.endheaders()
                (statuscode, statusmessage, header) = webservice.getreply()
                if statuscode == 200:
                    stream = cStringIO.StringIO(webservice.getfile().read())
                    if header.getheader('Content-Encoding') == 'gzip':
                        gzipper = gzip.GzipFile(fileobj = stream)
                        handler.ParseFile(gzipper)
                        return
                    else:
                        handler.ParseFile(stream)
                        return 
                else:
                    raise RuntimeError(u'%d %s' % (statuscode, statusmessage ))
            except (RuntimeError, socket.timeout), e:
                message = u'[http://%s%s] %s' % (args.host, args.url + request, str(e))
                if type(e) == RuntimeError:
                    raise RuntimeError(message)
                else:
                    raise socket.timeout(message)
        except Exception, e:
            if n == args.num_try:
                raise
            logger.exception(e)
            time.sleep(n * 5)
        n += 1

        
class XMLParser(object):
    '''Общий класс XML парсера'''
    def __init__(self):
        self._parser = xml.parsers.expat.ParserCreate()
        self._parser.StartElementHandler = self.next
    
    def ParseFile(self, f):
        self._parser.ParseFile(f)
    
    def next(self, tags, attrs):
        pass

class CSVParser(object):
    '''Общий класс CSV парсера'''
    def __init__(self):
        pass
    
    def ParseFile(self, f):
        parser = unicodecsv.UnicodeReader(f)
        for i, row in enumerate(parser):
            if i != 0: 
                self.next(row)
    
    def next(self, row):
        pass

class MarshesXMLParser(XMLParser):
    '''XML Парсер запроса getMarsches'''
    def __init__(self):
        super(MarshesXMLParser, self).__init__()
        self.marshes = {}

    def next(self, tag, attrs):
        if tag == 'row':
            try:
                mr_id = int(attrs['mr_id'])
                tt_id = int(attrs['tt_id'])
                if args.correct_transport_type and (tt_id == 2 or tt_id == 3):
                    tt_id = 1
                self.marshes[mr_id] = { 'transport': transport_type[tt_id], 'name': attrs['mr_num'], 'description': attrs['mr_title'] }
            except:
                logger.debug(u'Неизвестный тип в таблице tbmarshes mr_id=%s' % attrs['mr_id'])

class MarshesCSVParser(CSVParser):
    '''CSV Парсер запроса getMarshes'''
    def __init__(self):
        super(MarshesCSVParser, self).__init__()
        self.marshes = {}
    
    def next(self, row):
        try:
            mr_id = int(row[0])
            tt_id = int(row[1])
            if args.correct_transport_type and (tt_id == 2 or tt_id == 3):
                tt_id = 1
            self.marshes[mr_id] = { 'transport': transport_type[tt_id], 'name': row[3], 'description': row[4] }
        except:
            logger.debug(u'Неизвестный тип в таблице tbmarshes mr_id=%s' % row[0])

class MarshVariantsXMLParser(XMLParser):
    '''XML парсер запроса getMarshVariants'''
    def __init__(self):
        super(MarshVariantsXMLParser, self).__init__()
        self.marsh_variants = tree()
    
    def next(self, tag, attrs):
        if tag == 'row':
            mr_id = int(attrs['mr_id'])
            mv_id = int(attrs['mv_id'])
            mv_enddateexists = int(attrs['mv_enddateexists'])
            if mv_enddateexists:
                mv_enddate = time.mktime(time.strptime(attrs['mv_enddate'], "%Y-%m-%d %H:%M:%S"))
                if mv_enddate < current_time:
                    return
            mv_startdate = time.mktime(time.strptime(attrs['mv_startdate'], "%Y-%m-%d %H:%M:%S"))
            if mv_startdate > current_time:
                return
            mv_checksum = int(attrs['mv_checksum'])
            redis_key = 'checksum:tn:%s:marshvariants:%d:%d' % (args.url[1:], mr_id, mv_id)
            redis_checksum = redis_client.get(redis_key)
            if redis_checksum:
                if mv_checksum == int(redis_checksum):
                    change = False
                else:
                    change = True
            else:
                change = True
            self.marsh_variants[mr_id][mv_startdate][mv_id] = { 'checksum': mv_checksum, 'change': change }

class MarshVariantsCSVParser(CSVParser):
    '''CSV парсер запроса getMarshVariants'''
    def __init__(self):
        super(MarshVariantsCSVParser, self).__init__()
        self.marsh_variants = tree()
    
    def next(self, row):
        mr_id = int(row[1])
        mv_id = int(row[0])
        mv_enddateexists = int(row[5])
        if mv_enddateexists:
            mv_enddate = time.mktime(time.strptime(row[4], "%Y-%m-%d %H:%M:%S"))
            if mv_enddate < current_time:
                return
        mv_startdate = time.mktime(time.strptime(row[3], "%Y-%m-%d %H:%M:%S"))
        if mv_startdate > current_time:
            return
        mv_checksum = int(row[6])
        redis_key = 'checksum:tn:%s:marshvariants:%d:%d' % (args.url[1:], mr_id, mv_id)
        redis_checksum = redis_client.get(redis_key)
        if redis_checksum:
            if mv_checksum == int(redis_checksum):
                change = False
            else:
                change = True
        else:
            change = True
        self.marsh_variants[mr_id][mv_startdate][mv_id] = { 'checksum': mv_checksum, 'change': change }
                                   
class RaspVariantsXMLParser(XMLParser):
    '''XML парсер запроса getRaspVariants'''
    def __init__(self):
        super(RaspVariantsXMLParser, self).__init__()
        self.rasp_variants = tree()
    
    def next(self, tag, attrs):
        if tag == 'row':
            rv_enable = int(attrs['rv_enable'])
            if not rv_enable:
                return
            rv_enddateexists = int(attrs['rv_enddateexists'])
            if rv_enddateexists:
                rv_enddate = time.mktime(time.strptime(attrs['rv_enddate'], "%Y-%m-%d %H:%M:%S"))
                if rv_enddate < current_time:
                    return
            mr_id = int(attrs['mr_id'])
            srv_id = int(attrs['srv_id'])
            rv_id = int(attrs['rv_id'])
            try:
                rv_dow = int(attrs['rv_dow']) & 127
            except:
                rv_dow = 127
            rv_startdate = time.mktime(time.strptime(attrs['rv_startdate'], "%Y-%m-%d %H:%M:%S"))
            if rv_startdate > current_time:
                return
            rv_checksum = int(attrs['rv_checksum'])
            redis_key = 'checksum:tn:%s:raspvariants:%d:%d:%d' % (args.url[1:], mr_id, srv_id, rv_id)
            redis_checksum = redis_client.get(redis_key)
            if redis_checksum:
                if rv_checksum == int(redis_checksum):
                    change = False
                else:
                    change = True
            else:
                change = True
            mask = 1
            for day_week in xrange(7):
                if mask & rv_dow:
                    self.rasp_variants[mr_id][day_week][rv_startdate][(srv_id, rv_id)] = { 'mask': rv_dow, 'checksum': rv_checksum, 'change': change }
                mask = mask << 1

class RaspVariantsCSVParser(CSVParser):
    '''CSV парсер запроса getRaspVariants'''
    def __init__(self):
        super(RaspVariantsCSVParser, self).__init__()
        self.rasp_variants = tree()
    
    def next(self, row):
        rv_enable = int(row[10])
        if not rv_enable:
            return
        rv_enddateexists = int(row[7])
        if rv_enddateexists:
            rv_enddate = time.mktime(time.strptime(row[6], "%Y-%m-%d %H:%M:%S"))
            if rv_enddate < current_time:
                return
        mr_id = int(row[2])
        srv_id = int(row[0])
        rv_id = int(row[1])
        try:
            rv_dow = int(row[3]) & 127
        except:
            rv_dow = 127
        rv_startdate = time.mktime(time.strptime(row[5], "%Y-%m-%d %H:%M:%S"))
        if rv_startdate > current_time:
            return
        rv_checksum = int(row[9])
        redis_key = 'checksum:tn:%s:raspvariants:%d:%d:%d' % (args.url[1:], mr_id, srv_id, rv_id)
        redis_checksum = redis_client.get(redis_key)
        if redis_checksum:
            if rv_checksum == int(redis_checksum):
                change = False
            else:
                change = True
        else:
            change = True
        mask = 1
        for day_week in xrange(7):
            if mask & rv_dow:
                self.rasp_variants[mr_id][day_week][rv_startdate][(srv_id, rv_id)] = { 'mask': rv_dow, 'checksum': rv_checksum, 'change': change }
            mask = mask << 1

class RaceCardXMLParser(XMLParser):
    '''XML парсер запроса getRaceCard, последовательности остановок направления'''
    def __init__(self):
        super(RaceCardXMLParser, self).__init__()
        self.race_card = tree()
    
    def next(self, tag, attrs):
        if tag == 'row':
            direction = ord(attrs['rl_racetype'][0]) - ord('A')
            rc_orderby = int(attrs['rc_orderby'])
            st_id = int(attrs['st_id'])
            rc_distance = float(attrs['rc_distance'])
            self.race_card[direction][rc_orderby] = { 'st_id': st_id, 'distance': rc_distance }


class RaceCardCSVParser(CSVParser):
    '''CSV парсер запроса getRaceCard, последовательности остановок направления'''
    def __init__(self):
        super(RaceCardCSVParser, self).__init__()
        self.race_card = tree()
    
    def next(self, row):
        direction = ord(row[1][0]) - ord('A')
        rc_orderby = int(row[2])
        st_id = int(row[3])
        rc_distance = float(row[5])
        self.race_card[direction][rc_orderby] = { 'st_id': st_id, 'distance': rc_distance }


class RaceCoordXMLParser(XMLParser):
    '''XML парсер запроса getRaceCoord, геометрии направления'''
    def __init__(self):
        super(RaceCoordXMLParser, self).__init__()
        self.race_coord = tree()
    
    def next(self, tag, attrs):
        if tag == 'row':
            direction = ord(attrs['rl_racetype'][0]) - ord('A')
            rd_orderby = int(attrs['rd_orderby'])
            lat = float(attrs['rd_lat'])
            lng = float(attrs['rd_long'])
            self.race_coord[direction][rd_orderby] = {'coord': {'lat': lat, 'long': lng }}
    

class RaceCoordCSVParser(CSVParser):
    '''CSV парсер запроса getRaceCoord, геометрии направления'''
    def __init__(self):
        super(RaceCoordCSVParser, self).__init__()
        self.race_coord = tree()
    
    def next(self, row):
        direction = ord(row[1][0]) - ord('A')
        rd_orderby = int(row[2])
        lat = float(row[3])
        lng = float(row[4])
        self.race_coord[direction][rd_orderby] = {'coord': {'lat': lat, 'long': lng }}

def find_best_place(refer, index_st, st_id, rt_time):
    if index_st >= 0 and index_st < len(refer) and refer[index_st]['st_id'] == st_id:
        refer[index_st]['time'].append(rt_time)
        return
    else:
        for i in xrange(1,4):
            index_new = index_st + i
            if index_new >= 0 and index_new < len(refer) and refer[index_new]['st_id'] == st_id:
                refer[index_new]['time'].append(rt_time)
                return
            index_new = index_st - i
            if index_new >= 0 and index_new < len(refer) and refer[index_new]['st_id'] == st_id:
                refer[index_new]['time'].append(rt_time)
                return

class RaspTimeXMLParser(XMLParser):
    '''XML парсер запроса getRaspTime'''
    def __init__(self):
        super(RaspTimeXMLParser, self).__init__()
        self.rasp_time = tree()
    
    def next(self, tag, attrs):
        if tag == 'row':
            direction = ord(attrs['rl_racetype'][0]) - ord('A')
            st_id = int(attrs['st_id'])
            try:
                rt_time = int(attrs['rt_time'])
            except ValueError:
                logger.info(u'В таблице tbrasptime неверные данные rt_time="%s" при srv_id="%s" rv_id="%s"' % (attrs['rt_time'], attrs['srv_id'], attrs['rv_id']))
                return
            rt_orderby = int(attrs['rt_orderby'])
            gr_id = int(attrs['gr_id'])
            rt_racenum = int(attrs['rt_racenum'])
            self.rasp_time[direction][gr_id][rt_racenum][rt_orderby] = {'st_id': st_id, 'time': rt_time }

class RaspTimeCSVParser(CSVParser):
    '''CSV парсер запроса getRaspTime'''
    def __init__(self):
        super(RaspTimeCSVParser, self).__init__()
        self.rasp_time = tree()
    
    def next(self, row):
        direction = ord(row[6][0]) - ord('A')
        st_id = int(row[7])
        try:
            rt_time = int(row[8])
        except ValueError:
            logger.info(u'В таблице tbrasptime неверные данные rt_time="%s" при srv_id="%s" rv_id="%s"' % (row[8], row[0], row[1]))
            return
        rt_orderby = int(row[3])
        gr_id = int(row[2])
        rt_racenum = int(row[10])
        self.rasp_time[direction][gr_id][rt_racenum][rt_orderby] = {'st_id': st_id, 'time': rt_time }

class CheckSumXMLParser(XMLParser):
    '''XML парсер запроса getChecksum'''
    def __init__(self):
        super(CheckSumXMLParser, self).__init__()
        self.checksum = {}
    
    def next(self, tag, attrs):
        if tag == 'row':
            checksum = int(attrs['cs_checksum'])
            self.checksum[attrs['cs_tablename']] = checksum

class CheckSumCSVParser(CSVParser):
    '''CSV парсер запроса getChecksum'''
    def __init__(self):
        super(CheckSumCSVParser, self).__init__()
        self.checksum = {}
    
    def next(self, row):
        checksum = int(row[1])
        self.checksum[row[0]] = checksum

class StopsXMLParser(XMLParser):
    '''XML парсер запроса getStops'''
    def __init__(self):
        super(StopsXMLParser, self).__init__()
        self.stations = {}
    
    def next(self, tag, attrs):
        if tag == 'row':
            st_id = int(attrs['st_id'])
            lat = float(attrs['st_lat'])
            lng = float(attrs['st_long'])
            self.stations[st_id] = {'name': attrs['st_title'], 'location': {'lat': lat, 'long': lng}, 'tags': set() }

class StopsCSVParser(CSVParser):
    '''CSV парсер запроса getStops'''
    def __init__(self):
        super(StopsCSVParser, self).__init__()
        self.stations = {}
    
    def next(self, row):
        st_id = int(row[0])
        lat = float(row[5])
        lng = float(row[6])
        self.stations[st_id] = {'name': row[3], 'location': {'lat': lat, 'long': lng}, 'tags': set() }

def check_table(name_table):
    if args.format == 'xml':
        handler = CheckSumXMLParser()
        request = '/getChecksum.php?cs_tablename=%s' % name_table
    else:
        handler = CheckSumCSVParser()
        request =  '/getChecksum.php?cs_tablename=%s&fmt=csv' % name_table
    http_request(request, handler)
    if name_table in handler.checksum:
        checksum_new = handler.checksum[name_table]
        key = 'checksum:tn:%s:%s' % (args.url[1:], name_table)
        checksum_old = redis_client.get(key)
        if checksum_old:
            if checksum_new != int(checksum_old):
                logger.info(u'Контрольная сумма таблицы %s не совпадает %d <> %s, требуется полная перегрузка данных' % (name_table, checksum_new, checksum_old))
                return (checksum_new, True)
            else:
                return (checksum_new, False)
        else:
            logger.info(u'Контрольная сумма таблицы %s, отсутствует в Redis, требуется полная перегрузка данных' % name_table)
            return (checksum_new, True)
    else:
        logger.info(u'В таблице tbchecksum отсуствует контрольная сумма таблицы %s' % name_table)
        return (None, True)

def get_local_ids(index, doc_type):
    ids = set()
    prefix = '%d:' % group_code
    query = {'query': {'prefix': { '_id': prefix }}}
    try:
        for hit in scan(es_client, query, '10m', index=index, doc_type=doc_type,  fields=''):
            ids.add(hit['_id'])
    except:
        pass
    return ids

file_lock = threading.Lock()

def save_json_to_file(file_descriptor, meta, body=None):
    file_lock.acquire()
    try:
        file_descriptor.write(u'%s\n' % json.dumps(meta, ensure_ascii=False))
        if body:
            file_descriptor.write(u'%s\n' % json.dumps(body, ensure_ascii=False))    
    finally:
        file_lock.release()

def delete_old_files(mask, days_delta):
    '''Удаление json файлов, которые старее days_delta дней'''
    current_time_date = datetime.date.fromtimestamp(current_time)
    delta = datetime.timedelta(days=days_delta)
    for name_file in glob.glob(mask):
        date_file = time.mktime(time.strptime(os.path.splitext(os.path.basename(name_file))[0].split('_')[1], "%Y-%m-%dT%H:%M:%S"))
        if current_time_date - datetime.date.fromtimestamp(date_file) > delta:
            logger.info(u'Удален старый файл %s' % os.path.basename(name_file))
            os.remove(name_file)

class SynchroStations(object):
    '''Синхронизация остановок транспорта'''
    def __init__(self):
        self.report = tree()
        self.lock = threading.Lock()
    
    def synchro(self):
        self.checksum, self.change = check_table('tbstops')
        if not args.only:
            self.ids = get_local_ids(name_index_es[args.url[1:]], 'station')
        else:
            self.ids = set()
            self.change = True
        self.process_stops()
    
    def process_stops(self):
        if args.format == 'xml':
            handler = StopsXMLParser()
            request = '/getStops.php'
        else:
            handler = StopsCSVParser()
            request = '/getStops.php?fmt=csv'
        http_request(request, handler)
        self.stations = handler.stations
        logger.debug(u'Получен список остановок')
    
    def set_tags(self, st_id, transport):
        self.lock.acquire()
        try:
            if st_id in self.stations:
                self.stations[st_id]['tags'].add(transport)
        finally:
            self.lock.release()
    
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
            if complex_id in self.ids:
                station_es = es_client.get(index=name_index_es[args.url[1:]], doc_type = 'station', id = complex_id)
                if station_es['_source'] != station_tn:
                    meta = {'index': {'_index': name_index_es[args.url[1:]], '_type': 'station', '_id': complex_id}}
                    save_json_to_file(file_descriptor, meta, station_tn)
                    self.report_stations('update', st_id)
                self.ids.discard(complex_id)
            else:
                meta = {'index': {'_index': name_index_es[args.url[1:]], '_type': 'station', '_id': complex_id}}
                save_json_to_file(file_descriptor, meta, station_tn)
                self.report_stations('insert', st_id)
        for complex_id in self.ids:
            meta = {'delete': {'_index': name_index_es[args.url[1:]], '_type': 'station', '_id': complex_id}}
            save_json_to_file(file_descriptor, meta)
            st_id = int(complex_id.split(':')[-1])
            self.report_stations('delete', st_id)

    def get_json(self, st_id):
        if len(self.stations[st_id]['tags']) == 0:
            return None
        doc_id = u'%d:%d' % (group_code, st_id)
        tags = []
        for transport in sorted(self.stations[st_id]['tags']):
            tags.append(transport)
        station = {
            'id': doc_id,
            'location': [
                self.stations[st_id]['location']['long'],
                self.stations[st_id]['location']['lat']
            ],
            'tags': tags,
            'name': self.stations[st_id]['name'],
            'region': name_region[args.url[1:]]
        }
        return station
    
    def report_stations(self, operation, st_id):
        if operation not in self.report:
            self.report[operation] = set()
        self.report[operation].add(st_id)

    def save_report(self):
        for operation in self.report:
            key = 'report_info:tn:change:%f:station:%s' % (current_time, operation)
            if redis_client.exists(key):
                exists = True
            else:
                exists = False
            redis_client.incrby(key, len(self.report[operation]))
            if not exists:
                redis_client.expire(key, 30 * 24 * 60 * 60) # key dead after 30 days
    
    def set_checksum(self):
        if self.checksum != None:
            key = 'checksum:tn:%s:tbstops' % args.url[1:]
            redis_client.set(key, self.checksum)
    
    def summary(self):
        insert = 0
        update = 0
        delete = 0
        if 'insert' in self.report:
            insert = len(self.report['insert'])
        if 'update' in self.report:
            update = len(self.report['update'])
        if 'delete' in self.report:
            delete = len(self.report['delete'])
        return (insert, update, delete)

class SynchroRoutes(object):
    '''Синхронизация маршрутов'''
    def __init__(self):
        self.stations = SynchroStations()
        self.stations.synchro()
        self.marsh_variants = tree()
        self.rasp_variants = tree()
        self.report = tree()
        self.report_lock = threading.Lock()
        self.route_ids_lock = threading.Lock()
    
    def del_route_id(self, doc_id):
        self.route_ids_lock.acquire()
        try:
            self._route_ids.discard(doc_id)
        finally:
            self.route_ids_lock.release()
    
    def present_route_id(self, doc_id):
        self.route_ids_lock.acquire()
        try:
            if doc_id in self._route_ids:
                return True
            else:
                return False
        finally:
            self.route_ids_lock.release()
            

    def synchro(self, file_descriptor):
        self.checksum, self.change = check_table('tbmarshes')
        if not args.only:
            self._route_ids = get_local_ids(name_index_es[args.url[1:]], 'route')
        else:
            self._route_ids = set()
            self.change = True
        self.process_marshes()
        self.process_marshvariants()
        self.process_raspvariants()
        change_checksum = False
        threadpool = ThreadPool(args.max_thread)
        total_marches = len(self.marshes)
        for i, mr_id in enumerate(self.marshes):
            if self.change or self.stations.change or self.change_marsh_variants(mr_id) or self.change_rasp_variants(mr_id):
                logger.info(u'[%d/%d] Обрабатывается маршрут %s %s mr_id=%d' %(i + 1, total_marches, self.marshes[mr_id]['name'], self.marshes[mr_id]['description'], mr_id))
                change_checksum = True
                routeExtra = RouteExtra(self, mr_id, file_descriptor)
                threadpool.start(routeExtra)
            else:
                self.local_process_marsh_variants(mr_id, file_descriptor)
        threadpool.finish()
        self.stations.form_file(file_descriptor)
        for complex_id in self._route_ids:
            meta = {'delete': {'_index': name_index_es[args.url[1:]], '_type': 'route', '_id': complex_id}}
            save_json_to_file(file_descriptor, meta)
            complex_id_split = complex_id.split(':')
            mr_id = int(complex_id_split[1])
            direction = int(complex_id_split[2])
            self.report_changes('delete', mr_id, direction, self.report_description_route_local(complex_id))
            self.del_report_routes(mr_id, direction)
        return change_checksum

    def process_marshes(self):
        if args.format == 'xml':
            handler = MarshesXMLParser()
            request = '/getMarshes.php'
        else:
            handler = MarshesCSVParser()
            request = '/getMarshes.php?fmt=csv'
        http_request(request, handler)
        self.marshes = handler.marshes
        logger.debug(u'Получен список маршрутов')

    def process_marshvariants(self):
        if args.format == 'xml':
            handler = MarshVariantsXMLParser()
            request = '/getMarshVariants.php'
        else:
            handler = MarshVariantsCSVParser()
            request = '/getMarshVariants.php?fmt=csv'
        http_request(request, handler)
        for mr_id in handler.marsh_variants:
            mv_startdate = sorted(handler.marsh_variants[mr_id].keys())[-1]
            mv_id = sorted(handler.marsh_variants[mr_id][mv_startdate].keys())[-1]
            self.marsh_variants[mr_id][mv_id] = handler.marsh_variants[mr_id][mv_startdate][mv_id]
        logger.debug(u'Получен список вариантов маршрутов')

    def process_raspvariants(self):
        if args.format == 'xml':
            handler = RaspVariantsXMLParser()
            request = '/getRaspVariants.php'
        else:
            handler = RaspVariantsCSVParser()
            request = '/getRaspVariants.php?fmt=csv'
        http_request(request, handler)
        for mr_id in handler.rasp_variants:
            for day_week in handler.rasp_variants[mr_id]:
                rv_startdate = sorted(handler.rasp_variants[mr_id][day_week].keys())[-1]
                (srv_id, rv_id) = sorted(handler.rasp_variants[mr_id][day_week][rv_startdate].keys())[-1]
                self.rasp_variants[mr_id][(srv_id, rv_id)] = handler.rasp_variants[mr_id][day_week][rv_startdate][(srv_id, rv_id)]
        logger.debug(u'Получен список вариантов расписаний')
    
    def change_marsh_variants(self, mr_id):
        if mr_id in self.marsh_variants:
            for mv_id in self.marsh_variants[mr_id]:
                if self.marsh_variants[mr_id][mv_id]['change']:
                    return True
        return False
    
    def change_rasp_variants(self, mr_id):
        if mr_id in self.rasp_variants:
            for (srv_id, rv_id) in self.rasp_variants[mr_id]:
                if self.rasp_variants[mr_id][(srv_id, rv_id)]['change']:
                    return True
        return False
    
    def local_process_marsh_variants(self, mr_id, file_descriptor):
        prefix = '%d:%d:' % (group_code, mr_id)
        for hit in scan(es_client, {'query': {'prefix': { '_id': prefix }}}, '10m', index=name_index_es[args.url[1:]], doc_type='route'):
            complex_id = hit['_id']
            direction = int(complex_id.split(':')[-1])
            route_es = hit['_source']
            route = self.get_route_from_local(mr_id, direction, route_es)
            if route != route_es:
                meta = {'index': {'_index': name_index_es[args.url[1:]], '_type': 'route', '_id': complex_id}}
                save_json_to_file(file_descriptor, meta, route)
                report_description = u'%8s %-80s %s/%d/%d' % (route['ru']['name'], route['ru']['direction'], complex_id.split(':')[0], mr_id, direction)
                self.report_changes('update', mr_id, direction, report_description)
            for item_station in route['stations']:
                st_id = int(item_station['id'].split(':')[-1])
                self.stations.set_tags(st_id, route['transport'])
            self.del_route_id(complex_id)
    
    def get_route_from_local(self, mr_id, direction, route_es):
        doc_id = '%d:%d:%d' % (group_code, mr_id, direction)
        name = self.marshes[mr_id]['name']
        if not (direction == 0 or direction == 1):
            name = name + u'*'
        name_direction = self.get_name_direction_local(route_es)
        route = {
            'id': doc_id,
            'name': name,
            'direction': name_direction,
            'region': name_region[args.url[1:]],
            'transport': self.marshes[mr_id]['transport'],
            'stations': route_es['stations'],
            'geometry': route_es['geometry'],
            'valid': route_es['valid']
        }
        if 'time_ext' in route_es:
            route['time_ext'] = route_es['time_ext']
        return route

    def get_name_direction_local(self, route_es):
        firststation_id = int(route_es['stations'][0]['id'].split(':')[-1])
        laststation_id = int(route_es['stations'][-1]['id'].split(':')[-1])
        name_direction = u'%s - %s' % (self.stations.get_station(firststation_id)['name'], self.stations.get_station(laststation_id)['name'])
        return name_direction
    
    def set_checksum(self):
        self.stations.set_checksum()
        if self.checksum:
            key = 'checksum:tn:%s:tbmarshes' % args.url[1:]
            redis_client.set(key, self.checksum)
        for key in redis_client.keys('checksum:tn:%s:marshvariants:*' % args.url[1:]):
            redis_client.delete(key)
        for key in redis_client.keys('checksum:tn:%s:raspvariants:*' % args.url[1:]):
            redis_client.delete(key)
        for mr_id in self.marshes:
            for mv_id in self.marsh_variants[mr_id]:
                key = 'checksum:tn:%s:marshvariants:%d:%d' % (args.url[1:], mr_id, mv_id)
                redis_client.set(key, self.marsh_variants[mr_id][mv_id]['checksum'])
            for (srv_id, rv_id) in self.rasp_variants[mr_id]:
                if 'created' not in self.rasp_variants[mr_id][(srv_id, rv_id)]:
                    key = 'checksum:tn:%s:raspvariants:%d:%d:%d' % (args.url[1:], mr_id, srv_id, rv_id)
                    redis_client.set(key, self.rasp_variants[mr_id][srv_id, rv_id]['checksum'])
    
    def report_changes(self,operation, mr_id, direction, description):
        self.report_lock.acquire()
        try:
            if operation == 'insert' or operation == 'delete':
                if self.exists_report_changes('update', mr_id, direction):
                    del self.report['change']['update'][mr_id][direction]
                self.report['change'][operation][mr_id][direction] = description
            elif operation == 'update':
                if not (self.exists_report_changes('insert', mr_id, direction) or self.exists_report_changes('delete', mr_id, direction)):
                    self.report['change'][operation][mr_id][direction] = description
        finally:
            self.report_lock.release()

    def exists_report_changes(self, operation, mr_id, direction):
        if 'change' in self.report:
            if operation in self.report['change']:
                if mr_id in self.report['change'][operation]:
                    if direction in self.report['change'][operation][mr_id]:
                        return True
        return False
    
    def report_routes(self, mr_id, direction, loaded = False, validate_schedule = False, validate_geometry = False, generate_schedule = False):
        self.report_lock.acquire()
        try:
            self.report['routes']['add'][mr_id][direction] = {'loaded': loaded, 'validate_schedule': validate_schedule, 'validate_geometry': validate_geometry, 'generate_schedule': generate_schedule}
        finally:
            self.report_lock.release()
    
    def report_routes_index(self, mr_id, direction, index, value):
        self.report_lock.acquire()
        try:
            self.report['routes']['add'][mr_id][direction][index] = value
        finally:
            self.report_lock.release()
    
    def del_report_routes(self, mr_id, direction):
        self.report_lock.acquire()
        try:
            self.report['routes']['del'][mr_id][direction] = ''
        finally:
            self.report_lock.release()
    
    def save_report(self):
        self.stations.save_report()
        for operation in self.report['change']:
            for mr_id in self.report['change'][operation]:
                for direction in self.report['change'][operation][mr_id]:
                    key = 'report_info:tn:change:%f:route:%s' % (current_time, operation)
                    if redis_client.exists(key):
                        exists = True
                    else:
                        exists = False
                    redis_client.rpush(key, self.report['change'][operation][mr_id][direction])
                    if not exists:
                        redis_client.expire(key, 30 * 24 * 60 * 60) # key dead after 30 days
        for mr_id in self.report['routes']['del']:
            for direction in self.report['routes']['del'][mr_id]:
                key = 'report_info:tn:routes:%s:%d:%d:%d' % (args.url[1:], group_code, mr_id, direction)
                redis_client.delete(key)
        for mr_id in self.report['routes']['add']:
            for direction in self.report['routes']['add'][mr_id]:
                key = 'report_info:tn:routes:%s:%d:%d:%d' % (args.url[1:], group_code, mr_id, direction)
                if redis_client.exists(key):
                    redis_client.delete(key)
                redis_client.rpush(key, int(self.report['routes']['add'][mr_id][direction]['loaded']), int(self.report['routes']['add'][mr_id][direction]['validate_schedule']), int(self.report['routes']['add'][mr_id][direction]['validate_geometry']), int(self.report['routes']['add'][mr_id][direction]['generate_schedule']))
    
    def report_description_route_local(self, complex_id):
        complex_id_split = complex_id.split(':')
        okato = complex_id_split[0]
        mr_id = int(complex_id_split[1])
        direction = int(complex_id_split[2])
        route_es = es_client.get(index = name_index_es[args.url[1:]], doc_type = 'route', id = complex_id)
        name_direction = route_es['_source']['direction']
        if 'time_ext' in route_es:
            name_direction = name_direction + ' (' + route_es['time_ext'] + u')'
        name = route_es['_source']['name']
        return  u'%8s %-80s %s/%d/%d' % (name, name_direction, okato, mr_id, direction)
    
    def summary(self):
        insert = 0
        update = 0
        delete = 0
        for operation in self.report['change']:
            for mr_id in self.report['change'][operation]:
                if operation == 'insert':
                    insert += len(self.report['change'][operation][mr_id])
                elif operation == 'update':
                    update += len(self.report['change'][operation][mr_id])
                elif operation == 'delete':
                    delete += len(self.report['change'][operation][mr_id])
        return (insert, update, delete)

class ThreadPool(object):
    '''Представляет из себя пул потоков, он будет следить за их количеством и выполнением'''
    def __init__(self, max_thread):
        self.max_thread = max_thread
        self.threads =[]
    
    def start(self, thread):
        if len(self.threads) >= self.max_thread:
            self.check_thread()
        thread.start()
        self.threads.append(thread)
    
    def check_thread(self):
        n = 1
        while True:
            logger.debug(u'Запуск check_thread len(self.threads)=%d' % len(self.threads))
            finished = []
            for thread in self.threads:
                if not thread.is_alive():
                    finished.append(thread)
            logger.debug(u'Выявлено %d завершившихся потоков' % len(finished))
            if len(finished) > 0:
                for thread in finished:
                    self.threads.remove(thread)
                logger.debug(u'После удаления завершившихся потоков len(self.threads)=%d' % len(self.threads))
                break
            else:
                time.sleep(1)
            n += 1
            if n > 10:
                for thread in self.threads:
                    logger.debug(u'Оставшиеся потоки %s mr_id=%d' % (str(thread), thread.mr_id))
                n = 0
    
    def finish(self):
        while len(self.threads) > 0:
            self.check_thread()

class RouteExtra(threading.Thread):
    '''Обрабатывает геометрию и расписание маршрутов'''
    def __init__(self, marshes, mr_id, file_descriptor):
        super(RouteExtra, self).__init__()
        self.marshes = marshes
        self.mr_id = mr_id
        self.file_descriptor = file_descriptor
        self.es_client = Elasticsearch([{'host': args.host_es, 'port': args.port_es}])
        self.project = partial(
            pyproj.transform,
            pyproj.Proj(init='EPSG:4326'),
            pyproj.Proj(init=name_proj[args.url[1:]]))
        self.race_card = {}
        self.race_coord = {}
        self.rasp_time = tree()

    def run(self):
        try:
            for mv_id in self.marshes.marsh_variants[self.mr_id]:
                self.process_racecard(mv_id)
                self.process_racecoord(mv_id)
            for (srv_id, rv_id) in self.marshes.rasp_variants[self.mr_id]:
                self.process_rasptime(srv_id, rv_id)
            self.form_file()
        except Exception, e:
            logger.exception(e, extra={'marker': 'nagios'})
            os.kill(os.getpid(), signal.SIGTERM)
    
    def process_racecard(self, mv_id):
        if args.format == 'xml':
            handler = RaceCardXMLParser()
            request = '/getRaceCard.php?mv_id=%d' % mv_id
        else:
            handler = RaceCardCSVParser()
            request = '/getRaceCard.php?mv_id=%d&fmt=csv' % mv_id
        http_request(request, handler)
        for direction in handler.race_card:
            self.race_card[direction] = [handler.race_card[direction][k] for k in sorted(handler.race_card[direction].keys())]
        logger.debug(u'Скачена последовательность остановок для маршрута mr_id=%d, mv_id=%d, %s %s' % (self.mr_id, mv_id, self.marshes.marshes[self.mr_id]['name'], self.marshes.marshes[self.mr_id]['description']))
    
    def process_racecoord(self, mv_id):
        if args.format == 'xml':
            handler = RaceCoordXMLParser()
            request = '/getRaceCoord.php?mv_id=%d' % mv_id
        else:
            handler = RaceCoordCSVParser()
            request = '/getRaceCoord.php?mv_id=%d&fmt=csv' % mv_id
        http_request(request, handler)
        for direction in handler.race_coord:
            self.race_coord[direction] = [handler.race_coord[direction][k] for k in sorted(handler.race_coord[direction].keys())]
        logger.debug(u'Скачена геометрия для маршрута mr_id=%d, mv_id=%d, %s %s' % (self.mr_id, mv_id, self.marshes.marshes[self.mr_id]['name'], self.marshes.marshes[self.mr_id]['description']))
    
    def process_rasptime(self, srv_id, rv_id):
        if args.format == 'xml':
            handler = RaspTimeXMLParser()
            request = '/getRaspTime.php?srv_id=%d&rv_id=%d' % (srv_id, rv_id)
        else:
            handler = RaspTimeCSVParser()
            request = '/getRaspTime.php?srv_id=%d&rv_id=%d&fmt=csv' % (srv_id, rv_id)
        http_request(request, handler)
        for direction in self.race_card:
            self.rasp_time[(srv_id, rv_id)][direction] = []
            for item in self.race_card[direction]:
                self.rasp_time[(srv_id, rv_id)][direction].append({'st_id': item['st_id'], 'time': []})
        for direction in handler.rasp_time:
            for gr_id in sorted(handler.rasp_time[direction].keys()):
                for rt_racenum in sorted(handler.rasp_time[direction][gr_id].keys()):
                    for index_st, rt_orderby in enumerate(sorted(handler.rasp_time[direction][gr_id][rt_racenum].keys())):
                        rt_time = handler.rasp_time[direction][gr_id][rt_racenum][rt_orderby]['time']
                        st_id = handler.rasp_time[direction][gr_id][rt_racenum][rt_orderby]['st_id']
                        find_best_place(self.rasp_time[(srv_id, rv_id)][direction], index_st, st_id, rt_time)
        for direction in self.rasp_time[(srv_id, rv_id)]:
            for item in self.rasp_time[(srv_id, rv_id)][direction]:
                item['time'] = sorted(item['time'])
        logger.debug(u'Скачены расписания для маршрута mr_id=%d, srv_id=%d, rv_id=%d, %s %s' % (self.mr_id, srv_id, rv_id, self.marshes.marshes[self.mr_id]['name'], self.marshes.marshes[self.mr_id]['description']))
    
    def form_file(self):
        logger.debug(u'Обработка маршрута %s %s mr_id=%d' % (self.marshes.marshes[self.mr_id]['name'], self.marshes.marshes[self.mr_id]['description'], self.mr_id))
        for direction in self.race_card:
            self.marshes.report_routes(self.mr_id, direction)
            if not self.need_generate(direction):
                continue
            self.marshes.report_routes_index(self.mr_id, direction, 'loaded' , True)
            self.form_route(direction)
    
    def need_generate(self, direction):
        return self.need_generate_station(direction) and self.need_generate_geometry(direction)
    
    def need_generate_station(self, direction):
        need_deleted = set()
        for i, item in enumerate(self.race_card[direction]):
            st_id = item['st_id']
            station = self.marshes.stations.get_station(st_id)
            if station == None:
                logger.info(u'На маршруте %s %s mr_id=%d, направление %s остановка st_id=%d отсутствует в справочнике остановок' % (self.marshes.marshes[self.mr_id]['name'], self.marshes.marshes[self.mr_id]['description'], self.mr_id, chr(direction + ord('A')), st_id))
                need_deleted.add(i)
            else:
                self.marshes.stations.set_tags(st_id, self.marshes.marshes[self.mr_id]['transport'])
        if len(need_deleted) > 0:
            race_card = []
            for i, item in enumerate(self.race_card[direction]):
                if i not in need_deleted:
                    race_card.append[item]
            self.race_card[direction] = race_card
        if len(self.race_card[direction]) < 2:
            logger.info(u'На маршруте %s %s mr_id=%d, направление %s менее 2 остановок' % (self.marshes.marshes[self.mr_id]['name'], self.marshes.marshes[self.mr_id]['description'], self.mr_id, chr(direction + ord('A'))))
            return False
        return True
    
    def need_generate_geometry(self, direction):
        if len(self.race_coord) == 0:
            logger.info(u'На маршруте %s %s mr_id=%d отсутствует геометрия' % (self.marshes.marshes[self.mr_id]['name'], self.marshes.marshes[self.mr_id]['description'], self.mr_id))
            return False
        if direction not in self.race_coord:
            logger.info(u'На маршруте %s %s mr_id=%d, направление %s отсутствует геометрия' % (self.marshes.marshes[self.mr_id]['name'], self.marshes.marshes[self.mr_id]['description'], self.mr_id, chr(direction + ord('A'))))
            return False
        if len(self.race_coord[direction]) < 2:
            logger.info(u'На маршруте %s %s mr_id=%d, направление %s в геометрии менее 2 точек' % (self.marshes.marshes[self.mr_id]['name'], self.marshes.marshes[self.mr_id]['description'], self.mr_id, chr(direction + ord('A'))))
            return False
        return True
    
    def form_route(self, direction):
        route = self.create_route(direction)
        complex_id = route['id']
        if self.marshes.present_route_id(complex_id):
            route_es = self.es_client.get(index = name_index_es[args.url[1:]], doc_type = 'route', id = complex_id)
            if route_es['_source'] != route:
                meta = {'index': {'_index': name_index_es[args.url[1:]], '_type': 'route', '_id': complex_id}}
                save_json_to_file(self.file_descriptor, meta, route)
                self.marshes.report_changes('update', self.mr_id, direction, self.report_description_route(direction))
            self.marshes.del_route_id(complex_id)
        else:
            meta = {'index': {'_index': name_index_es[args.url[1:]], '_type': 'route', '_id': complex_id}}
            save_json_to_file(self.file_descriptor, meta, route)
            self.marshes.report_changes('insert', self.mr_id, direction, self.report_description_route(direction))

    def create_route(self, direction):
        valid = self.validate(direction)
        doc_id = '%d:%d:%d' % (group_code, self.mr_id, direction)
        name = self.marshes.marshes[self.mr_id]['name']
        if not (direction == 0 or direction == 1):
            name = name + u'*'
        name_direction, text_direction_time = self.get_name_direction(direction)
        geometry = self.create_geometry(direction)
        schedules = self.create_schedules(direction)
        stations = []
        for i, item in enumerate(self.race_card[direction]):
            st_id = item['st_id']
            id_station = '%d:%d' % (group_code, st_id)
            stations.append({ 'id': id_station, 'distance': item['distance'] * 1000, 'schedule': schedules[i] })
        route = {
            'id': doc_id,
            'name': name,
            'direction': name_direction,
            'region': name_region[args.url[1:]],
            'transport': self.marshes.marshes[self.mr_id]['transport'],
            'stations': stations,
            'geometry': geometry,
            'valid': valid
        }
        if text_direction_time != u'':
            route['time_ext'] = text_direction_time
        return route

    def report_description_route(self, direction):
        name = self.marshes.marshes[self.mr_id]['name']
        if not (direction == 0 or direction == 1):
            name = name + u'*'
        name_direction, text_direction_time = self.get_name_direction(direction)
        if text_direction_time != u'':
            name_direction = name_direction + u' (' + text_direction_time + u')'
        return  u'%8s %-80s %d/%d/%d' % (name, name_direction, group_code, self.mr_id, direction)

    def get_name_direction(self, direction):
        firststation_id = self.race_card[direction][0]['st_id']
        laststation_id = self.race_card[direction][-1]['st_id']
        name_direction = u'%s - %s' % (self.marshes.stations.get_station(firststation_id)['name'], self.marshes.stations.get_station(laststation_id)['name'])
        text_direction_time = u''
        if not (direction == 0 or direction == 1):
            direction_time_set = set()
            for (srv_id, rv_id) in self.rasp_time:
                direction_time_begin_present = False
                direction_time_end_present = False
                if direction in self.rasp_time[(srv_id, rv_id)]:
                    if len(self.rasp_time[(srv_id, rv_id)][direction][0]['time']) > 0:
                        direction_time_begin = self.rasp_time[(srv_id, rv_id)][direction][0]['time'][0] % 1440
                        direction_time_begin_present = True
                    if len(self.rasp_time[(srv_id, rv_id)][direction][-1]['time']) > 0:
                        direction_time_end = self.rasp_time[(srv_id, rv_id)][direction][-1]['time'][-1] % 1440
                        direction_time_end_present = True
                if direction_time_begin_present and direction_time_end_present:
                    direction_time = u'%d:%02d-%d:%02d' % (direction_time_begin / 60, direction_time_begin % 60, direction_time_end / 60, direction_time_end % 60)
                    direction_time_set.add(direction_time)
            if len(direction_time_set) > 0:
                for direction_time in direction_time_set:
                    text_direction_time = text_direction_time + direction_time + u'/'
                text_direction_time = text_direction_time[:-1]
        return (name_direction, text_direction_time)
    
    def create_geometry(self, direction):
        points = []
        for point in self.race_coord[direction]:
            points.append(point['coord']['long'])
            points.append(point['coord']['lat'])
        return points

    def create_schedules(self, direction):
        schedules = []
        for index_st, item in enumerate(self.race_card[direction]):
            datas_json = []
            for (srv_id, rv_id) in self.rasp_time:
                mask = self.marshes.rasp_variants[self.mr_id][(srv_id, rv_id)]['mask']
                weekday = []
                running_one = 1
                for i in xrange(7):
                    if mask & running_one:
                        weekday.append(week_day_en[i])
                    running_one = running_one << 1
                time_str= u''
                for time_int in self.rasp_time[(srv_id, rv_id)][direction][index_st]['time']:
                    time_stop = time_int % 1440
                    time_str = time_str + (u'%d:%02d ' % (time_stop / 60, time_stop % 60))
                if len(time_str) > 0:
                    time_str = time_str[:-1]
                data_json = {
                    'time': time_str,
                    'weekday': weekday
                }
                datas_json.append(data_json)
            schedules.append(datas_json)
        return schedules
    
    def validate(self, direction):
        validate_schedule = self.validate_rasptime(direction)
        self.marshes.report_routes_index(self.mr_id, direction, 'validate_schedule', validate_schedule)
        validate_geometry = self.validate_geometry(direction)
        self.marshes.report_routes_index(self.mr_id, direction, 'validate_geometry', validate_geometry)
        return validate_schedule and validate_geometry

    def validate_geometry(self, direction):
        '''Осуществляет проверку геометрии маршрута. Остановки должны лежать на маршруте'''
        line = LineString([(item['coord']['long'], item['coord']['lat']) for item in self.race_coord[direction]])
        line_proj = transform(self.project, line)
        result = True
        for station in self.race_card[direction]:
            st_id = station['st_id']
            stop = self.marshes.stations.get_station(st_id)
            if stop:
                lat = stop['location']['lat']
                lng = stop['location']['long']
                point = Point(lng, lat)
                point_proj = transform(self.project, point)
                distance = line_proj.distance(point_proj)
                if distance > args.distance:
                    logger.info(u'На маршруте %s %s mr_id=%d, направление %s остановка %s st_id=%d не лежит на геометрии маршрута, расстояние до ближайшей точки маршрута %f метров' % (self.marshes.marshes[self.mr_id]['name'], self.marshes.marshes[self.mr_id]['description'], self.mr_id, chr(direction + ord('A')), self.marshes.stations.get_station(st_id)['name'], st_id, distance))
                    result = False
        return result

    def validate_rasptime(self, direction):
        if self.rasp_time == tree():
            logger.info(u'На маршруте %s %s mr_id=%d полностью отсутствует расписание' % (self.marshes.marshes[self.mr_id]['name'], self.marshes.marshes[self.mr_id]['description'], self.mr_id))
            if args.create_schedule:
                if self.check_enable_create_schedule(None, -1, -1):
                    logger.info(u'Создано расписание для маршрута %s %s mr_id=%d' % (self.marshes.marshes[self.mr_id]['name'], self.marshes.marshes[self.mr_id]['description'], self.mr_id))
                else:
                    logger.info(u'Нельзя создать расписание для маршрута %s %s mr_id=%d, причина: отсуствует ключ в redis' % (self.marshes.marshes[self.mr_id]['name'], self.marshes.marshes[self.mr_id]['description'], self.mr_id))
                    return False
            else:
                logger.info(u'Нельзя создать расписание для маршрута %s %s mr_id=%d, причина: не указана опция командной строки' % (self.marshes.marshes[self.mr_id]['name'], self.marshes.marshes[self.mr_id]['description'], self.mr_id))
                return False
        mask = 0
        result = True
        for (srv_id, rv_id) in self.rasp_time:
            if self.empty_schedule_in_direction(srv_id, rv_id, direction):
                logger.info(u'В варианте расписания (srv_id=%d, rv_id=%d) маршрута %s %s mr_id=%d отсутствует направление %s' % (srv_id, rv_id, self.marshes.marshes[self.mr_id]['name'], self.marshes.marshes[self.mr_id]['description'], self.mr_id, chr(direction + ord('A'))))
                if args.create_schedule:
                    if self.check_enable_create_schedule(direction, srv_id, rv_id):
                        logger.info(u'Создано расписание (srv_id=%d, rv_id=%d) для маршрута %s %s mr_id=%d направления %s' % (srv_id, rv_id, self.marshes.marshes[self.mr_id]['name'], self.marshes.marshes[self.mr_id]['description'], self.mr_id, chr(direction + ord('A'))))
                    else:
                        logger.info(u'Нельзя создать расписание (srv_id=%d, rv_id=%d) для маршрута %s %s mr_id=%d направления %s, причина: отсуствует ключ в redis' % (srv_id, rv_id, self.marshes.marshes[self.mr_id]['name'], self.marshes.marshes[self.mr_id]['description'], self.mr_id, chr(direction + ord('A'))))
                        result = False
                        continue
                else:
                    logger.info(u'Нельзя создать расписание (srv_id=%d, rv_id=%d) для маршрута %s %s mr_id=%d направления %s, причина: не указана опция командной строки' % (srv_id, rv_id, self.marshes.marshes[self.mr_id]['name'], self.marshes.marshes[self.mr_id]['description'], self.mr_id, chr(direction + ord('A'))))
                    result = False
                    continue
            for i, item in enumerate(self.rasp_time[(srv_id, rv_id)][direction]):
                st_id = item['st_id']
                if len(item['time']) == 0:
                    logger.info(u'На маршруте %s %s mr_id=%d, направление %s, (srv_id=%d, rv_id=%d) отсутствует расписание на остановке %s st_id=%d' % (self.marshes.marshes[self.mr_id]['name'], self.marshes.marshes[self.mr_id]['description'], self.mr_id, chr(direction + ord('A')), srv_id, rv_id, self.marshes.stations.get_station(st_id)['name'], st_id))
                    result = False
                    break
                if i == 0:
                    count_shedule = len(item['time'])
                else:
                    if count_shedule != len(item['time']):
                        logger.info(u'На маршруте %s %s mr_id=%d, направление %s, (srv_id=%d, rv_id=%d) количество прибытий транспорта на остановке %s st_id=%d не совпадает с первой остановкой %d <> %d' % (self.marshes.marshes[self.mr_id]['name'], self.marshes.marshes[self.mr_id]['description'], self.mr_id, chr(direction + ord('A')), srv_id, rv_id, self.marshes.stations.get_station(st_id)['name'], st_id, len(item['time']), count_shedule))
                        result = False
                        continue
                    for j, rt_time in enumerate(item['time']):
                        if rt_time < pred_rasp_time[j]:
                            time_pred = pred_rasp_time[j] % 1440
                            time_current = rt_time % 1440
                            logger.info(u'На маршруте %s %s mr_id=%d, направление %s, (srv_id=%d, rv_id=%d) время прибытия транспорта на остановку %s std_id=%d %d:%02d меньше времени прибытия на предыдущую остановку %d:%02d' % (self.marshes.marshes[self.mr_id]['name'], self.marshes.marshes[self.mr_id]['description'], self.mr_id, chr(direction + ord('A')), srv_id, rv_id, self.marshes.stations.get_station(st_id)['name'], st_id, time_current / 60, time_current % 60, time_pred / 60, time_pred % 60))
                            result = False
                pred_rasp_time = item['time']
            mask = mask | self.marshes.rasp_variants[self.mr_id][(srv_id, rv_id)]['mask']
        if (mask & 127) != 127:
            running_one = 1
            list_week_day = u''
            for i in xrange(7):
                if (running_one & mask) == 0:
                    list_week_day = list_week_day + week_day[i] + u', '
                running_one = running_one << 1
            list_week_day = list_week_day[:-2]
            logger.info(u'На маршруте %s %s mr_id=%d, направление %s нет расписания на следующие дни недели: %s' % (self.marshes.marshes[self.mr_id]['name'], self.marshes.marshes[self.mr_id]['description'], self.mr_id, chr(direction + ord('A')), list_week_day))
        return result

    def empty_schedule_in_direction(self, srv_id, rv_id, direction):
        for item in self.rasp_time[(srv_id, rv_id)][direction]:
            if len(item['time']) > 0:
                return False
        return True

    def check_enable_create_schedule(self, direction, srv_id, rv_id):
        if direction != None:
            value = redis_client.get('enable_create_schedule:tn:%s:%d:%d' % (args.url[1:], self.mr_id, direction))
            if value:
                self.create_schedule(direction, srv_id, rv_id, value)
                return True
            else:
                return self.check_enable_create_schedule_im(direction, srv_id, rv_id)
        else:
            return self.check_enable_create_schedule_im(direction, srv_id, rv_id)

    def check_enable_create_schedule_im(self, direction, srv_id, rv_id):
        value = redis_client.get('enable_create_schedule:tn:%s:%d:-' % (args.url[1:], self.mr_id ))
        if value:
            self.create_schedule(direction, srv_id, rv_id, value)
            return True
        else:
            value = redis_client.get('enable_create_schedule:tn:%s:-:-' % args.url[1:])
            if value:
                self.create_schedule(direction, srv_id, rv_id, value)
                return True
            else:
                return False

    def create_schedule(self, direction, srv_id, rv_id, value):
        if direction == None:
            for race_type in self.race_card:
                self.create_schedule_im(race_type, srv_id, rv_id, value)
        else:
            self.create_schedule_im(direction, srv_id, rv_id, value)

    def create_schedule_im(self, direction, srv_id, rv_id, value):
        self.rasp_time[(srv_id, rv_id)][direction] = []
        for item in self.race_card[direction]:
            st_id = item['st_id']
            self.rasp_time[(srv_id, rv_id)][direction].append({'st_id': st_id, 'time':[]})
        if value.split(':')[0] == 'C':
            self.create_schedule_im_circle(direction, srv_id, rv_id, value)
        else:
            self.create_schedule_im_line(direction, srv_id, rv_id, value)
        if (srv_id, rv_id) not in self.marshes.rasp_variants[self.mr_id]:
            self.marshes.rasp_variants[self.mr_id][(srv_id, rv_id)] = {'mask': 127, 'created': True, 'change': True }
        else:
            self.marshes.rasp_variants[self.mr_id][(srv_id, rv_id)]['change'] = True
        self.marshes.report_routes_index(self.mr_id, direction, 'generate_schedule', True)

    def create_schedule_im_line(self, direction, srv_id, rv_id, value):
        value_split = value.split(':')
        time_beg = float(value_split[1])
        time_end = float(value_split[2])
        step = float(value_split[3])
        while time_beg < time_end:
            time_stop = time_beg
            for i, item in enumerate(self.race_card[direction]):
                distance = item['distance']
                self.rasp_time[(srv_id, rv_id)][direction][i]['time'].append(int(time_stop))
                time_stop = time_stop + distance * 1000.0 / 333.33 + 1.0 # 1000.0 meters in kilometers, 333.33 meters in minutes are speed bus
            time_beg = time_beg + step

    def create_schedule_im_circle(self, direction, srv_id, rv_id, value):
        value_split = value.split(':')
        time_beg = float(value_split[1])
        time_end = float(value_split[2])
        count_bus = int(value_split[3])
        time_route = 0
        for item in self.race_card[direction]:
            distance = item['distance']
            time_route = time_route + distance * 1000.0 / 333.33 + 1.0
        step = time_route / count_bus
        for j in xrange(count_bus):
            time_stop = time_beg
            while time_stop < time_end:
                for i, item in enumerate(self.race_card[direction]):
                    self.rasp_time[(srv_id, rv_id)][direction][i]['time'].append(int(time_stop))
                    distance = item['distance']
                    if i != (len(self.race_card[direction]) - 1):
                        time_stop = time_stop + distance * 1000.0 / 333.33 + 1.0
            time_beg = time_beg + step
        for item in self.rasp_time[(srv_id, rv_id)][direction]:
            item['time'] = sorted(item['time'])

try:
    current_time = time.time()
    group_code = group_codes[args.url[1:]]
    redis_client = redis.StrictRedis( host = args.host_redis, port = args.port_redis, db = args.db_redis )
    es_client = Elasticsearch([{'host': args.host_es, 'port': args.port_es}])
    socket.setdefaulttimeout(args.timeout)
    synchro_routes = SynchroRoutes()
    name_file = 'elasticsearch/%s_%s.json' % (args.url[1:], time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(current_time)))
    f = codecs.open(name_file, "w", encoding="utf-8")
    change_checksum = synchro_routes.synchro(f)
    f.close()
    if args.only:
        (dir_insert, dir_update, dir_delete) = synchro_routes.summary()
        (st_insert, st_update, st_delete) = synchro_routes.stations.summary()
        logger.info(u'Создан файл %s, в нем: %d направлений %d остановок', name_file, dir_insert, st_insert)    
        sys.exit(0)
    statinfo = os.stat(name_file)
    if statinfo.st_size > 0:
        logger.info(u'Загрузка обновлений в ElasticSearch')
        ret = os.system('curl -S -XPOST "http://%s:%d/_bulk" --data-binary @%s > /dev/null 2>&1' % (args.host_es, args.port_es, name_file))
        if ret == 0:
            logger.info(u'Обновление контрольных сумм, после успешного обновления ElasticSearch')
            synchro_routes.set_checksum()
            synchro_routes.save_report()
            current = datetime.datetime.now(tzlocal.get_localzone()).replace(microsecond=0).isoformat()
            redis_client.set("tn.last_update", current)
            redis_client.publish("tn.last_update", current)
            (dir_insert, dir_update, dir_delete) = synchro_routes.summary()
            (st_insert, st_update, st_delete) = synchro_routes.stations.summary()
            logger.info(u'Добавлено: %d направлений %d остановок, Изменено: %d направлений %d остановок, Удалено: %d направлений %d остановок за %.1f сек.' % (dir_insert, st_insert, dir_update, st_update, dir_delete, st_delete, time.time() - current_time), extra={'marker': 'nagios'})
    else:
        os.remove(name_file)
        if change_checksum:
            synchro_routes.set_checksum()
        logger.subinfo(u'Изменений не обнаружено, выполнено за %.1f сек.' % (time.time() - current_time), extra={'marker': 'nagios'})
    delete_old_files('elasticsearch/%s_*.json' % args.url[1:], 7)
except Exception, e:
    logger.exception(e, extra={'marker': 'nagios'})