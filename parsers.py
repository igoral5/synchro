#!/usr/bin/env python2
# -*- coding: utf-8 -*-
'''Парсеры XML и CSV запросов'''

import xml.parsers.expat
import unicodecsv
import const
import util
import time

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
        argparser = unicodecsv.UnicodeReader(f)
        for i, row in enumerate(argparser):
            if i != 0: 
                self.next(row)
    
    def next(self, row):
        pass

class MarshesXMLParser(XMLParser):
    '''XML Парсер запроса getMarsches'''
    def __init__(self, av_tt, cor_tt=False, logger=None):
        super(MarshesXMLParser, self).__init__()
        self.marshes = {}
        self.av_tt = av_tt
        self.cor_tt = cor_tt
        self.logger = logger

    def next(self, tag, attrs):
        if tag == 'row':
            try:
                mr_id = int(attrs['mr_id'])
                tt_id = int(attrs['tt_id'])
                if self.cor_tt and (tt_id == 2 or tt_id == 3):
                    tt_id = 1
                if tt_id not in self.av_tt:
                    return
                self.marshes[mr_id] = { 'transport': const.transport_type[tt_id], 'name': attrs['mr_num'], 'description': attrs['mr_title'] }
            except:
                if self.logger:
                    self.logger.debug(u'Неизвестный тип в таблице tbmarshes mr_id=%s' % attrs['mr_id'])

class MarshesCSVParser(CSVParser):
    '''CSV Парсер запроса getMarshes'''
    def __init__(self, av_tt, cor_tt=False, logger=None):
        super(MarshesCSVParser, self).__init__()
        self.marshes = {}
        self.av_tt = av_tt
        self.cor_tt = cor_tt
        self.logger = logger
    
    def next(self, row):
        try:
            mr_id = int(row[0])
            tt_id = int(row[1])
            if self.cor_tt and (tt_id == 2 or tt_id == 3):
                tt_id = 1
            if tt_id not in self.av_tt:
                return
            self.marshes[mr_id] = { 'transport': const.transport_type[tt_id], 'name': row[3], 'description': row[4] }
        except:
            if self.logger:
                self.logger.debug(u'Неизвестный тип в таблице tbmarshes mr_id=%s' % row[0])

class MarshVariantsXMLParser(XMLParser):
    '''XML парсер запроса getMarshVariants'''
    def __init__(self, current_time, redis_client=None, url=None):
        super(MarshVariantsXMLParser, self).__init__()
        self.marsh_variants = util.tree()
        self.current_time = current_time
        self.redis_client = redis_client
        self.url = url
    
    def next(self, tag, attrs):
        if tag == 'row':
            mr_id = int(attrs['mr_id'])
            mv_id = int(attrs['mv_id'])
            mv_enddateexists = int(attrs['mv_enddateexists'])
            if mv_enddateexists:
                mv_enddate = time.mktime(time.strptime(attrs['mv_enddate'], "%Y-%m-%d %H:%M:%S"))
                if mv_enddate < self.current_time:
                    return
            mv_startdate = time.mktime(time.strptime(attrs['mv_startdate'], "%Y-%m-%d %H:%M:%S"))
            if mv_startdate > self.current_time:
                return
            mv_checksum = int(attrs['mv_checksum'])
            change = True
            if self.redis_client and self.url:
                redis_key = 'checksum:tn:%s:marshvariants:%d:%d' % (self.url, mr_id, mv_id)
                redis_checksum = self.redis_client.get(redis_key)
                if redis_checksum:
                    if mv_checksum == int(redis_checksum):
                        change = False
            self.marsh_variants[mr_id][mv_startdate][mv_id] = { 'checksum': mv_checksum, 'change': change }

class MarshVariantsCSVParser(CSVParser):
    '''CSV парсер запроса getMarshVariants'''
    def __init__(self, current_time, redis_client=None, url=None):
        super(MarshVariantsCSVParser, self).__init__()
        self.marsh_variants = util.tree()
        self.current_time = current_time
        self.redis_client = redis_client
        self.url = url
    
    def next(self, row):
        mr_id = int(row[1])
        mv_id = int(row[0])
        mv_enddateexists = int(row[5])
        if mv_enddateexists:
            mv_enddate = time.mktime(time.strptime(row[4], "%Y-%m-%d %H:%M:%S"))
            if mv_enddate < self.current_time:
                return
        mv_startdate = time.mktime(time.strptime(row[3], "%Y-%m-%d %H:%M:%S"))
        if mv_startdate > self.current_time:
            return
        mv_checksum = int(row[6])
        change = True
        if self.redis_client and self.url:
            redis_key = 'checksum:tn:%s:marshvariants:%d:%d' % (self.url, mr_id, mv_id)
            redis_checksum = self.redis_client.get(redis_key)
            if redis_checksum:
                if mv_checksum == int(redis_checksum):
                    change = False
        self.marsh_variants[mr_id][mv_startdate][mv_id] = { 'checksum': mv_checksum, 'change': change }
                                   
class RaspVariantsXMLParser(XMLParser):
    '''XML парсер запроса getRaspVariants'''
    def __init__(self, current_time, redis_client=None, url=None):
        super(RaspVariantsXMLParser, self).__init__()
        self.rasp_variants = util.tree()
        self.current_time = current_time
        self.redis_client = redis_client
        self.url = url
    
    def next(self, tag, attrs):
        if tag == 'row':
            rv_enable = int(attrs['rv_enable'])
            if not rv_enable:
                return
            rv_enddateexists = int(attrs['rv_enddateexists'])
            if rv_enddateexists:
                rv_enddate = time.mktime(time.strptime(attrs['rv_enddate'], "%Y-%m-%d %H:%M:%S"))
                if rv_enddate < self.current_time:
                    return
            mr_id = int(attrs['mr_id'])
            srv_id = int(attrs['srv_id'])
            rv_id = int(attrs['rv_id'])
            try:
                rv_dow = int(attrs['rv_dow']) & 127
            except:
                rv_dow = 127
            rv_startdate = time.mktime(time.strptime(attrs['rv_startdate'], "%Y-%m-%d %H:%M:%S"))
            if rv_startdate > self.current_time:
                return
            rv_checksum = int(attrs['rv_checksum'])
            change = True
            if self.redis_client and self.url:
                redis_key = 'checksum:tn:%s:raspvariants:%d:%d:%d' % (self.url, mr_id, srv_id, rv_id)
                redis_checksum = self.redis_client.get(redis_key)
                if redis_checksum:
                    if rv_checksum == int(redis_checksum):
                        change = False
            mask = 1
            for day_week in xrange(7):
                if mask & rv_dow:
                    self.rasp_variants[mr_id][day_week][rv_startdate][(srv_id, rv_id)] = { 'mask': rv_dow, 'checksum': rv_checksum, 'change': change }
                mask = mask << 1

class RaspVariantsCSVParser(CSVParser):
    '''CSV парсер запроса getRaspVariants'''
    def __init__(self, current_time, redis_client=None, url=None):
        super(RaspVariantsCSVParser, self).__init__()
        self.rasp_variants = util.tree()
        self.current_time = current_time
        self.redis_client = redis_client
        self.url = url
    
    def next(self, row):
        rv_enable = int(row[10])
        if not rv_enable:
            return
        rv_enddateexists = int(row[7])
        if rv_enddateexists:
            rv_enddate = time.mktime(time.strptime(row[6], "%Y-%m-%d %H:%M:%S"))
            if rv_enddate < self.current_time:
                return
        mr_id = int(row[2])
        srv_id = int(row[0])
        rv_id = int(row[1])
        try:
            rv_dow = int(row[3]) & 127
        except:
            rv_dow = 127
        rv_startdate = time.mktime(time.strptime(row[5], "%Y-%m-%d %H:%M:%S"))
        if rv_startdate > self.current_time:
            return
        rv_checksum = int(row[9])
        change = True
        if self.redis_client and self.url:
            redis_key = 'checksum:tn:%s:raspvariants:%d:%d:%d' % (self.url, mr_id, srv_id, rv_id)
            redis_checksum = self.redis_client.get(redis_key)
            if redis_checksum:
                if rv_checksum == int(redis_checksum):
                    change = False
        mask = 1
        for day_week in xrange(7):
            if mask & rv_dow:
                self.rasp_variants[mr_id][day_week][rv_startdate][(srv_id, rv_id)] = { 'mask': rv_dow, 'checksum': rv_checksum, 'change': change }
            mask = mask << 1

class RaceCardXMLParser(XMLParser):
    '''XML парсер запроса getRaceCard, последовательности остановок направления'''
    def __init__(self):
        super(RaceCardXMLParser, self).__init__()
        self.race_card = util.tree()
    
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
        self.race_card = util.tree()
    
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
        self.race_coord = util.tree()
    
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
        self.race_coord = util.tree()
    
    def next(self, row):
        direction = ord(row[1][0]) - ord('A')
        rd_orderby = int(row[2])
        lat = float(row[3])
        lng = float(row[4])
        self.race_coord[direction][rd_orderby] = {'coord': {'lat': lat, 'long': lng }}

class RaspTimeXMLParser(XMLParser):
    '''XML парсер запроса getRaspTime'''
    def __init__(self, logger=None):
        super(RaspTimeXMLParser, self).__init__()
        self.rasp_time = util.tree()
        self.logger = logger
    
    def next(self, tag, attrs):
        if tag == 'row':
            direction = ord(attrs['rl_racetype'][0]) - ord('A')
            st_id = int(attrs['st_id'])
            try:
                rt_time = int(attrs['rt_time'])
            except ValueError:
                if self.logger:
                    self.logger.info(u'В таблице tbrasptime неверные данные rt_time="%s" при srv_id="%s" rv_id="%s"' % (attrs['rt_time'], attrs['srv_id'], attrs['rv_id']))
                return
            rt_orderby = int(attrs['rt_orderby'])
            gr_id = int(attrs['gr_id'])
            rt_racenum = int(attrs['rt_racenum'])
            self.rasp_time[direction][gr_id][rt_racenum][rt_orderby] = {'st_id': st_id, 'time': rt_time }

class RaspTimeCSVParser(CSVParser):
    '''CSV парсер запроса getRaspTime'''
    def __init__(self, logger=None):
        super(RaspTimeCSVParser, self).__init__()
        self.rasp_time = util.tree()
        self.logger = logger
    
    def next(self, row):
        direction = ord(row[6][0]) - ord('A')
        st_id = int(row[7])
        try:
            rt_time = int(row[8])
        except ValueError:
            if self.logger:
                self.logger.info(u'В таблице tbrasptime неверные данные rt_time="%s" при srv_id="%s" rv_id="%s"' % (row[8], row[0], row[1]))
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
