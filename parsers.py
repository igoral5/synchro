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
        self.end()
    
    def next(self, tags, attrs):
        pass
    
    def end(self):
        pass

class CSVParser(object):
    '''Общий класс CSV парсера'''
    def __init__(self):
        pass
    
    def ParseFile(self, f):
        csvparser = unicodecsv.UnicodeReader(f)
        for i, row in enumerate(csvparser):
            if i != 0: 
                self.next(row)
        self.end()
    
    def next(self, row):
        pass
    
    def end(self):
        pass

class MarshesParser(object):
    '''Парсер запроса getMarsches'''
    def __init__(self, av_tt, cor_tt=False, logger=None):
        self.marshes = {}
        self.av_tt = av_tt
        self.cor_tt = cor_tt
        self.logger = logger
    
    def set(self, mr_id_r, tt_id_r, mr_num_r, mr_title_r):
        try:
            mr_id = int(mr_id_r)
            tt_id = int(tt_id_r)
            if self.cor_tt and (tt_id == 2 or tt_id == 3):
                tt_id = 1
            if tt_id not in self.av_tt:
                return
            self.marshes[mr_id] = { 'transport': const.transport_type[tt_id], 'name': mr_num_r, 'description': mr_title_r }
        except:
            if self.logger:
                self.logger.debug(u'Неизвестный тип в таблице tbmarshes mr_id=%s' % mr_id_r)

class MarshesXMLParser(MarshesParser, XMLParser):
    '''XML Парсер запроса getMarsches'''
    def __init__(self, av_tt, cor_tt=False, logger=None):
        MarshesParser.__init__(self, av_tt, cor_tt=cor_tt, logger=logger)
        XMLParser.__init__(self)

    def next(self, tag, attrs):
        if tag == 'row':
            self.set(attrs['mr_id'], attrs['tt_id'], attrs['mr_num'], attrs['mr_title'])

class MarshesCSVParser(MarshesParser, CSVParser):
    '''CSV Парсер запроса getMarshes'''
    def __init__(self, av_tt, cor_tt=False, logger=None):
        MarshesParser.__init__(self, av_tt, cor_tt=cor_tt, logger=logger)
        CSVParser.__init__(self)
    
    def next(self, row):
        self.set(row[0], row[1], row[3], row[4])

class MarshVariantsParser(object):
    '''Парсер запроса getMarshVariants'''
    def __init__(self, current_time, redis_client=None, url=None):
        self.tmp_marsh_variants = util.tree()
        self.current_time = current_time
        self.redis_client = redis_client
        self.url = url
    
    def set(self, mr_id_r, mv_id_r, mv_enddateexists_r, mv_enddate_r, mv_startdate_r, mv_checksum_r):
        mr_id = int(mr_id_r)
        mv_id = int(mv_id_r)
        mv_enddateexists = int(mv_enddateexists_r)
        if mv_enddateexists:
            mv_enddate = time.mktime(time.strptime(mv_enddate_r, "%Y-%m-%d %H:%M:%S"))
            if mv_enddate < self.current_time:
                return
        mv_startdate = time.mktime(time.strptime(mv_startdate_r, "%Y-%m-%d %H:%M:%S"))
        if mv_startdate > self.current_time:
            return
        mv_checksum = int(mv_checksum_r)
        change = True
        if self.redis_client and self.url:
            redis_key = 'checksum:tn:%s:marshvariants:%d:%d' % (self.url, mr_id, mv_id)
            redis_checksum = self.redis_client.get(redis_key)
            if redis_checksum:
                if mv_checksum == int(redis_checksum):
                    change = False
        self.tmp_marsh_variants[mr_id][mv_startdate][mv_id] = { 'checksum': mv_checksum, 'change': change }
    
    def end(self):
        self.marsh_variants = util.tree()
        for mr_id in self.tmp_marsh_variants:
            mv_startdate = sorted(self.tmp_marsh_variants[mr_id].keys())[-1]
            mv_id = sorted(self.tmp_marsh_variants[mr_id][mv_startdate].keys())[-1]
            self.marsh_variants[mr_id][mv_id] = self.tmp_marsh_variants[mr_id][mv_startdate][mv_id]

class MarshVariantsXMLParser(MarshVariantsParser, XMLParser):
    '''XML парсер запроса getMarshVariants'''
    def __init__(self, current_time, redis_client=None, url=None):
        MarshVariantsParser.__init__(self, current_time, redis_client=redis_client, url=url)
        XMLParser.__init__(self)
    
    def next(self, tag, attrs):
        if tag == 'row':
            self.set(attrs['mr_id'], attrs['mv_id'], attrs['mv_enddateexists'], attrs['mv_enddate'], attrs['mv_startdate'], attrs['mv_checksum'])

class MarshVariantsCSVParser(MarshVariantsParser, CSVParser):
    '''CSV парсер запроса getMarshVariants'''
    def __init__(self, current_time, redis_client=None, url=None):
        MarshVariantsParser.__init__(self, current_time, redis_client=redis_client, url=url)
        CSVParser.__init__(self)
    
    def next(self, row):
        self.set(row[1], row[0], row[5], row[4], row[3], row[6])

class RaspVariantsParser(object):
    '''Парсер запроса getRaspVariants'''
    def __init__(self, current_time, redis_client=None, url=None):
        self.tmp_rasp_variants = util.tree()
        self.current_time = current_time
        self.redis_client = redis_client
        self.url = url
    
    def set(self, rv_enable_r, rv_enddateexists_r, rv_enddate_r, mr_id_r, srv_id_r, rv_id_r, rv_dow_r, rv_startdate_r, rv_checksum_r):
        rv_enable = int(rv_enable_r)
        if not rv_enable:
            return
        rv_enddateexists = int(rv_enddateexists_r)
        if rv_enddateexists:
            rv_enddate = time.mktime(time.strptime(rv_enddate_r, "%Y-%m-%d %H:%M:%S"))
            if rv_enddate < self.current_time:
                return
        mr_id = int(mr_id_r)
        srv_id = int(srv_id_r)
        rv_id = int(rv_id_r)
        try:
            rv_dow = int(rv_dow_r) & 127
        except:
            rv_dow = 127
        rv_startdate = time.mktime(time.strptime(rv_startdate_r, "%Y-%m-%d %H:%M:%S"))
        if rv_startdate > self.current_time:
            return
        rv_checksum = int(rv_checksum_r)
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
                self.tmp_rasp_variants[mr_id][day_week][rv_startdate][(srv_id, rv_id)] = { 'mask': rv_dow, 'checksum': rv_checksum, 'change': change }
            mask = mask << 1
    
    def end(self):
        self.rasp_variants = util.tree()
        for mr_id in self.tmp_rasp_variants:
            for day_week in self.tmp_rasp_variants[mr_id]:
                rv_startdate = sorted(self.tmp_rasp_variants[mr_id][day_week].keys())[-1]
                (srv_id, rv_id) = sorted(self.tmp_rasp_variants[mr_id][day_week][rv_startdate].keys())[-1]
                self.rasp_variants[mr_id][(srv_id, rv_id)] = self.tmp_rasp_variants[mr_id][day_week][rv_startdate][(srv_id, rv_id)]

class RaspVariantsXMLParser(RaspVariantsParser, XMLParser):
    '''XML парсер запроса getRaspVariants'''
    def __init__(self, current_time, redis_client=None, url=None):
        RaspVariantsParser.__init__(self, current_time, redis_client=redis_client, url=url)
        XMLParser.__init__(self)
    
    def next(self, tag, attrs):
        if tag == 'row':
            self.set(attrs['rv_enable'], attrs['rv_enddateexists'], attrs['rv_enddate'], attrs['mr_id'], attrs['srv_id'], attrs['rv_id'], attrs['rv_dow'], attrs['rv_startdate'], attrs['rv_checksum'])

class RaspVariantsCSVParser(RaspVariantsParser, CSVParser):
    '''CSV парсер запроса getRaspVariants'''
    def __init__(self, current_time, redis_client=None, url=None):
        RaspVariantsParser.__init__(self, current_time, redis_client=redis_client, url=url)
        CSVParser.__init__(self)
    
    def next(self, row):
        self.set(row[10], row[7], row[6], row[2], row[0], row[1], row[3], row[5], row[9])

class RaceCardParser(object):
    '''Парсер запроса getRaceCard, последовательности остановок направления'''
    def __init__(self):
        self.tmp_race_card = util.tree()
    
    def set(self, rl_racetype_r, rc_orderby_r, st_id_r, rc_distance_r):
        direction = ord(rl_racetype_r[0]) - ord('A')
        rc_orderby = int(rc_orderby_r)
        st_id = int(st_id_r)
        rc_distance = float(rc_distance_r)
        self.tmp_race_card[direction][rc_orderby] = { 'st_id': st_id, 'distance': rc_distance }
    
    def end(self):
        self.race_card = util.tree()
        for direction in self.tmp_race_card:
            self.race_card[direction] = [self.tmp_race_card[direction][k] for k in sorted(self.tmp_race_card[direction].keys())]

class RaceCardXMLParser(RaceCardParser, XMLParser):
    '''XML парсер запроса getRaceCard, последовательности остановок направления'''
    def __init__(self):
        RaceCardParser.__init__(self)
        XMLParser.__init__(self)
    
    def next(self, tag, attrs):
        if tag == 'row':
            self.set(attrs['rl_racetype'], attrs['rc_orderby'], attrs['st_id'], attrs['rc_distance'])

class RaceCardCSVParser(RaceCardParser, CSVParser):
    '''CSV парсер запроса getRaceCard, последовательности остановок направления'''
    def __init__(self):
        RaceCardParser.__init__(self)
        CSVParser.__init__(self)
    
    def next(self, row):
        self.set(row[1], row[2], row[3], row[5])

class RaceCoordParser(object):
    '''Парсер запроса getRaceCoord, геометрии направления'''
    def __init__(self):
        self.tmp_race_coord = util.tree()
    
    def set(self, rl_racetype_r, rd_orderby_r, rd_lat_r, rd_long_r):
        direction = ord(rl_racetype_r[0]) - ord('A')
        rd_orderby = int(rd_orderby_r)
        lat = float(rd_lat_r)
        lng = float(rd_long_r)
        self.tmp_race_coord[direction][rd_orderby] = {'coord': {'lat': lat, 'long': lng }}
    
    def end(self):
        self.race_coord = util.tree()
        for direction in self.tmp_race_coord:
            self.race_coord[direction] = [self.tmp_race_coord[direction][k] for k in sorted(self.tmp_race_coord[direction].keys())]

class RaceCoordXMLParser(RaceCoordParser, XMLParser):
    '''XML парсер запроса getRaceCoord, геометрии направления'''
    def __init__(self):
        RaceCoordParser.__init__(self)
        XMLParser.__init__(self)
    
    def next(self, tag, attrs):
        if tag == 'row':
            self.set(attrs['rl_racetype'], attrs['rd_orderby'], attrs['rd_lat'], attrs['rd_long'])

class RaceCoordCSVParser(RaceCoordParser, CSVParser):
    '''CSV парсер запроса getRaceCoord, геометрии направления'''
    def __init__(self):
        RaceCoordParser.__init__(self)
        CSVParser.__init__(self)
    
    def next(self, row):
        self.set(row[1], row[2], row[3], row[4])

class RaspTimeParser(object):
    '''Парсер запроса getRaspTime'''
    def __init__(self, srv_id, rv_id, race_card, logger=None):
        self.tmp_rasp_time = util.tree()
        self.srv_id = srv_id
        self.rv_id = rv_id
        self.race_card = race_card
        self.logger = logger
    
    def set(self, rl_racetype_r, st_id_r, rt_time_r, rt_orderby_r, gr_id_r, rt_racenum_r):
        direction = ord(rl_racetype_r[0]) - ord('A')
        st_id = int(st_id_r)
        try:
            rt_time = int(rt_time_r)
        except ValueError:
            if self.logger:
                self.logger.info(u'В таблице tbrasptime неверные данные rt_time="%s" при srv_id="%d" rv_id="%d"' % (rt_time_r, self.srv_id, self.rv_id))
            return
        rt_orderby = int(rt_orderby_r)
        gr_id = int(gr_id_r)
        rt_racenum = int(rt_racenum_r)
        self.tmp_rasp_time[direction][gr_id][rt_racenum][rt_orderby] = {'st_id': st_id, 'time': rt_time }
    
    def end(self):
        self.rasp_time = util.tree()
        for direction in self.race_card:
            self.rasp_time[(self.srv_id, self.rv_id)][direction] = []
            for item in self.race_card[direction]:
                self.rasp_time[(self.srv_id, self.rv_id)][direction].append({'st_id': item['st_id'], 'time': []})
        for direction in self.tmp_rasp_time:
            for gr_id in sorted(self.tmp_rasp_time[direction].keys()):
                for rt_racenum in sorted(self.tmp_rasp_time[direction][gr_id].keys()):
                    for index_st, rt_orderby in enumerate(sorted(self.tmp_rasp_time[direction][gr_id][rt_racenum].keys())):
                        rt_time = self.tmp_rasp_time[direction][gr_id][rt_racenum][rt_orderby]['time']
                        st_id = self.tmp_rasp_time[direction][gr_id][rt_racenum][rt_orderby]['st_id']
                        self.find_best_place(direction, index_st, st_id, rt_time)
        for direction in self.rasp_time[(self.srv_id, self.rv_id)]:
            for item in self.rasp_time[(self.srv_id, self.rv_id)][direction]:
                item['time'] = sorted(item['time'])
    
    def find_best_place(self, direction, index_st, st_id, rt_time):
        refer = self.rasp_time[(self.srv_id, self.rv_id)][direction]
        if index_st >= 0 and index_st < len(refer) and refer[index_st]['st_id'] == st_id:
            refer[index_st]['time'].append(rt_time)
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

class RaspTimeXMLParser(RaspTimeParser, XMLParser):
    '''XML парсер запроса getRaspTime'''
    def __init__(self, srv_id, rv_id, race_card, logger=None):
        RaspTimeParser.__init__(self, srv_id, rv_id, race_card, logger=logger)
        XMLParser.__init__(self)
    
    def next(self, tag, attrs):
        if tag == 'row':
            self.set(attrs['rl_racetype'], attrs['st_id'], attrs['rt_time'], attrs['rt_orderby'], attrs['gr_id'], attrs['rt_racenum'])

class RaspTimeCSVParser(RaspTimeParser, CSVParser):
    '''CSV парсер запроса getRaspTime'''
    def __init__(self, srv_id, rv_id, race_card, logger=None):
        RaspTimeParser.__init__(self, srv_id, rv_id, race_card, logger=logger)
        CSVParser.__init__(self)
 
    def next(self, row):
        self.set(row[6], row[7], row[8], row[3], row[2], row[10])

class CheckSumParser(object):
    '''Парсер запроса getChecksum'''
    def __init__(self):
        self.checksum = {}
    
    def set(self, cs_checksum_r, cs_tablename_r):
        checksum = int(cs_checksum_r)
        self.checksum[cs_tablename_r] = checksum

class CheckSumXMLParser(CheckSumParser, XMLParser):
    '''XML парсер запроса getChecksum'''
    def __init__(self):
        CheckSumParser.__init__(self)
        XMLParser.__init__(self)
    
    def next(self, tag, attrs):
        if tag == 'row':
            self.set(attrs['cs_checksum'], attrs['cs_tablename'])

class CheckSumCSVParser(CheckSumParser, CSVParser):
    '''CSV парсер запроса getChecksum'''
    def __init__(self):
        CheckSumParser.__init__(self)
        CSVParser.__init__(self)
    
    def next(self, row):
        self.set(row[1], row[0])

class StopsParser(object):
    '''Парсер запроса getStops'''
    def __init__(self):
        self.stations = {}
    
    def set(self, st_id_r, st_lat_r, st_long_r, st_title_r):
        st_id = int(st_id_r)
        lat = float(st_lat_r)
        lng = float(st_long_r)
        self.stations[st_id] = {'name': st_title_r, 'location': {'lat': lat, 'long': lng}, 'tags': set() }

class StopsXMLParser(StopsParser, XMLParser):
    '''XML парсер запроса getStops'''
    def __init__(self):
        StopsParser.__init__(self)
        XMLParser.__init__(self)
    
    def next(self, tag, attrs):
        if tag == 'row':
            self.set(attrs['st_id'], attrs['st_lat'], attrs['st_long'], attrs['st_title'])

class StopsCSVParser(StopsParser, CSVParser):
    '''CSV парсер запроса getStops'''
    def __init__(self):
        StopsParser.__init__(self)
        CSVParser.__init__(self)
    
    def next(self, row):
        self.set(row[0], row[5], row[6], row[3])

