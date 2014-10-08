#!/usr/bin/env python2
# -*- coding: utf-8 -*-
'''Различные общие классы и функции'''

import collections
import json
import base64
import httplib
import cStringIO
import gzip
import socket
import time
import threading
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import datetime
import glob
import os
import parsers
import sys
import codecs
import locale
import re
import tempfile
import ConfigParser

def tree():
    return collections.defaultdict(tree)

file_lock = threading.Lock()

def save_json_to_file(file_descriptor, meta, body=None):
    file_lock.acquire()
    try:
        file_descriptor.write(u'%s\n' % json.dumps(meta, ensure_ascii=False))
        if body:
            file_descriptor.write(u'%s\n' % json.dumps(body, ensure_ascii=False))    
    finally:
        file_lock.release()
    
def http_request(request, handler, conf, logger=None):
    '''Выполняет HTTP запрос'''
    n = 1
    while True:
        try:
            try:
                webservice = httplib.HTTP(conf.get('host'))
                webservice.putrequest('GET', '/' + conf.section + request)
                webservice.putheader('Host', conf.get('host'))
                if conf.has_option('user') and conf.has_option('passwd'):
                    auth = base64.encodestring('%s:%s' % (conf.get('user'), conf.get('passwd'))).replace('\n', '')
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
                message = u'[http://%s/%s try:%d] %s' % (conf.get('host'), conf.section + request, n, str(e))
                if type(e) == RuntimeError:
                    raise RuntimeError(message)
                else:
                    raise socket.timeout(message)
        except Exception, e:
            if n == conf.getint('num-try'):
                raise
            if logger:
                logger.exception(e)
        time.sleep(n * 5)
        n += 1

def get_local_ids(es_client, index, doc_type, group_code):
    ids = set()
    prefix = '%d:' % group_code
    query = {'query': {'prefix': { '_id': prefix }}}
    try:
        for hit in scan(es_client, query, '10m', index=index, doc_type=doc_type,  fields=''):
            ids.add(hit['_id'])
    except:
        pass
    return ids

def delete_old_files(mask, days_delta, current_time, logger=None):
    '''Удаление json файлов, которые старее days_delta дней'''
    current_time_date = datetime.date.fromtimestamp(current_time)
    delta = datetime.timedelta(days=days_delta)
    for name_file in glob.glob(mask):
        date_file = time.mktime(time.strptime(os.path.splitext(os.path.basename(name_file))[0].split('_')[1], "%Y-%m-%dT%H:%M:%S"))
        if current_time_date - datetime.date.fromtimestamp(date_file) > delta:
            os.remove(name_file)
            if logger:
                logger.info(u'Удален старый файл %s' % os.path.basename(name_file))

def check_table(name_table, conf, redis_client=None, logger=None):
    if conf.get('format') == 'xml':
        handler = parsers.CheckSumXMLParser()
        request = '/getChecksum.php?cs_tablename=%s' % name_table
    else:
        handler = parsers.CheckSumCSVParser()
        request =  '/getChecksum.php?cs_tablename=%s&fmt=csv' % name_table
    http_request(request, handler, conf, logger=logger)
    if name_table in handler.checksum:
        checksum_new = handler.checksum[name_table]
        if redis_client:
            key = 'checksum:tn:%s:%s' % (conf.section, name_table)
            checksum_old = redis_client.get(key)
            if checksum_old:
                if checksum_new != int(checksum_old):
                    if logger:
                        logger.info(u'Контрольная сумма таблицы %s не совпадает %d <> %s, требуется полная перегрузка данных' % (name_table, checksum_new, checksum_old))
                    return (checksum_new, True)
                else:
                    return (checksum_new, False)
            else:
                if logger:
                    logger.info(u'Контрольная сумма таблицы %s, отсутствует в Redis, требуется полная перегрузка данных' % name_table)
                return (checksum_new, True)
        else:
            if logger:
                logger.info(u'Контрольная сумма таблицы %s, не проверялась в Redis' % name_table)
            return (checksum_new, True)
    else:
        if logger:
            logger.info(u'В таблице tbchecksum отсуствует контрольная сумма таблицы %s' % name_table)
        return (None, True)

def delete_old_doc(es_client, index, doc_type, group_code, file_descriptor):
    ids = get_local_ids(es_client, index, doc_type, group_code)
    for _id in ids:
        meta = {'delete': {'_index': index, '_type': doc_type, '_id': _id}}
        save_json_to_file(file_descriptor, meta)
        
def conf_io():
    if sys.stdout.encoding is None:
        sys.stdout = codecs.getwriter(locale.getpreferredencoding())(sys.stdout)
    if sys.stderr.encoding is None:
        sys.stderr = codecs.getwriter(locale.getpreferredencoding())(sys.stderr)
    locale.setlocale(locale.LC_ALL, '')

class TranslateName(object):
    '''Класс предназначен для трансляции имен индексов при сравнении и копировании'''
    def __init__(self, source, destination):
        source_split = source.split(',')
        destination_split = destination.split(',')
        if self.template(source_split) or self.template(destination_split):
            self.not_translate = True
        elif source_split == destination_split:
            self.not_translate = True
        elif len(source_split) == len(destination_split):
            self.convert = {}
            for i1, i2 in zip(source_split, destination_split):
                self.convert[i1] = i2
            self.not_translate = False
        else:
            self.not_translate = True
    
    def trans(self, source_name):
        if self.not_translate:
            return source_name
        else:
            try:
                return self.convert[source_name]
            except:
                return source_name
    
    def template(self, name_index):
        for s in name_index:
            if s[0] == '_':
                return True
            if s.find('*') != -1:
                return True
            if s.find('?') != -1:
                return True
        return False

def parse_args(args, logger=None):
    '''Функция передназначена для парсинга аргументов при сравнении и копировании'''
    regexp = re.compile(u'^(http://)?([\w\.-]+)(:(\d+))?/([\w\*\.\?,-]+)(/([\w\*\.\?,-]+))?/?$', re.IGNORECASE | re.UNICODE)
    res = regexp.match(args.source[0])
    if res:
        args.source_host = res.group(2)
        source_index = res.group(5)
        source_doc = res.group(7)
        if res.group(4):
            args.source_port = int(res.group(4))
        else:
            args.source_port = 9200
    else:
        if logger:
            logger.error(u'Неверный формат источника ElasticSearch')
        else:
            print >> sys.stderr, u'Неверный формат источника ElasticSearch'
        sys.exit(1)
    res = regexp.match(args.destination[0])
    if res:
        args.destination_host = res.group(2)
        destination_index = res.group(5)
        destination_doc = res.group(7)
        if res.group(4):
            args.destination_port = int(res.group(4))
        else:
            args.destination_port = 9200
    else:
        if logger:
            logger.error(u'Неверный формат получателя ElasticSearch')
        else:
            print >> sys.stderr, u'Неверный формат получателя ElasticSearch'
        sys.exit(1)
    args.es_source = Elasticsearch([{'host': args.source_host, 'port': args.source_port}])
    args.es_dest = Elasticsearch([{'host': args.destination_host, 'port': args.destination_port}])
    if args.query_sour:
        try:
            query_source = json.loads(args.query_sour, encoding='utf-8')
        except:
            if logger:
                logger.error(u'Неверный формат запроса источника')
            else:
                print >> sys.stderr, u'Неверный формат запроса источника'
            sys.exit(1)
    else:
        if args.group_code:
            query_source = {'query': {'prefix': { '_id': '%d:' % args.group_code }}}
        else:
            query_source = {'query': {'match_all': {}}}
    if args.query_dest:
        try:
            query_destination = json.loads(args.query_dest, encoding='utf-8')
        except:
            if logger:
                logger.error(u'Неверный формат запроса получателя')
            else:
                print >> sys.stderr, u'Неверный формат запроса получателя'
            sys.exit(1)
    else:
        if args.query_sour:
            query_destination = query_source
        else:
            if args.group_code:
                query_destination = {'query': {'prefix': { '_id': '%d:' % args.group_code }}}
            else:
                query_destination = {'query': {'match_all': {}}}
    if destination_doc:
        args.documents_destination = scan(args.es_dest, query=query_destination, index=destination_index, doc_type=destination_doc, fields='')
    else:
        args.documents_destination = scan(args.es_dest, query=query_destination, index=destination_index, fields='')
    if source_doc:
        args.documents_source = scan(args.es_source, query=query_source, index=source_index, doc_type=source_doc)
    else:
        args.documents_source = scan(args.es_source, query=query_source, index=source_index)
    args.translate = TranslateName(source_index, destination_index)

class TwoTmpFiles(object):
    def __init__(self):
        self.file1 = codecs.open(tempfile.mktemp(), 'w', encoding='utf-8')
        self.file2 = codecs.open(tempfile.mktemp(), 'w', encoding='utf-8')
    
    def __enter__(self):
        return (self.file1, self.file2)
    
    def __exit__(self, exception_type, exception_val, trace):
        if not self.file1.closed:
            self.file1.close()
        if not self.file2.closed:
            self.file2.close()
        os.unlink(self.file1.name)
        os.unlink(self.file2.name)

class Configuration(object):
    '''Класс конфигурации'''
    def __init__(self, name_file):
        fp = codecs.open(name_file, 'r', encoding='utf-8')
        self.conf = ConfigParser.ConfigParser()
        self.conf.readfp(fp, name_file)
        fp.close()
        self.name_file = name_file
    
    def set_section(self, section):
        if not self.conf.has_section(section):
            raise RuntimeError(u'Section %s not found in %s' % (section, self.name_file))
        self.section = section
    
    def get(self, option):
        return self.conf.get(self.section, option)
    
    def getboolean(self, option):
        return self.conf.getboolean(self.section, option)
    
    def getfloat(self, option):
        return self.conf.getfloat(self.section, option)
    
    def getint(self, option):
        return self.conf.getint(self.section, option)
    
    def sections(self):
        return self.conf.sections()
    
    def has_option(self, option):
        return self.conf.has_option(self.section, option)
