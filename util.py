#!/usr/bin/env python2
# -*- coding: utf-8 -*-
'''Различные общие функции'''

import collections
import json
import base64
import httplib
import cStringIO
import gzip
import socket
import time
import threading
from elasticsearch.helpers import scan
import datetime
import glob
import os
import parsers

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
    
def http_request(request, handler, args, logger=None):
    '''Выполняет HTTP запрос'''
    n = 1
    while True:
        try:
            try:
                webservice = httplib.HTTP(args.host)
                webservice.putrequest('GET', args.url + request)
                webservice.putheader('Host', args.host)
                if args.user and args.passwd:
                    auth = base64.encodestring('%s:%s' % (args.user, args.passwd)).replace('\n', '')
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
                message = u'[http://%s%s try:%d] %s' % (args.host, args.url + request, n, str(e))
                if type(e) == RuntimeError:
                    raise RuntimeError(message)
                else:
                    raise socket.timeout(message)
        except Exception, e:
            if n == args.num_try:
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

def check_table(name_table, args, redis_client=None, logger=None):
    if args.format == 'xml':
        handler = parsers.CheckSumXMLParser()
        request = '/getChecksum.php?cs_tablename=%s' % name_table
    else:
        handler = parsers.CheckSumCSVParser()
        request =  '/getChecksum.php?cs_tablename=%s&fmt=csv' % name_table
    http_request(request, handler, args, logger=logger)
    if name_table in handler.checksum:
        checksum_new = handler.checksum[name_table]
        if redis_client:
            key = 'checksum:tn:%s:%s' % (args.url[1:], name_table)
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
