#!/usr/bin/env python2
# -*- coding: utf-8 -*-
'''Скрипт выводит отчет о изменениях за отчетный период, по умолчанию за прошедшие сутки'''

import sys
import redis
import argparse
import time
import datetime
import util

util.conf_io()

report = util.tree()

parser = argparse.ArgumentParser(description='Report on the changes state routes and stations.')
parser.add_argument("--host-redis", dest='host_redis', help='Host name redis, default localhost', default='localhost')
parser.add_argument("--port-redis", dest='port_redis', help='Number port redis, default 6379', type=int, default=6379)
parser.add_argument("--db-redis", dest='db_redis', help='Number database redis, default 0', type=int, default=0)
parser.add_argument("--start", dest='start', help='Beginning of reporting period in format "2013-12-31 12:55:00", default еnd of the reporting period minus one day', default='')
parser.add_argument("--finish", dest='finish', help='End of the reporting period in format "2013-12-31 12:55:00", default current time', default=time.strftime("%Y-%m-%d %H:%M:%S"))
args = parser.parse_args()

try:
    finish = time.mktime(time.strptime(args.finish, "%Y-%m-%d %H:%M:%S"))
except:
    print >> sys.stderr, 'Format parameter --finish incorrect, right format "2013-12-31 19:55:00"'
    sys.exit(1)

if args.start == '':
    args.start = (datetime.datetime.strptime(args.finish, "%Y-%m-%d %H:%M:%S") - datetime.timedelta(days = 1)).strftime("%Y-%m-%d %H:%M:%S")

try:
    start = time.mktime(time.strptime(args.start, "%Y-%m-%d %H:%M:%S"))
except:
    print >> sys.stderr, 'Format parameter --start incorrect, right format "2013-12-31 19:55:00"'
    sys.exit(1)

redis_client = redis.StrictRedis( host = args.host_redis, port = args.port_redis, db = args.db_redis )
for key in redis_client.keys('report_info:tn:change:*'):
    key_split = key.split(':')
    time_update = float(key_split[3])
    if start > time_update or finish < time_update:
        continue
    section = key_split[4]
    operation = key_split[5]
    if section == 'station':
        report[time_update][section][operation] = int(redis_client.get(key))
    else:
        report[time_update][section][operation] = [unicode(item, 'utf-8') for item in redis_client.lrange(key, 0, -1)]
print u'                      Отчет об изменениях, произошедших с %s по %s' % (args.start, args.finish)
print
i = 0 
for time_update in sorted(report.keys()):
    i = i + 1
    print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time_update))
    print
    if 'route' in report[time_update]:
        if 'insert' in report[time_update]['route']:
            print u'   Добавлено %3d новых направлений' % len(report[time_update]['route']['insert'])
            for string in report[time_update]['route']['insert']:
                print u'     %s' % string
            print
        if 'update' in report[time_update]['route']:
            print u'   Изменено  %3d направлений' % len(report[time_update]['route']['update'])
            for string in report[time_update]['route']['update']:
                print u'     %s' % string
            print
        if 'delete' in report[time_update]['route']:
            print u'   Удалено   %3d направлений' % len(report[time_update]['route']['delete'])
            for string in report[time_update]['route']['delete']:
                print u'     %s' % string
            print
    if 'geometry' in report[time_update]:
        if 'insert' in report[time_update]['geometry']:
            print u'   Добавлено %3d новых геометрий' % len(report[time_update]['geometry']['insert'])
            for string in report[time_update]['geometry']['insert']:
                print u'     %s' % string
            print
        if 'update' in report[time_update]['geometry']:
            print u'   Изменено  %3d геометрий' % len(report[time_update]['geometry']['update'])
            for string in report[time_update]['geometry']['update']:
                print u'     %s' % string
            print
        if 'delete' in report[time_update]['geometry']:
            print u'   Удалено   %3d геометрий' % len(report[time_update]['geometry']['delete'])
            for string in report[time_update]['geometry']['delete']:
                print u'     %s' % string
            print
    if 'schedule' in report[time_update]:
        if 'insert' in report[time_update]['schedule']:
            print u'   Добавлено %3d новых расписаний' % len(report[time_update]['schedule']['insert'])
            for string in report[time_update]['schedule']['insert']:
                print u'     %s' % string
            print
        if 'update' in report[time_update]['schedule']:
            print u'   Изменено  %3d расписаний' % len(report[time_update]['schedule']['update'])
            for string in report[time_update]['schedule']['update']:
                print u'     %s' % string
            print
        if 'delete' in report[time_update]['schedule']:
            print u'   Удалено   %3d расписаний' % len(report[time_update]['schedule']['delete'])
            for string in report[time_update]['schedule']['delete']:
                print u'     %s' % string
            print
    if 'station' in report[time_update]:
        if 'insert' in report[time_update]['station']:
            print u'   Добавлено %3d новых остановок' % report[time_update]['station']['insert']
        if 'update' in report[time_update]['station']:
            print u'   Изменено  %3d остановок' % report[time_update]['station']['update']
        if 'delete' in report[time_update]['station']:
            print u'   Удалено   %3d остановок' % report[time_update]['station']['delete']
        print
if i == 0:
    print u'  Изменений не обнаружено'
