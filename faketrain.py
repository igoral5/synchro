#!/usr/bin/env python2
# -*- coding: utf-8 -*-
'''Подготовливает фейковые данные по электричкам'''

import argparse
import util
import codecs
import json
import datetime

util.conf_io()

argparser = argparse.ArgumentParser(description='Prepare data for ElasticSearch with suburban')
argparser.add_argument("source_file", metavar='source_file', nargs=1, help="Source file")
argparser.add_argument("destination_file", metavar='destination_file', nargs=1, help="Destination file")
args = argparser.parse_args()

source_file = codecs.open(args.source_file[0], "r", encoding='utf-8')
destination_file = codecs.open(args.destination_file[0], 'w', encoding='utf-8')
cur_date = datetime.date(2014, 1, 1)
delta = datetime.timedelta(days=1)
schedule = []
while cur_date.year == 2014:
    schedule.append(u"%02d.%02d" % (cur_date.day, cur_date.month))
    cur_date = cur_date + delta

i = 0
for line in source_file:
    if i % 2 == 0:
        destination_file.write(u'%s' % line)
    else:
        suburban = json.loads(line)
        suburban['schedule'] = schedule
        destination_file.write(u"%s\n" % json.dumps(suburban, ensure_ascii=False))
    i += 1
source_file.close()
destination_file.close()




