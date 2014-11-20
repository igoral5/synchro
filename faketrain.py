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

def main():
    with codecs.open(args.source_file[0], "r", encoding='utf-8') as source_file, codecs.open(args.destination_file[0], 'w', encoding='utf-8') as destination_file:
        cur_date = datetime.date(2012, 1, 1)
        delta = datetime.timedelta(days=1)
        schedule = []
        while cur_date.year == 2012:
            schedule.append(u"%02d.%02d" % (cur_date.day, cur_date.month))
            cur_date = cur_date + delta
        for i, line in enumerate(source_file):
            if i % 2 == 0:
                meta = json.loads(line)
                destination_file.write(line)
            else:
                if meta[u'index'][u'_type'] == u'route' and meta[u'index'][u'_id'].split(u':')[0] == u'8001':
                    suburban = json.loads(line)
                    suburban['schedule'] = schedule
                    destination_file.write(u"%s\n" % json.dumps(suburban, ensure_ascii=False))
                else:
                    destination_file.write(line)

if __name__ == '__main__':
    main()





