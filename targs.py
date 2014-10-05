#!/usr/bin/env python2
# -*- coding: utf-8 -*-
'''Проверка работы argparse'''

import argparse
import util

util.conf_io()

argparser = argparse.ArgumentParser(description='Test work argparse.')
argparser.add_argument("index1", metavar='index1', nargs=1, help='Names index source ElasticSearch')
argparser.add_argument("index2", metavar='index2', nargs='?', help='Names index destination ElasticSearch')
args = argparser.parse_args()

print u'Значение index1', args.index1[0]

if args.index2:
    print u'Указан аргумент index2', args.index2
else:
    print u'Не указан аргумент index2'