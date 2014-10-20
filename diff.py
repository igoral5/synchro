#!/usr/bin/env python2
# -*- coding: utf-8 -*-
'''Проверяет документы в двух различных ElasticSearch документы на соответcтвие'''

import argparse
import os
import sys
import json
import logging
import util

util.conf_io()

argparser = argparse.ArgumentParser(description='Shows the difference between the indices ElasticSearch.')
argparser.add_argument("source", metavar='source', nargs=1, help='URL source ElasticSearch in format [http://]name_host.com[:9200]/name_index[/doc_type]')
argparser.add_argument("destination", metavar='destination', nargs=1, help='URL destination ElasticSearch in format [http://]name_host.com[:9200]/name_index[/doc_type]')
argparser.add_argument("--query-sour", dest='query_sour', help="Query for source ElasticSearch")
argparser.add_argument("--query-dest", dest='query_dest', help="Query for destination ElasticSearch")
argparser.add_argument("-g", "--group-code", dest='group_code', help='Group code for query, not working with keys --query-sour and --query-dest', type=int)
argparser.add_argument("-m", "--meld", dest='meld', help="Use meld for show difference", action='store_true')
argparser.add_argument("-f", "--first", dest='first', help="Show only the first N rows of differences, default 10", type=int, default=10)
args = argparser.parse_args()

logger = logging.getLogger('elasticsearch')
logger.addHandler(logging.NullHandler())

util.parse_args(args)

def main():
    dest_ids = set()
    try:
        for hit in args.documents_destination:
            index2 = hit['_index']
            id2=hit['_id']
            type2=hit['_type']
            dest_ids.add((index2, type2, id2))
    except:
        pass
    
    change = False
    
    difference = 0
    try:
        for hit in args.documents_source:
            index1 = hit['_index']
            index2 = args.translate.trans(index1)
            id1=hit['_id']
            type1=hit['_type']
            doc1=hit['_source']
            if (index2, type1, id1) in dest_ids:
                hit2=args.es_dest.get(index2, doc_type=type1, id=id1)
                doc2=hit2['_source']
                if doc1 != doc2:
                    print u'Различия index=%s, doc_type=%s, id=%s' % (index1, type1, id1)
                    if args.meld:
                        if difference < args.first:
                            with util.TwoTmpFiles() as (tmp1, tmp2):
                                json.dump(doc1, tmp1, ensure_ascii=False, sort_keys=True, indent=4, separators=(',', ': '))
                                tmp1.close()
                                json.dump(doc2, tmp2, ensure_ascii=False, sort_keys=True, indent=4, separators=(',', ': '))
                                tmp2.close()
                                os.system('meld %s -L "[%s:%d] %s" %s -L "[%s:%d] %s"' % (tmp1.name, args.source_host, args.source_port, id1, tmp2.name, args.destination_host, args.destination_port, id1))
                        difference += 1
                    change = True
                dest_ids.discard((index2, type1, id1))
            else:
                print u'В приемнике не найден документ index=%s, doc_type=%s, id=%s' % (index2, type1, id1)
                change = True
    except Exception as e:
        print >> sys.stderr, u'Возникла ошибка при чтении источника', e
        sys.exit(1)
    
    for (index2, type2, id2) in dest_ids:
        print u'В источнике не найден документ index=%s, doc_type=%s, id=%s' % (index2, type2, id2)
        change = True
    
    if not change:
        print u'Различий не найдено'

if __name__ == '__main__':
    main()