#!/usr/bin/env python2
# -*- coding: utf-8 -*-
'''Проверка работы с DB2'''

import ibm_db
import sys
import argparse
import util

util.conf_io()

conf = util.Configuration('tdb2.conf')

argparser = argparse.ArgumentParser(description='Makes request to database DB2.')
argparser.add_argument("-d", "--database", dest='database', help="Name database and name section in configuration, default SAMPLE", default='SAMPLE', choices=conf.sections())
argparser.add_argument("request", metavar='request', nargs=1, help='Request to database DB2')
args = argparser.parse_args()

conf.set_section(args.database)

def convert_to_float(s):
    if s:
        return float(s.replace(',', '.'))
    return s

def convert_to_int(s):
    if s:
        return int(s)
    return s

def without_convert(s):
    if s:
        return unicode(s).strip()
    return s

def conv(ts, cs):
    res = u''
    for i, (t, c) in enumerate(zip(ts, cs)):
        value = c['fn'](t)
        if i != 0:
            res += u'|'
        if value != None:
            res += c['format'].format(value)
        else:
            res += u'NULL'.center(c['size'])
    return res

def main():
    conn = None
    stmt = None
    try:
        conn = ibm_db.connect('DATABASE=%s;HOSTNAME=%s;PORT=%d;PROTOCOL=%s;UID=%s;PWD=%s;' % (conf.section, conf.get('hostname'), conf.getint('port'), 
                                                                                              conf.get('protocol'), conf.get('user'), conf.get('passwd')), '', '')
        stmt = ibm_db.exec_immediate(conn, unicode(args.request[0], 'utf-8'))
        try:
            result = ibm_db.fetch_tuple(stmt)
        except:
            print u'Обработано строк %d' % ibm_db.num_rows(stmt)
            return
        column_conv = []
        head = u''
        underline=u''
        for i in xrange(len(result)):
            if i != 0:
                head += u'|'
                underline += u'+'
            name = ibm_db.field_name(stmt, i)
            size = ibm_db.field_display_size(stmt, i)
            if len(name) > size:
                size = len(name)
            if ibm_db.field_nullable(stmt, i) and len(u'NULL') > size:
                size = len(u'NULL')
            type_field = ibm_db.field_type(stmt, i)
            if type_field == 'float' or type_field == 'real' or type_field == 'decimal':
                column_conv.append({'size': size, 'format': u'{0:%d.%df}' % (size, (size - ibm_db.field_precision(stmt, i))), 'fn': convert_to_float})
            elif type_field == 'int' or type_field == 'bigint':
                column_conv.append({'size': size, 'format': u'{0:%dd}' % size, 'fn': convert_to_int})
            else:
                column_conv.append({'size': size, 'format': u'{0:%ds}' % size, 'fn': without_convert})
            head += name.center(size)
            underline += u'-' * size
        print head
        print underline
        while( result ):
            print conv(result, column_conv)
            result = ibm_db.fetch_tuple(stmt)
    except Exception as e:
        print >> sys.stderr, e
        sys.exit(-1)
    finally:
        if stmt:
            ibm_db.free_result(stmt)
        if conn:
            ibm_db.close(conn)

if __name__ == '__main__':
    main()
