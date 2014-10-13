#!/usr/bin/env python2
# -*- coding: utf-8 -*-
'''Проверка работы с DB2'''

import ibm_db
import sys
import util

def convert_to_float(s):
    if s:
        s = s.replace(',', '.')
        return float(s)
    else:
        return s

def convert_to_int(s):
    if s:
        return int(s)
    else:
        return s

def without_convert(s):
    if s:
        return unicode(s).strip()
    else:
        return s

def conv(ts, cs):
    res = u''
    for i, (t, c) in enumerate(zip(ts, cs)):
        value = c['fn'](t)
        if i != 0:
            res += u'|'
        if value:
            res += c['format'].format(value)
        else:
            res += u'NULL'.center(c['size'])
    return res

util.conf_io()
conn = None
stmt = None
try:
    conn = ibm_db.connect('DATABASE=SAMPLE;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=Phone46;', '', '')
    stmt = ibm_db.columns(conn, None, None, sys.argv[1].upper())
    row = ibm_db.fetch_assoc(stmt)
    columns_conv = []
    head = u''
    underline=u''
    i = 0
    while row:
        if len(row['COLUMN_NAME']) > row['COLUMN_SIZE']:
            size = len(row['COLUMN_NAME'])
        else:
            size = row['COLUMN_SIZE']
        if row['NULLABLE'] and size < len(u'NULL'):
            size = len(u'NULL')
        if i != 0:
            head += u'|'
            underline += u'+'
        if row['TYPE_NAME'] == u'DECIMAL':
            size += 1
            columns_conv.append({'fn': convert_to_float, 'size': size, 'format': u'{0:%d.%df}' % (size, row['DECIMAL_DIGITS'])})
        elif row['TYPE_NAME'] == 'SMALLINT' or row['TYPE_NAME'] == 'INT' or row['TYPE_NAME'] == 'BIGINT':
            columns_conv.append({'fn': convert_to_int, 'size': size, 'format': u'{0:%dd}' % size})
        else:
            columns_conv.append({'fn': without_convert, 'size': size, 'format': u'{0:%ds}' % size})
        head += row['COLUMN_NAME'].center(size)
        underline += u'-' * size
        row = ibm_db.fetch_assoc(stmt)
        i += 1
    print head
    print underline
    ibm_db.free_result(stmt)
    stmt = None
    stmt = ibm_db.exec_immediate(conn, u'select * from %s' % sys.argv[1])
    result = ibm_db.fetch_tuple(stmt)
    while( result ):
        print conv(result, columns_conv)
        result = ibm_db.fetch_tuple(stmt)
except Exception as e:
    print e
    sys.exit(-1)
finally:
    if stmt:
        ibm_db.free_result(stmt)
    if conn:
        ibm_db.close(conn)
