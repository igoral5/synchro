#!/usr/bin/env python2
# -*- coding: utf-8 -*-
'''Проверка работы с DB2'''

import ibm_db
import sys
import util

def convert_to_float(s):
    s = s.replace(',', '.')
    return float(s)

def convert_to_int(s):
    return int(s)

def without_convert(s):
    return unicode(s)

def conv(ts, cs):
    res = []
    for t, c in zip(ts, cs):
        res.append(c(t))
    return res

util.conf_io()
conn = None
stmt = None
try:
    conn = ibm_db.connect('DATABASE=SAMPLE;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=Phone46;', '', '')
    stmt = ibm_db.columns(conn, None, None, 'EMPLOYEE')
    row = ibm_db.fetch_assoc(stmt)
    columns_conv = []
    head = u''
    underline=u''
    str_format = u''
    i = 0
    while row:
        if len(row['COLUMN_NAME']) > row['COLUMN_SIZE']:
            size = len(row['COLUMN_NAME'])
        else:
            size = row['COLUMN_SIZE']
        if i != 0:
            head += u'|'
            underline += u'+'
            str_format += u'|'
        head += row['COLUMN_NAME'].center(size)
        underline += u'-' * size
        if row['TYPE_NAME'] == u'DECIMAL':
            str_format += '{0[%d]:%d.%df}' % (i, size, row['DECIMAL_DIGITS'])
            columns_conv.append(convert_to_float)
        elif row['TYPE_NAME'] == 'SMALLINT' or row['TYPE_NAME'] == 'INT' or row['TYPE_NAME'] == 'BIGINT':
            str_format += '{0[%d]:%dd}' % (i, size)
            columns_conv.append(convert_to_int)
        else:
            str_format += '{0[%d]:%ds}' % (i, size)
            columns_conv.append(without_convert)
        row = ibm_db.fetch_assoc(stmt)
        i += 1
    print head
    print underline
    ibm_db.free_result(stmt)
    stmt = None
    stmt = ibm_db.exec_immediate(conn, u'select * from employee')
    result = ibm_db.fetch_tuple(stmt)
    while( result ):
        res = conv(result, columns_conv)
        print str_format.format(res)
        result = ibm_db.fetch_tuple(stmt)
except Exception as e:
    print e
    sys.exit(-1)
finally:
    if stmt:
        ibm_db.free_result(stmt)
    if conn:
        ibm_db.close(conn)
