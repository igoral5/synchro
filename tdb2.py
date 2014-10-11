#!/usr/bin/env python2
# -*- coding: utf-8 -*-
'''Проверка работы с DB2'''

import ibm_db

conn = ibm_db.connect("DATABASE=SAMPLE;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=Phone46;", "", "")
stmt = ibm_db.exec_immediate(conn, 'select * from employee')
result = ibm_db.fetch_tuple(stmt)
while( result ):
    for item in result:
        print item,
    print
    result = ibm_db.fetch_tuple(stmt)
