#!/bin/bash
if [ -f elasticsearch/escopy_running ]; then
        echo `date` 'update elasticsearch running, exit'
        exit 0
fi
touch elasticsearch/escopy_running
./synchro.py moscow
./synchro.py podolsk
./synchro.py krasnoyarsk
rm elasticsearch/escopy_running

