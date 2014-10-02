#!/bin/bash
if [ -f elasticsearch/escopy_running ]; then
        echo `date` 'update elasticsearch running, exit'
        exit 0
fi
touch elasticsearch/escopy_running
./synchro.py --url="/moscow" --host "213.79.91.85:47536" --create --format csv --thread 7 --log-txt synchro-moscow.log --log-json synchro-moscow.json
./synchro.py --url="/podolsk" --create --log-txt synchro-podolsk.log --thread 7 --log-json synchro-podolsk.json
./synchro.py --url="/krasnoyarsk" --host 80.255.132.233:8080 --create --thread 7 --log-txt synchro-krasnoyarsk.log --log-json synchro-krasnoyarsk.json
rm elasticsearch/escopy_running

