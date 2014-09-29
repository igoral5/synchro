#!/usr/bin/env python2
# -*- coding: utf-8 -*-
''' Script for importing POI data from csv file into Elasticsearch '''
import os
import logging
import json
import re
from collections import OrderedDict
import csv
import elasticsearch as es
import tempfile
import shutil
# from itertools import islice
import poi_renaming

#------------------------------------------------------------------------------
# Config
#------------------------------------------------------------------------------
logging.basicConfig(format = '[%(asctime)-15s] [%(levelname)s] [%(name)s] - %(message)s', level=logging.ERROR)
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

ES_NODES = [{'host': 'localhost', 'port':9200}] # elasticsearch nodes

ES_IO_TIMEOUT = 1200 # seconds

# input data files
FILES = {
    'poi' : 'Poi.csv',
    'poi_types' : 'PoiType.csv',
    'aux_poi_types' : 'AdditionalType.csv'
}

REGION_TAGS = {
    "Москва": "moskva",
    "Московская обл.": "podolsk",
    "Краснодарский край": "sochi",
    "Красноярский край": "krasnoyarsk"
}

# don't import next types
POI_TYPES_TO_SKIP = [
    -1,    # белки (???) 
    2001,  # Опасные перекрёстки
    2002,  # Засады на дорогах
    2004,  # Лежачие полицейские
    2014,   # Опасные повороты
    11004, #Железнодорожные станции;zd_vogz.png;0
    11006, # станции метро
]

# Extract some pois into new type (ACHTUNG! temp solution)
# e.g. AEROEXPRESS doesn't have a type. These pois are railway stations, but we need to import them as distinct type
POI_TYPES_TO_ADD = [
    {
        'id' : -1, # use this id cause we skip this type in orig. data 
        'name' : 'Аэроэкспресс',
        'icon' : 'small_AEROEKSPRESS.png',
        'is_target_poi': lambda poi: poi['BrandName'] == 'AEROEKSPRESS'
    },
]

# copy from csv file header
POI_CSV_FIELDS = ('POIID;Name;OtherNames;Type;Address;PhoneNumber;WebPage;Email;Description;'
    'Latitude;Longitude;Region;Territory;AnUnusedField;BrandName').split(';')

#------------------------------------------------------------------------------
# Utils
#------------------------------------------------------------------------------
def StripAddress(addr):
    ''' Remove redundant address parts '''
    redundants = (
        "Россия", "Москва", "Московская обл.",
        "Подольск г.", "Красноярский край", "Красноярск г.",
        "Краснодарский край", "Сочи"
    )
    if addr is None: return ''
    parts = addr.split(", ")
    new_parts = []
    for i in range(len(parts)):
        if parts[i] not in redundants:
            new_parts += parts[i:]
            break
    return ", ".join(new_parts)

#------------------------------------------------------------------------------
# Main
#------------------------------------------------------------------------------
def RemoveAllPOIs():
    ''' Remove all POIs from elastic region_* indices  '''
    es_client = es.Elasticsearch(hosts = ES_NODES, timeout=ES_IO_TIMEOUT)
    query = { "query": { "match_all": { } } }
    try:
        es_client.delete_by_query(index='region_*', doc_type='poi', body=query )
    except es.exceptions.NotFoundError: # there is no region_* indices
        pass

def ReadPOIs(do_insert, generate_files):
    ''' Read POI from csv file. 

    Args:
        do_insert (bool) - insert poi into elasticsearch
        generate_files (bool) - create files for bulk insert
    '''
    def remove_old_files():
        files = os.listdir('.')
        for fname in files:
            if re.match("region_.+\.json", fname):
                os.remove(fname)

    def read_poi_types(fname):
        types = {}
        with open(fname) as f:
            for poi_type in csv.DictReader(f, fieldnames=('id', 'parent', 'name', 'icon'), delimiter=';'):
                for (k,v) in poi_type.iteritems():
                    if v=='NULL': poi_type[k]=None                
                try:
                    poi_type['id'] = int(poi_type['id'])
                except ValueError:
                    log.warn('Wrong POI type record: %s', str(poi_type).decode('string-escape'))
                    continue
                types[poi_type['id']] = poi_type
        for poi_type in POI_TYPES_TO_ADD: 
            types[poi_type['id']] = poi_type
            log.info('New POI type was added by script %s', poi_type['name'])
        return types

    def read_aux_poi_types(fname):
        aux_types = {} # key - poi_id, value - list of type ids
        with open(fname) as f:
            for row in csv.reader(f, delimiter=';'):
                try:
                    (poi_id, type_id) = (int(row[0]),int(row[1]))
                except ValueError:
                    log.warn('Wrong Aux POI type record:', row)
                    continue
                if poi_id not in aux_types:
                    aux_types[poi_id] = [type_id]
                else:
                    aux_types[poi_id].append(type_id)
        return aux_types

    poi_types = read_poi_types(FILES['poi_types'])
    aux_poi_types = read_aux_poi_types(FILES['aux_poi_types'])

    def poi_to_insert_cmd(region_tag, poi):
        poi_id = poi['POIID']
        type_id = poi['Type']
        poi['Name'] = poi_renaming.Rename(type_id, poi['Name'])

        if type_id in poi_types: 
            t = poi_types[type_id]
            types =[ t['name'] ]
            icons = [ t['icon'] ]
        else:
            types = []
            icons = []
            log.warn('Unknown type id %s in poi record: %s', type_id, poi)
        if poi_id in aux_poi_types:
            for aux_type_id in aux_poi_types[poi_id]:
                t = poi_types[aux_type_id]
                types.append(t['name'])
                icons.append(t['icon'])
        names = [poi['Name']]
        if poi['OtherNames']:
            names.append(poi['OtherNames'])
        poi_dict = OrderedDict([
                ('id', str(poi_id)),
                ('location', [poi['Longitude'], poi['Latitude']]),
                ('tags', [region_tag]),
                ('type_codes', [type_id]),
                ('types', types),
                ('icons', icons),
                ('name', names[0]),
                ('address', poi['Address']),
                ('description', poi['Description']),
                ('phones', poi['PhoneNumber']),
                ('webpage', poi['WebPage']),
                ('email', poi['Email']),
                ('brand', poi['BrandName'])
            ])

        nullable_fields = ['description', 'phones', 'webpage', 'email', 'brand']
        for field in nullable_fields:
            if poi_dict[field] is None or len(poi_dict[field]) == 0:
                del poi_dict[field]

        action_str = json.dumps({
            'index': OrderedDict([
                    ('_index', 'region_' + region_tag),
                    ('_type', 'poi'),
                    ('_id', poi_id)
                ])
        })
        data_str = json.dumps(poi_dict, ensure_ascii=False)
        return '\n'.join((action_str,data_str))

    out_files = {} # key - region, value - file object
    try:
        with open(FILES['poi']) as f:
            # main importing cycle
            for poi in  csv.DictReader(f, fieldnames = POI_CSV_FIELDS, delimiter=';'):
                # convert data
                for (k,v) in poi.iteritems():
                    if v=='NULL': poi[k]=None
                    else:
                        poi[k] = v
                try:
                    poi['POIID'] = int(poi['POIID'])
                    poi['Type'] = int(poi['Type'])
                    poi['Address'] = StripAddress(poi['Address'])
                    poi['Latitude'] = float(poi['Latitude'])/1000000.0
                    poi['Longitude'] = float(poi['Longitude'])/1000000.0
                    if poi['Type'] in POI_TYPES_TO_SKIP:
                        continue
                except (ValueError, TypeError):
                    log.warning('Wrong POI record: %s', str(poi).decode('string-escape'))
                    continue
                for i in POI_TYPES_TO_ADD:
                    if i['is_target_poi'](poi):
                        log.info('POI "%s" will be imported with new type "%s"', poi['Name'], i['name'])
                        poi['Type'] = i['id']

                region_tag = REGION_TAGS[poi['Region']]
                if not out_files.has_key(region_tag):
                    out_files[region_tag] = tempfile.NamedTemporaryFile('w+', delete=False)
                out_files[region_tag].write(poi_to_insert_cmd(region_tag, poi) + '\n')
            # POI read done

        if do_insert:
            log.info('Inserting POI into elastic...')
            es_client = es.Elasticsearch(hosts = ES_NODES, timeout=ES_IO_TIMEOUT) 
            for f in out_files.values():
                f.seek(0)
                es_client.bulk(body=f.read())
                # while True:
                #     n_lines = list(islice(f,1024))
                #     if not n_lines:
                #         break
                #     es_client.bulk(body=''.join(n_lines))

        if generate_files:
            log.info('Generating bulk insert files...')
            remove_old_files()
            for (region,f) in out_files.items():
                shutil.move(f.name, './region_'+region+'.json')
                del out_files[region]
    finally:
        for f in out_files.values():
            os.remove(f.name)

def main():
    import argparse
    arg_parser = argparse.ArgumentParser(description='*** Script to import POI data from csv to elasticsearch ***')
    arg_parser.add_argument('-d', '--delete', action='store_true', help='delete all POIs from elastic')
    arg_parser.add_argument('-i','--insert', action='store_true',
                            help='import new POIs directly into elastic')
    arg_parser.add_argument('-f','--generate-files', action='store_true',
                            help='import new POIs into bulk insert json files (region_*.json)')
    arg_parser.add_argument('-s','--server', help='elasticsearch server')
    args = arg_parser.parse_args()
    if args.server:
        for n in ES_NODES:
            n['host'] = args.server
    try:
        if args.delete:
            log.info('Removing previous POIs...')
            RemoveAllPOIs()
        # if args.insert:
        #    raise Exception('Direct insert mode disabled, cause there is problem with some network configs and elasticsearch python lib behaviour\n'
        #     'use curl command to insert data:\n'
        #     'curl -S -XPOST "http://servername:9200/_bulk" --data-binary @region_moskva.json >/dev/nul')
        #    # e.g. when using server name projects instead of projects.shturmann.local (proxy, etc, etc)

        if args.insert or args.generate_files:
            log.info('Importing POI data ...')
            ReadPOIs(args.insert, args.generate_files)
        log.info('Done')
    except Exception as err:
        log.exception(err)

if __name__ == '__main__':
    main()
