#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import os.path
script_dir = os.path.dirname(os.path.realpath(__file__))

import csv
import json
import collections
import re

def convert(types, values):
    return [t(v) for t, v in zip(types, values)]

streets = {}
with open(script_dir + "/addresses.csv", "r") as f:
    months = ['янв', 'фев', 'мар', 'апр', 'май', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек']
    regexps = []
    for month in months:
        regexps.append(re.compile(r"((\d+)\.%s|%s\.(\d+))" % (month, month)))

    for row in csv.reader(f, delimiter=";"):
        if row[2] == 'Вход в метро': continue

        lat = float(row[0].replace(",", "."))
        lon = float(row[1].replace(",", "."))
        street_name = row[5]
        house_name = row[6]

        for i in range(len(regexps)):
            m = regexps[i].match(house_name)
            if m:
                if m.group(3) is None:
                    house_name = "%d/%d" % (int(m.group(2)), i+1)
                else:
                    house_name = "%d/%d" % (i+1, int(m.group(3)))

        if street_name in streets:
            street = streets[street_name]
        else:
            street = collections.OrderedDict()
            street["name"] = street_name
            street["houses"] = []
            streets[street_name] = street

        house = collections.OrderedDict()
        house["name"] = house_name
        house["location"] = [lon, lat]
        street["houses"].append(house)

f = open(script_dir + "/addresses.json", "w")

def write_street(_id, name):
    first = { "index" : collections.OrderedDict() }
    first["index"]["_index"] = "region_moskva"
    first["index"]["_type"] = "street"
    first["index"]["_id"] = _id

    second = collections.OrderedDict()
    second["region"] = "moskva"
    second["name"] = name

    f.write(json.dumps(first, ensure_ascii=False, encoding="utf8").encode("utf8") + "\n")
    f.write(json.dumps(second, ensure_ascii=False, encoding="utf8").encode("utf8") + "\n")

def write_house(_id, street_name, name, location):
    first = { "index" : collections.OrderedDict() }
    first["index"]["_index"] = "region_moskva"
    first["index"]["_type"] = "house"
    first["index"]["_id"] = _id

    second = collections.OrderedDict()
    second["region"] = "moskva"
    second["name"] = name
    second["address"] = street_name + ", " + name
    second["location"] = location

    f.write(json.dumps(first, ensure_ascii=False, encoding="utf8").encode("utf8") + "\n")
    f.write(json.dumps(second, ensure_ascii=False, encoding="utf8").encode("utf8") + "\n")

for i, k in enumerate(streets):
    write_street(str(i+1), streets[k]["name"])

    houses = streets[k]["houses"]
    for j in xrange(len(houses)):
        write_house(str(i+1)+":"+str(j+1), streets[k]["name"], houses[j]["name"], houses[j]["location"])

f.close()
