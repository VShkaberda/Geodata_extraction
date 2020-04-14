# -*- coding: utf-8 -*-
"""
Created on Tue Apr 14 13:24:43 2020

@author: v.shkaberda
"""

import numpy as np
import csv
import json

CITIES_REF = {}
CITY = 'Київ'

with open('cities.csv', 'r') as city_file:
    reader = csv.reader(city_file, delimiter=';')
    for row in reader:
        CITIES_REF[row[0].split(' ', 1)[1]] = row[1]

CITY_ID = '3600421866'


def properties_id_sort(feature):
    properties = feature['properties']['@id'].split('/')[0]
    try:
        street = feature['properties']['addr:street']
        housenumber = feature['properties']['addr:housenumber']
    except KeyError:
        street, housenumber = '0', '0'
    return (street,
            housenumber,
            0 if properties == 'way' else
            1 if properties == 'relation' else
            2 if properties == 'node' else 3,
            )


def write_csv(addr_dict):
    with open('.\\csv\\data.csv', 'a') as f:
        f.write('CityID;Streetname;Housenumber;type;lat;lon\n')
        for (k, v) in addr_dict.items():
            f.write(CITY_ID + ';')
            f.write(';'.join(k) + ';')
            f.write(';'.join(map(str, v)))
            f.write('\n')


def main():
    addr_dict = {}
    shapes = set()
    with open('export.geojson', 'r', encoding='utf-8') as f:
        data = json.load(f)
        data = data['features']
        # sore s we have always ways before nodes
        data.sort(key=properties_id_sort)
    for feature in data:
#        try:
#            if feature['properties']['addr:street'] == 'Гагаріна вулиця' and feature['properties']['addr:housenumber'] == '23а':
#                print(feature['geometry']['coordinates'])
#            else:
#                continue
#        except KeyError:
#            continue
        # continue if addr:street or addr:housenumber not exists
        try:
            str_house = (feature['properties']['addr:street'],
                         feature['properties']['addr:housenumber'])
        except KeyError:
            continue
        if str_house not in addr_dict:
            poly = np.array(feature['geometry']['coordinates'])
            shape = poly.shape
            # polygon
            try:
                shapes.add((1, poly.shape))
                lon, lat = np.average(feature['geometry']['coordinates'][0], axis=0)
            # exact coordinates
            except IndexError:
                shapes.add((2, poly.shape))
                lon, lat = feature['geometry']['coordinates']
            # multipolygon
            except ValueError:
                shapes.add((3, poly.shape))
                lon, lat = np.average(feature['geometry']['coordinates'][0], axis=1)[0]
            # multipolygon
            except TypeError:
                print(feature['geometry']['coordinates'])
                print(np.average(feature['geometry']['coordinates'][0], axis=1))
                raise
            # add to addr_dict
            try:
                addr_dict[str_house] = (feature['properties']['@id'].split('/')[0],
                                        round(lat, 6),
                                        round(lon, 6)
                                        )
            except TypeError:
                print(feature['properties']['addr:street'],
                      feature['properties']['addr:housenumber'])
                raise
    print(shapes)
    #write_csv(addr_dict)


if __name__ == '__main__':
    main()
    print('Done.')
