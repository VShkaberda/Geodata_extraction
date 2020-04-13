# -*- coding: utf-8 -*-
"""
Created on Mon Apr 13 20:14:10 2020

@author: v.shkaberda
"""
import csv
import time
import requests


with open('cities.csv', 'r') as ref:
    CITY_REFERENCE = set(ref.read().split('\n'))


def from_csv():
    """ Row generator.
    """
    with open('houses.csv', 'r') as f:
        for row in f:
            yield row


def read_csv(csv_fname=None):
    """ Row generator.
    """
    if csv_fname is None:
        csv_fname = 'houses.csv'
    with open(csv_fname, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')
        next(reader, None)
        for row in reader:
            yield row


def city_is_validated(data):
    """ Return True if we need to extract geodata.
    """
    if data in CITY_REFERENCE:
        return True
    return False


def clean_addr(s):
    try:
        return s.split(' ', 1)[1]
    except IndexError:
        return s


def geosort(x):
    return (0 if x['class']=='building' else 1, -x['importance'])


def return_coo(data, housenumber):
    """ Query NominatimAPI ad return latitude and longitude.
    """
    obl, rayon, city, postalcode, street, *_ = data
    street = clean_addr(street)
    city = clean_addr(city)
    url = (f'https://osm.fozzy.ua/search?'
           f'street={housenumber} {street}&'
           f'city={city}&'
           f'postalcode={postalcode}&'
           f'format=json&'
           f'limit=1'
           )
    response = requests.get(url, timeout=5)
    assert response.status_code == 200, 'Response status is {}'.format(response.status_code)
    resp_data = response.json()
    #resp_data.sort(key=geosort)
    return (resp_data[0]['lat'], resp_data[0]['lon'])


def main():
    address_id = 1
    with open('data.csv', 'a') as f, open('address_id.csv', 'a') as a_id:
        for data in read_csv('houses2.csv'):
            # City == data[2]
#            if not city_is_validated(data[2]):
#                continue
            a_id.write(';'.join([str(address_id)] + data[:5] + ['\n']))
            for housenumber in data[-2].split(','):
                try:
                    coo = return_coo(data, housenumber)
                    f.write('{};{};{};{}\n'.format(address_id, housenumber, *coo))
                # if no result was in response
                except IndexError:
                    pass
            address_id += 1
            if address_id % 100 == 0:
                print('Done ', address_id)


if __name__ == '__main__':
    start_time = time.time()
    main()
    print("Done in {:.3f} sec".format(time.time() - start_time))
    print("End.")
