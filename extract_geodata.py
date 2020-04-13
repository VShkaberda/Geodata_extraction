# -*- coding: utf-8 -*-
"""
Created on Mon Apr 13 20:14:10 2020

@author: v.shkaberda
"""
import csv

with open('cities.csv', 'r') as ref:
    CITY_REFERENCE = set(ref.read().split('\n'))


def from_csv():
    """ Row generator.
    """
    with open('houses.csv', 'r') as f:
        for row in f:
            yield row


def read_csv():
    """ Row generator.
    """
    with open('houses.csv', 'r') as csvfile:
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


def return_coo(data):
    """ Query NominatimAPI ad return latitude and longitude.
    """
    return 1


def main():
    address_id = 1
    with open('data.csv', 'a') as f, open('address_id.csv', 'a') as a_id:
        for data in read_csv():
            # City == data[2]
            if not city_is_validated(data[2]):
                continue
            a_id.write(';'.join([address_id] + data[:5] + ['\n']))
            for house in data[-2].split(','):
                coo = return_coo(house)
                f.write('{};{};{}\n'.format(address_id, *coo))
            address_id += 1


if __name__ == '__main__':
    main()
    print("End.")
