# -*- coding: utf-8 -*-
"""
Created on Tue Apr 14 00:06:47 2020

@author: Vadim Shkaberda
"""
import csv
import time
import threading
import requests

from contextlib import suppress
from queue import Queue

REPLACEMENT = {'пл.': 'площа',
               'пров.': 'провулок',
               'вул.': 'вулиця',
               'бульв.': 'бульвар',
               'кв-л': 'квартал'
               }


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


def clean_addr(s):
    """ Remove type name from name. E.g. 'city City' -> 'City'.
    """
    try:
        return s.split(' ', 1)[1]
    except IndexError:
        return s

def fix_street_name(sname):
    """ Convert short type names according to REPLACEMENT dictionary and return
    street name with type at the end.
    """
    with suppress(KeyError):
        try:
            sname = sname.split(' ', 1)
        except IndexError:
            return sname
        sname[0] = REPLACEMENT[sname[0]]
        return ' '.join(reversed(sname))


def geosort(x):
    """ Key function to sort results from response.
    """
    return (0 if x['class']=='building' else 1, -x['importance'])


def get_geodata(url):
    response = requests.get(url, timeout=10)
    return response.json()


def create_query(data):
    """ Query NominatimAPI ad return latitude and longitude.
    """
    host = 'http://116.202.237.170:7070/'
    rn, obl, _rayon, _city_type, city_name, postalcode, _street_type, street_name, house_number, street_type, *_ = data
    #street = fix_street_name(street)
    #city = clean_addr(city)
    # first attempt with strict query
    url = ['format=json&limit=1']
    if postalcode:
        url.append(f'postalcode={postalcode}&')
    if obl:
        url.append(f'state={obl}&')
    if city_name:
        url.append(f'city={city_name}&')
    if street_name:
        url.append(f'{street_name} {street_type}&')
        if house_number:
            url.append(f'{house_number} ')
        url.append('street=')
    url.append(f'{host}search?')
    url = ''.join(reversed(url))
    resp_data = get_geodata(url)
    if not resp_data:
        # second attempt
        url = f'{host}search?q='
        if house_number:
            url += f'{house_number}, '
        if street_name:
            url += f'{street_name}, '
        if city_name:
            url += f'{city_name}, '
        if obl:
            url += f'{obl}, '
        if postalcode:
            url += f'{postalcode}'
        url.strip(' ,')
        url += '&format=json&limit=1'
        resp_data = get_geodata(url)
    #resp_data.sort(key=geosort)
    return (resp_data[0]['class'], resp_data[0]['osm_type'],
            resp_data[0]['type'], resp_data[0]['importance'],
            resp_data[0]['lat'], resp_data[0]['lon'])


class Downloader(threading.Thread):
    """ Thread downloader.

    t_id: int, id of thread.
    queue: Queue, queue from which thread takes data.
    """
    def __init__(self, t_id, queue):
        #super().__init__()
        threading.Thread.__init__(self)
        self.t_id = t_id
        self.output_file = 'data' + str(t_id) + '.csv'
        self.queue = queue
        self.succeded = 0

    def succeded(self):
        return self.succeded

    def run(self):
        while True:
            data = self.queue.get()
            self.download(data)
            self.queue.task_done()

    def download(self, data):
        """ Download data and write it to file.
        """
        with open(self.output_file, 'a') as f:
            try:
                coo = create_query(data)
                f.write('{};{}\n'.format(';'.join(data), ';'.join(map(str, coo))))
                self.succeded += 1
            # if no result was in response
            except IndexError:
                f.write('{};;;;;;\n'.format(';'.join(data)))


def main():
    queue = Queue()
    downloaders = []

    # start threads
    for i in range(8):
        t = Downloader(i+1, queue)
        downloaders.append(t)
        t.setDaemon(True)
        t.start()

    for data in read_csv('ukrpochta_extended.csv'):
        queue.put(data)

    queue.join()

    for downloader in downloaders:
        print(downloader.succeded)


if __name__ == '__main__':
    start_time = time.time()
    main()
    print("Done in {:.3f} sec".format(time.time() - start_time))
    print("End.")
