# -*- coding: utf-8 -*-
"""
Created on Tue Apr 14 00:06:47 2020

@author: Vadim Shkaberda
"""
import csv
import time
import threading
import requests

from queue import Queue


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


def geosort(x):
    """ Key function to sort results from response.
    """
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
    response = requests.get(url, timeout=10)
    assert response.status_code == 200, 'Response status is {}'.format(response.status_code)
    resp_data = response.json()
    #resp_data.sort(key=geosort)
    return (resp_data[0]['lat'], resp_data[0]['lon'])



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

    def run(self):
        while True:
            data = self.queue.get()
            self.download(data)
            self.queue.task_done()

    def download(self, data):
        """ Download data and write it to file.
        """
        for housenumber in data[-2].split(','):
            try:
                coo = return_coo(data, housenumber)
                with open(self.output_file, 'a') as f:
                    f.write('{};{};{};{}\n'.format(';'.join(data[:5]), housenumber, *coo))
            # if no result was in response
            except IndexError:
                pass


def main():
    queue = Queue()

    # start threads
    for i in range(5):
        t = Downloader(i+1, queue)
        t.setDaemon(True)
        t.start()

    for data in read_csv('houses2.csv'):
        queue.put(data)

    queue.join()


if __name__ == '__main__':
    start_time = time.time()
    main()
    print("Done in {:.3f} sec".format(time.time() - start_time))
    print("End.")
