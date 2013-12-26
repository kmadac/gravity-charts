__author__ = 'kmadac'

import logging


def get_last_timestamp(r_server):
    return r_server.get('last_timestamp')


def set_last_record(r_server, year, day, hour):
    """
    Stores to db which data were synced last time.
    It has to be executed at the end of the sync script
    """
    r_server.set('last_record', '{0} {1} {2}'.format(year, day, hour))


def get_last_record(r_server):
    last_record = r_server.get('last_record')
    if last_record:
        return r_server.get(last_record).split()
    else:
        return None


def add_measurements(r_server, data):
    """
    Adds measurement data to DB
    Data are dictionary or list of dictionaries in format you can find in dataparser.py module
    """
    if isinstance(data, list):
        for m_record in data:
            logging.debug(m_record)
            r_server.rpush(m_record['timestamp'], m_record['deviation'])
            r_server.rpush(m_record['timestamp'], m_record['pressure'])

            r_server.set('last_timestamp', m_record['timestamp'])
    else:
        logging.debug(data)
        r_server.rpush(data['timestamp'], data['deviation'])
        r_server.rpush(data['timestamp'], data['pressure'])
        r_server.set('last_timestamp', data['timestamp'])
    return True