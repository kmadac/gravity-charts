__author__ = 'kmadac'

import logging

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def set_last_record(bucket, sensor_id, year, day, filename):
    """
    Stores to db which data were synced last time.
    It has to be executed at the end of the sync loop in sync script
    """
    bucket.set('{0}-last_record'.format(sensor_id), '{0} {1} {2}'.format(year, day, filename))


def get_last_record(bucket, sensor_id):
    last_record = bucket.get('{0}-last_record'.format(sensor_id))
    if last_record.value:
        last_record = last_record.value.split()
        return last_record
    else:
        return None


def add_measurements(bucket, data, sensor_id):
    """
    Adds measurement data to DB.
    Data is list of dictionaries in format you can find in dataparser.py module
    """
    if isinstance(data, list):
        old_ts_sec = None
        document = {}
        for m_record in data:
            sec_part, ms_part = m_record['timestamp'].split(".")
            key_measurement = '{0}-{1}'.format(sensor_id, sec_part)
            if old_ts_sec == sec_part:
                # if we are in same second
                document[int(ms_part[0])] = [m_record['deviation'], m_record['pressure']]
            else:
                # if we are in new second
                if document:
                    logger.debug("CB Set {0}".format(key_measurement))
                    bucket.set(key_measurement, document)
                document = {int(ms_part[0]): [m_record['deviation'], m_record['pressure']]}
                old_ts_sec = sec_part

            logger.debug(m_record)
    return True