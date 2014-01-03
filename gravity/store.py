__author__ = 'kmadac'

import logging
from couchbase import exceptions

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
        logger.debug("Last record: {0}".format(last_record.value))
        last_record = last_record.value.split()
        return last_record
    else:
        return None


def _get_prev_minute_timestamp(timestamp_sec):
    """
    Returns timestamp of minute to which timestamp_sec parameter belongs.
    It means that if timestamp_sec argument is 1407838210 (10h10m10s), function returns 1407838200(10h10m0s)
    """
    return timestamp_sec / 60 * 60


def add_measurements(bucket, data, sensor_id):
    """
    Adds measurement data to DB.
    Data is list of dictionaries in format you can find in dataparser.py module
    """
    if isinstance(data, list):
        minute_ts = _get_prev_minute_timestamp(int(data[0]['timestamp'].split(".")[0]))
        minute_db = bucket.get(str(minute_ts))
        # if minute_db.value:

        sec_ts, ms_part = data[0]['timestamp'].split(".")
        old_ts_min = _get_prev_minute_timestamp(int(sec_ts))

        document = {}
        for m_record in data:
            sec_ts, ms_part = m_record['timestamp'].split(".")
            sec_part = int(sec_ts) - _get_prev_minute_timestamp(int(sec_ts))
            ts_min = _get_prev_minute_timestamp(int(sec_ts))

            if old_ts_min == ts_min:
                if sec_part not in document:
                    document[sec_part] = {}
                document[sec_part][int(ms_part[0])] = [m_record['deviation'], m_record['pressure']]
            else:
                # if we are in new minute
                if document:
                    try:
                        key_measurement = '{0}-{1}'.format(sensor_id, old_ts_min)
                        bucket.set(key_measurement, document)
                    except exceptions.TemporaryFailError as e:
                        logger.debug("{0}".format(e.message))
                        raise e

                document = {sec_part: {int(ms_part[0]): [m_record['deviation'], m_record['pressure']]}}
                old_ts_min = _get_prev_minute_timestamp(int(sec_ts))

            logger.debug(m_record)
    return True
