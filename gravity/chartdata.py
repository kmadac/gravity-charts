__author__ = 'kmadac'

import time
import datetime
import pytz
import logging
from util import get_prev_minute_timestamp

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class granularity(object):
    subseconds = 0
    seconds = 1
    minutes = 60
    hours = 3600


def _avg_val(data):
    """
    Returns average of values returned from DB in format
    {u'1': [26, 276], u'0': [27, 278], u'3': [27, 278], u'2': [26, 278], u'5': [26, 282], u'4': [27, 268],
    u'7': [27, 270], u'6': [27, 286], u'9': [26, 263], u'8': [27, 293]}

    Return format is a one list in format [26, 286]. First value is sum of all first values divided by number of
    measures. Same for second argument
    """
    result = [0, 0]
    count = int(len(data))

    for key, val in data.iteritems():
        result[0] += val[0]
        result[1] += val[1]

    return result[0]/count, result[1]/count


def get(bucket, sensor, starttime, endtime, granularity=granularity.seconds):
    """
    Returns data for chartkick.py - tuple of unordered dictionaries in following format:
    {timestamp: value, timestamp: value, ...}
    Firt dictionary is deviation data and second is pressure data
    """
    pressure = {}
    deviation = {}

    utc = pytz.timezone("UTC")

    start_ts = int(time.mktime(starttime.timetuple()))
    end_ts = int(time.mktime(endtime.timetuple()))

    timestamps = [ts for ts in xrange(start_ts, end_ts + granularity, granularity)]

    minutes_ts = []
    for ts in timestamps:
        minute = get_prev_minute_timestamp(ts)
        if minute not in minutes_ts:
            minutes_ts.append(minute)

    keys = ['{0}-{1}'.format(sensor, ts) for ts in minutes_ts]

    datas = bucket.get_multi(keys)

    for ts in timestamps:
        minute = get_prev_minute_timestamp(ts)
        seconds = ts - minute
        val = datas.get('{0}-{1}'.format(sensor, minute)).value
        if val:
            keyname = datetime.datetime.fromtimestamp(ts, tz=utc).strftime("%Y-%m-%d %H:%M:%S")
            try:
                deviation[keyname] = _avg_val(val[str(seconds)])[0]
            except KeyError:
                pass
            try:
                pressure[keyname] = _avg_val(val[str(seconds)])[1]
            except KeyError:
                pass

    return deviation, pressure