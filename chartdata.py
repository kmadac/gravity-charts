__author__ = 'kmadac'

import time
import logging
import collections

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
    pressure = {}
    deviation = {}

    start_ts = int(time.mktime(starttime.timetuple()))
    end_ts = int(time.mktime(endtime.timetuple()))

    keys = ['{0}-{1}'.format(sensor, ts) for ts in xrange(start_ts, end_ts, granularity)]
    datas = bucket.get_multi(keys)

    for key, measure in datas.iteritems():
        if measure.value:
            deviation[int(key[2:])] = _avg_val(measure.value)[0]
            pressure[int(key[2:])] = _avg_val(measure.value)[1]

    deviation_ordered = collections.OrderedDict(sorted(deviation.items(), key=lambda t: t[0]))
    pressure_ordered = collections.OrderedDict(sorted(pressure.items(), key=lambda t: t[0]))

    return deviation_ordered, pressure_ordered
