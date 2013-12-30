__author__ = 'kmadac'

from gravity import chartdata

import config
import datetime
from sys import exit
from couchbase import Couchbase
import time


def main():
    starttime = datetime.datetime(year=2013, month=8, day=7, hour=11)
    endtime = datetime.datetime(year=2013, month=8, day=10, hour=11)

    cbclient = Couchbase.connect(host=config.DB_SERVER, bucket="default", quiet=True)

    granularity = (int(time.mktime(endtime.timetuple())) - int(time.mktime(starttime.timetuple()))) / 200
    deviation, pressure = chartdata.get(cbclient, 0, starttime, endtime, granularity=granularity)

    print deviation, len(deviation)

    return 0


if __name__ == "__main__":
    exit(main())