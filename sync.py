__author__ = 'kmadac'

import logging

import config
import sys
import redis
import datetime

import httpdata
import store


def next_hour(year, day, hour):
    last_hour = datetime.datetime.strptime("{0} {1} {2}".format(year, day, hour), "%Y %j %H")
    hour_later = last_hour + datetime.timedelta(hours=1)
    hour_later = hour_later.strftime("%Y %j %H").split()
    hour_later = [int(i) for i in hour_later]
    return hour_later


def main():
    logger = logging.getLogger('sync')
    # logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.DEBUG)

    r_server = redis.Redis(config.REDIS_SERVER)

    last_record = store.get_last_record(r_server)
    logger.info(last_record)

    if not last_record:
        year = 2013
        day = httpdata.get_days(config.HTTP_SENSORS[0], year)[0]
        hour = httpdata.get_hours(config.HTTP_SENSORS[0], year, day)[0]
    else:
        year, day, hour = next_hour(*last_record)

    while True:
        logger.debug('Day:{0} Hour:{1}'.format(day, hour))
        data = httpdata.get_data(config.HTTP_SENSORS[0], year, day, hour, '/tmp')
        if not data:
            break
        logger.debug('Data first:{0} Data last:{1}'.format(data[0], data[-1]))

        store.add_measurements(r_server, data)
        store.set_last_record(r_server, year, day, hour)

        last_record = [year, day, hour]
        year, day, hour = next_hour(*last_record)

    return 0


if __name__ == "__main__":
    sys.exit(main())