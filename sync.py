__author__ = 'kmadac'

import logging

import config
import sys
import redis

import httpdata
import store


def main():
    logger = logging.getLogger('sync')
    # logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.DEBUG)

    r_server = redis.Redis(config.REDIS_SERVER)

    logger.info(store.get_last_record(r_server))

    for day in range(250, 260):
        hours = httpdata.get_hours(config.HTTP_SENSORS[0], 2013, day)
        for hour in hours:
            logger.debug('Day:{0} Hour:{1}'.format(day, hour))
            data = httpdata.get_data(config.HTTP_SENSORS[0], 2013, day, hour, '/tmp')
            logger.debug('Data first:{0} Data last:{1}'.format(data[0], data[-1]))
            store.add_measurements(r_server, data)
            store.set_last_record(r_server, 2013, day, hour)

    return 0


if __name__ == "__main__":
    sys.exit(main())