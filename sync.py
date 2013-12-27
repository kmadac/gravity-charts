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

    last_record = store.get_last_record(r_server)
    logger.info(last_record)

    if not last_record:
        year = 2013
        day = httpdata.get_days(config.HTTP_SENSORS[0], year)[0]
        files = httpdata.get_files(config.HTTP_SENSORS[0], year, day)
        filename = files[0]
    else:
        year, day, filename = last_record
        files = httpdata.get_files(config.HTTP_SENSORS[0], year, day)
        year, day, filename, files = httpdata.next_file(config.HTTP_SENSORS[0], year, day, filename, files)

    while True:
        logger.debug('Day:{0} filename:{1}'.format(day, filename))
        data = httpdata.get_data(config.HTTP_SENSORS[0], year, day, filename, '/tmp')
        if not data:
            logger.info("No data parsed in {0}/{1}/{2}/{3}".format(config.HTTP_SENSORS[0], year, day, filename))
            break
        logger.debug('Data first:{0} Data last:{1}'.format(data[0], data[-1]))

        store.add_measurements(r_server, data)
        store.set_last_record(r_server, year, day, filename)

        year, day, filename, files = httpdata.next_file(config.HTTP_SENSORS[0], year, day, filename, files)

    return 0


if __name__ == "__main__":
    sys.exit(main())