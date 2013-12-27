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

    for idx, sensor in enumerate(config.HTTP_SENSORS):
        r_server = redis.Redis(config.REDIS_SERVER, db=idx)

        last_record = store.get_last_record(r_server)
        logger.info(last_record)

        if not last_record:
            year = 2013
            day = httpdata.get_days(sensor, year)[0]
            files = httpdata.get_files(sensor, year, day)
            filename = files[0]
        else:
            year, day, filename = last_record
            files = httpdata.get_files(sensor, year, day)
            year, day, filename, files = httpdata.next_file(sensor, year, day, filename, files)

        while True:
            logger.debug('Day:{0} filename:{1}'.format(day, filename))
            data = httpdata.get_data(sensor, year, day, filename, '/tmp')
            if not data:
                logger.info("No data parsed in {0}/{1}/{2}/{3}".format(sensor, year, day, filename))
                break
            logger.debug('Data first:{0} Data last:{1}'.format(data[0], data[-1]))

            store.add_measurements(r_server, data)
            store.set_last_record(r_server, year, day, filename)

            year, day, filename, files = httpdata.next_file(sensor, year, day, filename, files)

    return 0


if __name__ == "__main__":
    sys.exit(main())