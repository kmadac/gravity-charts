__author__ = 'kmadac'

import logging

import config
import sys
import redis
import datetime

import httpdata
import store


def next_day(year, day):
    last_day = datetime.datetime.strptime("{0} {1}".format(year, day), "%Y %j")
    day_later = last_day + datetime.timedelta(days=1)
    day_later = day_later.strftime("%Y %j").split()
    day_later = [int(i) for i in day_later]
    return day_later


def next_file(url, year, day, filename, filelist):
    """
    Returns year, day and filename of next measurement data.
    Function parameters are values of current measurement data.
    """
    try:
        filename_index = filelist.index(filename)
    except ValueError:
        return year, day, None, filelist

    if len(filelist) - 1 == filename_index:
        year, day = next_day(year, day)
        filelist = httpdata.get_files(url, year, day)
        if filelist:
            return year, day, filelist[0], filelist
        else:
            return year, day, None, filelist
    else:
        return year, day, filelist[filename_index + 1], filelist


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
        year, day, filename, files = next_file(config.HTTP_SENSORS[0], year, day, filename, files)

    while True:
        logger.debug('Day:{0} filename:{1}'.format(day, filename))
        data = httpdata.get_data(config.HTTP_SENSORS[0], year, day, filename, '/tmp')
        if not data:
            logger.info("No data parsed in {0}/{1}/{2}/{3}".format(config.HTTP_SENSORS[0], year, day, filename))
            break
        logger.debug('Data first:{0} Data last:{1}'.format(data[0], data[-1]))

        store.add_measurements(r_server, data)
        store.set_last_record(r_server, year, day, filename)

        year, day, filename, files = next_file(config.HTTP_SENSORS[0], year, day, filename, files)

    return 0


if __name__ == "__main__":
    sys.exit(main())