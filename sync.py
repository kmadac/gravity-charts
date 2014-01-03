__author__ = 'kmadac'

from gravity import store, httpdata

import logging
import config
import sys
from couchbase import Couchbase, exceptions

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
lhand = logging.StreamHandler()
logger.addHandler(lhand)


def main():
    cbclient = Couchbase.connect(host=config.DB_SERVER, bucket="default", quiet=True)

    for idx, sensor in enumerate(config.HTTP_SENSORS):
        last_record = store.get_last_record(cbclient, idx)
        logger.info(last_record)

        if not last_record:
            year = 2013
            days = httpdata.get_days(sensor, year)
            day = days[0]
            files = httpdata.get_files(sensor, year, day)
            filename = files[0]
        else:
            year, day, filename = last_record
            year, day = int(year), int(day)
            files = httpdata.get_files(sensor, year, day)
            year, day, filename, files = httpdata.next_file(sensor, year, day, filename, files)

        if year and day and filename and files:
            while True:
                logger.debug('Sensor:{0} Year:{1} Day:{2} filename:{3}'.format(idx, year, day, filename))
                data = httpdata.get_data(sensor, year, day, filename, '/media/kmadac/EXT_HDD_80GB')
                # if not data:
                #     logger.info("No data parsed in {0}/{1}/{2}/{3}".format(sensor, year, day, filename))
                #     break
                if data:
                    logger.debug('Data first:{0} Data last:{1}'.format(data[0], data[-1]))

                    temp_count = 0
                    while temp_count < 3:
                        try:
                            store.add_measurements(cbclient, data, idx)
                            store.set_last_record(cbclient, idx, year, day, filename)
                            break
                        except exceptions.TemporaryFailError:
                            logger.debug('Temporary DB error.')
                            temp_count += 1

                    year, day, filename, files = httpdata.next_file(sensor, year, day, filename, files)
                    if not year and not day and not filename and not files:
                        break

    return 0


if __name__ == "__main__":
    sys.exit(main())