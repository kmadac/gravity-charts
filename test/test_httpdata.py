__author__ = 'kmadac'

import unittest
import httpdata
import config


class HttpDataCase(unittest.TestCase):
    def test_getyears(self):
        years = httpdata.get_years(config.HTTP_SENSORS[0])
        self.assertEqual(years[:2], [2000, 2013])

    def test_getdays(self):
        days = httpdata.get_days(config.HTTP_SENSORS[0], 2013)
        self.assertEqual(days[0:2], [219, 220])

    def test_gethours(self):
        hours = httpdata.get_hours(config.HTTP_SENSORS[0], 2013, 219)
        self.assertEqual(hours[0], 12)

    def test_getfiles(self):
        files = httpdata.get_files(config.HTTP_SENSORS[0], 2013, 219)
        self.assertEqual(files[0], 'log_11h57m.gz')

if __name__ == '__main__':
    unittest.main()
