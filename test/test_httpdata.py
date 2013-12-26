__author__ = 'kmadac'

import unittest
import httpdata


class HttpDataCase(unittest.TestCase):
    def test_getyears(self):
        years = httpdata.get_years('http://192.168.123.179/kyvadlo/DATA/kyvadlo/LOG/kyvadlo_i2c-0_0x60')
        self.assertEqual(years[:2], [2000, 2013])

    def test_getdays(self):
        days = httpdata.get_days('http://192.168.123.179/kyvadlo/DATA/kyvadlo/LOG/kyvadlo_i2c-0_0x60', 2013)
        self.assertEqual(days[0:2], [219, 220])

    def test_gethours(self):
        hours = httpdata.get_hours('http://192.168.123.179/kyvadlo/DATA/kyvadlo/LOG/kyvadlo_i2c-0_0x60', 2013, 219)
        self.assertEqual(hours[0], 12)

if __name__ == '__main__':
    unittest.main()
