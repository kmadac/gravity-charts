__author__ = 'kmadac'

import unittest

from parser import *
import datetime


class TestParser(unittest.TestCase):
    def test_parseline(self):
        line = '1387666814.801992337         -51 10100000  287  286  900  943'
        result = parse_line(line)
        expected = {'datetime': datetime.datetime(2013, 12, 22, 0, 0, 14, 801992), 'deviation': -51,
                    'pressure': 943}

        self.assertEqual(expected, result)

if __name__ == '__main__':
    unittest.main()
