__author__ = 'kmadac'
import datetime
import re

lineregexp = re.compile("^(\d{10}.\d{9})\s+(-?\d+)\s+\d+\s+\d+\s+\d+\s+(\d+)\s+(\d+)$")


def parse_line(line):
    """
    Line parameter is string in format:
    '1387666814.801992337         -51 10100000  287  286  900  943'

    Return dictionary in following format:
    {'datetime': datetime of measurement, 'deviation': integer, 'pressure': integer }
    Return None if format of line is not parsable
    """
    result = lineregexp.match(line)
    result_dict = None
    if result:
        result_dict = {'datetime': datetime.datetime.fromtimestamp(float(result.groups()[0])),
                       'deviation': int(result.groups()[1]), 'pressure': int(result.groups()[3])}

    return result_dict

