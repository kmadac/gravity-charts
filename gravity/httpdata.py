from gravity import dataparser

__author__ = 'kmadac'

import requests
from BeautifulSoup import BeautifulSoup
import re
import os
import datetime


year = re.compile("^2\d\d\d$")
day = re.compile("[0|1|2|3]\d\d$")
files = re.compile("log_\d\dh\d\dm.gz$")


def _download_file(url, local_path='.'):
    local_filename = url.split('/')[-1]
    # NOTE the stream=True parameter
    r = requests.get(url, stream=True)
    with open(os.path.join(local_path, local_filename), 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
                f.flush()
    return local_filename


def get_years(url):
    """
    Return sorted list of years found on page
    """
    r = requests.get(url)
    if r.status_code == 200:
        parsed = BeautifulSoup(r.text)
        links = parsed.findAll('a')
        list_years = [int(link.text) for link in links if year.match(link.text)]
        return sorted(list_years)
    else:
        r.raise_for_status()


def get_days(url, year):
    """
    Return list of days found on page
    """
    # r = requests.get(url + "/" + str(year))
    r = requests.get('{0}/{1}/'.format(url, year))
    if r.status_code == 200:
        parsed = BeautifulSoup(r.text)
        links = parsed.findAll('a')
        list_days = [int(link.text) for link in links if day.match(link.text)]
        return sorted(list_days)
    else:
        r.raise_for_status()


def get_files(url, year, day):
    """
    Return list of hour data files found on page
    """

    # r = requests.get(url + "/" + str(year) + "/" + str(day))
    r = requests.get('{0}/{1}/{2:03}'.format(url, year, day))
    if r.status_code == 200:
        parsed = BeautifulSoup(r.text)
        links = parsed.findAll('a')
        list_files = [link.text for link in links if files.match(link.text)]
        return sorted(list_files)
    else:
        return None


def get_data(url, year, day, filename, destination_dir):
    """
    Reads file specified by year, day and filename paramaters and returns list of parsed dictionaries
    Look into dataparser.py module for dictionary format
    """
    # http_path = '{0}/{1}/{2}/{3}'.format(url, year, day, filename)
    http_path = '{0}/{1}/{2:03}/{3}'.format(url, year, day, filename)
    file_name = _download_file(http_path, local_path=destination_dir)
    measured_data = []
    with open(os.path.join(destination_dir, file_name)) as f:
        for line in f.xreadlines():
            parsed_line = dataparser.parse_line(line)
            if parsed_line:
                measured_data.append(parsed_line)

    os.unlink(os.path.join(destination_dir, file_name))
    return measured_data


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
        new_year, new_day = next_day(year, day)
        filelist = get_files(url, new_year, new_day)
        if filelist:
            # if new_day exists
            return new_year, new_day, filelist[0], filelist
        else:
            # if new day doesn't exists, let's check whether measurements for one day is missing,
            # or there are no data available anymore
            dayslist = get_days(url, year)
            if len(dayslist) - 1 == dayslist.index(day):
                return None, None, None, None
            else:
                day_index = dayslist.index(day)
                new_day = dayslist[day_index + 1]
                filelist = get_files(url, new_year, new_day)
                return new_year, new_day, filelist[0], filelist
    else:
        return year, day, filelist[filename_index + 1], filelist

