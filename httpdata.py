__author__ = 'kmadac'

import requests
from BeautifulSoup import BeautifulSoup
import re
import os
import dataparser

year = re.compile("^2\d\d\d$")
day = re.compile("[0|1|2|3]\d\d$")
hour = re.compile("log_(\d\d)h00m.gz$")
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
    r = requests.get(url + "/" + str(year))
    if r.status_code == 200:
        parsed = BeautifulSoup(r.text)
        links = parsed.findAll('a')
        list_days = [int(link.text) for link in links if day.match(link.text)]
        return sorted(list_days)
    else:
        r.raise_for_status()


def get_hours(url, year, day):
    """
    Return list of hour data files found on page as hours
    """
    r = requests.get(url + "/" + str(year) + "/" + str(day))
    if r.status_code == 200:
        parsed = BeautifulSoup(r.text)
        links = parsed.findAll('a')
        list_hours = [int(hour.match(link.text).groups()[0]) for link in links if hour.match(link.text)]
        return sorted(list_hours)
    else:
        r.raise_for_status()


def get_files(url, year, day):
    """
    Return list of hour data files found on page
    """
    r = requests.get(url + "/" + str(year) + "/" + str(day))
    if r.status_code == 200:
        parsed = BeautifulSoup(r.text)
        links = parsed.findAll('a')
        list_files = [link.text for link in links if files.match(link.text)]
        return sorted(list_files)
    else:
        r.raise_for_status()


def get_data(url, year, day, filename, destination_dir):
    """
    Reads file specified by year, day and filename paramaters and returns list of parsed dictionaries
    Look into dataparser.py module for dictionary format
    """
    http_path = '{0}/{1}/{2}/{3}'.format(url, year, day, filename)
    file_name = _download_file(http_path, local_path=destination_dir)
    measured_data = []
    with open(os.path.join(destination_dir, file_name)) as f:
        for line in f.xreadlines():
            parsed_line = dataparser.parse_line(line)
            if parsed_line:
                measured_data.append(parsed_line)

    os.unlink(os.path.join(destination_dir, file_name))
    return measured_data


