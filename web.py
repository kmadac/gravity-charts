__author__ = 'kmadac'

import config

from flask import Flask, render_template
from gravity import chartdata, store
from couchbase import Couchbase
import datetime
import time
import chartkick


def connect_db():
    return Couchbase.connect(host=config.DB_SERVER, bucket="default", quiet=True)

db = connect_db()

app = Flask('gravity-charts', static_folder=chartkick.js(), static_url_path='/static')
app.jinja_env.add_extension("chartkick.ext.charts")

@app.route('/')
def index():
    starttime = datetime.datetime(year=2013, month=8, day=9, hour=20)
    endtime = datetime.datetime(year=2013, month=8, day=9, hour=23)

    last_update_raw = store.get_last_record(db, 0)
    last_update_datetime = datetime.datetime.strptime("{0} {1}".format(last_update_raw[0], last_update_raw[1]),
                                                      "%Y %j")
    last_update = last_update_datetime.strftime("%d.%m.%Y")

    granularity = (int(time.mktime(endtime.timetuple())) - int(time.mktime(starttime.timetuple()))) / config.NUMBER_OF_POINTS

    deviation1, pressure1 = chartdata.get(db, 0, starttime, endtime, granularity=granularity)
    deviation2, pressure2 = chartdata.get(db, 1, starttime, endtime, granularity=granularity)

    data = [{'data': deviation1, 'name': 'Deviation 0x60'},
            {'data': deviation2, 'name': 'Deviation 0x61'}, {'data': pressure2, 'name': 'Pressure 0x61'}]

    return render_template('index.html', data=data, last_update=last_update)


if __name__ == '__main__':
    app.run(debug=True)