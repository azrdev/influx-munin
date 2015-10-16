#!/usr/bin/env python

"""Read a directory of munin RRD files and push their data to InfluxDB
"""

import sys
import rrdxml
from lxml import etree
from influxdb import InfluxDBClient
import os.path
import subprocess

DS_TYPES = {
    'a': 'absolute',
    'c': 'counter',
    'd': 'derive',
    'g': 'gauge',
}


def getMeasurementName(rrdfile):
    """Extract from the RRD filename the group, node, plugin, and field.

    See http://munin-monitoring.org/wiki/MuninFileNames
    """
    _path, _filename = os.path.split(rrdfile)
    group = os.path.basename(_path)
    _rrdstring, _ext = os.path.splitext(_filename)
    node, service, field, ds_type = _rrdstring.split('-')
    ds_type = DS_TYPES[ds_type]
    return (group, node, service, field, ds_type)


def rrd2xml(rrdfile):
    """Return the XML representation of the file with name rrdfile, as returned
    by rrdtool dump
    """
    # official rrdtool python binding is broken
    # see https://github.com/oetiker/rrdtool-1.x/issues/676
    return subprocess.check_output(['rrdtool', 'dump', rrdfile])


def processRRD(rrdfile, influxClient):
    """Push the data in the file with name rrdfile to the database influxClient
    is connected to
    """
    group, node, service, field, ds_type = getMeasurementName(rrdfile)
    measurement_name = ".".join([group, node, service, field])

    xml = rrd2xml(rrdfile)
    tree = etree.fromstring(xml)

    for cf, timestamp, value in rrdxml.cdps(tree):
        # FIXME: value garbage if plugin contains a CDEF (post-processing formula)
        body = [{
            "measurement": measurement_name,
            "tags": {
                "rrd_cf": cf,
            },
            "time": timestamp,
            "fields": {
                "value": value,
            },
        }]
        influxClient.write_points(
                body,
                time_precision="s",
                tags={"ds_type": ds_type, "source": "munin"},
                batch_size=50)


if __name__ == "__main__":
    # TODO: traverse directories
    rrdfile = sys.argv[1]
    # TODO: influx options database, login, ...
    processRRD(rrdfile, InfluxDBClient(database='test'))
