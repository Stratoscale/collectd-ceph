#!/usr/bin/env python
#
# vim: tabstop=4 shiftwidth=4

# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the
# Free Software Foundation; only version 2 of the License is applicable.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
#
# Authors:
#   Ricardo Rocha <ricardo@catalyst.net.nz>
#
# About this plugin:
#   This plugin collects information regarding Ceph OSDs.
#
# collectd:
#   http://collectd.org
# collectd-python:
#   http://collectd.org/documentation/manpages/collectd-python.5.shtml
# ceph osds:
#   http://ceph.com/docs/master/rados/operations/monitoring/#checking-osd-status
#

import collectd
import json
import traceback
import subprocess
import requests

import base


class CephOsdPlugin(base.Base):

    def __init__(self):
        base.Base.__init__(self)
        self.prefix = 'ceph'

    def get_stats(self):
        """Retrieves stats from ceph osds"""
        json_data = None

        ceph_cluster = "%s-%s" % (self.prefix, self.cluster)
        data = {ceph_cluster: {
            'pool': {'number': 0},
            'osd': {'up': 0, 'in': 0, 'down': 0, 'out': 0}
        }}
        try:
            if self.rest:
                json_data = self.get_stats_via_rest()
            else:
                json_data = self.get_stats_via_tool()
        except Exception as exc:
            collectd.error("ceph-osd: failed to ceph osd dump :: %s :: %s"
                           % (exc, traceback.format_exc()))
            return

        if json_data is None:
            collectd.error('ceph-osd: failed to ceph osd dump :: output was None')
            return

        # Number of pools
        data[ceph_cluster]['pool']['number'] = len(json_data['pools'])

        # Pool metadata
        for pool in json_data['pools']:
            data[ceph_cluster]["pool-%s" % pool['pool_name']] = {
                'size': pool['size'],
                'pg_num': pool['pg_num'],
                'pgp_num': pool['pg_placement_num'],
            }

        osd_data = data[ceph_cluster]['osd']

        # Number of osds in each possible state
        for osd in json_data['osds']:
            if osd['up'] == 1:
                osd_data['up'] += 1
            else:
                osd_data['down'] += 1
            if osd['in'] == 1:
                osd_data['in'] += 1
            else:
                osd_data['out'] += 1

        return data

    def get_stats_via_tool(self):
        """Retrieves stats using a subprocess."""
        raw_output = subprocess.check_output(
            ['ceph', 'osd', 'dump', '--format', 'json', '--cluster ', self.cluster])
        return json.loads(raw_output)

    def get_stats_via_rest(self):
        """Retrieves stats using a request."""
        response = requests.get(
            "http://{host}:{port}/api/v0.1/osd/dump".format(
                host=self.host,
                port=self.port
            ),
            headers={"Accept": "application/json"}
        )
        response.raise_for_status()
        return response.json()["output"]


try:
    plugin = CephOsdPlugin()
except Exception as exc:
    collectd.error("ceph-osd: failed to initialize ceph osd plugin :: %s :: %s"
                   % (exc, traceback.format_exc()))


def configure_callback(conf):
    """Received configuration information"""
    plugin.config_callback(conf)


def read_callback():
    """Callback triggered by collectd on read"""
    plugin.read_callback()


collectd.register_config(configure_callback)
collectd.register_read(read_callback, plugin.interval)
