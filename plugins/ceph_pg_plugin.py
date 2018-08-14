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
#   This plugin collects information regarding Ceph Placement Groups (PGs).
#
# collectd:
#   http://collectd.org
# collectd-python:
#   http://collectd.org/documentation/manpages/collectd-python.5.shtml
#

import collectd
import json
import traceback
import subprocess
import requests

import base


class CephPGPlugin(base.Base):

    def __init__(self):
        base.Base.__init__(self)
        self.prefix = 'ceph'

    def get_stats(self):
        """Retrieves stats from ceph pgs"""
        json_data = None

        ceph_cluster = "%s-%s" % (self.prefix, self.cluster)

        data = {ceph_cluster: {'pg': {}}}
        try:
            if self.rest:
                json_data = self.get_stats_via_rest()
            else:
                json_data = self.get_stats_via_tool()
        except Exception as exc:
            collectd.error("ceph-pg: failed to ceph pg dump :: %s :: %s"
                           % (exc, traceback.format_exc()))
            return

        if json_data is None:
            collectd.error('ceph-pg: failed to ceph osd dump :: output was None')
            return

        pg_data = data[ceph_cluster]['pg']

        # Number of pgs in each possible state
        for pg in json_data['pg_stats']:
            for state in pg['state'].split('+'):
                if state not in pg_data:
                    pg_data[state] = 0
                pg_data[state] += 1

        # osd perf data
        for osd in json_data['osd_stats']:
            data[ceph_cluster]["osd-%s" % osd['osd']] = {
                'kb_total':  osd['kb'],
                'kb_used': osd['kb_used'],
                'num_snap_trimming':  osd['num_snap_trimming'],
                'snap_trim_queue_len': osd['snap_trim_queue_len'],
                'apply_latency_ms':  osd['fs_perf_stat']['apply_latency_ms'],
                'commit_latency_ms':  osd['fs_perf_stat']['commit_latency_ms'],
            }

        return data

    def get_stats_via_tool(self):
        """Retrieves stats using a subprocess."""
        raw_output = subprocess.check_output(
            ['ceph', 'pg', 'dump', '--format', 'json', '--cluster ', self.cluster])
        return json.loads(raw_output)

    def get_stats_via_rest(self):
        """Retrieves stats using a request."""
        response = requests.get(
            "http://{host}:{port}/api/v0.1/pg/dump".format(
                host=self.host,
                port=self.port
            ),
            headers={"Accept": "application/json"}
        )
        response.raise_for_status()
        return response.json()["output"]


try:
    plugin = CephPGPlugin()
except Exception as exc:
    collectd.error("ceph-pg: failed to initialize ceph pg plugin :: %s :: %s"
                   % (exc, traceback.format_exc()))


def configure_callback(conf):
    """Received configuration information"""
    plugin.config_callback(conf)


def read_callback():
    """Callback triggered by collectd on read"""
    plugin.read_callback()


collectd.register_config(configure_callback)
collectd.register_read(read_callback, plugin.interval)
