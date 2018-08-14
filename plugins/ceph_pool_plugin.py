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
#   This plugin collects information regarding Ceph pools.
#
# collectd:
#   http://collectd.org
# collectd-python:
#   http://collectd.org/documentation/manpages/collectd-python.5.shtml
# ceph pools:
#   http://ceph.com/docs/master/rados/operations/pools/
#

import collectd
import json
import traceback
import subprocess
import requests

import base


class CephPoolPlugin(base.Base):

    def __init__(self):
        base.Base.__init__(self)
        self.prefix = 'ceph'

    def get_stats(self):
        """Retrieves stats from ceph pools"""

        ceph_cluster = "%s-%s" % (self.prefix, self.cluster)

        data = {ceph_cluster: {}}

        pool_stats_json = df_json = None
        try:
            if self.rest:
                pool_stats_json, df_json = self.get_stats_via_rest()
            else:
                pool_stats_json, df_json = self.get_stats_via_tool()

        except Exception as exc:
            collectd.error("ceph-pool: failed to ceph pool stats :: %s :: %s"
                           % (exc, traceback.format_exc()))
            return

        if pool_stats_json is None:
            collectd.error('ceph-pool: failed to ceph osd pool stats :: output was None')
            return

        if df_json is None:
            collectd.error('ceph-pool: failed to ceph df :: output was None')
            return

        # Push osd pool stats results
        for pool in pool_stats_json:
            pool_key = "pool-%s" % pool['pool_name']
            data[ceph_cluster][pool_key] = {}
            pool_data = data[ceph_cluster][pool_key]
            for stat in ('read_bytes_sec', 'write_bytes_sec', 'op_per_sec'):
                pool_data[stat] = pool['client_io_rate'][stat] if stat in pool['client_io_rate'] else 0

        # Push df results
        for pool in df_json['pools']:
            pool_data = data[ceph_cluster]["pool-%s" % pool['name']]
            for stat in ('bytes_used', 'kb_used', 'objects'):
                pool_data[stat] = pool['stats'][stat] if stat in pool['stats'] else 0

        # Push totals from df
        data[ceph_cluster]['cluster'] = {}
        if 'total_bytes' in df_json['stats']:
            # Ceph 0.84+
            data[ceph_cluster]['cluster']['total_space'] = int(df_json['stats']['total_bytes'])
            data[ceph_cluster]['cluster']['total_used'] = int(df_json['stats']['total_used_bytes'])
            data[ceph_cluster]['cluster']['total_avail'] = int(df_json['stats']['total_avail_bytes'])
        else:
            # Ceph < 0.84
            data[ceph_cluster]['cluster']['total_space'] = int(df_json['stats']['total_space']) * 1024.0
            data[ceph_cluster]['cluster']['total_used'] = int(df_json['stats']['total_used']) * 1024.0
            data[ceph_cluster]['cluster']['total_avail'] = int(df_json['stats']['total_avail']) * 1024.0

        return data

    def get_stats_via_tool(self):
        """Retrieves stats using subprocess commands."""
        raw_output = subprocess.check_output(
            ['ceph', 'osd', 'pool', 'stats', '-f', 'json', '--cluster ', self.cluster])
        pool_stats_json = json.loads(raw_output)

        raw_output = subprocess.check_output(
            ['ceph', 'df', '-f', 'json', '--cluster ', self.cluster])
        df_json = json.loads(raw_output)

        return pool_stats_json, df_json

    def get_stats_via_rest(self):
        """Retrieves stats using requests."""
        response = requests.get(
            "http://{host}:{port}/api/v0.1/df".format(
                host=self.host,
                port=self.port
            ),
            headers={"Accept": "application/json"}
        )
        response.raise_for_status()
        df_json = response.json()["output"]

        response = requests.get(
            "http://{host}:{port}/api/v0.1/osd/pool/stats".format(
                host=self.host,
                port=self.port
            ),
            headers={"Accept": "application/json"}
        )
        response.raise_for_status()
        pool_stats_json = response.json()["output"]

        return pool_stats_json, df_json

try:
    plugin = CephPoolPlugin()
except Exception as exc:
    collectd.error("ceph-pool: failed to initialize ceph pool plugin :: %s :: %s"
                   % (exc, traceback.format_exc()))


def configure_callback(conf):
    """Received configuration information"""
    plugin.config_callback(conf)


def read_callback():
    """Callback triggered by collectd on read"""
    plugin.read_callback()


collectd.register_config(configure_callback)
collectd.register_read(read_callback, plugin.interval)
