#    Copyright (c) 2020 John A Kline <john@johnkline.com>
#
#    See the file LICENSE.txt for your full rights.
#
"""Test processing packets."""

import configobj
import logging
import os
import time
import unittest

import weewx
import weewx.accum
from weeutil.weeutil import to_int
from weeutil.weeutil import timestamp_to_string

from typing import Any, Dict

import weeutil.logger

import user.loopdata
import cc3000_packets
import cc3000_cross_midnight_packets
import ip100_packets
import simulator_packets

log = logging.getLogger(__name__)

# Set up logging using the defaults.
weeutil.logger.setup('test_config', {})

class ProcessPacketTests(unittest.TestCase):

    def test_parse_cname(self):
        self.maxDiff = None

        cname = user.loopdata.LoopData.parse_cname('unit.label.outTemp')
        self.assertEqual(cname.field, 'unit.label.outTemp')
        self.assertEqual(cname.prefix, 'unit')
        self.assertEqual(cname.prefix2, 'label')
        self.assertEqual(cname.period, None)
        self.assertEqual(cname.obstype, 'outTemp')
        self.assertEqual(cname.agg_type, None)
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('2m.windGust.max.raw')
        self.assertEqual(cname.field, '2m.windGust.max.raw')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, '2m')
        self.assertEqual(cname.obstype, 'windGust')
        self.assertEqual(cname.agg_type, 'max')
        self.assertEqual(cname.format_spec, 'raw')

        cname = user.loopdata.LoopData.parse_cname('2m')
        self.assertEqual(cname, None)

        cname = user.loopdata.LoopData.parse_cname('2m.wind')
        self.assertEqual(cname, None)

        cname = user.loopdata.LoopData.parse_cname('2m.wind.max.formatted.foo')
        self.assertEqual(cname, None)

        cname = user.loopdata.LoopData.parse_cname('10m.windGust.max.raw')
        self.assertEqual(cname.field, '10m.windGust.max.raw')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, '10m')
        self.assertEqual(cname.obstype, 'windGust')
        self.assertEqual(cname.agg_type, 'max')
        self.assertEqual(cname.format_spec, 'raw')

        cname = user.loopdata.LoopData.parse_cname('10m')
        self.assertEqual(cname, None)

        cname = user.loopdata.LoopData.parse_cname('10m.wind')
        self.assertEqual(cname, None)

        cname = user.loopdata.LoopData.parse_cname('10m.wind.max.formatted.foo')
        self.assertEqual(cname, None)

        cname = user.loopdata.LoopData.parse_cname('current')
        self.assertEqual(cname, None)

        cname = user.loopdata.LoopData.parse_cname('current.wind.max.formatted.foo')
        self.assertEqual(cname, None)

        cname = user.loopdata.LoopData.parse_cname('day')
        self.assertEqual(cname, None)

        cname = user.loopdata.LoopData.parse_cname('hour')
        self.assertEqual(cname, None)

        cname = user.loopdata.LoopData.parse_cname('day.wind')
        self.assertEqual(cname, None)

        cname = user.loopdata.LoopData.parse_cname('hour.wind')
        self.assertEqual(cname, None)

        cname = user.loopdata.LoopData.parse_cname('day.wind.max.formatted.foo')
        self.assertEqual(cname, None)

        cname = user.loopdata.LoopData.parse_cname('hour.wind.max.formatted.foo')
        self.assertEqual(cname, None)

        cname = user.loopdata.LoopData.parse_cname('trend')
        self.assertEqual(cname, None)

        cname = user.loopdata.LoopData.parse_cname('trend.wind.formatted.foo')
        self.assertEqual(cname, None)

        cname = user.loopdata.LoopData.parse_cname('week')
        self.assertEqual(cname, None)

        cname = user.loopdata.LoopData.parse_cname('month')
        self.assertEqual(cname, None)

        cname = user.loopdata.LoopData.parse_cname('year')
        self.assertEqual(cname, None)

        cname = user.loopdata.LoopData.parse_cname('rainyear')
        self.assertEqual(cname, None)

        cname = user.loopdata.LoopData.parse_cname('week.outTemp')
        self.assertEqual(cname, None)

        cname = user.loopdata.LoopData.parse_cname('week.windrun_ENE.sum.formatted')
        self.assertEqual(cname, None)

        cname = user.loopdata.LoopData.parse_cname('month.windrun_ENE.sum.formatted')
        self.assertEqual(cname, None)

        cname = user.loopdata.LoopData.parse_cname('year.windrun_ENE.sum.formatted')
        self.assertEqual(cname, None)

        cname = user.loopdata.LoopData.parse_cname('rainyear.windrun_ENE.sum.formatted')
        self.assertEqual(cname, None)

        cname = user.loopdata.LoopData.parse_cname('week.outTemp.avg')
        self.assertEqual(cname.field, 'week.outTemp.avg')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'week')
        self.assertEqual(cname.obstype, 'outTemp')
        self.assertEqual(cname.agg_type, 'avg')
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('month.outTemp.avg')
        self.assertEqual(cname.field, 'month.outTemp.avg')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'month')
        self.assertEqual(cname.obstype, 'outTemp')
        self.assertEqual(cname.agg_type, 'avg')
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('year.outTemp.avg')
        self.assertEqual(cname.field, 'year.outTemp.avg')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'year')
        self.assertEqual(cname.obstype, 'outTemp')
        self.assertEqual(cname.agg_type, 'avg')
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('rainyear.outTemp.avg')
        self.assertEqual(cname.field, 'rainyear.outTemp.avg')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'rainyear')
        self.assertEqual(cname.obstype, 'outTemp')
        self.assertEqual(cname.agg_type, 'avg')
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('week.windGust.max.formatted')
        self.assertEqual(cname.field, 'week.windGust.max.formatted')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'week')
        self.assertEqual(cname.obstype, 'windGust')
        self.assertEqual(cname.agg_type, 'max')
        self.assertEqual(cname.format_spec, 'formatted')

        cname = user.loopdata.LoopData.parse_cname('month.windGust.max.formatted')
        self.assertEqual(cname.field, 'month.windGust.max.formatted')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'month')
        self.assertEqual(cname.obstype, 'windGust')
        self.assertEqual(cname.agg_type, 'max')
        self.assertEqual(cname.format_spec, 'formatted')

        cname = user.loopdata.LoopData.parse_cname('year.windGust.max.formatted')
        self.assertEqual(cname.field, 'year.windGust.max.formatted')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'year')
        self.assertEqual(cname.obstype, 'windGust')
        self.assertEqual(cname.agg_type, 'max')
        self.assertEqual(cname.format_spec, 'formatted')

        cname = user.loopdata.LoopData.parse_cname('rainyear.windGust.max.formatted')
        self.assertEqual(cname.field, 'rainyear.windGust.max.formatted')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'rainyear')
        self.assertEqual(cname.obstype, 'windGust')
        self.assertEqual(cname.agg_type, 'max')
        self.assertEqual(cname.format_spec, 'formatted')

        cname = user.loopdata.LoopData.parse_cname('2m.windGust.max.formatted')
        self.assertEqual(cname.field, '2m.windGust.max.formatted')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, '2m')
        self.assertEqual(cname.obstype, 'windGust')
        self.assertEqual(cname.agg_type, 'max')
        self.assertEqual(cname.format_spec, 'formatted')

        cname = user.loopdata.LoopData.parse_cname('10m.windGust.max.formatted')
        self.assertEqual(cname.field, '10m.windGust.max.formatted')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, '10m')
        self.assertEqual(cname.obstype, 'windGust')
        self.assertEqual(cname.agg_type, 'max')
        self.assertEqual(cname.format_spec, 'formatted')

        cname = user.loopdata.LoopData.parse_cname('10m.windGust.maxtime')
        self.assertEqual(cname.field, '10m.windGust.maxtime')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, '10m')
        self.assertEqual(cname.obstype, 'windGust')
        self.assertEqual(cname.agg_type, 'maxtime')
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('10m.outTemp.max.raw')
        self.assertEqual(cname.field, '10m.outTemp.max.raw')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, '10m')
        self.assertEqual(cname.obstype, 'outTemp')
        self.assertEqual(cname.agg_type, 'max')
        self.assertEqual(cname.format_spec, 'raw')

        cname = user.loopdata.LoopData.parse_cname('10m.outTemp.max.formatted')
        self.assertEqual(cname.field, '10m.outTemp.max.formatted')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, '10m')
        self.assertEqual(cname.obstype, 'outTemp')
        self.assertEqual(cname.agg_type, 'max')
        self.assertEqual(cname.format_spec, 'formatted')

        cname = user.loopdata.LoopData.parse_cname('10m.outTemp.maxtime')
        self.assertEqual(cname.field, '10m.outTemp.maxtime')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, '10m')
        self.assertEqual(cname.obstype, 'outTemp')
        self.assertEqual(cname.agg_type, 'maxtime')
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('unit.label.wind')
        self.assertEqual(cname.field, 'unit.label.wind')
        self.assertEqual(cname.prefix, 'unit')
        self.assertEqual(cname.prefix2, 'label')
        self.assertEqual(cname.period, None)
        self.assertEqual(cname.obstype, 'wind')
        self.assertEqual(cname.agg_type, None)
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('trend.barometer')
        self.assertEqual(cname.field, 'trend.barometer')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'trend')
        self.assertEqual(cname.obstype, 'barometer')
        self.assertEqual(cname.agg_type, None)
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('trend.barometer.formatted')
        self.assertEqual(cname.field, 'trend.barometer.formatted')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'trend')
        self.assertEqual(cname.obstype, 'barometer')
        self.assertEqual(cname.agg_type, None)
        self.assertEqual(cname.format_spec, 'formatted')

        cname = user.loopdata.LoopData.parse_cname('trend.barometer.code')
        self.assertEqual(cname.field, 'trend.barometer.code')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'trend')
        self.assertEqual(cname.obstype, 'barometer')
        self.assertEqual(cname.agg_type, None)
        self.assertEqual(cname.format_spec, 'code')

        cname = user.loopdata.LoopData.parse_cname('trend.barometer.desc')
        self.assertEqual(cname.field, 'trend.barometer.desc')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'trend')
        self.assertEqual(cname.obstype, 'barometer')
        self.assertEqual(cname.agg_type, None)
        self.assertEqual(cname.format_spec, 'desc')

        cname = user.loopdata.LoopData.parse_cname('trend.outTemp')
        self.assertEqual(cname.field, 'trend.outTemp')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'trend')
        self.assertEqual(cname.obstype, 'outTemp')
        self.assertEqual(cname.agg_type, None)
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('trend.outTemp.formatted')
        self.assertEqual(cname.field, 'trend.outTemp.formatted')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'trend')
        self.assertEqual(cname.obstype, 'outTemp')
        self.assertEqual(cname.agg_type, None)
        self.assertEqual(cname.format_spec, 'formatted')

        cname = user.loopdata.LoopData.parse_cname('trend.dewpoint')
        self.assertEqual(cname.field, 'trend.dewpoint')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'trend')
        self.assertEqual(cname.obstype, 'dewpoint')
        self.assertEqual(cname.agg_type, None)
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('trend.dewpoint.formatted')
        self.assertEqual(cname.field, 'trend.dewpoint.formatted')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'trend')
        self.assertEqual(cname.obstype, 'dewpoint')
        self.assertEqual(cname.agg_type, None)
        self.assertEqual(cname.format_spec, 'formatted')

        cname = user.loopdata.LoopData.parse_cname('current.outTemp')
        self.assertEqual(cname.field, 'current.outTemp')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'current')
        self.assertEqual(cname.obstype, 'outTemp')
        self.assertEqual(cname.agg_type, None)
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('current.dateTime')
        self.assertEqual(cname.field, 'current.dateTime')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'current')
        self.assertEqual(cname.obstype, 'dateTime')
        self.assertEqual(cname.agg_type, None)
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('current.dateTime.raw')
        self.assertEqual(cname.field, 'current.dateTime.raw')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'current')
        self.assertEqual(cname.obstype, 'dateTime')
        self.assertEqual(cname.agg_type, None)
        self.assertEqual(cname.format_spec, 'raw')

        cname = user.loopdata.LoopData.parse_cname('current.windSpeed')
        self.assertEqual(cname.field, 'current.windSpeed')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'current')
        self.assertEqual(cname.obstype, 'windSpeed')
        self.assertEqual(cname.agg_type, None)
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('current.windSpeed.ordinal_compass')
        self.assertEqual(cname.field, 'current.windSpeed.ordinal_compass')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'current')
        self.assertEqual(cname.obstype, 'windSpeed')
        self.assertEqual(cname.agg_type, None)
        self.assertEqual(cname.format_spec, 'ordinal_compass')

        cname = user.loopdata.LoopData.parse_cname('day.rain.sum')
        self.assertEqual(cname.field, 'day.rain.sum')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'day')
        self.assertEqual(cname.obstype, 'rain')
        self.assertEqual(cname.agg_type, 'sum')
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('hour.rain.sum')
        self.assertEqual(cname.field, 'hour.rain.sum')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'hour')
        self.assertEqual(cname.obstype, 'rain')
        self.assertEqual(cname.agg_type, 'sum')
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('day.rain.sum.raw')
        self.assertEqual(cname.field, 'day.rain.sum.raw')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'day')
        self.assertEqual(cname.obstype, 'rain')
        self.assertEqual(cname.agg_type, 'sum')
        self.assertEqual(cname.format_spec, 'raw')

        cname = user.loopdata.LoopData.parse_cname('hour.rain.sum.raw')
        self.assertEqual(cname.field, 'hour.rain.sum.raw')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'hour')
        self.assertEqual(cname.obstype, 'rain')
        self.assertEqual(cname.agg_type, 'sum')
        self.assertEqual(cname.format_spec, 'raw')

        cname = user.loopdata.LoopData.parse_cname('day.rain.formatted')
        self.assertEqual(cname, None)

        cname = user.loopdata.LoopData.parse_cname('day.windGust.max')
        self.assertEqual(cname.field, 'day.windGust.max')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'day')
        self.assertEqual(cname.obstype, 'windGust')
        self.assertEqual(cname.agg_type, 'max')
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('day.windDir.max')
        self.assertEqual(cname.field, 'day.windDir.max')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'day')
        self.assertEqual(cname.obstype, 'windDir')
        self.assertEqual(cname.agg_type, 'max')
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('day.wind.maxtime')
        self.assertEqual(cname.field, 'day.wind.maxtime')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'day')
        self.assertEqual(cname.obstype, 'wind')
        self.assertEqual(cname.agg_type, 'maxtime')
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('day.wind.max')
        self.assertEqual(cname.field, 'day.wind.max')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'day')
        self.assertEqual(cname.obstype, 'wind')
        self.assertEqual(cname.agg_type, 'max')
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('day.wind.gustdir')
        self.assertEqual(cname.field, 'day.wind.gustdir')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'day')
        self.assertEqual(cname.obstype, 'wind')
        self.assertEqual(cname.agg_type, 'gustdir')
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('day.wind.vecavg')
        self.assertEqual(cname.field, 'day.wind.vecavg')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'day')
        self.assertEqual(cname.obstype, 'wind')
        self.assertEqual(cname.agg_type, 'vecavg')
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('day.wind.vecdir')
        self.assertEqual(cname.field, 'day.wind.vecdir')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'day')
        self.assertEqual(cname.obstype, 'wind')
        self.assertEqual(cname.agg_type, 'vecdir')
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('day.wind.rms')
        self.assertEqual(cname.field, 'day.wind.rms')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'day')
        self.assertEqual(cname.obstype, 'wind')
        self.assertEqual(cname.agg_type, 'rms')
        self.assertEqual(cname.format_spec, None)


        cname = user.loopdata.LoopData.parse_cname('day.wind.avg')
        self.assertEqual(cname.field, 'day.wind.avg')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'day')
        self.assertEqual(cname.obstype, 'wind')
        self.assertEqual(cname.agg_type, 'avg')
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('year.windrun.sum.formatted')
        self.assertEqual(cname.field, 'year.windrun.sum.formatted')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'year')
        self.assertEqual(cname.obstype, 'windrun')
        self.assertEqual(cname.agg_type, 'sum')
        self.assertEqual(cname.format_spec, 'formatted')

        cname = user.loopdata.LoopData.parse_cname('hour.windrun_ENE.sum.formatted')
        self.assertEqual(cname.field, 'hour.windrun_ENE.sum.formatted')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'hour')
        self.assertEqual(cname.obstype, 'windrun_ENE')
        self.assertEqual(cname.agg_type, 'sum')
        self.assertEqual(cname.format_spec, 'formatted')

        cname = user.loopdata.LoopData.parse_cname('day.windrun_W.sum')
        self.assertEqual(cname.field, 'day.windrun_W.sum')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'day')
        self.assertEqual(cname.obstype, 'windrun_W')
        self.assertEqual(cname.agg_type, 'sum')
        self.assertEqual(cname.format_spec, None)

    def test_compose_loop_data_dir(self):
        config_dict       : Dict[str, Any] = { 'WEEWX_ROOT'   : '/etc/weewx' }
        target_report_dict: Dict[str, Any] = { 'HTML_ROOT'    : 'public_html/weatherboard'}
        file_spec_dict    : Dict[str, Any] = { 'loop_data_dir': '.'}

        self.assertEqual(user.loopdata.LoopData.compose_loop_data_dir(
            config_dict, target_report_dict, file_spec_dict), '/etc/weewx/public_html/weatherboard/.')

        self.assertEqual(user.loopdata.LoopData.compose_loop_data_dir(
            config_dict, target_report_dict, {'loop_data_dir':'/var/weewx/loopdata'}), '/var/weewx/loopdata')

        self.assertEqual(user.loopdata.LoopData.compose_loop_data_dir(
            config_dict, {'HTML_ROOT':'/home/weewx/public_html/weatherboard'}, file_spec_dict), '/home/weewx/public_html/weatherboard/.')

        self.assertEqual(user.loopdata.LoopData.compose_loop_data_dir(
            config_dict, target_report_dict, {'loop_data_dir':'foobar'}), '/etc/weewx/public_html/weatherboard/foobar')

    def test_get_fields_to_include(self):

        specified_fields = [ 'current.dateTime.raw', 'current.outTemp', 'trend.barometer.code',
            'trend.barometer.desc', '2m.wind.max', '2m.wind.gustdir', '10m.wind.max', '10m.wind.gustdir', 'hour.inTemp.min', 'hour.inTemp.mintime',
            'day.barometer.min', 'day.barometer.max', 'day.wind.max', 'day.wind.gustdir', 'day.wind.maxtime' ]

        (fields_to_include, current_obstypes, trend_obstypes, rainyear_obstypes,
            year_obstypes, month_obstypes, week_obstypes, day_obstypes, hour_obstypes,
            ten_min_obstypes, two_min_obstypes) = user.loopdata.LoopData.get_fields_to_include(specified_fields)

        self.assertEqual(len(fields_to_include), 15)
        self.assertTrue(user.loopdata.CheetahName(
            'current.dateTime.raw', None, None, 'current', 'dateTime', None, 'raw') in fields_to_include)
        self.assertTrue(user.loopdata.CheetahName(
            'current.outTemp', None, None, 'current', 'outTemp', None, None) in fields_to_include)
        self.assertTrue(user.loopdata.CheetahName(
            'trend.barometer.code', None, None, 'trend', 'barometer', None, 'code') in fields_to_include)
        self.assertTrue(user.loopdata.CheetahName(
            'trend.barometer.desc', None, None, 'trend', 'barometer', None, 'desc') in fields_to_include)
        self.assertTrue(user.loopdata.CheetahName(
            '2m.wind.max', None, None, '2m', 'wind', 'max', None) in fields_to_include)
        self.assertTrue(user.loopdata.CheetahName(
            '2m.wind.gustdir', None, None, '2m', 'wind', 'gustdir', None) in fields_to_include)
        self.assertTrue(user.loopdata.CheetahName(
            '10m.wind.max', None, None, '10m', 'wind', 'max', None) in fields_to_include)
        self.assertTrue(user.loopdata.CheetahName(
            '10m.wind.gustdir', None, None, '10m', 'wind', 'gustdir', None) in fields_to_include)
        self.assertTrue(user.loopdata.CheetahName(
            'hour.inTemp.min', None, None, 'hour', 'inTemp', 'min', None) in fields_to_include)
        self.assertTrue(user.loopdata.CheetahName(
            'hour.inTemp.mintime', None, None, 'hour', 'inTemp', 'mintime', None) in fields_to_include)
        self.assertTrue(user.loopdata.CheetahName(
            'day.barometer.min', None, None, 'day', 'barometer', 'min', None) in fields_to_include)
        self.assertTrue(user.loopdata.CheetahName(
            'day.barometer.max', None, None, 'day', 'barometer', 'max', None) in fields_to_include)
        self.assertTrue(user.loopdata.CheetahName(
            'day.wind.max', None, None, 'day', 'wind', 'max', None) in fields_to_include)
        self.assertTrue(user.loopdata.CheetahName(
            'day.wind.gustdir', None, None, 'day', 'wind', 'gustdir', None) in fields_to_include)
        self.assertTrue(user.loopdata.CheetahName(
            'day.wind.maxtime', None, None, 'day', 'wind', 'maxtime', None) in fields_to_include)

        self.assertEqual(len(current_obstypes), 9)
        self.assertTrue('inTemp' in current_obstypes)
        self.assertTrue('outTemp' in current_obstypes)
        self.assertTrue('barometer' in current_obstypes)
        self.assertTrue('wind' in current_obstypes)
        self.assertTrue('windDir' in current_obstypes)
        self.assertTrue('windGust' in current_obstypes)
        self.assertTrue('windGustDir' in current_obstypes)
        self.assertTrue('windSpeed' in current_obstypes)

        self.assertEqual(len(trend_obstypes), 1)
        self.assertTrue('barometer' in trend_obstypes)

        self.assertEqual(len(ten_min_obstypes), 5)
        self.assertTrue('wind' in ten_min_obstypes)
        self.assertTrue('windDir' in ten_min_obstypes)
        self.assertTrue('windGust' in ten_min_obstypes)
        self.assertTrue('windGustDir' in ten_min_obstypes)
        self.assertTrue('windSpeed' in ten_min_obstypes)

        self.assertEqual(len(hour_obstypes), 1)
        self.assertTrue('inTemp' in hour_obstypes)

        self.assertEqual(len(day_obstypes), 6)
        self.assertTrue('barometer' in day_obstypes)
        self.assertTrue('wind' in day_obstypes)
        self.assertTrue('windDir' in day_obstypes)
        self.assertTrue('windGust' in day_obstypes)
        self.assertTrue('windGustDir' in day_obstypes)
        self.assertTrue('windSpeed' in day_obstypes)

    def test_get_barometer_trend_mbar(self):
        # Forecast descriptions for the 3 hour change in barometer readings.
        # Falling (or rising) slowly: 0.1 - 1.5mb in 3 hours
        # Falling (or rising): 1.6 - 3.5mb in 3 hours
        # Falling (or rising) quickly: 3.6 - 6.0mb in 3 hours
        # Falling (or rising) very rapidly: More than 6.0mb in 3 hours

        baroTrend = user.loopdata.LoopProcessor.get_barometer_trend(9.0, 'mbar', 'group_pressure', 10800)
        self.assertEqual(baroTrend, user.loopdata.BarometerTrend.RISING_VERY_RAPIDLY)

        baroTrend = user.loopdata.LoopProcessor.get_barometer_trend(6.1, 'mbar', 'group_pressure', 10800)
        self.assertEqual(baroTrend, user.loopdata.BarometerTrend.RISING_VERY_RAPIDLY)

        baroTrend = user.loopdata.LoopProcessor.get_barometer_trend(6.0, 'mbar', 'group_pressure', 10800)
        self.assertEqual(baroTrend, user.loopdata.BarometerTrend.RISING_QUICKLY)

        baroTrend = user.loopdata.LoopProcessor.get_barometer_trend(3.6, 'mbar', 'group_pressure', 10800)
        self.assertEqual(baroTrend, user.loopdata.BarometerTrend.RISING_QUICKLY)

        baroTrend = user.loopdata.LoopProcessor.get_barometer_trend(3.5, 'mbar', 'group_pressure', 10800)
        self.assertEqual(baroTrend, user.loopdata.BarometerTrend.RISING)

        baroTrend = user.loopdata.LoopProcessor.get_barometer_trend(1.6, 'mbar', 'group_pressure', 10800)
        self.assertEqual(baroTrend, user.loopdata.BarometerTrend.RISING)

        baroTrend = user.loopdata.LoopProcessor.get_barometer_trend(1.5, 'mbar', 'group_pressure', 10800)
        self.assertEqual(baroTrend, user.loopdata.BarometerTrend.RISING_SLOWLY)

        baroTrend = user.loopdata.LoopProcessor.get_barometer_trend(0.1, 'mbar', 'group_pressure', 10800)
        self.assertEqual(baroTrend, user.loopdata.BarometerTrend.RISING_SLOWLY)

        baroTrend = user.loopdata.LoopProcessor.get_barometer_trend(0.09, 'mbar', 'group_pressure', 10800)
        self.assertEqual(baroTrend, user.loopdata.BarometerTrend.STEADY)

        baroTrend = user.loopdata.LoopProcessor.get_barometer_trend(0.0, 'mbar', 'group_pressure', 10800)
        self.assertEqual(baroTrend, user.loopdata.BarometerTrend.STEADY)

        baroTrend = user.loopdata.LoopProcessor.get_barometer_trend(-0.09, 'mbar', 'group_pressure', 10800)
        self.assertEqual(baroTrend, user.loopdata.BarometerTrend.STEADY)

        baroTrend = user.loopdata.LoopProcessor.get_barometer_trend(-0.1, 'mbar', 'group_pressure', 10800)
        self.assertEqual(baroTrend, user.loopdata.BarometerTrend.FALLING_SLOWLY)

        baroTrend = user.loopdata.LoopProcessor.get_barometer_trend(-1.5, 'mbar', 'group_pressure', 10800)
        self.assertEqual(baroTrend, user.loopdata.BarometerTrend.FALLING_SLOWLY)

        baroTrend = user.loopdata.LoopProcessor.get_barometer_trend(-1.6, 'mbar', 'group_pressure', 10800)
        self.assertEqual(baroTrend, user.loopdata.BarometerTrend.FALLING)

        baroTrend = user.loopdata.LoopProcessor.get_barometer_trend(-3.5, 'mbar', 'group_pressure', 10800)
        self.assertEqual(baroTrend, user.loopdata.BarometerTrend.FALLING)

        baroTrend = user.loopdata.LoopProcessor.get_barometer_trend(-3.6, 'mbar', 'group_pressure', 10800)
        self.assertEqual(baroTrend, user.loopdata.BarometerTrend.FALLING_QUICKLY)

        baroTrend = user.loopdata.LoopProcessor.get_barometer_trend(-6.0, 'mbar', 'group_pressure', 10800)
        self.assertEqual(baroTrend, user.loopdata.BarometerTrend.FALLING_QUICKLY)

        baroTrend = user.loopdata.LoopProcessor.get_barometer_trend(-6.1, 'mbar', 'group_pressure', 10800)
        self.assertEqual(baroTrend, user.loopdata.BarometerTrend.FALLING_VERY_RAPIDLY)

        baroTrend = user.loopdata.LoopProcessor.get_barometer_trend(-9.0, 'mbar', 'group_pressure', 10800)
        self.assertEqual(baroTrend, user.loopdata.BarometerTrend.FALLING_VERY_RAPIDLY)

    def test_get_barometer_trend_inHg(self):
        # Forecast descriptions for the 3 hour change in barometer readings.
        # These are approximations (converted from mbars)
        # Falling (or rising) slowly: 0.002953 - 0.044294 inHg in 3 hours
        # Falling (or rising): 0.047248 - 0.10335 inHg in 3 hours
        # Falling (or rising) quickly: 0.106308 - 0.177179 inHg in 3 hours
        # Falling (or rising) very rapidly: More than 0.17719 inHg in 3 hours

        baroTrend = user.loopdata.LoopProcessor.get_barometer_trend(0.26577, 'inHg', 'group_pressure', 10800)
        self.assertEqual(baroTrend, user.loopdata.BarometerTrend.RISING_VERY_RAPIDLY)

        baroTrend = user.loopdata.LoopProcessor.get_barometer_trend(0.17719, 'inHg', 'group_pressure', 10800)
        self.assertEqual(baroTrend, user.loopdata.BarometerTrend.RISING_VERY_RAPIDLY)

        baroTrend = user.loopdata.LoopProcessor.get_barometer_trend(0.177179, 'inHg', 'group_pressure', 10800)
        self.assertEqual(baroTrend, user.loopdata.BarometerTrend.RISING_QUICKLY)

        baroTrend = user.loopdata.LoopProcessor.get_barometer_trend(0.106308, 'inHg', 'group_pressure', 10800)
        self.assertEqual(baroTrend, user.loopdata.BarometerTrend.RISING_QUICKLY)

        baroTrend = user.loopdata.LoopProcessor.get_barometer_trend(0.10335, 'inHg', 'group_pressure', 10800)
        self.assertEqual(baroTrend, user.loopdata.BarometerTrend.RISING)

        baroTrend = user.loopdata.LoopProcessor.get_barometer_trend(0.047248, 'inHg', 'group_pressure', 10800)
        self.assertEqual(baroTrend, user.loopdata.BarometerTrend.RISING)

        baroTrend = user.loopdata.LoopProcessor.get_barometer_trend(0.044294, 'inHg', 'group_pressure', 10800)
        self.assertEqual(baroTrend, user.loopdata.BarometerTrend.RISING_SLOWLY)

        baroTrend = user.loopdata.LoopProcessor.get_barometer_trend(0.002953, 'inHg', 'group_pressure', 10800)
        self.assertEqual(baroTrend, user.loopdata.BarometerTrend.RISING_SLOWLY)

        baroTrend = user.loopdata.LoopProcessor.get_barometer_trend(0.002657698, 'inHg', 'group_pressure', 10800)
        self.assertEqual(baroTrend, user.loopdata.BarometerTrend.STEADY)

        baroTrend = user.loopdata.LoopProcessor.get_barometer_trend(0.0, 'inHg', 'group_pressure', 10800)
        self.assertEqual(baroTrend, user.loopdata.BarometerTrend.STEADY)

        baroTrend = user.loopdata.LoopProcessor.get_barometer_trend(-0.002657698, 'inHg', 'group_pressure', 10800)
        self.assertEqual(baroTrend, user.loopdata.BarometerTrend.STEADY)

        baroTrend = user.loopdata.LoopProcessor.get_barometer_trend(-0.002953, 'inHg', 'group_pressure', 10800)
        self.assertEqual(baroTrend, user.loopdata.BarometerTrend.FALLING_SLOWLY)

        baroTrend = user.loopdata.LoopProcessor.get_barometer_trend(-0.044294, 'inHg', 'group_pressure', 10800)
        self.assertEqual(baroTrend, user.loopdata.BarometerTrend.FALLING_SLOWLY)

        baroTrend = user.loopdata.LoopProcessor.get_barometer_trend(-0.047248, 'inHg', 'group_pressure', 10800)
        self.assertEqual(baroTrend, user.loopdata.BarometerTrend.FALLING)

        baroTrend = user.loopdata.LoopProcessor.get_barometer_trend(-0.10335, 'inHg', 'group_pressure', 10800)
        self.assertEqual(baroTrend, user.loopdata.BarometerTrend.FALLING)

        baroTrend = user.loopdata.LoopProcessor.get_barometer_trend(-0.106308, 'inHg', 'group_pressure', 10800)
        self.assertEqual(baroTrend, user.loopdata.BarometerTrend.FALLING_QUICKLY)

        baroTrend = user.loopdata.LoopProcessor.get_barometer_trend(-0.177179, 'inHg', 'group_pressure', 10800)
        self.assertEqual(baroTrend, user.loopdata.BarometerTrend.FALLING_QUICKLY)

        baroTrend = user.loopdata.LoopProcessor.get_barometer_trend(-0.17719, 'inHg', 'group_pressure', 10800)
        self.assertEqual(baroTrend, user.loopdata.BarometerTrend.FALLING_VERY_RAPIDLY)

        baroTrend = user.loopdata.LoopProcessor.get_barometer_trend(-0.26577, 'inHg', 'group_pressure', 10800)
        self.assertEqual(baroTrend, user.loopdata.BarometerTrend.FALLING_VERY_RAPIDLY)

    def test_save_period_packet(self):
        config_dict = ProcessPacketTests._get_config_dict('us')
        (fields_to_include, current_obstypes, trend_obstypes, rainyear_obstypes,
            year_obstypes, month_obstypes, week_obstypes, day_obstypes, hour_obstypes,
            ten_min_obstypes, two_min_obstypes) = user.loopdata.LoopData.get_fields_to_include(_get_specified_fields())

        unit_system = weewx.units.unit_constants[config_dict['StdConvert'].get('target_unit', 'US').upper()]

        ten_min_packets = []
        dateTime: int = int(time.time())
        for i in range(1000):
            dateTime += 2
            pkt = { 'dateTime': dateTime, 'usUnits': unit_system, 'outTemp': 72.4 }
            user.loopdata.LoopProcessor.save_period_packet(
                pkt['dateTime'], pkt, ten_min_packets, 600, ten_min_obstypes)
        # last packet should be dateTime
        self.assertEqual(ten_min_packets[-1].timestamp, dateTime)
        # the first packet should be dateTime - 598 (the being time is not included)
        self.assertEqual(ten_min_packets[0].timestamp, dateTime - 598)

    def test_prune_period_packet(self):
        """ test that packet is pruned to just the observations needed. """

        pkt = { 'dateTime': 123456789, 'usUnits': 1, 'windSpeed': 10, 'windDir': 27 }
        in_use_obstypes = { 'barometer' }
        new_pkt = user.loopdata.LoopProcessor.prune_period_packet(pkt['dateTime'], pkt, in_use_obstypes)
        self.assertEqual(len(new_pkt), 2)
        self.assertEqual(new_pkt['dateTime'], 123456789)
        self.assertEqual(new_pkt['usUnits'], 1)

        pkt = { 'dateTime': 123456789, 'usUnits': 1, 'windSpeed': 10, 'windDir': 27 }
        in_use_obstypes = { 'windSpeed' }
        new_pkt = user.loopdata.LoopProcessor.prune_period_packet(pkt['dateTime'], pkt, in_use_obstypes)
        self.assertEqual(len(new_pkt), 3)
        self.assertEqual(new_pkt['dateTime'], 123456789)
        self.assertEqual(new_pkt['usUnits'], 1)
        self.assertEqual(new_pkt['windSpeed'], 10)

        pkt = { 'dateTime': 123456789, 'usUnits': 1, 'windSpeed': 10, 'windDir': 27, 'barometer': 1035.01 }
        in_use_obstypes = { 'windSpeed', 'barometer', 'windDir' }
        new_pkt = user.loopdata.LoopProcessor.prune_period_packet(pkt['dateTime'], pkt, in_use_obstypes)
        self.assertEqual(len(new_pkt), 5)
        self.assertEqual(new_pkt['dateTime'], 123456789)
        self.assertEqual(new_pkt['usUnits'], 1)
        self.assertEqual(new_pkt['windSpeed'], 10)
        self.assertEqual(new_pkt['barometer'], 1035.01)

    def test_changing_periods(self):

        config_dict = ProcessPacketTests._get_config_dict('us')
        unit_system = weewx.units.unit_constants[config_dict['StdConvert'].get('target_unit', 'US').upper()]

        # July 1, 2020 Noon PDT
        pkt = {'dateTime': 1593630000, 'usUnits': 1, 'outTemp': 77.4}
        pkt_time = pkt['dateTime']

        (rainyear_accum, rainyear_start, year_accum, month_accum, week_accum,
            week_start, day_accum, hour_accum) = ProcessPacketTests._get_accums(
            config_dict, pkt_time)
        self.assertEqual(week_start, 6)

        converter, formatter = ProcessPacketTests._get_converter_and_formatter(config_dict)
        self.assertEqual(type(converter), weewx.units.Converter)
        self.assertEqual(type(formatter), weewx.units.Formatter)

        specified_fields = [ 'current.outTemp', 'trend.outTemp',
                             '2m.outTemp.max', '2m.outTemp.min', '2m.outTemp.avg',
                             '10m.outTemp.max', '10m.outTemp.min', '10m.outTemp.avg',
                             'hour.outTemp.max', 'hour.outTemp.min', 'hour.outTemp.avg',
                             'day.outTemp.max', 'day.outTemp.min', 'day.outTemp.avg',
                             'week.outTemp.max', 'week.outTemp.min', 'week.outTemp.avg',
                             'month.outTemp.max', 'month.outTemp.min', 'month.outTemp.avg',
                             'year.outTemp.max', 'year.outTemp.min', 'year.outTemp.avg',
                             'rainyear.outTemp.max', 'rainyear.outTemp.min', 'rainyear.outTemp.avg']

        (fields_to_include, current_obstypes, trend_obstypes, rainyear_obstypes,
            year_obstypes, month_obstypes, week_obstypes, day_obstypes, hour_obstypes,
            ten_min_obstypes, two_min_obstypes) = user.loopdata.LoopData.get_fields_to_include(specified_fields)

        trend_packets = []
        ten_min_packets = []
        two_min_packets = []
        time_delta = 10800
        loop_frequency = 2.0
        baro_trend_descs = user.loopdata.LoopData.construct_baro_trend_descs({})

        # First packet.
        (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, trend_packets, time_delta, trend_obstypes,
            baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        # Next packet 1 minute later
        pkt = {'dateTime': 1593630060, 'usUnits': 1, 'outTemp': 77.3}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, trend_packets, time_delta, trend_obstypes,
            baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        self.maxDiff = None
        self.assertEqual(loopdata_pkt['current.outTemp'], '77.3°F')
        self.assertEqual(loopdata_pkt['2m.outTemp.max'], '77.4°F')
        self.assertEqual(loopdata_pkt['2m.outTemp.min'], '77.3°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.max'], '77.4°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.min'], '77.3°F')
        # New hour, since previous record (noon) was part of prev. hour.
        self.assertEqual(loopdata_pkt['hour.outTemp.max'], '77.3°F')
        self.assertEqual(loopdata_pkt['hour.outTemp.min'], '77.3°F')
        self.assertEqual(loopdata_pkt['trend.outTemp'], '-0.1°F')

        # Next packet 9 minute later
        pkt = {'dateTime': 1593630600, 'usUnits': 1, 'outTemp': 77.2}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, trend_packets, time_delta, trend_obstypes,
            baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        self.maxDiff = None
        self.assertEqual(loopdata_pkt['current.outTemp'], '77.2°F')
        # Previous max should have dropped off of 10m.
        self.assertEqual(loopdata_pkt['10m.outTemp.max'], '77.3°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.min'], '77.2°F')
        # hour
        self.assertEqual(loopdata_pkt['hour.outTemp.max'], '77.3°F')
        self.assertEqual(loopdata_pkt['hour.outTemp.min'], '77.2°F')
        self.assertEqual(loopdata_pkt['trend.outTemp'], '-0.2°F')

        # Next packet 2:51 later
        pkt = {'dateTime': 1593640860, 'usUnits': 1, 'outTemp': 76.9}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, trend_packets, time_delta, trend_obstypes,
            baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        self.maxDiff = None
        self.assertEqual(loopdata_pkt['current.outTemp'], '76.9°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.max'], '76.9°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.min'], '76.9°F')
        self.assertEqual(loopdata_pkt['hour.outTemp.max'], '76.9°F')
        self.assertEqual(loopdata_pkt['hour.outTemp.min'], '76.9°F')
        self.assertEqual(loopdata_pkt['trend.outTemp'], '-0.3°F')

        # Next packet 4:00 later
        pkt = {'dateTime': 1593655260, 'usUnits': 1, 'outTemp': 75.0}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum,hour_obstypes, trend_packets, time_delta, trend_obstypes,
            baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        self.maxDiff = None
        self.assertEqual(loopdata_pkt['current.outTemp'], '75.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.max'], '75.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.min'], '75.0°F')
        self.assertEqual(loopdata_pkt['hour.outTemp.max'], '75.0°F')
        self.assertEqual(loopdata_pkt['hour.outTemp.min'], '75.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.min'], '75.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.max'], '77.4°F')
        self.assertEqual(loopdata_pkt.get('trend.outTemp'), None)

        # Next packet 20:00 later
        pkt = {'dateTime': 1593727260, 'usUnits': 1, 'outTemp': 70.0}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum,hour_obstypes, trend_packets, time_delta, trend_obstypes,
            baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        self.maxDiff = None
        self.assertEqual(loopdata_pkt['current.outTemp'], '70.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.max'], '70.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.min'], '70.0°F')
        self.assertEqual(loopdata_pkt['hour.outTemp.max'], '70.0°F')
        self.assertEqual(loopdata_pkt['hour.outTemp.min'], '70.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.min'], '70.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.max'], '70.0°F')
        self.assertEqual(loopdata_pkt.get('trend.outTemp'), None)

        # Add another temp a minute later so we get a trend
        pkt = {'dateTime': 1593727320, 'usUnits': 1, 'outTemp': 70.0}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum,hour_obstypes, trend_packets, time_delta, trend_obstypes,
            baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        self.maxDiff = None
        self.assertEqual(loopdata_pkt['current.outTemp'],  '70.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.max'],  '70.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.min'],  '70.0°F')
        self.assertEqual(loopdata_pkt['hour.outTemp.max'],  '70.0°F')
        self.assertEqual(loopdata_pkt['hour.outTemp.min'],  '70.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.min'],  '70.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.max'],  '70.0°F')
        self.assertEqual(loopdata_pkt['trend.outTemp'],    '0.0°F')
        self.assertEqual(loopdata_pkt['week.outTemp.min'], '70.0°F')
        self.assertEqual(loopdata_pkt['week.outTemp.max'], '77.4°F')
        self.assertEqual(loopdata_pkt['month.outTemp.min'], '70.0°F')
        self.assertEqual(loopdata_pkt['month.outTemp.max'], '77.4°F')

        # Jump a week
        pkt = {'dateTime': 1594332120, 'usUnits': 1, 'outTemp': 66.0}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum,hour_obstypes, trend_packets, time_delta, trend_obstypes,
            baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        self.maxDiff = None
        self.assertEqual(loopdata_pkt['current.outTemp'],  '66.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.max'],  '66.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.min'],  '66.0°F')
        self.assertEqual(loopdata_pkt['hour.outTemp.max'],  '66.0°F')
        self.assertEqual(loopdata_pkt['hour.outTemp.min'],  '66.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.min'],  '66.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.max'],  '66.0°F')
        self.assertEqual(loopdata_pkt['week.outTemp.min'], '66.0°F')
        self.assertEqual(loopdata_pkt['week.outTemp.max'], '66.0°F')
        self.assertEqual(loopdata_pkt['month.outTemp.min'], '66.0°F')
        self.assertEqual(loopdata_pkt['month.outTemp.max'], '77.4°F')
        self.assertEqual(loopdata_pkt.get('trend.outTemp'), None)

        # Jump a month
        pkt = {'dateTime': 1597010520, 'usUnits': 1, 'outTemp': 88.0}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum,hour_obstypes, trend_packets, time_delta, trend_obstypes,
            baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        self.maxDiff = None
        self.assertEqual(loopdata_pkt['current.outTemp'],   '88.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.max'],   '88.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.min'],   '88.0°F')
        self.assertEqual(loopdata_pkt['hour.outTemp.max'],   '88.0°F')
        self.assertEqual(loopdata_pkt['hour.outTemp.min'],   '88.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.min'],   '88.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.max'],   '88.0°F')
        self.assertEqual(loopdata_pkt['week.outTemp.min'],  '88.0°F')
        self.assertEqual(loopdata_pkt['week.outTemp.max'],  '88.0°F')
        self.assertEqual(loopdata_pkt['month.outTemp.min'], '88.0°F')
        self.assertEqual(loopdata_pkt['month.outTemp.max'], '88.0°F')
        self.assertEqual(loopdata_pkt['year.outTemp.min'],  '66.0°F')
        self.assertEqual(loopdata_pkt['year.outTemp.max'],  '88.0°F')
        self.assertEqual(loopdata_pkt.get('trend.outTemp'), None)

        # Jump a year
        pkt = {'dateTime': 1628546520, 'usUnits': 1, 'outTemp': 99.0}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum,hour_obstypes, trend_packets, time_delta, trend_obstypes,
            baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        self.maxDiff = None
        self.assertEqual(loopdata_pkt['current.outTemp'],   '99.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.max'],   '99.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.min'],   '99.0°F')
        self.assertEqual(loopdata_pkt['hour.outTemp.max'],   '99.0°F')
        self.assertEqual(loopdata_pkt['hour.outTemp.min'],   '99.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.min'],   '99.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.max'],   '99.0°F')
        self.assertEqual(loopdata_pkt['week.outTemp.min'],  '99.0°F')
        self.assertEqual(loopdata_pkt['week.outTemp.max'],  '99.0°F')
        self.assertEqual(loopdata_pkt['month.outTemp.min'], '99.0°F')
        self.assertEqual(loopdata_pkt['month.outTemp.max'], '99.0°F')
        self.assertEqual(loopdata_pkt['year.outTemp.min'],  '99.0°F')
        self.assertEqual(loopdata_pkt['year.outTemp.max'],  '99.0°F')
        self.assertEqual(loopdata_pkt.get('trend.outTemp'), None)

        # Jump a minute
        pkt = {'dateTime': 1628546580, 'usUnits': 1, 'outTemp': 97.0}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum,hour_obstypes, trend_packets, time_delta, trend_obstypes,
            baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        self.maxDiff = None
        self.assertEqual(loopdata_pkt['current.outTemp'],       '97.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.max'],       '99.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.min'],       '97.0°F')
        self.assertEqual(loopdata_pkt['hour.outTemp.max'],       '99.0°F')
        self.assertEqual(loopdata_pkt['hour.outTemp.min'],       '97.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.min'],       '97.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.max'],       '99.0°F')
        self.assertEqual(loopdata_pkt['week.outTemp.min'],      '97.0°F')
        self.assertEqual(loopdata_pkt['week.outTemp.max'],      '99.0°F')
        self.assertEqual(loopdata_pkt['month.outTemp.min'],     '97.0°F')
        self.assertEqual(loopdata_pkt['month.outTemp.max'],     '99.0°F')
        self.assertEqual(loopdata_pkt['year.outTemp.min'],      '97.0°F')
        self.assertEqual(loopdata_pkt['year.outTemp.max'],      '99.0°F')
        self.assertEqual(loopdata_pkt['rainyear.outTemp.min'],  '97.0°F')
        self.assertEqual(loopdata_pkt['rainyear.outTemp.max'],  '99.0°F')
        self.assertEqual(loopdata_pkt['trend.outTemp'],         '-2.0°F')

        # Jump to October 15 (new rain year)
        # Friday, October 15, 2021 12:00:00 PM GMT-07:00 DST
        pkt = {'dateTime': 1634324400, 'usUnits': 1, 'outTemp': 41.0}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum,hour_obstypes, trend_packets, time_delta, trend_obstypes,
            baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        self.maxDiff = None
        self.assertEqual(loopdata_pkt['current.outTemp'],       '41.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.max'],       '41.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.min'],       '41.0°F')
        self.assertEqual(loopdata_pkt['hour.outTemp.max'],       '41.0°F')
        self.assertEqual(loopdata_pkt['hour.outTemp.min'],       '41.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.min'],       '41.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.max'],       '41.0°F')
        self.assertEqual(loopdata_pkt['week.outTemp.min'],      '41.0°F')
        self.assertEqual(loopdata_pkt['week.outTemp.max'],      '41.0°F')
        self.assertEqual(loopdata_pkt['month.outTemp.min'],     '41.0°F')
        self.assertEqual(loopdata_pkt['month.outTemp.max'],     '41.0°F')
        self.assertEqual(loopdata_pkt['year.outTemp.min'],      '41.0°F')
        self.assertEqual(loopdata_pkt['year.outTemp.max'],      '99.0°F')
        self.assertEqual(loopdata_pkt['rainyear.outTemp.min'],  '41.0°F')
        self.assertEqual(loopdata_pkt['rainyear.outTemp.max'],  '41.0°F')
        self.assertEqual(loopdata_pkt.get('trend.outTemp'), None)

        # 1s later
        pkt = {'dateTime': 1634324401, 'usUnits': 1, 'outTemp': 42.0}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum,hour_obstypes, trend_packets, time_delta, trend_obstypes,
            baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        self.maxDiff = None
        self.assertEqual(loopdata_pkt['current.outTemp'],       '42.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.min'],       '41.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.max'],       '42.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.avg'],       '41.5°F')
        # One second later starts new hour.
        self.assertEqual(loopdata_pkt['hour.outTemp.min'],       '42.0°F')
        self.assertEqual(loopdata_pkt['hour.outTemp.max'],       '42.0°F')
        self.assertEqual(loopdata_pkt['hour.outTemp.avg'],       '42.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.min'],       '41.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.max'],       '42.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.avg'],       '41.5°F')
        self.assertEqual(loopdata_pkt['week.outTemp.min'],      '41.0°F')
        self.assertEqual(loopdata_pkt['week.outTemp.max'],      '42.0°F')
        self.assertEqual(loopdata_pkt['week.outTemp.avg'],      '41.5°F')
        self.assertEqual(loopdata_pkt['month.outTemp.min'],     '41.0°F')
        self.assertEqual(loopdata_pkt['month.outTemp.max'],     '42.0°F')
        self.assertEqual(loopdata_pkt['month.outTemp.avg'],     '41.5°F')
        self.assertEqual(loopdata_pkt['year.outTemp.min'],      '41.0°F')
        self.assertEqual(loopdata_pkt['year.outTemp.max'],      '99.0°F')
        self.assertEqual(loopdata_pkt['year.outTemp.avg'],      '69.8°F')
        self.assertEqual(loopdata_pkt['rainyear.outTemp.min'],  '41.0°F')
        self.assertEqual(loopdata_pkt['rainyear.outTemp.max'],  '42.0°F')
        self.assertEqual(loopdata_pkt['rainyear.outTemp.avg'],  '41.5°F')
        self.assertEqual(loopdata_pkt['trend.outTemp'],         '1.0°F')

        # About 2 days later, Sunday, October 17, 2021 04:24:27 PM PDT
        pkt = {'dateTime': 1634513067, 'usUnits': 1, 'outTemp': 88.0}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum,hour_obstypes, trend_packets, time_delta, trend_obstypes,
            baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        # Next day, Monday, October 18, 2021 04:24:27 PM PDT
        pkt = {'dateTime': 1634599467, 'usUnits': 1, 'outTemp': 87.5}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum,hour_obstypes, trend_packets, time_delta, trend_obstypes,
            baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        # 6 days later, Saturday, October 23, 2021 04:24:27 PM PDT
        pkt = {'dateTime': 1635031467, 'usUnits': 1, 'outTemp': 87.0}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum,hour_obstypes, trend_packets, time_delta, trend_obstypes,
            baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        # Next day, starts a new week, the high should be 85 (from today)
        # Sunday, October 24, 2021 04:24:27 PM PDT
        pkt = {'dateTime': 1635117867, 'usUnits': 1, 'outTemp': 85.0}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum,hour_obstypes, trend_packets, time_delta, trend_obstypes,
            baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        # Make sure we have a new week accumulator that starts this Sunday 2021-10-24.
        self.assertEqual(timestamp_to_string(week_accum.timespan.start), '2021-10-24 00:00:00 PDT (1635058800)')
        self.assertEqual(week_accum['outTemp'].max, 85.0)
        self.assertEqual(loopdata_pkt['week.outTemp.max'], '85.0°F')

    def test_changing_periods_week_start_0(self):

        config_dict = ProcessPacketTests._get_config_dict('us')
        config_dict['Station']['week_start'] = 0
        unit_system = weewx.units.unit_constants[config_dict['StdConvert'].get('target_unit', 'US').upper()]

        # July 1, 2020 Noon PDT
        pkt = {'dateTime': 1593630000, 'usUnits': 1, 'outTemp': 77.4}
        pkt_time = pkt['dateTime']

        (rainyear_accum, rainyear_start, year_accum, month_accum, week_accum,
            week_start, day_accum, hour_accum) = ProcessPacketTests._get_accums(
            config_dict, pkt_time)
        self.assertEqual(week_start, 0)

        converter, formatter = ProcessPacketTests._get_converter_and_formatter(config_dict)
        self.assertEqual(type(converter), weewx.units.Converter)
        self.assertEqual(type(formatter), weewx.units.Formatter)

        specified_fields = [ 'current.outTemp', 'trend.outTemp',
                             '10m.outTemp.max', '10m.outTemp.min', '10m.outTemp.avg',
                             'day.outTemp.max', 'day.outTemp.min', 'day.outTemp.avg',
                             'week.outTemp.max', 'week.outTemp.min', 'week.outTemp.avg',
                             'month.outTemp.max', 'month.outTemp.min', 'month.outTemp.avg',
                             'year.outTemp.max', 'year.outTemp.min', 'year.outTemp.avg',
                             'rainyear.outTemp.max', 'rainyear.outTemp.min', 'rainyear.outTemp.avg']

        (fields_to_include, current_obstypes, trend_obstypes, rainyear_obstypes,
            year_obstypes, month_obstypes, week_obstypes, day_obstypes, hour_obstypes,
            ten_min_obstypes, two_min_obstypes) = user.loopdata.LoopData.get_fields_to_include(specified_fields)

        trend_packets = []
        ten_min_packets = []
        two_min_packets = []
        time_delta = 10800
        loop_frequency = 2.0
        baro_trend_descs = user.loopdata.LoopData.construct_baro_trend_descs({})

        # First packet.
        (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, trend_packets, time_delta, trend_obstypes,
            baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        # Next packet 1 minute later
        pkt = {'dateTime': 1593630060, 'usUnits': 1, 'outTemp': 77.3}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, trend_packets, time_delta, trend_obstypes,
            baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        self.maxDiff = None
        self.assertEqual(loopdata_pkt['current.outTemp'], '77.3°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.max'], '77.4°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.min'], '77.3°F')
        self.assertEqual(loopdata_pkt['trend.outTemp'], '-0.1°F')

        # Next packet 9 minute later
        pkt = {'dateTime': 1593630600, 'usUnits': 1, 'outTemp': 77.2}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, trend_packets, time_delta, trend_obstypes,
            baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        self.maxDiff = None
        self.assertEqual(loopdata_pkt['current.outTemp'], '77.2°F')
        # Previous max should have dropped off of 10m.
        self.assertEqual(loopdata_pkt['10m.outTemp.max'], '77.3°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.min'], '77.2°F')
        self.assertEqual(loopdata_pkt['trend.outTemp'], '-0.2°F')

        # Next packet 2:51 later
        pkt = {'dateTime': 1593640860, 'usUnits': 1, 'outTemp': 76.9}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, trend_packets, time_delta, trend_obstypes,
            baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        self.maxDiff = None
        self.assertEqual(loopdata_pkt['current.outTemp'], '76.9°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.max'], '76.9°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.min'], '76.9°F')
        self.assertEqual(loopdata_pkt['trend.outTemp'], '-0.3°F')

        # Next packet 4:00 later
        pkt = {'dateTime': 1593655260, 'usUnits': 1, 'outTemp': 75.0}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, trend_packets, time_delta, trend_obstypes,
            baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        self.maxDiff = None
        self.assertEqual(loopdata_pkt['current.outTemp'], '75.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.max'], '75.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.min'], '75.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.min'], '75.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.max'], '77.4°F')
        self.assertEqual(loopdata_pkt.get('trend.outTemp'), None)

        # Next packet 20:00 later
        pkt = {'dateTime': 1593727260, 'usUnits': 1, 'outTemp': 70.0}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, trend_packets, time_delta, trend_obstypes,
            baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        self.maxDiff = None
        self.assertEqual(loopdata_pkt['current.outTemp'], '70.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.max'], '70.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.min'], '70.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.min'], '70.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.max'], '70.0°F')
        self.assertEqual(loopdata_pkt.get('trend.outTemp'), None)

        # Add another temp a minute later so we get a trend
        pkt = {'dateTime': 1593727320, 'usUnits': 1, 'outTemp': 70.0}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, trend_packets, time_delta, trend_obstypes,
            baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        self.maxDiff = None
        self.assertEqual(loopdata_pkt['current.outTemp'],  '70.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.max'],  '70.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.min'],  '70.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.min'],  '70.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.max'],  '70.0°F')
        self.assertEqual(loopdata_pkt['trend.outTemp'],    '0.0°F')
        self.assertEqual(loopdata_pkt['week.outTemp.min'], '70.0°F')
        self.assertEqual(loopdata_pkt['week.outTemp.max'], '77.4°F')
        self.assertEqual(loopdata_pkt['month.outTemp.min'], '70.0°F')
        self.assertEqual(loopdata_pkt['month.outTemp.max'], '77.4°F')

        # Jump a week
        pkt = {'dateTime': 1594332120, 'usUnits': 1, 'outTemp': 66.0}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, trend_packets, time_delta, trend_obstypes,
            baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        self.maxDiff = None
        self.assertEqual(loopdata_pkt['current.outTemp'],  '66.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.max'],  '66.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.min'],  '66.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.min'],  '66.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.max'],  '66.0°F')
        self.assertEqual(loopdata_pkt['week.outTemp.min'], '66.0°F')
        self.assertEqual(loopdata_pkt['week.outTemp.max'], '66.0°F')
        self.assertEqual(loopdata_pkt['month.outTemp.min'], '66.0°F')
        self.assertEqual(loopdata_pkt['month.outTemp.max'], '77.4°F')
        self.assertEqual(loopdata_pkt.get('trend.outTemp'), None)

        # Jump a month
        pkt = {'dateTime': 1597010520, 'usUnits': 1, 'outTemp': 88.0}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, trend_packets, time_delta, trend_obstypes,
            baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        self.maxDiff = None
        self.assertEqual(loopdata_pkt['current.outTemp'],   '88.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.max'],   '88.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.min'],   '88.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.min'],   '88.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.max'],   '88.0°F')
        self.assertEqual(loopdata_pkt['week.outTemp.min'],  '88.0°F')
        self.assertEqual(loopdata_pkt['week.outTemp.max'],  '88.0°F')
        self.assertEqual(loopdata_pkt['month.outTemp.min'], '88.0°F')
        self.assertEqual(loopdata_pkt['month.outTemp.max'], '88.0°F')
        self.assertEqual(loopdata_pkt['year.outTemp.min'],  '66.0°F')
        self.assertEqual(loopdata_pkt['year.outTemp.max'],  '88.0°F')
        self.assertEqual(loopdata_pkt.get('trend.outTemp'), None)

        # Jump a year
        pkt = {'dateTime': 1628546520, 'usUnits': 1, 'outTemp': 99.0}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, trend_packets, time_delta, trend_obstypes,
            baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        self.maxDiff = None
        self.assertEqual(loopdata_pkt['current.outTemp'],   '99.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.max'],   '99.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.min'],   '99.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.min'],   '99.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.max'],   '99.0°F')
        self.assertEqual(loopdata_pkt['week.outTemp.min'],  '99.0°F')
        self.assertEqual(loopdata_pkt['week.outTemp.max'],  '99.0°F')
        self.assertEqual(loopdata_pkt['month.outTemp.min'], '99.0°F')
        self.assertEqual(loopdata_pkt['month.outTemp.max'], '99.0°F')
        self.assertEqual(loopdata_pkt['year.outTemp.min'],  '99.0°F')
        self.assertEqual(loopdata_pkt['year.outTemp.max'],  '99.0°F')
        self.assertEqual(loopdata_pkt.get('trend.outTemp'), None)

        # Jump a minute
        pkt = {'dateTime': 1628546580, 'usUnits': 1, 'outTemp': 97.0}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, trend_packets, time_delta, trend_obstypes,
            baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        self.maxDiff = None
        self.assertEqual(loopdata_pkt['current.outTemp'],       '97.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.max'],       '99.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.min'],       '97.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.min'],       '97.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.max'],       '99.0°F')
        self.assertEqual(loopdata_pkt['week.outTemp.min'],      '97.0°F')
        self.assertEqual(loopdata_pkt['week.outTemp.max'],      '99.0°F')
        self.assertEqual(loopdata_pkt['month.outTemp.min'],     '97.0°F')
        self.assertEqual(loopdata_pkt['month.outTemp.max'],     '99.0°F')
        self.assertEqual(loopdata_pkt['year.outTemp.min'],      '97.0°F')
        self.assertEqual(loopdata_pkt['year.outTemp.max'],      '99.0°F')
        self.assertEqual(loopdata_pkt['rainyear.outTemp.min'],  '97.0°F')
        self.assertEqual(loopdata_pkt['rainyear.outTemp.max'],  '99.0°F')
        self.assertEqual(loopdata_pkt['trend.outTemp'],         '-2.0°F')

        # Jump to October 15 (new rain year)
        pkt = {'dateTime': 1634324400, 'usUnits': 1, 'outTemp': 41.0}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, trend_packets, time_delta, trend_obstypes,
            baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        self.maxDiff = None
        self.assertEqual(loopdata_pkt['current.outTemp'],       '41.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.max'],       '41.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.min'],       '41.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.min'],       '41.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.max'],       '41.0°F')
        self.assertEqual(loopdata_pkt['week.outTemp.min'],      '41.0°F')
        self.assertEqual(loopdata_pkt['week.outTemp.max'],      '41.0°F')
        self.assertEqual(loopdata_pkt['month.outTemp.min'],     '41.0°F')
        self.assertEqual(loopdata_pkt['month.outTemp.max'],     '41.0°F')
        self.assertEqual(loopdata_pkt['year.outTemp.min'],      '41.0°F')
        self.assertEqual(loopdata_pkt['year.outTemp.max'],      '99.0°F')
        self.assertEqual(loopdata_pkt['rainyear.outTemp.min'],  '41.0°F')
        self.assertEqual(loopdata_pkt['rainyear.outTemp.max'],  '41.0°F')
        self.assertEqual(loopdata_pkt.get('trend.outTemp'), None)

        # 1s later
        pkt = {'dateTime': 1634324401, 'usUnits': 1, 'outTemp': 42.0}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, trend_packets, time_delta, trend_obstypes,
            baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        self.maxDiff = None
        self.assertEqual(loopdata_pkt['current.outTemp'],       '42.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.min'],       '41.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.max'],       '42.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.avg'],       '41.5°F')
        self.assertEqual(loopdata_pkt['day.outTemp.min'],       '41.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.max'],       '42.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.avg'],       '41.5°F')
        self.assertEqual(loopdata_pkt['week.outTemp.min'],      '41.0°F')
        self.assertEqual(loopdata_pkt['week.outTemp.max'],      '42.0°F')
        self.assertEqual(loopdata_pkt['week.outTemp.avg'],      '41.5°F')
        self.assertEqual(loopdata_pkt['month.outTemp.min'],     '41.0°F')
        self.assertEqual(loopdata_pkt['month.outTemp.max'],     '42.0°F')
        self.assertEqual(loopdata_pkt['month.outTemp.avg'],     '41.5°F')
        self.assertEqual(loopdata_pkt['year.outTemp.min'],      '41.0°F')
        self.assertEqual(loopdata_pkt['year.outTemp.max'],      '99.0°F')
        self.assertEqual(loopdata_pkt['year.outTemp.avg'],      '69.8°F')
        self.assertEqual(loopdata_pkt['rainyear.outTemp.min'],  '41.0°F')
        self.assertEqual(loopdata_pkt['rainyear.outTemp.max'],  '42.0°F')
        self.assertEqual(loopdata_pkt['rainyear.outTemp.avg'],  '41.5°F')
        self.assertEqual(loopdata_pkt['trend.outTemp'],         '1.0°F')

        # About 2 days later, Sunday, October 17, 2021 04:24:27 PM PDT
        pkt = {'dateTime': 1634513067, 'usUnits': 1, 'outTemp': 88.0}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, trend_packets, time_delta, trend_obstypes,
            baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        # Make sure we still have the old accumulator that started Monday 2021-10-11.
        self.assertEqual(timestamp_to_string(week_accum.timespan.start), '2021-10-11 00:00:00 PDT (1633935600)')

        # Next day, Monday, October 18, 2021 04:24:27 PM PDT
        pkt = {'dateTime': 1634599467, 'usUnits': 1, 'outTemp': 87.5}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, trend_packets, time_delta, trend_obstypes,
            baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        # Make sure we still have a new accumulator that started Monday 2021-10-18.
        self.assertEqual(timestamp_to_string(week_accum.timespan.start), '2021-10-18 00:00:00 PDT (1634540400)')

        # 6 days later, Saturday, October 23, 2021 04:24:27 PM PDT
        pkt = {'dateTime': 1635031467, 'usUnits': 1, 'outTemp': 87.0}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, trend_packets, time_delta, trend_obstypes,
            baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        # Next day DOES NOT START a new week since week_start is 0, high should be 87.5 (last Monday)
        # Sunday, October 24, 2021 04:24:27 PM PDT
        pkt = {'dateTime': 1635117867, 'usUnits': 1, 'outTemp': 85.0}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, trend_packets, time_delta, trend_obstypes,
            baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        # Make sure we still have the accumulator that started Monday 2021-10-18.
        self.assertEqual(timestamp_to_string(week_accum.timespan.start), '2021-10-18 00:00:00 PDT (1634540400)')
        self.assertEqual(week_accum['outTemp'].max, 87.5)
        self.assertEqual(loopdata_pkt['week.outTemp.max'], '87.5°F')

    def test_ip100_packet_processing(self):

        config_dict = ProcessPacketTests._get_config_dict('us')
        unit_system = weewx.units.unit_constants[config_dict['StdConvert'].get('target_unit', 'US').upper()]

        first_pkt_time, pkts = ip100_packets.IP100Packets._get_packets()
        (rainyear_accum, rainyear_start, year_accum, month_accum, week_accum,
            week_start, day_accum, hour_accum) = ProcessPacketTests._get_accums(
            config_dict, first_pkt_time)

        converter, formatter = ProcessPacketTests._get_converter_and_formatter(config_dict)
        self.assertEqual(type(converter), weewx.units.Converter)
        self.assertEqual(type(formatter), weewx.units.Formatter)

        (fields_to_include, current_obstypes, trend_obstypes, rainyear_obstypes,
            year_obstypes, month_obstypes, week_obstypes, day_obstypes, hour_obstypes,
            ten_min_obstypes, two_min_obstypes) = user.loopdata.LoopData.get_fields_to_include(_get_specified_fields())

        trend_packets = []
        ten_min_packets = []
        two_min_packets = []
        loop_frequency = 2.0
        time_delta = 10800
        baro_trend_descs = user.loopdata.LoopData.construct_baro_trend_descs({})

        for pkt in pkts:
            pkt_time = to_int(pkt['dateTime'])
            (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
                day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
                pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
                fields_to_include, current_obstypes, rainyear_accum,
                rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
                month_accum, month_obstypes, week_accum, week_start, week_obstypes,
                day_accum, day_obstypes, hour_accum, hour_obstypes, trend_packets, time_delta, trend_obstypes,
                baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        # {'dateTime': 1593883054, 'usUnits': 1, 'outTemp': 71.6, 'barometer': 30.060048358389471, 'dewpoint': 60.48739574937819
        # {'dateTime': 1593883332, 'usUnits': 1, 'outTemp': 72.0, 'barometer': 30.055425865734495, 'dewpoint': 59.57749595318801

        self.maxDiff = None

        self.assertEqual(loopdata_pkt['unit.label.outTemp'], '°F')

        self.assertEqual(loopdata_pkt['current.dateTime.raw'], 1593883332)
        self.assertEqual(loopdata_pkt['current.dateTime'], '07/04/20 10:22:12')

        self.assertEqual(loopdata_pkt['10m.windGust.max'], '6 mph')
        self.assertEqual(loopdata_pkt['10m.windGust.max.formatted'], '6')
        self.assertEqual(loopdata_pkt['10m.windGust.max.raw'], 6.5)
        self.assertEqual(loopdata_pkt['10m.windGust.maxtime'], '07/04/20 10:18:20')
        self.assertEqual(loopdata_pkt['10m.windGust.maxtime.raw'], 1593883100)

        self.assertEqual(loopdata_pkt['10m.outTemp.max'], '72.1°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.max.formatted'], '72.1')
        self.assertEqual(loopdata_pkt['10m.outTemp.max.raw'], 72.1)
        self.assertEqual(loopdata_pkt['10m.outTemp.maxtime'], '07/04/20 10:22:02')
        self.assertEqual(loopdata_pkt['10m.outTemp.maxtime.raw'], 1593883322)

        self.assertEqual(loopdata_pkt['hour.windGust.max'], '6 mph')
        self.assertEqual(loopdata_pkt['hour.windGust.max.formatted'], '6')
        self.assertEqual(loopdata_pkt['hour.windGust.max.raw'], 6.5)
        self.assertEqual(loopdata_pkt['hour.windGust.maxtime'], '07/04/20 10:18:20')
        self.assertEqual(loopdata_pkt['hour.windGust.maxtime.raw'], 1593883100)

        self.assertEqual(loopdata_pkt['hour.outTemp.max'], '72.1°F')
        self.assertEqual(loopdata_pkt['hour.outTemp.max.formatted'], '72.1')
        self.assertEqual(loopdata_pkt['hour.outTemp.max.raw'], 72.1)
        self.assertEqual(loopdata_pkt['hour.outTemp.maxtime'], '07/04/20 10:22:02')
        self.assertEqual(loopdata_pkt['hour.outTemp.maxtime.raw'], 1593883322)

        self.assertEqual(loopdata_pkt['current.outTemp'], '72.0°F')
        self.assertEqual(loopdata_pkt['current.barometer'], '30.055 inHg')
        self.assertEqual(loopdata_pkt['current.windSpeed'], '6 mph')
        self.assertEqual(loopdata_pkt['current.windDir'], '45°')
        self.assertEqual(loopdata_pkt['current.windDir.ordinal_compass'], 'NE')

        # 30.055425865734495 - 30.060048358389471
        self.assertEqual(loopdata_pkt['trend.barometer'], '-0.005 inHg')
        self.assertTrue(loopdata_pkt['trend.barometer.raw'] < -0.0046224926 and loopdata_pkt['trend.barometer.raw'] > -0.0046224927)
        self.assertEqual(loopdata_pkt['trend.barometer.formatted'], '-0.005')
        self.assertEqual(loopdata_pkt['trend.barometer.code'], -1)
        self.assertEqual(loopdata_pkt['trend.barometer.desc'], 'Falling Slowly')

        # 72.0 - 71.6
        self.assertEqual(loopdata_pkt['trend.outTemp'], '0.4°F')
        self.assertTrue(loopdata_pkt['trend.outTemp.raw'] < 0.4001 and loopdata_pkt['trend.outTemp.raw'] > 0.3999)
        self.assertEqual(loopdata_pkt['trend.outTemp.formatted'], '0.4')

        # 59.57749595318801 - 60.48739574937819
        self.assertEqual(loopdata_pkt['trend.dewpoint'], '-0.9°F')
        self.assertTrue(loopdata_pkt['trend.dewpoint.raw'] < -0.9098997961 and loopdata_pkt['trend.dewpoint.raw'] > -0.9098997962)
        self.assertEqual(loopdata_pkt['trend.dewpoint.formatted'], '-0.9')

        self.assertEqual(loopdata_pkt['day.rain.sum'], '0.00 in')
        self.assertEqual(loopdata_pkt['day.rain.sum.formatted'], '0.00')
        self.assertEqual(loopdata_pkt['unit.label.rain'], ' in')

        self.assertEqual(loopdata_pkt['day.outTemp.avg'], '72.1°F')
        self.assertEqual(loopdata_pkt['day.barometer.avg'], '30.055 inHg')
        self.assertEqual(loopdata_pkt['day.windSpeed.avg'], '3 mph')
        self.assertEqual(loopdata_pkt['day.windDir.avg'], '88°')

        self.assertEqual(loopdata_pkt['day.outTemp.max'], '89.6°F')
        self.assertEqual(loopdata_pkt['day.barometer.max'], '30.060 inHg')
        self.assertEqual(loopdata_pkt['day.windSpeed.max'], '17 mph')
        self.assertEqual(loopdata_pkt['day.windDir.max'], '360°')

        self.assertEqual(loopdata_pkt['day.outTemp.min'], '71.4°F')
        self.assertEqual(loopdata_pkt['day.barometer.min'], '30.055 inHg')
        self.assertEqual(loopdata_pkt['day.windSpeed.min'], '1 mph')
        self.assertEqual(loopdata_pkt['day.windDir.min'], '22°')

        self.assertEqual(loopdata_pkt['unit.label.outTemp'], '°F')
        self.assertEqual(loopdata_pkt['unit.label.barometer'], ' inHg')
        self.assertEqual(loopdata_pkt['unit.label.windSpeed'], ' mph')
        self.assertEqual(loopdata_pkt['unit.label.windDir'], '°')

        self.assertEqual(loopdata_pkt['unit.label.wind'], ' mph')
        self.assertEqual(loopdata_pkt['day.wind.maxtime'], '07/04/20 06:40:00')
        self.assertEqual(loopdata_pkt['day.wind.max.formatted'], '20')
        self.assertEqual(loopdata_pkt['day.wind.max'], '20 mph')
        self.assertEqual(loopdata_pkt['day.wind.gustdir.formatted'], '244')
        self.assertEqual(loopdata_pkt['day.wind.gustdir.ordinal_compass'], 'WSW')
        self.assertEqual(loopdata_pkt['day.wind.gustdir'], '244°')

        self.assertEqual(loopdata_pkt['day.wind.mintime'], '07/04/20 10:17:48')
        self.assertEqual(loopdata_pkt['day.wind.min.formatted'], '1')
        self.assertEqual(loopdata_pkt['day.wind.min'], '1 mph')
        self.assertEqual(loopdata_pkt['unit.label.wind'], ' mph')

        self.assertEqual(loopdata_pkt['day.wind.avg.formatted'], '3')
        self.assertEqual(loopdata_pkt['day.wind.avg'], '3 mph')

        self.assertEqual(loopdata_pkt['day.wind.rms.formatted'], '4')
        self.assertEqual(loopdata_pkt['day.wind.rms'], '4 mph')

        self.assertEqual(loopdata_pkt['day.wind.vecavg.formatted'], '3')
        self.assertEqual(loopdata_pkt['day.wind.vecavg'], '3 mph')

        self.assertEqual(loopdata_pkt['day.wind.vecdir.formatted'], '26')
        self.assertEqual(loopdata_pkt['day.wind.vecdir'], '26°')

    def test_ip100_us_packets_to_metric_db_to_us_report_processing(self):

        config_dict = ProcessPacketTests._get_config_dict('db-metric.report-us')
        unit_system = weewx.units.unit_constants[config_dict['StdConvert'].get('target_unit', 'US').upper()]

        first_pkt_time, pkts = ip100_packets.IP100Packets._get_packets()
        (rainyear_accum, rainyear_start, year_accum, month_accum, week_accum,
            week_start, day_accum, hour_accum) = ProcessPacketTests._get_accums(
            config_dict, first_pkt_time)

        converter, formatter = ProcessPacketTests._get_converter_and_formatter(config_dict)
        self.assertEqual(type(converter), weewx.units.Converter)
        self.assertEqual(type(formatter), weewx.units.Formatter)

        (fields_to_include, current_obstypes, trend_obstypes, rainyear_obstypes,
            year_obstypes, month_obstypes, week_obstypes, day_obstypes, hour_obstypes,
            ten_min_obstypes, two_min_obstypes) = user.loopdata.LoopData.get_fields_to_include(_get_specified_fields())

        trend_packets = []
        ten_min_packets = []
        two_min_packets = []
        loop_frequency = 2.0
        time_delta = 10800
        baro_trend_descs = user.loopdata.LoopData.construct_baro_trend_descs({})

        for pkt in pkts:
            pkt_time = to_int(pkt['dateTime'])
            (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
                day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
                pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
                fields_to_include, current_obstypes, rainyear_accum,
                rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
                month_accum, month_obstypes, week_accum, week_start, week_obstypes,
                day_accum, day_obstypes, hour_accum, hour_obstypes, trend_packets, time_delta, trend_obstypes,
                baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        # {'dateTime': 1593883054, 'usUnits': 1, 'outTemp': 71.6, 'barometer': 30.060048358389471, 'dewpoint': 60.48739574937819
        # {'dateTime': 1593883332, 'usUnits': 1, 'outTemp': 72.0, 'barometer': 30.055425865734495, 'dewpoint': 59.57749595318801

        self.maxDiff = None

        self.assertEqual(loopdata_pkt['unit.label.outTemp'], '°F')

        self.assertEqual(loopdata_pkt['current.dateTime.raw'], 1593883332)
        self.assertEqual(loopdata_pkt['current.dateTime'], '07/04/20 10:22:12')

        self.assertEqual(loopdata_pkt['10m.windGust.max'], '7 mph')
        self.assertEqual(loopdata_pkt['10m.windGust.max.formatted'], '7')
        self.assertTrue(loopdata_pkt['10m.windGust.max.raw'] > 6.5 and loopdata_pkt['10m.windGust.max.raw'] < 6.51)
        self.assertEqual(loopdata_pkt['10m.windGust.maxtime'], '07/04/20 10:18:20')
        self.assertEqual(loopdata_pkt['10m.windGust.maxtime.raw'], 1593883100)

        self.assertEqual(loopdata_pkt['10m.outTemp.max'], '72.1°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.max.formatted'], '72.1')
        self.assertEqual(loopdata_pkt['10m.outTemp.max.raw'], 72.1)
        self.assertEqual(loopdata_pkt['10m.outTemp.maxtime'], '07/04/20 10:22:02')
        self.assertEqual(loopdata_pkt['10m.outTemp.maxtime.raw'], 1593883322)

        self.assertEqual(loopdata_pkt['current.outTemp'], '72.0°F')
        self.assertEqual(loopdata_pkt['current.barometer'], '30.055 inHg')
        self.assertEqual(loopdata_pkt['current.windSpeed'], '6 mph')
        self.assertEqual(loopdata_pkt['current.windDir'], '45°')
        self.assertEqual(loopdata_pkt['current.windDir.ordinal_compass'], 'NE')

        # 30.055425865734495 - 30.060048358389471
        self.assertEqual(loopdata_pkt['trend.barometer'], '-0.005 inHg')
        self.assertTrue(loopdata_pkt['trend.barometer.raw'] < -0.0046224926 and loopdata_pkt['trend.barometer.raw'] > -0.0046224927)
        self.assertEqual(loopdata_pkt['trend.barometer.formatted'], '-0.005')
        self.assertEqual(loopdata_pkt['trend.barometer.code'], -1)
        self.assertEqual(loopdata_pkt['trend.barometer.desc'], 'Falling Slowly')

        # 72.0 - 71.6
        self.assertEqual(loopdata_pkt['trend.outTemp'], '0.4°F')
        self.assertTrue(loopdata_pkt['trend.outTemp.raw'] < 0.4001 and loopdata_pkt['trend.outTemp.raw'] > 0.3999)
        self.assertEqual(loopdata_pkt['trend.outTemp.formatted'], '0.4')

        # 59.57749595318801 - 60.48739574937819
        self.assertEqual(loopdata_pkt['trend.dewpoint'], '-0.9°F')
        self.assertTrue(loopdata_pkt['trend.dewpoint.raw'] < -0.9098997961 and loopdata_pkt['trend.dewpoint.raw'] > -0.9098997962)
        self.assertEqual(loopdata_pkt['trend.dewpoint.formatted'], '-0.9')

        self.assertEqual(loopdata_pkt['day.rain.sum'], '0.00 in')
        self.assertEqual(loopdata_pkt['day.rain.sum.formatted'], '0.00')
        self.assertEqual(loopdata_pkt['unit.label.rain'], ' in')

        self.assertEqual(loopdata_pkt['day.outTemp.avg'], '72.1°F')
        self.assertEqual(loopdata_pkt['day.barometer.avg'], '30.055 inHg')
        self.assertEqual(loopdata_pkt['day.windSpeed.avg'], '3 mph')
        self.assertEqual(loopdata_pkt['day.windDir.avg'], '88°')

        self.assertEqual(loopdata_pkt['day.outTemp.max'], '89.6°F')
        self.assertEqual(loopdata_pkt['day.barometer.max'], '30.060 inHg')
        self.assertEqual(loopdata_pkt['day.windSpeed.max'], '17 mph')
        self.assertEqual(loopdata_pkt['day.windDir.max'], '360°')

        self.assertEqual(loopdata_pkt['day.outTemp.min'], '71.4°F')
        self.assertEqual(loopdata_pkt['day.barometer.min'], '30.055 inHg')
        self.assertEqual(loopdata_pkt['day.windSpeed.min'], '1 mph')
        self.assertEqual(loopdata_pkt['day.windDir.min'], '22°')

        self.assertEqual(loopdata_pkt['unit.label.outTemp'], '°F')
        self.assertEqual(loopdata_pkt['unit.label.barometer'], ' inHg')
        self.assertEqual(loopdata_pkt['unit.label.windSpeed'], ' mph')
        self.assertEqual(loopdata_pkt['unit.label.windDir'], '°')

        self.assertEqual(loopdata_pkt['unit.label.wind'], ' mph')
        self.assertEqual(loopdata_pkt['day.wind.maxtime'], '07/04/20 06:40:00')
        self.assertEqual(loopdata_pkt['day.wind.max.formatted'], '20')
        self.assertEqual(loopdata_pkt['day.wind.max'], '20 mph')
        self.assertEqual(loopdata_pkt['day.wind.gustdir.formatted'], '244')
        self.assertEqual(loopdata_pkt['day.wind.gustdir.ordinal_compass'], 'WSW')
        self.assertEqual(loopdata_pkt['day.wind.gustdir'], '244°')

        self.assertEqual(loopdata_pkt['day.wind.mintime'], '07/04/20 10:17:48')
        self.assertEqual(loopdata_pkt['day.wind.min.formatted'], '1')
        self.assertEqual(loopdata_pkt['day.wind.min'], '1 mph')
        self.assertEqual(loopdata_pkt['unit.label.wind'], ' mph')

        self.assertEqual(loopdata_pkt['day.wind.avg.formatted'], '3')
        self.assertEqual(loopdata_pkt['day.wind.avg'], '3 mph')

        self.assertEqual(loopdata_pkt['day.wind.rms.formatted'], '4')
        self.assertEqual(loopdata_pkt['day.wind.rms'], '4 mph')

        self.assertEqual(loopdata_pkt['day.wind.vecavg.formatted'], '3')
        self.assertEqual(loopdata_pkt['day.wind.vecavg'], '3 mph')

        self.assertEqual(loopdata_pkt['day.wind.vecdir.formatted'], '26')
        self.assertEqual(loopdata_pkt['day.wind.vecdir'], '26°')

    def test_cc3000_packet_processing(self):

        config_dict = ProcessPacketTests._get_config_dict('us')
        unit_system = weewx.units.unit_constants[config_dict['StdConvert'].get('target_unit', 'US').upper()]

        first_pkt_time, pkts = cc3000_packets.CC3000Packets._get_packets()
        (rainyear_accum, rainyear_start, year_accum, month_accum, week_accum,
            week_start, day_accum, hour_accum) = ProcessPacketTests._get_accums(
            config_dict, first_pkt_time)

        converter, formatter = ProcessPacketTests._get_converter_and_formatter(config_dict)
        self.assertEqual(type(converter), weewx.units.Converter)
        self.assertEqual(type(formatter), weewx.units.Formatter)

        (fields_to_include, current_obstypes, trend_obstypes, rainyear_obstypes,
            year_obstypes, month_obstypes, week_obstypes, day_obstypes, hour_obstypes,
            ten_min_obstypes, two_min_obstypes) = user.loopdata.LoopData.get_fields_to_include(_get_specified_fields())

        trend_packets = []
        ten_min_packets = []
        two_min_packets = []
        loop_frequency = 2.0
        time_delta = 10800
        baro_trend_descs = user.loopdata.LoopData.construct_baro_trend_descs({})

        for pkt in pkts:
            pkt_time = to_int(pkt['dateTime'])
            (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
                day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
                pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
                fields_to_include, current_obstypes, rainyear_accum,
                rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
                month_accum, month_obstypes, week_accum, week_start, week_obstypes,
                day_accum, day_obstypes, hour_accum, hour_obstypes, trend_packets, time_delta, trend_obstypes,
                baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        # {'dateTime': 1593975030, 'outTemp': 76.1, 'barometer': 30.014857385736513, 'dewpoint': 54.73645937493746
        # {'dateTime': 1593975366, 'outTemp': 75.4, 'barometer': 30.005222168998216, 'dewpoint': 56.53264564000546

        self.maxDiff = None

        self.assertEqual(loopdata_pkt['unit.label.outTemp'], '°F')

        self.assertEqual(loopdata_pkt['current.dateTime.raw'], 1593975366)
        self.assertEqual(loopdata_pkt['current.dateTime'], '07/05/20 11:56:06')

        self.assertEqual(loopdata_pkt['10m.windGust.max'], '7 mph')
        self.assertEqual(loopdata_pkt['10m.windGust.max.formatted'], '7')
        self.assertEqual(loopdata_pkt['10m.windGust.max.raw'], 7.2)
        self.assertEqual(loopdata_pkt['10m.windGust.maxtime'], '07/05/20 11:50:30')
        self.assertEqual(loopdata_pkt['10m.windGust.maxtime.raw'], 1593975030)

        self.assertEqual(loopdata_pkt['10m.outTemp.max'], '76.3°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.max.formatted'], '76.3')
        self.assertEqual(loopdata_pkt['10m.outTemp.max.raw'], 76.3)
        self.assertEqual(loopdata_pkt['10m.outTemp.maxtime'], '07/05/20 11:50:34')
        self.assertEqual(loopdata_pkt['10m.outTemp.maxtime.raw'], 1593975034)

        self.assertEqual(loopdata_pkt['hour.windGust.max'], '7 mph')
        self.assertEqual(loopdata_pkt['hour.windGust.max.formatted'], '7')
        self.assertEqual(loopdata_pkt['hour.windGust.max.raw'], 7.2)
        self.assertEqual(loopdata_pkt['hour.windGust.maxtime'], '07/05/20 11:50:30')
        self.assertEqual(loopdata_pkt['hour.windGust.maxtime.raw'], 1593975030)

        self.assertEqual(loopdata_pkt['hour.outTemp.max'], '76.3°F')
        self.assertEqual(loopdata_pkt['hour.outTemp.max.formatted'], '76.3')
        self.assertEqual(loopdata_pkt['hour.outTemp.max.raw'], 76.3)
        self.assertEqual(loopdata_pkt['hour.outTemp.maxtime'], '07/05/20 11:50:34')
        self.assertEqual(loopdata_pkt['hour.outTemp.maxtime.raw'], 1593975034)

        self.assertEqual(loopdata_pkt['current.outTemp'], '75.4°F')
        self.assertEqual(loopdata_pkt['current.barometer'], '30.005 inHg')
        self.assertEqual(loopdata_pkt['current.windSpeed'], '2 mph')
        self.assertEqual(loopdata_pkt['current.windDir'], '45°')
        self.assertEqual(loopdata_pkt['current.windDir.ordinal_compass'], 'NE')

        # 30.005222168998216 - 30.014857385736513
        self.assertEqual(loopdata_pkt['trend.barometer'], '-0.010 inHg')
        self.assertTrue(loopdata_pkt['trend.barometer.raw'] > -0.0096352168 and loopdata_pkt['trend.barometer.raw'] < -0.0096352167)
        self.assertEqual(loopdata_pkt['trend.barometer.formatted'], '-0.010')
        self.assertEqual(loopdata_pkt['trend.barometer.code'], -1)
        self.assertEqual(loopdata_pkt['trend.barometer.desc'], 'Falling Slowly')

        # 75.4 - 76.1
        self.assertEqual(loopdata_pkt['trend.outTemp'], '-0.7°F')
        self.assertTrue(loopdata_pkt['trend.outTemp.raw'] > -0.7001 and loopdata_pkt['trend.outTemp.raw'] < -0.6999)
        self.assertEqual(loopdata_pkt['trend.outTemp.formatted'], '-0.7')

        # 56.53264564000546 - 54.73645937493746
        self.assertEqual(loopdata_pkt['trend.dewpoint'], '1.8°F')
        self.assertTrue(loopdata_pkt['trend.dewpoint.raw'] > 1.7961862650 and loopdata_pkt['trend.dewpoint.raw'] < 1.7961862651)
        self.assertEqual(loopdata_pkt['trend.dewpoint.formatted'], '1.8')

        self.assertEqual(loopdata_pkt['day.rain.sum'], '0.00 in')
        self.assertEqual(loopdata_pkt['day.rain.sum.formatted'], '0.00')
        self.assertEqual(loopdata_pkt['unit.label.rain'], ' in')

        self.assertEqual(loopdata_pkt['day.outTemp.avg'], '75.4°F')
        self.assertEqual(loopdata_pkt['day.barometer.avg'], '30.005 inHg')
        self.assertEqual(loopdata_pkt['day.windSpeed.avg'], '4 mph')
        self.assertEqual(loopdata_pkt['day.windDir.avg'], '166°')

        self.assertEqual(loopdata_pkt['day.outTemp.max'], '76.3°F')
        self.assertEqual(loopdata_pkt['day.barometer.max'], '30.015 inHg')
        self.assertEqual(loopdata_pkt['day.windSpeed.max'], '7 mph')
        self.assertEqual(loopdata_pkt['day.windDir.max'], '360°')

        self.assertEqual(loopdata_pkt['day.outTemp.min'], '74.9°F')
        self.assertEqual(loopdata_pkt['day.barometer.min'], '30.005 inHg')
        self.assertEqual(loopdata_pkt['day.windSpeed.min'], '0 mph')
        self.assertEqual(loopdata_pkt['day.windDir.min'], '22°')

        self.assertEqual(loopdata_pkt['unit.label.outTemp'], '°F')
        self.assertEqual(loopdata_pkt['unit.label.barometer'], ' inHg')
        self.assertEqual(loopdata_pkt['unit.label.windSpeed'], ' mph')
        self.assertEqual(loopdata_pkt['unit.label.windDir'], '°')

        self.assertEqual(loopdata_pkt['unit.label.wind'], ' mph')
        self.assertEqual(loopdata_pkt['day.wind.maxtime'], '07/05/20 11:50:30')
        self.assertEqual(loopdata_pkt['day.wind.max.formatted'], '7')
        self.assertEqual(loopdata_pkt['day.wind.max'], '7 mph')
        self.assertEqual(loopdata_pkt['day.wind.gustdir.formatted'], '22')
        self.assertEqual(loopdata_pkt['day.wind.gustdir.ordinal_compass'], 'NNE')
        self.assertEqual(loopdata_pkt['day.wind.gustdir'], '22°')

        self.assertEqual(loopdata_pkt['day.wind.mintime'], '07/05/20 11:51:58')
        self.assertEqual(loopdata_pkt['day.wind.min.formatted'], '0')
        self.assertEqual(loopdata_pkt['day.wind.min'], '0 mph')
        self.assertEqual(loopdata_pkt['unit.label.wind'], ' mph')

        self.assertEqual(loopdata_pkt['day.wind.avg.formatted'], '4')
        self.assertEqual(loopdata_pkt['day.wind.avg'], '4 mph')

        self.assertEqual(loopdata_pkt['day.wind.rms.formatted'], '4')
        self.assertEqual(loopdata_pkt['day.wind.rms'], '4 mph')

        self.assertEqual(loopdata_pkt['day.wind.vecavg.formatted'], '3')
        self.assertEqual(loopdata_pkt['day.wind.vecavg'], '3 mph')

        self.assertEqual(loopdata_pkt['day.wind.vecdir.formatted'], '22')
        self.assertEqual(loopdata_pkt['day.wind.vecdir'], '22°')

    def test_cc3000_packet_processing_us_device_us_database_metric_report(self):

        config_dict = ProcessPacketTests._get_config_dict('db-us.report-metric')
        unit_system = weewx.units.unit_constants[config_dict['StdConvert'].get('target_unit', 'US').upper()]

        first_pkt_time, pkts = cc3000_packets.CC3000Packets._get_packets()
        (rainyear_accum, rainyear_start, year_accum, month_accum, week_accum,
            week_start, day_accum, hour_accum) = ProcessPacketTests._get_accums(
            config_dict, first_pkt_time)

        converter, formatter = ProcessPacketTests._get_converter_and_formatter(config_dict)
        self.assertEqual(type(converter), weewx.units.Converter)
        self.assertEqual(type(formatter), weewx.units.Formatter)

        (fields_to_include, current_obstypes, trend_obstypes, rainyear_obstypes,
            year_obstypes, month_obstypes, week_obstypes, day_obstypes, hour_obstypes,
            ten_min_obstypes, two_min_obstypes) = user.loopdata.LoopData.get_fields_to_include(_get_specified_fields())

        trend_packets = []
        ten_min_packets = []
        two_min_packets = []
        loop_frequency = 2.0
        time_delta = 10800
        baro_trend_descs = user.loopdata.LoopData.construct_baro_trend_descs({})

        for pkt in pkts:
            pkt_time = to_int(pkt['dateTime'])
            (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
                day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
                pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
                fields_to_include, current_obstypes, rainyear_accum,
                rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
                month_accum, month_obstypes, week_accum, week_start, week_obstypes,
                day_accum, day_obstypes, hour_accum, hour_obstypes, trend_packets, time_delta, trend_obstypes,
                baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        # {'dateTime': 1593975030, 'outTemp': 76.1, 'barometer': 30.014857385736513, 'dewpoint': 54.73645937493746
        # {'dateTime': 1593975366, 'outTemp': 75.4, 'barometer': 30.005222168998216, 'dewpoint': 56.53264564000546

        self.maxDiff = None

        self.assertEqual(loopdata_pkt['unit.label.outTemp'], '°C')

        self.assertEqual(loopdata_pkt['current.dateTime.raw'], 1593975366)
        self.assertEqual(loopdata_pkt['current.dateTime'], '07/05/20 11:56:06')

        self.assertEqual(loopdata_pkt['10m.windGust.max'], '12 km/h')
        self.assertEqual(loopdata_pkt['10m.windGust.max.formatted'], '12')
        self.assertTrue(loopdata_pkt['10m.windGust.max.raw'] > 11.5872 and loopdata_pkt['10m.windGust.max.raw'] < 11.5873)
        self.assertEqual(loopdata_pkt['10m.windGust.maxtime'], '07/05/20 11:50:30')
        self.assertEqual(loopdata_pkt['10m.windGust.maxtime.raw'], 1593975030)

        self.assertEqual(loopdata_pkt['10m.outTemp.max'], '24.6°C')
        self.assertEqual(loopdata_pkt['10m.outTemp.max.formatted'], '24.6')
        self.assertTrue(loopdata_pkt['10m.outTemp.max.raw'] > 24.611 and loopdata_pkt['10m.outTemp.max.raw'] < 24.612)
        self.assertEqual(loopdata_pkt['10m.outTemp.maxtime'], '07/05/20 11:50:34')
        self.assertEqual(loopdata_pkt['10m.outTemp.maxtime.raw'], 1593975034)

        self.assertEqual(loopdata_pkt['current.outTemp'], '24.1°C')
        self.assertEqual(loopdata_pkt['current.barometer'], '1016.1 mbar')
        self.assertEqual(loopdata_pkt['current.windSpeed'], '3 km/h')
        self.assertEqual(loopdata_pkt['current.windDir'], '45°')
        self.assertEqual(loopdata_pkt['current.windDir.ordinal_compass'], 'NE')

        # 30.005222168998216 - 30.014857385736513
        self.assertEqual(loopdata_pkt['trend.barometer'], '-0.3 mbar')
        self.assertTrue(loopdata_pkt['trend.barometer.raw'] > -0.3262858387019 and loopdata_pkt['trend.barometer.raw'] < -0.3262858387018)
        self.assertEqual(loopdata_pkt['trend.barometer.formatted'], '-0.3')
        self.assertEqual(loopdata_pkt['trend.barometer.code'], -1)
        self.assertEqual(loopdata_pkt['trend.barometer.desc'], 'Falling Slowly')

        # 75.4 - 76.1
        self.assertEqual(loopdata_pkt['trend.outTemp'], '-0.4°C')
        self.assertTrue(loopdata_pkt['trend.outTemp.raw'] > -0.3889 and loopdata_pkt['trend.outTemp.raw'] < -0.3888)
        self.assertEqual(loopdata_pkt['trend.outTemp.formatted'], '-0.4')

        # 56.53264564000546 - 54.73645937493746
        self.assertEqual(loopdata_pkt['trend.dewpoint'], '1.0°C')
        self.assertTrue(loopdata_pkt['trend.dewpoint.raw'] > 0.99788125837and loopdata_pkt['trend.dewpoint.raw'] < 0.99788126)
        self.assertEqual(loopdata_pkt['trend.dewpoint.formatted'], '1.0')

        self.assertEqual(loopdata_pkt['day.rain.sum'], '0.0 mm')
        self.assertEqual(loopdata_pkt['day.rain.sum.formatted'], '0.0')
        self.assertEqual(loopdata_pkt['unit.label.rain'], ' mm')

        self.assertEqual(loopdata_pkt['day.outTemp.avg'], '24.1°C')
        self.assertEqual(loopdata_pkt['day.barometer.avg'], '1016.1 mbar')
        self.assertEqual(loopdata_pkt['day.windSpeed.avg'], '6 km/h')
        self.assertEqual(loopdata_pkt['day.windDir.avg'], '166°')

        self.assertEqual(loopdata_pkt['day.outTemp.max'], '24.6°C')
        self.assertEqual(loopdata_pkt['day.barometer.max'], '1016.4 mbar')
        self.assertEqual(loopdata_pkt['day.windSpeed.max'], '12 km/h')
        self.assertEqual(loopdata_pkt['day.windDir.max'], '360°')

        self.assertEqual(loopdata_pkt['day.outTemp.min'], '23.8°C')
        self.assertEqual(loopdata_pkt['day.barometer.min'], '1016.1 mbar')
        self.assertEqual(loopdata_pkt['day.windSpeed.min'], '0 km/h')
        self.assertEqual(loopdata_pkt['day.windDir.min'], '22°')

        self.assertEqual(loopdata_pkt['unit.label.outTemp'], '°C')
        self.assertEqual(loopdata_pkt['unit.label.barometer'], ' mbar')
        self.assertEqual(loopdata_pkt['unit.label.windSpeed'], ' km/h')
        self.assertEqual(loopdata_pkt['unit.label.windDir'], '°')

        self.assertEqual(loopdata_pkt['unit.label.wind'], ' km/h')
        self.assertEqual(loopdata_pkt['day.wind.maxtime'], '07/05/20 11:50:30')
        self.assertEqual(loopdata_pkt['day.wind.max.formatted'], '12')
        self.assertEqual(loopdata_pkt['day.wind.max'], '12 km/h')
        self.assertEqual(loopdata_pkt['day.wind.gustdir.formatted'], '22')
        self.assertEqual(loopdata_pkt['day.wind.gustdir.ordinal_compass'], 'NNE')
        self.assertEqual(loopdata_pkt['day.wind.gustdir'], '22°')

        self.assertEqual(loopdata_pkt['day.wind.mintime'], '07/05/20 11:51:58')
        self.assertEqual(loopdata_pkt['day.wind.min.formatted'], '0')
        self.assertEqual(loopdata_pkt['day.wind.min'], '0 km/h')
        self.assertEqual(loopdata_pkt['unit.label.wind'], ' km/h')

        self.assertEqual(loopdata_pkt['day.wind.avg.formatted'], '6')
        self.assertEqual(loopdata_pkt['day.wind.avg'], '6 km/h')

        self.assertEqual(loopdata_pkt['day.wind.rms.formatted'], '6')
        self.assertEqual(loopdata_pkt['day.wind.rms'], '6 km/h')

        self.assertEqual(loopdata_pkt['day.wind.vecavg.formatted'], '5')
        self.assertEqual(loopdata_pkt['day.wind.vecavg'], '5 km/h')

        self.assertEqual(loopdata_pkt['day.wind.vecdir.formatted'], '22')
        self.assertEqual(loopdata_pkt['day.wind.vecdir'], '22°')

    def test_cc3000_cross_midnight_packet_processing(self):

        config_dict = ProcessPacketTests._get_config_dict('us')
        unit_system = weewx.units.unit_constants[config_dict['StdConvert'].get('target_unit', 'US').upper()]

        first_pkt_time, pkts = cc3000_cross_midnight_packets.CC3000CrossMidnightPackets._get_pre_midnight_packets()
        (rainyear_accum, rainyear_start, year_accum, month_accum, week_accum,
            week_start, day_accum, hour_accum) = ProcessPacketTests._get_accums(
            config_dict, first_pkt_time)

        converter, formatter = ProcessPacketTests._get_converter_and_formatter(config_dict)
        self.assertEqual(type(converter), weewx.units.Converter)
        self.assertEqual(type(formatter), weewx.units.Formatter)

        (fields_to_include, current_obstypes, trend_obstypes, rainyear_obstypes,
            year_obstypes, month_obstypes, week_obstypes, day_obstypes, hour_obstypes,
            ten_min_obstypes, two_min_obstypes) = user.loopdata.LoopData.get_fields_to_include(_get_specified_fields())

        trend_packets = []
        ten_min_packets = []
        two_min_packets = []
        loop_frequency = 2.0
        time_delta = 10800
        baro_trend_descs = user.loopdata.LoopData.construct_baro_trend_descs({})

        # Pre Midnight

        for pkt in pkts:
            pkt_time = to_int(pkt['dateTime'])
            (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
                day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
                pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
                fields_to_include, current_obstypes, rainyear_accum,
                rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
                month_accum, month_obstypes, week_accum, week_start, week_obstypes,
                day_accum, day_obstypes, hour_accum, hour_obstypes, trend_packets, time_delta, trend_obstypes,
                baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        # {'dateTime': 1595487600, 'outTemp': 57.3, 'outHumidity': 89.0, 'pressure': 29.85,

        self.maxDiff = None

        self.assertEqual(loopdata_pkt['unit.label.outTemp'], '°F')

        self.assertEqual(loopdata_pkt['current.dateTime.raw'], 1595487600)
        self.assertEqual(loopdata_pkt['current.dateTime'], '07/23/20 00:00:00')

        self.assertEqual(loopdata_pkt['2m.windGust.max'], '0 mph')
        self.assertEqual(loopdata_pkt['2m.windGust.max.formatted'], '0')
        self.assertEqual(loopdata_pkt['2m.windGust.max.raw'], 0.0)
        self.assertEqual(loopdata_pkt['2m.windGust.maxtime'], '07/22/20 23:58:02')
        self.assertEqual(loopdata_pkt['2m.windGust.maxtime.raw'], 1595487482)

        self.assertEqual(loopdata_pkt['2m.outTemp.max'], '57.3°F')
        self.assertEqual(loopdata_pkt['2m.outTemp.max.formatted'], '57.3')
        self.assertEqual(loopdata_pkt['2m.outTemp.max.raw'], 57.3)
        self.assertEqual(loopdata_pkt['2m.outTemp.maxtime'], '07/22/20 23:58:02')
        self.assertEqual(loopdata_pkt['2m.outTemp.maxtime.raw'], 1595487482)

        self.assertEqual(loopdata_pkt['10m.windGust.max'], '0 mph')
        self.assertEqual(loopdata_pkt['10m.windGust.max.formatted'], '0')
        self.assertEqual(loopdata_pkt['10m.windGust.max.raw'], 0.0)
        self.assertEqual(loopdata_pkt['10m.windGust.maxtime'], '07/22/20 23:50:02')
        self.assertEqual(loopdata_pkt['10m.windGust.maxtime.raw'], 1595487002)

        self.assertEqual(loopdata_pkt['10m.outTemp.max'], '57.3°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.max.formatted'], '57.3')
        self.assertEqual(loopdata_pkt['10m.outTemp.max.raw'], 57.3)
        self.assertEqual(loopdata_pkt['10m.outTemp.maxtime'], '07/22/20 23:56:10')
        self.assertEqual(loopdata_pkt['10m.outTemp.maxtime.raw'], 1595487370)

        self.assertEqual(loopdata_pkt['hour.windGust.max'], '0 mph')
        self.assertEqual(loopdata_pkt['hour.windGust.max.formatted'], '0')
        self.assertEqual(loopdata_pkt['hour.windGust.max.raw'], 0.0)
        self.assertEqual(loopdata_pkt['hour.windGust.maxtime'], '07/22/20 23:45:00')
        self.assertEqual(loopdata_pkt['hour.windGust.maxtime.raw'], 1595486700)

        self.assertEqual(loopdata_pkt['hour.outTemp.max'], '57.4°F')
        self.assertEqual(loopdata_pkt['hour.outTemp.max.formatted'], '57.4')
        self.assertEqual(loopdata_pkt['hour.outTemp.max.raw'], 57.4)
        self.assertEqual(loopdata_pkt['hour.outTemp.maxtime'], '07/22/20 23:45:00')
        self.assertEqual(loopdata_pkt['hour.outTemp.maxtime.raw'], 1595486700)

        self.assertEqual(loopdata_pkt['current.outTemp'], '57.3°F')
        self.assertEqual(loopdata_pkt['current.barometer'], '29.876 inHg')
        self.assertEqual(loopdata_pkt['current.windSpeed'], '0 mph')
        self.assertEqual(loopdata_pkt.get('current.windDir'), None)
        self.assertEqual(loopdata_pkt.get('current.windDir.ordinal_compass'), None)

        self.assertEqual(loopdata_pkt['trend.barometer'], '0.000 inHg')
        self.assertTrue(loopdata_pkt['trend.barometer.raw'] > .00000502 and loopdata_pkt['trend.barometer.raw'] < .00000503)
        self.assertEqual(loopdata_pkt['trend.barometer.formatted'], '0.000')
        self.assertEqual(loopdata_pkt['trend.barometer.code'], 0)
        self.assertEqual(loopdata_pkt['trend.barometer.desc'], 'Steady')

        self.assertEqual(loopdata_pkt['trend.outTemp'], '-0.1°F')
        #  -0.10000000000000142
        self.assertTrue(loopdata_pkt['trend.outTemp.raw'] > -0.1001 and loopdata_pkt['trend.outTemp.raw'] < -0.1000)
        self.assertEqual(loopdata_pkt['trend.outTemp.formatted'], '-0.1')

        self.assertEqual(loopdata_pkt['trend.dewpoint'], '0.5°F')
        # 0.5235414384975599
        self.assertTrue(loopdata_pkt['trend.dewpoint.raw'] > 0.523 and loopdata_pkt['trend.dewpoint.raw'] < 0.524)
        self.assertEqual(loopdata_pkt['trend.dewpoint.formatted'], '0.5')

        self.assertEqual(loopdata_pkt['day.rain.sum'], '0.00 in')
        self.assertEqual(loopdata_pkt['day.rain.sum.formatted'], '0.00')
        self.assertEqual(loopdata_pkt['unit.label.rain'], ' in')

        self.assertEqual(loopdata_pkt['day.outTemp.avg'], '57.3°F')
        self.assertEqual(loopdata_pkt['day.barometer.avg'], '29.877 inHg')
        self.assertEqual(loopdata_pkt['day.windSpeed.avg'], '0 mph')
        self.assertEqual(loopdata_pkt.get('day.windDir.avg'), None)

        self.assertEqual(loopdata_pkt['day.outTemp.max'], '57.4°F')
        self.assertEqual(loopdata_pkt['day.barometer.max'], '29.886 inHg')
        self.assertEqual(loopdata_pkt['day.windSpeed.max'], '0 mph')
        self.assertEqual(loopdata_pkt.get('day.windDir.max'), None)

        self.assertEqual(loopdata_pkt['day.outTemp.min'], '57.2°F')
        self.assertEqual(loopdata_pkt['day.barometer.min'], '29.876 inHg')
        self.assertEqual(loopdata_pkt['day.windSpeed.min'], '0 mph')
        self.assertEqual(loopdata_pkt.get('day.windDir.min'), None)

        self.assertEqual(loopdata_pkt['unit.label.outTemp'], '°F')
        self.assertEqual(loopdata_pkt['unit.label.barometer'], ' inHg')
        self.assertEqual(loopdata_pkt['unit.label.windSpeed'], ' mph')
        self.assertEqual(loopdata_pkt['unit.label.windDir'], '°')

        self.assertEqual(loopdata_pkt['unit.label.wind'], ' mph')
        self.assertEqual(loopdata_pkt['day.wind.maxtime'], '07/22/20 23:45:00')
        self.assertEqual(loopdata_pkt['day.wind.max.formatted'], '0')
        self.assertEqual(loopdata_pkt['day.wind.max'], '0 mph')
        self.assertEqual(loopdata_pkt.get('day.wind.gustdir.formatted'), None)
        self.assertEqual(loopdata_pkt.get('day.wind.gustdir.ordinal_compass'), None)
        self.assertEqual(loopdata_pkt.get('day.wind.gustdir'), None)

        self.assertEqual(loopdata_pkt['day.wind.mintime'], '07/22/20 23:45:00')
        self.assertEqual(loopdata_pkt['day.wind.min.formatted'], '0')
        self.assertEqual(loopdata_pkt['day.wind.min'], '0 mph')
        self.assertEqual(loopdata_pkt['unit.label.wind'], ' mph')

        self.assertEqual(loopdata_pkt['day.wind.avg.formatted'], '0')
        self.assertEqual(loopdata_pkt['day.wind.avg'], '0 mph')

        self.assertEqual(loopdata_pkt['day.wind.rms.formatted'], '0')
        self.assertEqual(loopdata_pkt['day.wind.rms'], '0 mph')

        self.assertEqual(loopdata_pkt['day.wind.vecavg.formatted'], '0')
        self.assertEqual(loopdata_pkt['day.wind.vecavg'], '0 mph')

        self.assertEqual(loopdata_pkt.get('day.wind.vecdir.formatted'), None)
        self.assertEqual(loopdata_pkt.get('day.wind.vecdir'), None)

        # Post Midnight

        first_pkt_time, pkts = cc3000_cross_midnight_packets.CC3000CrossMidnightPackets._get_post_midnight_packets()

        for pkt in pkts:
            pkt_time = to_int(pkt['dateTime'])
            (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
                day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
                pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
                fields_to_include, current_obstypes, rainyear_accum,
                rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
                month_accum, month_obstypes, week_accum, week_start, week_obstypes,
                day_accum, day_obstypes, hour_accum, hour_obstypes, trend_packets, time_delta, trend_obstypes,
                baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        # {'dateTime': 1595488500, 'outTemp': 58.2, 'outHumidity': 90.0, 'pressure': 29.85,

        self.maxDiff = None

        self.assertEqual(loopdata_pkt['unit.label.outTemp'], '°F')

        self.assertEqual(loopdata_pkt['current.dateTime.raw'], 1595488500)
        self.assertEqual(loopdata_pkt['current.dateTime'], '07/23/20 00:15:00')

        self.assertEqual(loopdata_pkt['10m.windGust.max'], '2 mph')
        self.assertEqual(loopdata_pkt['10m.windGust.max.formatted'], '2')
        self.assertEqual(loopdata_pkt['10m.windGust.max.raw'], 1.9)
        self.assertEqual(loopdata_pkt['10m.windGust.maxtime'], '07/23/20 00:13:02')
        self.assertEqual(loopdata_pkt['10m.windGust.maxtime.raw'], 1595488382)

        self.assertEqual(loopdata_pkt['10m.outTemp.max'], '58.2°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.max.formatted'], '58.2')
        self.assertEqual(loopdata_pkt['10m.outTemp.max.raw'], 58.2)
        self.assertEqual(loopdata_pkt['10m.outTemp.maxtime'], '07/23/20 00:14:10')
        self.assertEqual(loopdata_pkt['10m.outTemp.maxtime.raw'], 1595488450)

        self.assertEqual(loopdata_pkt['hour.windGust.max'], '2 mph')
        self.assertEqual(loopdata_pkt['hour.windGust.max.formatted'], '2')
        self.assertEqual(loopdata_pkt['hour.windGust.max.raw'], 1.9)
        self.assertEqual(loopdata_pkt['hour.windGust.maxtime'], '07/23/20 00:13:02')
        self.assertEqual(loopdata_pkt['hour.windGust.maxtime.raw'], 1595488382)

        self.assertEqual(loopdata_pkt['hour.outTemp.max'], '58.2°F')
        self.assertEqual(loopdata_pkt['hour.outTemp.max.formatted'], '58.2')
        self.assertEqual(loopdata_pkt['hour.outTemp.max.raw'], 58.2)
        self.assertEqual(loopdata_pkt['hour.outTemp.maxtime'], '07/23/20 00:14:10')
        self.assertEqual(loopdata_pkt['hour.outTemp.maxtime.raw'], 1595488450)

        self.assertEqual(loopdata_pkt['current.outTemp'], '58.2°F')
        self.assertEqual(loopdata_pkt['current.barometer'], '29.876 inHg')
        self.assertEqual(loopdata_pkt['current.windSpeed'], '2 mph')
        self.assertEqual(loopdata_pkt['current.windDir'], '45°')
        self.assertEqual(loopdata_pkt['current.windDir.ordinal_compass'], 'NE')

        self.assertEqual(loopdata_pkt['trend.barometer'], '-0.000 inHg')
        # -4.016249190996746e-05
        self.assertTrue(loopdata_pkt['trend.barometer.raw'] > -0.000040162492 and loopdata_pkt['trend.barometer.raw'] < -0.000040162491)
        self.assertEqual(loopdata_pkt['trend.barometer.formatted'], '-0.000')
        self.assertEqual(loopdata_pkt['trend.barometer.code'], 0)
        self.assertEqual(loopdata_pkt['trend.barometer.desc'], 'Steady')

        self.assertEqual(loopdata_pkt['trend.outTemp'], '0.8°F')
        # 0.8000000000000043
        self.assertTrue(loopdata_pkt['trend.outTemp.raw'] > 0.8 and loopdata_pkt['trend.outTemp.raw'] < 0.800001)
        self.assertEqual(loopdata_pkt['trend.outTemp.formatted'], '0.8')

        self.assertEqual(loopdata_pkt['trend.dewpoint'], '1.7°F')
        # 1.7183489643594356
        self.assertTrue(loopdata_pkt['trend.dewpoint.raw'] > 1.7183489 and loopdata_pkt['trend.dewpoint.raw'] < 1.718349)
        self.assertEqual(loopdata_pkt['trend.dewpoint.formatted'], '1.7')

        self.assertEqual(loopdata_pkt['day.rain.sum'], '0.00 in')
        self.assertEqual(loopdata_pkt['day.rain.sum.formatted'], '0.00')
        self.assertEqual(loopdata_pkt['unit.label.rain'], ' in')

        self.assertEqual(loopdata_pkt['day.outTemp.avg'], '57.6°F')
        self.assertEqual(loopdata_pkt['day.barometer.avg'], '29.878 inHg')
        self.assertEqual(loopdata_pkt['day.windSpeed.avg'], '0 mph')
        self.assertEqual(loopdata_pkt['day.windDir.avg'], '45°')

        self.assertEqual(loopdata_pkt['day.outTemp.max'], '58.2°F')
        self.assertEqual(loopdata_pkt['day.barometer.max'], '29.886 inHg')
        self.assertEqual(loopdata_pkt['day.windSpeed.max'], '2 mph')
        self.assertEqual(loopdata_pkt['day.windDir.max'], '45°')

        self.assertEqual(loopdata_pkt['day.outTemp.min'], '57.3°F')
        self.assertEqual(loopdata_pkt['day.barometer.min'], '29.876 inHg')
        self.assertEqual(loopdata_pkt['day.windSpeed.min'], '0 mph')
        self.assertEqual(loopdata_pkt['day.windDir.min'], '45°')

        self.assertEqual(loopdata_pkt['unit.label.outTemp'], '°F')
        self.assertEqual(loopdata_pkt['unit.label.barometer'], ' inHg')
        self.assertEqual(loopdata_pkt['unit.label.windSpeed'], ' mph')
        self.assertEqual(loopdata_pkt['unit.label.windDir'], '°')

        self.assertEqual(loopdata_pkt['unit.label.wind'], ' mph')
        self.assertEqual(loopdata_pkt['day.wind.maxtime'], '07/23/20 00:13:02')
        self.assertEqual(loopdata_pkt['day.wind.max.formatted'], '2')
        self.assertEqual(loopdata_pkt['day.wind.max'], '2 mph')
        self.assertEqual(loopdata_pkt['day.wind.gustdir.formatted'], '45')
        self.assertEqual(loopdata_pkt['day.wind.gustdir.ordinal_compass'], 'NE')
        self.assertEqual(loopdata_pkt['day.wind.gustdir'], '45°')

        self.assertEqual(loopdata_pkt['day.wind.mintime'], '07/23/20 00:00:02')
        self.assertEqual(loopdata_pkt['day.wind.min.formatted'], '0')
        self.assertEqual(loopdata_pkt['day.wind.min'], '0 mph')
        self.assertEqual(loopdata_pkt['unit.label.wind'], ' mph')

        self.assertEqual(loopdata_pkt['day.wind.avg.formatted'], '0')
        self.assertEqual(loopdata_pkt['day.wind.avg'], '0 mph')

        self.assertEqual(loopdata_pkt['day.wind.rms.formatted'], '1')
        self.assertEqual(loopdata_pkt['day.wind.rms'], '1 mph')

        self.assertEqual(loopdata_pkt['day.wind.vecavg.formatted'], '0')
        self.assertEqual(loopdata_pkt['day.wind.vecavg'], '0 mph')

        self.assertEqual(loopdata_pkt['day.wind.vecdir.formatted'], '45')
        self.assertEqual(loopdata_pkt['day.wind.vecdir'], '45°')

    def test_simulator_packet_processing(self):

        config_dict = ProcessPacketTests._get_config_dict('metric')
        unit_system = weewx.units.unit_constants[config_dict['StdConvert'].get('target_unit', 'US').upper()]

        first_pkt_time, pkts = simulator_packets.SimulatorPackets._get_packets()
        (rainyear_accum, rainyear_start, year_accum, month_accum, week_accum,
            week_start, day_accum, hour_accum) = ProcessPacketTests._get_accums(
            config_dict, first_pkt_time)

        converter, formatter = ProcessPacketTests._get_converter_and_formatter(config_dict)
        self.assertEqual(type(converter), weewx.units.Converter)
        self.assertEqual(type(formatter), weewx.units.Formatter)

        (fields_to_include, current_obstypes, trend_obstypes, rainyear_obstypes,
            year_obstypes, month_obstypes, week_obstypes, day_obstypes, hour_obstypes,
            ten_min_obstypes, two_min_obstypes) = user.loopdata.LoopData.get_fields_to_include(_get_specified_fields())

        trend_packets = []
        ten_min_packets = []
        two_min_packets = []
        loop_frequency = 2.0
        time_delta = 10800
        baro_trend_descs = user.loopdata.LoopData.construct_baro_trend_descs({})

        for pkt in pkts:
            pkt_time = to_int(pkt['dateTime'])
            (loopdata_pkt, rainyear_accum, year_accum, month_accum, week_accum,
                day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
                pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
                fields_to_include, current_obstypes, rainyear_accum,
                rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
                month_accum, month_obstypes, week_accum, week_start, week_obstypes,
                day_accum, day_obstypes, hour_accum, hour_obstypes, trend_packets, time_delta, trend_obstypes,
                baro_trend_descs, ten_min_packets, ten_min_obstypes, two_min_packets, two_min_obstypes)

        # {'dateTime': 1593976709, 'outTemp': 0.3770915275499615,  'barometer': 1053.1667173695532, 'dewpoint': -2.6645899102645934
        # {'dateTime': 1593977615, 'outTemp': 0.032246952164187964,'barometer': 1053.1483031344253, 'dewpoint': -3.003421962855377

        self.maxDiff = None

        self.assertEqual(loopdata_pkt['unit.label.outTemp'], '°C')

        self.assertEqual(loopdata_pkt['current.dateTime.raw'], 1593977615)
        self.assertEqual(loopdata_pkt['current.dateTime'], '07/05/20 12:33:35')

        self.assertEqual(loopdata_pkt['10m.windGust.max'], '0 km/h')
        self.assertEqual(loopdata_pkt['10m.windGust.max.formatted'], '0')
        self.assertTrue(loopdata_pkt['10m.windGust.max.raw'] > 0.0052 and loopdata_pkt['10m.windGust.max.raw'] < 0.0053)
        self.assertEqual(loopdata_pkt['10m.windGust.maxtime'], '07/05/20 12:33:35')
        self.assertEqual(loopdata_pkt['10m.windGust.maxtime.raw'], 1593977615)

        self.assertEqual(loopdata_pkt['10m.outTemp.max'], '1.4°C')
        self.assertEqual(loopdata_pkt['10m.outTemp.max.formatted'], '1.4')
        self.assertTrue(loopdata_pkt['10m.outTemp.max.raw'] > 1.357893759375 and loopdata_pkt['10m.outTemp.max.raw'] < 1.357893759376)
        self.assertEqual(loopdata_pkt['10m.outTemp.maxtime'], '07/05/20 12:33:19')
        self.assertEqual(loopdata_pkt['10m.outTemp.maxtime.raw'], 1593977599)

        self.assertEqual(loopdata_pkt['current.outTemp'], '0.0°C')
        self.assertEqual(loopdata_pkt['current.barometer'], '1053.1 mbar')
        self.assertEqual(loopdata_pkt['current.windSpeed'], '0 km/h')
        self.assertEqual(loopdata_pkt['current.windDir'], '360°')
        self.assertEqual(loopdata_pkt['current.windDir.ordinal_compass'], 'N')

        # 1053.1483031344253 - 1053.1667173695532
        self.assertEqual(loopdata_pkt['trend.barometer'], '-0.0 mbar')
        self.assertTrue(loopdata_pkt['trend.barometer.raw'] < -0.0184142351 and loopdata_pkt['trend.barometer.raw'] > -0.0184142352)
        self.assertEqual(loopdata_pkt['trend.barometer.formatted'], '-0.0')
        self.assertEqual(loopdata_pkt['trend.barometer.code'], 0)
        self.assertEqual(loopdata_pkt['trend.barometer.desc'], 'Steady')

        # 0.032246952164187964 - 0.3770915275499615
        self.assertEqual(loopdata_pkt['trend.outTemp'], '-0.3°C')
        self.assertTrue(loopdata_pkt['trend.outTemp.raw'] < -0.3448445753 and loopdata_pkt['trend.outTemp.raw'] > -0.3448445754)
        self.assertEqual(loopdata_pkt['trend.outTemp.formatted'], '-0.3')

        # -3.003421962855377 - -2.6645899102645934
        self.assertEqual(loopdata_pkt['trend.dewpoint'], '-0.3°C')
        self.assertTrue(loopdata_pkt['trend.dewpoint.raw'] < -0.3388320525 and loopdata_pkt['trend.dewpoint.raw'] > -0.3388320526)
        self.assertEqual(loopdata_pkt['trend.dewpoint.formatted'], '-0.3')

        self.assertEqual(loopdata_pkt['day.rain.sum'], '0.0 mm')
        self.assertEqual(loopdata_pkt['day.rain.sum.formatted'], '0.0')
        self.assertEqual(loopdata_pkt['unit.label.rain'], ' mm')

        self.assertEqual(loopdata_pkt['day.outTemp.avg'], '0.2°C')
        self.assertEqual(loopdata_pkt['day.barometer.avg'], '1053.2 mbar')
        self.assertEqual(loopdata_pkt['day.windSpeed.avg'], '0 km/h')
        self.assertEqual(loopdata_pkt['day.windDir.avg'], '360°')

        self.assertEqual(loopdata_pkt['day.outTemp.max'], '1.5°C')
        self.assertEqual(loopdata_pkt['day.barometer.max'], '1053.2 mbar')
        self.assertEqual(loopdata_pkt['day.windSpeed.max'], '0 km/h')
        self.assertEqual(loopdata_pkt['day.windDir.max'], '360°')

        self.assertEqual(loopdata_pkt['day.outTemp.min'], '0.0°C')
        self.assertEqual(loopdata_pkt['day.barometer.min'], '1053.1 mbar')
        self.assertEqual(loopdata_pkt['day.windSpeed.min'], '0 km/h')
        self.assertEqual(loopdata_pkt['day.windDir.min'], '360°')

        self.assertEqual(loopdata_pkt['unit.label.outTemp'], '°C')
        self.assertEqual(loopdata_pkt['unit.label.barometer'], ' mbar')
        self.assertEqual(loopdata_pkt['unit.label.windSpeed'], ' km/h')
        self.assertEqual(loopdata_pkt['unit.label.windDir'], '°')

        self.assertEqual(loopdata_pkt['unit.label.wind'], ' km/h')
        self.assertEqual(loopdata_pkt['day.wind.maxtime'], '07/05/20 12:33:35')
        self.assertEqual(loopdata_pkt['day.wind.max.formatted'], '0')
        self.assertEqual(loopdata_pkt['day.wind.max'], '0 km/h')
        self.assertEqual(loopdata_pkt['day.wind.gustdir.formatted'], '360')
        self.assertEqual(loopdata_pkt['day.wind.gustdir.ordinal_compass'], 'N')
        self.assertEqual(loopdata_pkt['day.wind.gustdir'], '360°')

        self.assertEqual(loopdata_pkt['day.wind.mintime'], '07/05/20 12:18:29')
        self.assertEqual(loopdata_pkt['day.wind.min.formatted'], '0')
        self.assertEqual(loopdata_pkt['day.wind.min'], '0 km/h')
        self.assertEqual(loopdata_pkt['unit.label.wind'], ' km/h')

        self.assertEqual(loopdata_pkt['day.wind.avg.formatted'], '0')
        self.assertEqual(loopdata_pkt['day.wind.avg'], '0 km/h')

        self.assertEqual(loopdata_pkt['day.wind.rms.formatted'], '0')
        self.assertEqual(loopdata_pkt['day.wind.rms'], '0 km/h')

        self.assertEqual(loopdata_pkt['day.wind.vecavg.formatted'], '0')
        self.assertEqual(loopdata_pkt['day.wind.vecavg'], '0 km/h')

        self.assertEqual(loopdata_pkt['day.wind.vecdir.formatted'], '360')
        self.assertEqual(loopdata_pkt['day.wind.vecdir'], '360°')

    @staticmethod
    def _get_accums(config_dict, pkt_time):
        """
        Returns rainyear_accum, rainyear_start, year_accum, month_accum, week_accum, week_start, day_accum.
        """

        unit_system = weewx.units.unit_constants[config_dict['StdConvert'].get('target_unit', 'US').upper()]
        rainyear_start: int = to_int(config_dict['Station']['rain_year_start'])
        week_start: int = to_int(config_dict['Station']['week_start'])

        span = weeutil.weeutil.archiveRainYearSpan(pkt_time, rainyear_start)
        rainyear_accum = weewx.accum.Accum(span, unit_system)

        span = weeutil.weeutil.archiveYearSpan(pkt_time)
        year_accum = weewx.accum.Accum(span, unit_system)

        span = weeutil.weeutil.archiveMonthSpan(pkt_time)
        month_accum = weewx.accum.Accum(span, unit_system)

        span = weeutil.weeutil.archiveWeekSpan(pkt_time, week_start)
        week_accum = weewx.accum.Accum(span, unit_system)

        timespan = weeutil.weeutil.archiveDaySpan(pkt_time)
        day_accum = weewx.accum.Accum(timespan, unit_system=unit_system)

        span = weeutil.weeutil.archiveHoursAgoSpan(pkt_time)
        hour_accum = weewx.accum.Accum(span, unit_system)

        return rainyear_accum, rainyear_start, year_accum, month_accum, week_accum, week_start, day_accum, hour_accum

    @staticmethod
    def _get_config_dict(kind):
        os.environ['TZ'] = 'America/Los_Angeles'
        return configobj.ConfigObj('bin/user/tests/weewx.conf.%s' % kind, encoding='utf-8')

    @staticmethod
    def _get_converter_and_formatter(config_dict):
        target_report_dict = user.loopdata.LoopData.get_target_report_dict(config_dict, 'SeasonsReport')

        converter = weewx.units.Converter.fromSkinDict(target_report_dict)
        formatter = weewx.units.Formatter.fromSkinDict(target_report_dict)

        return converter, formatter

def _get_specified_fields():
    return [
        '2m.windGust.max',
        '2m.windGust.max.formatted',
        '2m.windGust.max.raw',
        '2m.windGust.maxtime',
        '2m.windGust.maxtime.raw',
        '2m.outTemp.max',
        '2m.outTemp.max.formatted',
        '2m.outTemp.max.raw',
        '2m.outTemp.maxtime',
        '2m.outTemp.maxtime.raw',
        '10m.windGust.max',
        '10m.windGust.max.formatted',
        '10m.windGust.max.raw',
        '10m.windGust.maxtime',
        '10m.windGust.maxtime.raw',
        '10m.outTemp.max',
        '10m.outTemp.max.formatted',
        '10m.outTemp.max.raw',
        '10m.outTemp.maxtime',
        '10m.outTemp.maxtime.raw',
        'current.dateTime.raw',
        'current.dateTime',
        'unit.label.outTemp',
        'current.outTemp',
        'current.barometer',
        'current.windSpeed',
        'current.windDir',
        'current.windDir.ordinal_compass',
        'hour.windGust.max',
        'hour.windGust.max.formatted',
        'hour.windGust.max.raw',
        'hour.windGust.maxtime',
        'hour.windGust.maxtime.raw',
        'hour.outTemp.max',
        'hour.outTemp.max.formatted',
        'hour.outTemp.max.raw',
        'hour.outTemp.maxtime',
        'hour.outTemp.maxtime.raw',
        'day.barometer.avg',
        'day.barometer.max',
        'day.barometer.min',
        'day.outTemp.avg',
        'day.outTemp.min',
        'day.outTemp.max',
        'day.rain.sum',
        'day.rain.sum.formatted',
        'day.wind.avg',
        'day.wind.avg.formatted',
        'day.wind.max',
        'day.wind.max.formatted',
        'day.wind.gustdir',
        'day.wind.gustdir.formatted',
        'day.wind.gustdir.ordinal_compass',
        'day.wind.maxtime',
        'day.wind.min',
        'day.wind.min.formatted',
        'day.wind.mintime',
        'day.wind.rms',
        'day.wind.rms.formatted',
        'day.wind.vecavg',
        'day.wind.vecavg.formatted',
        'day.wind.vecdir',
        'day.wind.vecdir.formatted',
        'day.windDir.avg',
        'day.windDir.max',
        'day.windDir.min',
        'day.windSpeed.avg',
        'day.windSpeed.max',
        'day.windSpeed.min',
        'trend.barometer',
        'trend.barometer.raw',
        'trend.barometer.formatted',
        'trend.barometer.code',
        'trend.barometer.desc',
        'trend.outTemp',
        'trend.outTemp.raw',
        'trend.outTemp.formatted',
        'trend.dewpoint',
        'trend.dewpoint.raw',
        'trend.dewpoint.formatted',
        'unit.label.barometer',
        'unit.label.rain',
        'unit.label.wind',
        'unit.label.windDir',
        'unit.label.windSpeed',
        ]

if __name__ == '__main__':
    unittest.main()
