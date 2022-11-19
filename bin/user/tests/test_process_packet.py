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
import vantagepro2_packets

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

        cname = user.loopdata.LoopData.parse_cname('alltime')
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

        cname = user.loopdata.LoopData.parse_cname('alltime.windrun_ENE.sum.formatted')
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

        cname = user.loopdata.LoopData.parse_cname('alltime.outTemp.avg')
        self.assertEqual(cname.field, 'alltime.outTemp.avg')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'alltime')
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

        cname = user.loopdata.LoopData.parse_cname('alltime.windGust.max.formatted')
        self.assertEqual(cname.field, 'alltime.windGust.max.formatted')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'alltime')
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

        cname = user.loopdata.LoopData.parse_cname('24h.rain.sum.raw')
        self.assertEqual(cname.field, '24h.rain.sum.raw')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, '24h')
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
            'trend.barometer.desc', '2m.wind.max', '2m.wind.gustdir', '10m.wind.max', '10m.wind.gustdir', '10m.windSpeed.max', '10m.windDir.max', '24h.rain.sum', 'hour.inTemp.min', 'hour.inTemp.mintime',
            'day.barometer.min', 'day.barometer.max', 'day.wind.max', 'day.wind.gustdir', 'day.wind.maxtime' ]

        (fields_to_include, current_obstypes, trend_obstypes, alltime_obstypes, rainyear_obstypes,
            year_obstypes, month_obstypes, week_obstypes, day_obstypes, hour_obstypes,
            twentyfour_hour_obstypes, ten_min_obstypes, two_min_obstypes) = user.loopdata.LoopData.get_fields_to_include(specified_fields)

        self.assertEqual(len(fields_to_include), 18)
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
            '10m.windSpeed.max', None, None, '10m', 'windSpeed', 'max', None) in fields_to_include)
        self.assertTrue(user.loopdata.CheetahName(
            '10m.windDir.max', None, None, '10m', 'windDir', 'max', None) in fields_to_include)
        self.assertTrue(user.loopdata.CheetahName(
            '24h.rain.sum', None, None, '24h', 'rain', 'sum', None) in fields_to_include)
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

        self.assertEqual(len(current_obstypes), 10)
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

        self.assertEqual(len(twentyfour_hour_obstypes), 1)
        self.assertTrue('rain' in twentyfour_hour_obstypes)

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
        pkt = {'dateTime': 1593630000, 'usUnits': 1, 'outTemp': 77.4, 'rain': 0.01}
        pkt_time = pkt['dateTime']

        (alltime_accum, rainyear_accum, rainyear_start, year_accum, month_accum, week_accum,
            week_start, day_accum, hour_accum, twentyfour_hour_accum, ten_min_accum, two_min_accum, trend_accum) = ProcessPacketTests._get_accums(
            config_dict, pkt_time)
        self.assertEqual(week_start, 6)

        converter, formatter = ProcessPacketTests._get_converter_and_formatter(config_dict)
        self.assertEqual(type(converter), weewx.units.Converter)
        self.assertEqual(type(formatter), weewx.units.Formatter)

        specified_fields = [ 'current.outTemp', 'trend.outTemp',
                             '2m.outTemp.max', '2m.outTemp.min', '2m.outTemp.avg',
                             '10m.outTemp.max', '10m.outTemp.min', '10m.outTemp.avg',
                             '24h.rain.sum', '24h.outTemp.min', '24h.outTemp.avg',
                             'hour.outTemp.max', 'hour.outTemp.min', 'hour.outTemp.avg',
                             'day.outTemp.max', 'day.outTemp.min', 'day.outTemp.avg',
                             'week.outTemp.max', 'week.outTemp.min', 'week.outTemp.avg',
                             'month.outTemp.max', 'month.outTemp.min', 'month.outTemp.avg',
                             'year.outTemp.max', 'year.outTemp.min', 'year.outTemp.avg',
                             'rainyear.outTemp.max', 'rainyear.outTemp.min', 'rainyear.outTemp.avg',
                             'alltime.outTemp.max', 'alltime.outTemp.min', 'alltime.outTemp.avg']

        (fields_to_include, current_obstypes, trend_obstypes, alltime_obstypes, rainyear_obstypes,
            year_obstypes, month_obstypes, week_obstypes, day_obstypes, hour_obstypes,
            twentyfour_hour_obstypes, ten_min_obstypes, two_min_obstypes) = user.loopdata.LoopData.get_fields_to_include(specified_fields)

        time_delta = 10800
        loop_frequency = 2.0
        baro_trend_descs = user.loopdata.LoopData.construct_baro_trend_descs({})

        # First packet.
        (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
            baro_trend_descs)

        # Next packet 1 minute later
        pkt = {'dateTime': 1593630060, 'usUnits': 1, 'outTemp': 77.3}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
            baro_trend_descs)

        self.maxDiff = None
        self.assertEqual(loopdata_pkt['current.outTemp'], '77.3°F')
        self.assertEqual(loopdata_pkt['2m.outTemp.max'], '77.4°F')
        self.assertEqual(loopdata_pkt['2m.outTemp.min'], '77.3°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.max'], '77.4°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.min'], '77.3°F')
        self.assertEqual(loopdata_pkt['24h.rain.sum'], '0.01 in')
        self.assertEqual(loopdata_pkt['24h.outTemp.min'], '77.3°F')
        self.assertEqual(loopdata_pkt['24h.outTemp.avg'], '77.3°F')
        # New hour, since previous record (noon) was part of prev. hour.
        self.assertEqual(loopdata_pkt['hour.outTemp.max'], '77.3°F')
        self.assertEqual(loopdata_pkt['hour.outTemp.min'], '77.3°F')
        self.assertEqual(loopdata_pkt['trend.outTemp'], '-18.0°F')

        # Next packet 9 minute later
        pkt = {'dateTime': 1593630600, 'usUnits': 1, 'outTemp': 77.2, 'rain': 0.01}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
            baro_trend_descs)

        self.maxDiff = None
        self.assertEqual(loopdata_pkt['current.outTemp'], '77.2°F')
        # Previous max should have dropped off of 10m.
        self.assertEqual(loopdata_pkt['10m.outTemp.max'], '77.3°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.min'], '77.2°F')
        # hour
        self.assertEqual(loopdata_pkt['hour.outTemp.max'], '77.3°F')
        self.assertEqual(loopdata_pkt['hour.outTemp.min'], '77.2°F')
        self.assertEqual(loopdata_pkt['trend.outTemp'], '-3.6°F')
        # 24h
        self.assertEqual(loopdata_pkt['24h.rain.sum'], '0.02 in')

        # Next packet 2:51 later
        pkt = {'dateTime': 1593640860, 'usUnits': 1, 'outTemp': 76.9, 'rain': 0.00}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
            baro_trend_descs)

        self.maxDiff = None
        self.assertEqual(loopdata_pkt['current.outTemp'], '76.9°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.max'], '76.9°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.min'], '76.9°F')
        self.assertEqual(loopdata_pkt['hour.outTemp.max'], '76.9°F')
        self.assertEqual(loopdata_pkt['hour.outTemp.min'], '76.9°F')
        self.assertEqual(loopdata_pkt['trend.outTemp'], '-0.3°F')
        # 24h
        self.assertEqual(loopdata_pkt['24h.rain.sum'], '0.02 in')

        # Next packet 4:00 later
        pkt = {'dateTime': 1593655260, 'usUnits': 1, 'outTemp': 75.0, 'rain': 0.01}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum,hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
            baro_trend_descs)

        self.maxDiff = None
        self.assertEqual(loopdata_pkt['current.outTemp'], '75.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.max'], '75.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.min'], '75.0°F')
        self.assertEqual(loopdata_pkt['hour.outTemp.max'], '75.0°F')
        self.assertEqual(loopdata_pkt['hour.outTemp.min'], '75.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.min'], '75.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.max'], '77.4°F')
        self.assertEqual(loopdata_pkt.get('trend.outTemp'), None)
        self.assertEqual(loopdata_pkt['24h.rain.sum'], '0.03 in')

        # Next packet 20:00 later
        pkt = {'dateTime': 1593727260, 'usUnits': 1, 'outTemp': 70.0, 'rain': 0.00}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum,hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
            baro_trend_descs)

        self.maxDiff = None
        self.assertEqual(loopdata_pkt['current.outTemp'], '70.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.max'], '70.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.min'], '70.0°F')
        self.assertEqual(loopdata_pkt['hour.outTemp.max'], '70.0°F')
        self.assertEqual(loopdata_pkt['hour.outTemp.min'], '70.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.min'], '70.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.max'], '70.0°F')
        self.assertEqual(loopdata_pkt.get('trend.outTemp'), None)
        # 24h
        self.assertEqual(loopdata_pkt['24h.rain.sum'], '0.01 in')

        # Add another temp a minute later so we get a trend
        pkt = {'dateTime': 1593727320, 'usUnits': 1, 'outTemp': 70.0, 'rain': 0.04}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum,hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
            baro_trend_descs)

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
        # 24h
        self.assertEqual(loopdata_pkt['24h.rain.sum'], '0.05 in')

        # Jump a week
        pkt = {'dateTime': 1594332120, 'usUnits': 1, 'outTemp': 66.0, 'rain': 0.00}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum,hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
            baro_trend_descs)

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
        # 24h
        self.assertEqual(loopdata_pkt['24h.rain.sum'], '0.00 in')

        # Jump a month
        pkt = {'dateTime': 1597010520, 'usUnits': 1, 'outTemp': 88.0, 'rain': 0.01}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum,hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
            baro_trend_descs)

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
        pkt = {'dateTime': 1628546520, 'usUnits': 1, 'outTemp': 99.0, 'rain': 0.02}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum,hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
            baro_trend_descs)

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
        pkt = {'dateTime': 1628546580, 'usUnits': 1, 'outTemp': 97.0, 'rain': 0.01}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum,hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
            baro_trend_descs)

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
        self.assertEqual(loopdata_pkt['alltime.outTemp.min'],   '66.0°F')
        self.assertEqual(loopdata_pkt['alltime.outTemp.max'],   '99.0°F')
        self.assertEqual(loopdata_pkt['trend.outTemp'],         '-360.0°F')

        # Jump to October 15 (new rain year)
        # Friday, October 15, 2021 12:00:00 PM GMT-07:00 DST
        pkt = {'dateTime': 1634324400, 'usUnits': 1, 'outTemp': 41.0, 'rain': 0.00}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum,hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
            baro_trend_descs)

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
        self.assertEqual(loopdata_pkt['alltime.outTemp.min'],   '41.0°F')
        self.assertEqual(loopdata_pkt['alltime.outTemp.max'],   '99.0°F')
        self.assertEqual(loopdata_pkt.get('trend.outTemp'), None)

        # 1s later
        pkt = {'dateTime': 1634324401, 'usUnits': 1, 'outTemp': 42.0, 'rain': 0.01}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum,hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
            baro_trend_descs)

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
        self.assertEqual(loopdata_pkt['alltime.outTemp.min'],   '41.0°F')
        self.assertEqual(loopdata_pkt['alltime.outTemp.max'],   '99.0°F')
        self.assertEqual(loopdata_pkt['alltime.outTemp.avg'],   '73.6°F')
        self.assertEqual(loopdata_pkt['trend.outTemp'],         '10800.0°F')

        # About 2 days later, Sunday, October 17, 2021 04:24:27 PM PDT
        pkt = {'dateTime': 1634513067, 'usUnits': 1, 'outTemp': 88.0, 'rain': 0.00}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum,hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
            baro_trend_descs)

        # Next day, Monday, October 18, 2021 04:24:27 PM PDT
        pkt = {'dateTime': 1634599467, 'usUnits': 1, 'outTemp': 87.5, 'rain': 0.01}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum,hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
            baro_trend_descs)

        # 6 days later, Saturday, October 23, 2021 04:24:27 PM PDT
        pkt = {'dateTime': 1635031467, 'usUnits': 1, 'outTemp': 87.0, 'rain': 0.04}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum,hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
            baro_trend_descs)

        # Next day, starts a new week, the high should be 85 (from today)
        # Sunday, October 24, 2021 04:24:27 PM PDT
        pkt = {'dateTime': 1635117867, 'usUnits': 1, 'outTemp': 85.0, 'rain': 0.01}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum,hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
            baro_trend_descs)

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

        (alltime_accum, rainyear_accum, rainyear_start, year_accum, month_accum, week_accum,
            week_start, day_accum, hour_accum, twentyfour_hour_accum, ten_min_accum, two_min_accum, trend_accum) = ProcessPacketTests._get_accums(
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
                             'rainyear.outTemp.max', 'rainyear.outTemp.min', 'rainyear.outTemp.avg',
                             'alltime.outTemp.max', 'alltime.outTemp.min', 'alltime.outTemp.avg']

        (fields_to_include, current_obstypes, trend_obstypes, alltime_obstypes, rainyear_obstypes,
            year_obstypes, month_obstypes, week_obstypes, day_obstypes, hour_obstypes,
            twentyfour_hour_obstypes, ten_min_obstypes, two_min_obstypes) = user.loopdata.LoopData.get_fields_to_include(specified_fields)

        time_delta = 10800
        loop_frequency = 2.0
        baro_trend_descs = user.loopdata.LoopData.construct_baro_trend_descs({})

        # First packet.
        (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
            baro_trend_descs)

        # Next packet 1 minute later
        pkt = {'dateTime': 1593630060, 'usUnits': 1, 'outTemp': 77.3}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
            baro_trend_descs)

        self.maxDiff = None
        self.assertEqual(loopdata_pkt['current.outTemp'], '77.3°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.max'], '77.4°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.min'], '77.3°F')
        self.assertEqual(loopdata_pkt['trend.outTemp'], '-18.0°F')

        # Next packet 9 minute later
        pkt = {'dateTime': 1593630600, 'usUnits': 1, 'outTemp': 77.2}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
            baro_trend_descs)

        self.maxDiff = None
        self.assertEqual(loopdata_pkt['current.outTemp'], '77.2°F')
        # Previous max should have dropped off of 10m.
        self.assertEqual(loopdata_pkt['10m.outTemp.max'], '77.3°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.min'], '77.2°F')
        self.assertEqual(loopdata_pkt['trend.outTemp'], '-3.6°F')

        # Next packet 2:51 later
        pkt = {'dateTime': 1593640860, 'usUnits': 1, 'outTemp': 76.9}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
            baro_trend_descs)

        self.maxDiff = None
        self.assertEqual(loopdata_pkt['current.outTemp'], '76.9°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.max'], '76.9°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.min'], '76.9°F')
        self.assertEqual(loopdata_pkt['trend.outTemp'], '-0.3°F')

        # Next packet 4:00 later
        pkt = {'dateTime': 1593655260, 'usUnits': 1, 'outTemp': 75.0}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
            baro_trend_descs)

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
        (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
            baro_trend_descs)

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
        (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
            baro_trend_descs)

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
        (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
            baro_trend_descs)

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
        (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
            baro_trend_descs)

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
        (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
            baro_trend_descs)

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
        (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
            baro_trend_descs)

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
        self.assertEqual(loopdata_pkt['alltime.outTemp.min'],   '66.0°F')
        self.assertEqual(loopdata_pkt['alltime.outTemp.max'],   '99.0°F')
        self.assertEqual(loopdata_pkt['trend.outTemp'],         '-360.0°F')

        # Jump to October 15 (new rain year)
        pkt = {'dateTime': 1634324400, 'usUnits': 1, 'outTemp': 41.0}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
            baro_trend_descs)

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
        self.assertEqual(loopdata_pkt['alltime.outTemp.min'],      '41.0°F')
        self.assertEqual(loopdata_pkt['alltime.outTemp.max'],      '99.0°F')
        self.assertEqual(loopdata_pkt.get('trend.outTemp'), None)

        # 1s later
        pkt = {'dateTime': 1634324401, 'usUnits': 1, 'outTemp': 42.0}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
            baro_trend_descs)

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
        self.assertEqual(loopdata_pkt['alltime.outTemp.min'],   '41.0°F')
        self.assertEqual(loopdata_pkt['alltime.outTemp.max'],   '99.0°F')
        self.assertEqual(loopdata_pkt['alltime.outTemp.avg'],   '73.6°F')
        self.assertEqual(loopdata_pkt['trend.outTemp'],         '10800.0°F')

        # About 2 days later, Sunday, October 17, 2021 04:24:27 PM PDT
        pkt = {'dateTime': 1634513067, 'usUnits': 1, 'outTemp': 88.0}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
            baro_trend_descs)

        # Make sure we still have the old accumulator that started Monday 2021-10-11.
        self.assertEqual(timestamp_to_string(week_accum.timespan.start), '2021-10-11 00:00:00 PDT (1633935600)')

        # Next day, Monday, October 18, 2021 04:24:27 PM PDT
        pkt = {'dateTime': 1634599467, 'usUnits': 1, 'outTemp': 87.5}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
            baro_trend_descs)

        # Make sure we still have a new accumulator that started Monday 2021-10-18.
        self.assertEqual(timestamp_to_string(week_accum.timespan.start), '2021-10-18 00:00:00 PDT (1634540400)')

        # 6 days later, Saturday, October 23, 2021 04:24:27 PM PDT
        pkt = {'dateTime': 1635031467, 'usUnits': 1, 'outTemp': 87.0}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
            baro_trend_descs)

        # Next day DOES NOT START a new week since week_start is 0, high should be 87.5 (last Monday)
        # Sunday, October 24, 2021 04:24:27 PM PDT
        pkt = {'dateTime': 1635117867, 'usUnits': 1, 'outTemp': 85.0}
        pkt_time = pkt['dateTime']
        (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
            baro_trend_descs)

        # Make sure we still have the accumulator that started Monday 2021-10-18.
        self.assertEqual(timestamp_to_string(week_accum.timespan.start), '2021-10-18 00:00:00 PDT (1634540400)')
        self.assertEqual(week_accum['outTemp'].max, 87.5)
        self.assertEqual(loopdata_pkt['week.outTemp.max'], '87.5°F')

    def test_new_db_startup(self):

        config_dict = ProcessPacketTests._get_config_dict('us')
        unit_system = weewx.units.unit_constants[config_dict['StdConvert'].get('target_unit', 'US').upper()]

        pkts = [ {'dateTime': 1665796967, 'usUnits': 1, 'windDir': 355.0, 'windSpeed': 4.0, 'outTemp': 69.1},
                 {'dateTime': 1665796969, 'usUnits': 1, 'windDir':   5.0, 'windSpeed': 3.0, 'outTemp': 69.2},
                 {'dateTime': 1665796971, 'usUnits': 1, 'windDir':  10.0, 'windSpeed': 2.0, 'outTemp': 69.3}]
        first_pkt_time = pkts[0]['dateTime']
        (alltime_accum, rainyear_accum, rainyear_start, year_accum, month_accum, week_accum,
            week_start, day_accum, hour_accum, twentyfour_hour_accum, ten_min_accum, two_min_accum, trend_accum) = ProcessPacketTests._get_accums(
            config_dict, first_pkt_time)

        converter, formatter = ProcessPacketTests._get_converter_and_formatter(config_dict)
        self.assertEqual(type(converter), weewx.units.Converter)
        self.assertEqual(type(formatter), weewx.units.Formatter)

        wind_fields = [
            '2m.outTemp.avg',
            '2m.outTemp.max',
            '2m.outTemp.maxtime.raw',
            '2m.outTemp.min',
            '2m.outTemp.mintime.raw',
            '2m.wind.avg',
            '2m.wind.rms',
            '2m.wind.max',
            '2m.wind.maxtime.raw',
            '2m.wind.min',
            '2m.wind.mintime.raw',
            '2m.wind.vecdir',
            '2m.windSpeed.avg',
            '2m.windSpeed.max',
            '2m.windSpeed.maxtime.raw',
            '2m.windSpeed.min',
            '2m.windSpeed.mintime.raw',
            '2m.windDir.avg',
            'day.outTemp.avg',
            'day.outTemp.max',
            'day.outTemp.maxtime.raw',
            'day.outTemp.min',
            'day.outTemp.mintime.raw',
            'day.wind.avg',
            'day.wind.rms',
            'day.wind.max',
            'day.wind.maxtime.raw',
            'day.wind.min',
            'day.wind.mintime.raw',
            'day.wind.vecdir',
            'day.windSpeed.avg',
            'day.windSpeed.max',
            'day.windSpeed.maxtime.raw',
            'day.windSpeed.min',
            'day.windSpeed.mintime.raw',
            'day.windDir.avg',
            'current.dateTime.raw',
            'current.dateTime',
            'current.windSpeed',
            'current.windDir',
            'current.windDir.ordinal_compass',
            'unit.label.outTemp',
            'unit.label.wind',
            'unit.label.windDir',
            'unit.label.windSpeed']

        (fields_to_include, current_obstypes, trend_obstypes, alltime_obstypes, rainyear_obstypes,
            year_obstypes, month_obstypes, week_obstypes, day_obstypes, hour_obstypes,
            twentyfour_hour_obstypes, ten_min_obstypes, two_min_obstypes) = user.loopdata.LoopData.get_fields_to_include(wind_fields)

        loop_frequency = 2.0
        time_delta = 10800
        baro_trend_descs = user.loopdata.LoopData.construct_baro_trend_descs({})

        self.maxDiff = None

        # Test when adding very first packet.
        pkt = pkts[0]
        pkt_time = to_int(pkt['dateTime'])
        (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
            baro_trend_descs)

        # {'dateTime': 1665796967, 'usUnits': 1, 'windDir': 355.0, 'windSpeed': 4.0, 'outTemp': 69.1}
        self.assertEqual(loopdata_pkt['unit.label.outTemp'], '°F')
        self.assertEqual(loopdata_pkt['unit.label.wind'], ' mph')

        self.assertEqual(loopdata_pkt['current.dateTime.raw'], 1665796967)
        self.assertEqual(loopdata_pkt['current.dateTime'], '10/14/22 18:22:47')

        self.assertEqual(loopdata_pkt['2m.outTemp.avg'], '69.1°F')
        self.assertEqual(loopdata_pkt['2m.outTemp.min'], '69.1°F')
        self.assertEqual(loopdata_pkt['2m.outTemp.mintime.raw'], 1665796967)
        self.assertEqual(loopdata_pkt['2m.outTemp.max'], '69.1°F')
        self.assertEqual(loopdata_pkt['2m.outTemp.maxtime.raw'], 1665796967)

        self.assertEqual(loopdata_pkt['2m.wind.vecdir'], '355°')
        self.assertEqual(loopdata_pkt['2m.windDir.avg'], '355°')

        self.assertEqual(loopdata_pkt['2m.wind.avg'], '4 mph')
        self.assertEqual(loopdata_pkt['2m.wind.rms'], '4 mph')
        self.assertEqual(loopdata_pkt['2m.windSpeed.avg'], '4 mph')

        self.assertEqual(loopdata_pkt['2m.wind.min'], '4 mph')
        self.assertEqual(loopdata_pkt['2m.wind.mintime.raw'], 1665796967)
        self.assertEqual(loopdata_pkt['2m.wind.max'], '4 mph')
        self.assertEqual(loopdata_pkt['2m.wind.maxtime.raw'], 1665796967)
        self.assertEqual(loopdata_pkt['2m.windSpeed.min'], '4 mph')
        self.assertEqual(loopdata_pkt['2m.windSpeed.mintime.raw'], 1665796967)
        self.assertEqual(loopdata_pkt['2m.windSpeed.max'], '4 mph')
        self.assertEqual(loopdata_pkt['2m.windSpeed.maxtime.raw'], 1665796967)

        # Repeat same with day.
        self.assertEqual(loopdata_pkt['day.outTemp.avg'], '69.1°F')
        self.assertEqual(loopdata_pkt['day.outTemp.min'], '69.1°F')
        self.assertEqual(loopdata_pkt['day.outTemp.mintime.raw'], 1665796967)
        self.assertEqual(loopdata_pkt['day.outTemp.max'], '69.1°F')
        self.assertEqual(loopdata_pkt['day.outTemp.maxtime.raw'], 1665796967)

        self.assertEqual(loopdata_pkt['day.wind.vecdir'], '355°')
        self.assertEqual(loopdata_pkt['day.windDir.avg'], '355°')

        self.assertEqual(loopdata_pkt['day.wind.avg'], '4 mph')
        self.assertEqual(loopdata_pkt['day.wind.rms'], '4 mph')
        self.assertEqual(loopdata_pkt['day.windSpeed.avg'], '4 mph')

        self.assertEqual(loopdata_pkt['day.wind.min'], '4 mph')
        self.assertEqual(loopdata_pkt['day.wind.mintime.raw'], 1665796967)
        self.assertEqual(loopdata_pkt['day.wind.max'], '4 mph')
        self.assertEqual(loopdata_pkt['day.wind.maxtime.raw'], 1665796967)
        self.assertEqual(loopdata_pkt['day.windSpeed.min'], '4 mph')
        self.assertEqual(loopdata_pkt['day.windSpeed.mintime.raw'], 1665796967)
        self.assertEqual(loopdata_pkt['day.windSpeed.max'], '4 mph')
        self.assertEqual(loopdata_pkt['day.windSpeed.maxtime.raw'], 1665796967)

        # Add 2nd packet.
        pkt = pkts[1]
        pkt_time = to_int(pkt['dateTime'])
        (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
            baro_trend_descs)

        # {'dateTime': 1665796967, 'usUnits': 1, 'windDir': 355.0, 'windSpeed': 4.0, 'outTemp': 69.1}
        # {'dateTime': 1665796969, 'usUnits': 1, 'windDir':   5.0, 'windSpeed': 3.0, 'outTemp': 69.2},
        self.assertEqual(loopdata_pkt['unit.label.outTemp'], '°F')
        self.assertEqual(loopdata_pkt['unit.label.wind'], ' mph')

        self.assertEqual(loopdata_pkt['current.dateTime.raw'], 1665796969)
        self.assertEqual(loopdata_pkt['current.dateTime'], '10/14/22 18:22:49')

        self.assertEqual(loopdata_pkt['2m.outTemp.avg'], '69.2°F')
        self.assertEqual(loopdata_pkt['2m.outTemp.min'], '69.1°F')
        self.assertEqual(loopdata_pkt['2m.outTemp.mintime.raw'], 1665796967)
        self.assertEqual(loopdata_pkt['2m.outTemp.max'], '69.2°F')
        self.assertEqual(loopdata_pkt['2m.outTemp.maxtime.raw'], 1665796969)

        self.assertEqual(loopdata_pkt['2m.wind.vecdir'], '359°')
        self.assertEqual(loopdata_pkt['2m.windDir.avg'], '180°')  # A bogus value, which is why we need to use wind.vecdir.

        self.assertEqual(loopdata_pkt['2m.wind.avg'], '4 mph')
        self.assertEqual(loopdata_pkt['2m.wind.rms'], '4 mph')
        self.assertEqual(loopdata_pkt['2m.windSpeed.avg'], '4 mph')

        self.assertEqual(loopdata_pkt['2m.wind.min'], '3 mph')
        self.assertEqual(loopdata_pkt['2m.wind.mintime.raw'], 1665796969)
        self.assertEqual(loopdata_pkt['2m.wind.max'], '4 mph')
        self.assertEqual(loopdata_pkt['2m.wind.maxtime.raw'], 1665796967)
        self.assertEqual(loopdata_pkt['2m.windSpeed.min'], '3 mph')
        self.assertEqual(loopdata_pkt['2m.windSpeed.mintime.raw'], 1665796969)
        self.assertEqual(loopdata_pkt['2m.windSpeed.max'], '4 mph')
        self.assertEqual(loopdata_pkt['2m.windSpeed.maxtime.raw'], 1665796967)

        # Repeat same with day.
        self.assertEqual(loopdata_pkt['day.outTemp.avg'], '69.2°F')
        self.assertEqual(loopdata_pkt['day.outTemp.min'], '69.1°F')
        self.assertEqual(loopdata_pkt['day.outTemp.mintime.raw'], 1665796967)
        self.assertEqual(loopdata_pkt['day.outTemp.max'], '69.2°F')
        self.assertEqual(loopdata_pkt['day.outTemp.maxtime.raw'], 1665796969)

        self.assertEqual(loopdata_pkt['day.wind.vecdir'], '359°')
        self.assertEqual(loopdata_pkt['day.windDir.avg'], '180°')  # A bogus value, which is why we need to use wind.vecdir.

        self.assertEqual(loopdata_pkt['day.wind.avg'], '4 mph')
        self.assertEqual(loopdata_pkt['day.wind.rms'], '4 mph')
        self.assertEqual(loopdata_pkt['day.windSpeed.avg'], '4 mph')

        self.assertEqual(loopdata_pkt['day.wind.min'], '3 mph')
        self.assertEqual(loopdata_pkt['day.wind.mintime.raw'], 1665796969)
        self.assertEqual(loopdata_pkt['day.wind.max'], '4 mph')
        self.assertEqual(loopdata_pkt['day.wind.maxtime.raw'], 1665796967)
        self.assertEqual(loopdata_pkt['day.windSpeed.min'], '3 mph')
        self.assertEqual(loopdata_pkt['day.windSpeed.mintime.raw'], 1665796969)
        self.assertEqual(loopdata_pkt['day.windSpeed.max'], '4 mph')
        self.assertEqual(loopdata_pkt['day.windSpeed.maxtime.raw'], 1665796967)

        # Add 3rd packet.
        pkt = pkts[2]
        pkt_time = to_int(pkt['dateTime'])
        (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
            baro_trend_descs)

        # {'dateTime': 1665796967, 'usUnits': 1, 'windDir': 355.0, 'windSpeed': 4.0, 'outTemp': 69.1}
        # {'dateTime': 1665796969, 'usUnits': 1, 'windDir':   5.0, 'windSpeed': 3.0, 'outTemp': 69.2}
        # {'dateTime': 1665796971, 'usUnits': 1, 'windDir':  10.0, 'windSpeed': 2.0, 'outTemp': 69.3}
        self.assertEqual(loopdata_pkt['unit.label.outTemp'], '°F')
        self.assertEqual(loopdata_pkt['unit.label.wind'], ' mph')

        self.assertEqual(loopdata_pkt['current.dateTime.raw'], 1665796971)
        self.assertEqual(loopdata_pkt['current.dateTime'], '10/14/22 18:22:51')

        self.assertEqual(loopdata_pkt['2m.outTemp.avg'], '69.2°F')
        self.assertEqual(loopdata_pkt['2m.outTemp.min'], '69.1°F')
        self.assertEqual(loopdata_pkt['2m.outTemp.mintime.raw'], 1665796967)
        self.assertEqual(loopdata_pkt['2m.outTemp.max'], '69.3°F')
        self.assertEqual(loopdata_pkt['2m.outTemp.maxtime.raw'], 1665796971)

        self.assertEqual(loopdata_pkt['2m.wind.vecdir'], '2°')
        self.assertEqual(loopdata_pkt['2m.windDir.avg'], '123°')  # A bogus value, which is why we need to use wind.vecdir.

        self.assertEqual(loopdata_pkt['2m.wind.avg'], '3 mph')
        self.assertEqual(loopdata_pkt['2m.wind.rms'], '3 mph')
        self.assertEqual(loopdata_pkt['2m.windSpeed.avg'], '3 mph')

        self.assertEqual(loopdata_pkt['2m.wind.min'], '2 mph')
        self.assertEqual(loopdata_pkt['2m.wind.mintime.raw'], 1665796971)
        self.assertEqual(loopdata_pkt['2m.wind.max'], '4 mph')
        self.assertEqual(loopdata_pkt['2m.wind.maxtime.raw'], 1665796967)
        self.assertEqual(loopdata_pkt['2m.windSpeed.min'], '2 mph')
        self.assertEqual(loopdata_pkt['2m.windSpeed.mintime.raw'], 1665796971)
        self.assertEqual(loopdata_pkt['2m.windSpeed.max'], '4 mph')
        self.assertEqual(loopdata_pkt['2m.windSpeed.maxtime.raw'], 1665796967)

        # Repeat same with day.
        self.assertEqual(loopdata_pkt['day.outTemp.avg'], '69.2°F')
        self.assertEqual(loopdata_pkt['day.outTemp.min'], '69.1°F')
        self.assertEqual(loopdata_pkt['day.outTemp.mintime.raw'], 1665796967)
        self.assertEqual(loopdata_pkt['day.outTemp.max'], '69.3°F')
        self.assertEqual(loopdata_pkt['day.outTemp.maxtime.raw'], 1665796971)

        self.assertEqual(loopdata_pkt['day.wind.vecdir'], '2°')
        self.assertEqual(loopdata_pkt['day.windDir.avg'], '123°')  # A bogus value, which is why we need to use wind.vecdir.

        self.assertEqual(loopdata_pkt['day.wind.avg'], '3 mph')
        self.assertEqual(loopdata_pkt['day.wind.rms'], '3 mph')
        self.assertEqual(loopdata_pkt['day.windSpeed.avg'], '3 mph')

        self.assertEqual(loopdata_pkt['day.wind.min'], '2 mph')
        self.assertEqual(loopdata_pkt['day.wind.mintime.raw'], 1665796971)
        self.assertEqual(loopdata_pkt['day.wind.max'], '4 mph')
        self.assertEqual(loopdata_pkt['day.wind.maxtime.raw'], 1665796967)
        self.assertEqual(loopdata_pkt['day.windSpeed.min'], '2 mph')
        self.assertEqual(loopdata_pkt['day.windSpeed.mintime.raw'], 1665796971)
        self.assertEqual(loopdata_pkt['day.windSpeed.max'], '4 mph')
        self.assertEqual(loopdata_pkt['day.windSpeed.maxtime.raw'], 1665796967)

    def test_wind(self):

        config_dict = ProcessPacketTests._get_config_dict('us')
        unit_system = weewx.units.unit_constants[config_dict['StdConvert'].get('target_unit', 'US').upper()]

        pkts = [ {'dateTime': 1665796967, 'usUnits': 1, 'windDir': 355.0, 'windGust': 4.0, 'windGustDir': 355.0, 'windrun': None, 'windSpeed': 4.0},
                 {'dateTime': 1665796969, 'usUnits': 1, 'windDir':   5.0, 'windGust': 3.0, 'windGustDir':   5.0, 'windrun': None, 'windSpeed': 3.0},
                 {'dateTime': 1665796971, 'usUnits': 1, 'windDir':  10.0, 'windGust': 2.0, 'windGustDir':  10.0, 'windrun': None, 'windSpeed': 2.0}]
        first_pkt_time = pkts[0]['dateTime']
        (alltime_accum, rainyear_accum, rainyear_start, year_accum, month_accum, week_accum,
            week_start, day_accum, hour_accum, twentyfour_hour_accum, ten_min_accum, two_min_accum, trend_accum) = ProcessPacketTests._get_accums(
            config_dict, first_pkt_time)

        converter, formatter = ProcessPacketTests._get_converter_and_formatter(config_dict)
        self.assertEqual(type(converter), weewx.units.Converter)
        self.assertEqual(type(formatter), weewx.units.Formatter)

        wind_fields = [
            '2m.wind.avg',
            '2m.wind.rms',
            '2m.wind.max',
            '2m.wind.maxtime.raw',
            '2m.wind.min',
            '2m.wind.mintime.raw',
            '2m.wind.vecdir',
            '2m.windSpeed.avg',
            '2m.windSpeed.max',
            '2m.windSpeed.maxtime.raw',
            '2m.windSpeed.min',
            '2m.windSpeed.mintime.raw',
            '2m.windDir.avg',
            '2m.windGust.max',
            '2m.windGust.max.formatted',
            '2m.windGust.max.raw',
            '2m.windGust.maxtime',
            '2m.windGust.maxtime.raw',
            'day.wind.avg',
            'day.wind.rms',
            'day.wind.max',
            'day.wind.maxtime.raw',
            'day.wind.min',
            'day.wind.mintime.raw',
            'day.wind.vecdir',
            'day.windSpeed.avg',
            'day.windSpeed.max',
            'day.windSpeed.maxtime.raw',
            'day.windSpeed.min',
            'day.windSpeed.mintime.raw',
            'day.windDir.avg',
            'day.windGust.max',
            'day.windGust.max.formatted',
            'day.windGust.max.raw',
            'day.windGust.maxtime',
            'day.windGust.maxtime.raw',
            'current.dateTime.raw',
            'current.dateTime',
            'current.windSpeed',
            'current.windDir',
            'current.windDir.ordinal_compass',
            'unit.label.wind',
            'unit.label.windDir',
            'unit.label.windSpeed']

        (fields_to_include, current_obstypes, trend_obstypes, alltime_obstypes, rainyear_obstypes,
            year_obstypes, month_obstypes, week_obstypes, day_obstypes, hour_obstypes,
            twentyfour_hour_obstypes, ten_min_obstypes, two_min_obstypes) = user.loopdata.LoopData.get_fields_to_include(wind_fields)

        loop_frequency = 2.0
        time_delta = 10800
        baro_trend_descs = user.loopdata.LoopData.construct_baro_trend_descs({})

        for pkt in pkts:
            pkt_time = to_int(pkt['dateTime'])
            (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
                day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
                pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
                fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
                rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
                month_accum, month_obstypes, week_accum, week_start, week_obstypes,
                day_accum, day_obstypes, hour_accum, hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
                baro_trend_descs)

        self.maxDiff = None

        self.assertEqual(loopdata_pkt['unit.label.wind'], ' mph')

        self.assertEqual(loopdata_pkt['current.dateTime.raw'], 1665796971)
        self.assertEqual(loopdata_pkt['current.dateTime'], '10/14/22 18:22:51')

        self.assertEqual(loopdata_pkt['2m.wind.vecdir'], '2°')
        self.assertEqual(loopdata_pkt['2m.windDir.avg'], '123°')  # A bogus value, which is why we need to use wind.vecdir.

        self.assertEqual(loopdata_pkt['2m.wind.avg'], '3 mph')
        self.assertEqual(loopdata_pkt['2m.wind.rms'], '3 mph')
        self.assertEqual(loopdata_pkt['2m.windSpeed.avg'], '3 mph')

        self.assertEqual(loopdata_pkt['2m.wind.min'], '2 mph')
        self.assertEqual(loopdata_pkt['2m.wind.mintime.raw'], 1665796971)
        self.assertEqual(loopdata_pkt['2m.wind.max'], '4 mph')
        self.assertEqual(loopdata_pkt['2m.wind.maxtime.raw'], 1665796967)
        self.assertEqual(loopdata_pkt['2m.windSpeed.min'], '2 mph')
        self.assertEqual(loopdata_pkt['2m.windSpeed.mintime.raw'], 1665796971)
        self.assertEqual(loopdata_pkt['2m.windSpeed.max'], '4 mph')
        self.assertEqual(loopdata_pkt['2m.windSpeed.maxtime.raw'], 1665796967)

        # Repeat same with day.
        self.assertEqual(loopdata_pkt['day.wind.vecdir'], '2°')
        self.assertEqual(loopdata_pkt['day.windDir.avg'], '123°')  # A bogus value, which is why we need to use wind.vecdir.

        self.assertEqual(loopdata_pkt['day.wind.avg'], '3 mph')
        self.assertEqual(loopdata_pkt['day.wind.rms'], '3 mph')
        self.assertEqual(loopdata_pkt['day.windSpeed.avg'], '3 mph')

        self.assertEqual(loopdata_pkt['day.wind.min'], '2 mph')
        self.assertEqual(loopdata_pkt['day.wind.mintime.raw'], 1665796971)
        self.assertEqual(loopdata_pkt['day.wind.max'], '4 mph')
        self.assertEqual(loopdata_pkt['day.wind.maxtime.raw'], 1665796967)
        self.assertEqual(loopdata_pkt['day.windSpeed.min'], '2 mph')
        self.assertEqual(loopdata_pkt['day.windSpeed.mintime.raw'], 1665796971)
        self.assertEqual(loopdata_pkt['day.windSpeed.max'], '4 mph')
        self.assertEqual(loopdata_pkt['day.windSpeed.maxtime.raw'], 1665796967)

    def test_wind2(self):

        config_dict = ProcessPacketTests._get_config_dict('us')
        unit_system = weewx.units.unit_constants[config_dict['StdConvert'].get('target_unit', 'US').upper()]

        pkts = [ {'dateTime': 1665796967, 'usUnits': 1, 'windDir': 355.0, 'windGust': 4.0, 'windGustDir': 355.0, 'windrun': None, 'windSpeed': 4.0},
                 {'dateTime': 1665796969, 'usUnits': 1, 'windDir':   5.0, 'windGust': 100.0, 'windGustDir':   5.0, 'windrun': None, 'windSpeed': 100.0}]
        first_pkt_time = pkts[0]['dateTime']
        (alltime_accum, rainyear_accum, rainyear_start, year_accum, month_accum, week_accum,
            week_start, day_accum, hour_accum, twentyfour_hour_accum, ten_min_accum, two_min_accum, trend_accum) = ProcessPacketTests._get_accums(
            config_dict, first_pkt_time)

        converter, formatter = ProcessPacketTests._get_converter_and_formatter(config_dict)
        self.assertEqual(type(converter), weewx.units.Converter)
        self.assertEqual(type(formatter), weewx.units.Formatter)

        wind_fields = [
            '2m.wind.avg',
            '2m.wind.rms',
            '2m.wind.max',
            '2m.wind.maxtime.raw',
            '2m.wind.min',
            '2m.wind.mintime.raw',
            '2m.wind.vecdir',
            '2m.windSpeed.avg',
            '2m.windSpeed.max',
            '2m.windSpeed.maxtime.raw',
            '2m.windSpeed.min',
            '2m.windSpeed.mintime.raw',
            '2m.windDir.avg',
            '2m.windGust.max',
            '2m.windGust.max.formatted',
            '2m.windGust.max.raw',
            '2m.windGust.maxtime',
            '2m.windGust.maxtime.raw',
            'day.wind.avg',
            'day.wind.rms',
            'day.wind.max',
            'day.wind.maxtime.raw',
            'day.wind.min',
            'day.wind.mintime.raw',
            'day.wind.vecdir',
            'day.windSpeed.avg',
            'day.windSpeed.max',
            'day.windSpeed.maxtime.raw',
            'day.windSpeed.min',
            'day.windSpeed.mintime.raw',
            'day.windDir.avg',
            'day.windGust.max',
            'day.windGust.max.formatted',
            'day.windGust.max.raw',
            'day.windGust.maxtime',
            'day.windGust.maxtime.raw',
            'current.dateTime.raw',
            'current.dateTime',
            'current.windSpeed',
            'current.windDir',
            'current.windDir.ordinal_compass',
            'unit.label.wind',
            'unit.label.windDir',
            'unit.label.windSpeed']

        (fields_to_include, current_obstypes, trend_obstypes, alltime_obstypes, rainyear_obstypes,
            year_obstypes, month_obstypes, week_obstypes, day_obstypes, hour_obstypes,
            twentyfour_hour_obstypes, ten_min_obstypes, two_min_obstypes) = user.loopdata.LoopData.get_fields_to_include(wind_fields)

        loop_frequency = 2.0
        time_delta = 10800
        baro_trend_descs = user.loopdata.LoopData.construct_baro_trend_descs({})

        for pkt in pkts:
            pkt_time = to_int(pkt['dateTime'])
            (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
                day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
                pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
                fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
                rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
                month_accum, month_obstypes, week_accum, week_start, week_obstypes,
                day_accum, day_obstypes, hour_accum, hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
                baro_trend_descs)

        self.maxDiff = None

        self.assertEqual(loopdata_pkt['unit.label.wind'], ' mph')

        self.assertEqual(loopdata_pkt['current.dateTime.raw'], 1665796969)
        self.assertEqual(loopdata_pkt['current.dateTime'], '10/14/22 18:22:49')

        self.assertEqual(loopdata_pkt['2m.wind.vecdir'], '5°')
        self.assertEqual(loopdata_pkt['2m.windDir.avg'], '180°')  # A bogus value, which is why we need to use wind.vecdir.

        self.assertEqual(loopdata_pkt['2m.wind.avg'], '52 mph')
        self.assertEqual(loopdata_pkt['2m.wind.rms'], '71 mph')   # RMS is a better 'average' than average
        self.assertEqual(loopdata_pkt['2m.windSpeed.avg'], '52 mph')

        self.assertEqual(loopdata_pkt['2m.wind.min'], '4 mph')
        self.assertEqual(loopdata_pkt['2m.wind.mintime.raw'], 1665796967)
        self.assertEqual(loopdata_pkt['2m.wind.max'], '100 mph')
        self.assertEqual(loopdata_pkt['2m.wind.maxtime.raw'], 1665796969)
        self.assertEqual(loopdata_pkt['2m.windSpeed.min'], '4 mph')
        self.assertEqual(loopdata_pkt['2m.windSpeed.mintime.raw'], 1665796967)
        self.assertEqual(loopdata_pkt['2m.windSpeed.max'], '100 mph')
        self.assertEqual(loopdata_pkt['2m.windSpeed.maxtime.raw'], 1665796969)

        # Repeat same with day.
        self.assertEqual(loopdata_pkt['day.wind.vecdir'], '5°')
        self.assertEqual(loopdata_pkt['day.windDir.avg'], '180°')  # A bogus value, which is why we need to use wind.vecdir.

        self.assertEqual(loopdata_pkt['day.wind.avg'], '52 mph')
        self.assertEqual(loopdata_pkt['day.wind.rms'], '71 mph')   # RMS is a better 'average' than average
        self.assertEqual(loopdata_pkt['day.windSpeed.avg'], '52 mph')

        self.assertEqual(loopdata_pkt['day.wind.min'], '4 mph')
        self.assertEqual(loopdata_pkt['day.wind.mintime.raw'], 1665796967)
        self.assertEqual(loopdata_pkt['day.wind.max'], '100 mph')
        self.assertEqual(loopdata_pkt['day.wind.maxtime.raw'], 1665796969)
        self.assertEqual(loopdata_pkt['day.windSpeed.min'], '4 mph')
        self.assertEqual(loopdata_pkt['day.windSpeed.mintime.raw'], 1665796967)
        self.assertEqual(loopdata_pkt['day.windSpeed.max'], '100 mph')
        self.assertEqual(loopdata_pkt['day.windSpeed.maxtime.raw'], 1665796969)

    def test_wind_rms(self):

        config_dict = ProcessPacketTests._get_config_dict('us')
        unit_system = weewx.units.unit_constants[config_dict['StdConvert'].get('target_unit', 'US').upper()]

        pkts = [ {'dateTime': 1665796967, 'usUnits': 1, 'windGust': 0.0, 'windSpeed': 0.0},
                 {'dateTime': 1665796969, 'usUnits': 1, 'windDir':   5.0, 'windGust': 200.0, 'windGustDir':   5.0, 'windrun': None, 'windSpeed': 200.0}]
        first_pkt_time = pkts[0]['dateTime']
        (alltime_accum, rainyear_accum, rainyear_start, year_accum, month_accum, week_accum,
            week_start, day_accum, hour_accum, twentyfour_hour_accum, ten_min_accum, two_min_accum, trend_accum) = ProcessPacketTests._get_accums(
            config_dict, first_pkt_time)

        converter, formatter = ProcessPacketTests._get_converter_and_formatter(config_dict)
        self.assertEqual(type(converter), weewx.units.Converter)
        self.assertEqual(type(formatter), weewx.units.Formatter)

        wind_fields = [
            '2m.wind.avg',
            '2m.wind.rms',
            '2m.wind.max',
            '2m.wind.maxtime.raw',
            '2m.wind.min',
            '2m.wind.mintime.raw',
            '2m.wind.vecdir',
            '2m.windSpeed.avg',
            '2m.windSpeed.max',
            '2m.windSpeed.maxtime.raw',
            '2m.windSpeed.min',
            '2m.windSpeed.mintime.raw',
            '2m.windDir.avg',
            '2m.windGust.max',
            '2m.windGust.max.formatted',
            '2m.windGust.max.raw',
            '2m.windGust.maxtime',
            '2m.windGust.maxtime.raw',
            'day.wind.avg',
            'day.wind.rms',
            'day.wind.max',
            'day.wind.maxtime.raw',
            'day.wind.min',
            'day.wind.mintime.raw',
            'day.wind.vecdir',
            'day.windSpeed.avg',
            'day.windSpeed.max',
            'day.windSpeed.maxtime.raw',
            'day.windSpeed.min',
            'day.windSpeed.mintime.raw',
            'day.windDir.avg',
            'day.windGust.max',
            'day.windGust.max.formatted',
            'day.windGust.max.raw',
            'day.windGust.maxtime',
            'day.windGust.maxtime.raw',
            'current.dateTime.raw',
            'current.dateTime',
            'current.windSpeed',
            'current.windDir',
            'current.windDir.ordinal_compass',
            'unit.label.wind',
            'unit.label.windDir',
            'unit.label.windSpeed']

        (fields_to_include, current_obstypes, trend_obstypes, alltime_obstypes, rainyear_obstypes,
            year_obstypes, month_obstypes, week_obstypes, day_obstypes, hour_obstypes,
            twentyfour_hour_obstypes, ten_min_obstypes, two_min_obstypes) = user.loopdata.LoopData.get_fields_to_include(wind_fields)

        loop_frequency = 2.0
        time_delta = 10800
        baro_trend_descs = user.loopdata.LoopData.construct_baro_trend_descs({})

        pkt = pkts[0]
        pkt_time = to_int(pkt['dateTime'])
        (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
            baro_trend_descs)

        self.maxDiff = None

        # {'dateTime': 1665796967, 'usUnits': 1, 'windGust': 0.0, 'windSpeed': 0.0}
        self.assertEqual(loopdata_pkt['unit.label.wind'], ' mph')

        self.assertEqual(loopdata_pkt['current.dateTime.raw'], 1665796967)
        self.assertEqual(loopdata_pkt['current.dateTime'], '10/14/22 18:22:47')

        self.assertFalse('2m.wind.vecdir' in loopdata_pkt)
        self.assertFalse('2m.windDir.avg' in loopdata_pkt)

        self.assertEqual(loopdata_pkt['2m.wind.avg'], '0 mph')
        self.assertEqual(loopdata_pkt['2m.wind.rms'], '0 mph')   # RMS is a better 'average' than average
        self.assertEqual(loopdata_pkt['2m.windSpeed.avg'], '0 mph')

        self.assertEqual(loopdata_pkt['2m.wind.min'], '0 mph')
        self.assertEqual(loopdata_pkt['2m.wind.mintime.raw'], 1665796967)
        self.assertEqual(loopdata_pkt['2m.wind.max'], '0 mph')
        self.assertEqual(loopdata_pkt['2m.wind.maxtime.raw'], 1665796967)
        self.assertEqual(loopdata_pkt['2m.windSpeed.min'], '0 mph')
        self.assertEqual(loopdata_pkt['2m.windSpeed.mintime.raw'], 1665796967)
        self.assertEqual(loopdata_pkt['2m.windSpeed.max'], '0 mph')
        self.assertEqual(loopdata_pkt['2m.windSpeed.maxtime.raw'], 1665796967)

        # Repeat for day.
        self.assertFalse('day.wind.vecdir' in loopdata_pkt)
        self.assertFalse('day.windDir.avg' in loopdata_pkt)

        self.assertEqual(loopdata_pkt['day.wind.avg'], '0 mph')
        self.assertEqual(loopdata_pkt['day.wind.rms'], '0 mph')   # RMS is a better 'average' than average
        self.assertEqual(loopdata_pkt['day.windSpeed.avg'], '0 mph')

        self.assertEqual(loopdata_pkt['day.wind.min'], '0 mph')
        self.assertEqual(loopdata_pkt['day.wind.mintime.raw'], 1665796967)
        self.assertEqual(loopdata_pkt['day.wind.max'], '0 mph')
        self.assertEqual(loopdata_pkt['day.wind.maxtime.raw'], 1665796967)
        self.assertEqual(loopdata_pkt['day.windSpeed.min'], '0 mph')
        self.assertEqual(loopdata_pkt['day.windSpeed.mintime.raw'], 1665796967)
        self.assertEqual(loopdata_pkt['day.windSpeed.max'], '0 mph')
        self.assertEqual(loopdata_pkt['day.windSpeed.maxtime.raw'], 1665796967)

        pkt = pkts[1]
        pkt_time = to_int(pkt['dateTime'])
        (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
            day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
            pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
            fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
            rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
            month_accum, month_obstypes, week_accum, week_start, week_obstypes,
            day_accum, day_obstypes, hour_accum, hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
            baro_trend_descs)

        # {'dateTime': 1665796967, 'usUnits': 1, 'windGust': 0.0, 'windSpeed': 0.0}
        # {'dateTime': 1665796969, 'usUnits': 1, 'windDir':   5.0, 'windGust': 200.0, 'windGustDir':   5.0, 'windrun': None, 'windSpeed': 200.0}
        self.assertEqual(loopdata_pkt['unit.label.wind'], ' mph')

        self.assertEqual(loopdata_pkt['current.dateTime.raw'], 1665796969)
        self.assertEqual(loopdata_pkt['current.dateTime'], '10/14/22 18:22:49')

        self.assertEqual(loopdata_pkt['2m.wind.vecdir'], '5°')
        self.assertEqual(loopdata_pkt['2m.windDir.avg'], '5°')

        self.assertEqual(loopdata_pkt['2m.wind.avg'], '100 mph')
        self.assertEqual(loopdata_pkt['2m.wind.rms'], '141 mph')   # RMS is a better 'average' than average
        self.assertEqual(loopdata_pkt['2m.windSpeed.avg'], '100 mph')

        self.assertEqual(loopdata_pkt['2m.wind.min'], '0 mph')
        self.assertEqual(loopdata_pkt['2m.wind.mintime.raw'], 1665796967)
        self.assertEqual(loopdata_pkt['2m.wind.max'], '200 mph')
        self.assertEqual(loopdata_pkt['2m.wind.maxtime.raw'], 1665796969)
        self.assertEqual(loopdata_pkt['2m.windSpeed.min'], '0 mph')
        self.assertEqual(loopdata_pkt['2m.windSpeed.mintime.raw'], 1665796967)
        self.assertEqual(loopdata_pkt['2m.windSpeed.max'], '200 mph')
        self.assertEqual(loopdata_pkt['2m.windSpeed.maxtime.raw'], 1665796969)

        # Repeat for day.
        self.assertEqual(loopdata_pkt['day.wind.vecdir'], '5°')
        self.assertEqual(loopdata_pkt['day.windDir.avg'], '5°')

        self.assertEqual(loopdata_pkt['day.wind.avg'], '100 mph')
        self.assertEqual(loopdata_pkt['day.wind.rms'], '141 mph')   # RMS is a better 'average' than average
        self.assertEqual(loopdata_pkt['day.windSpeed.avg'], '100 mph')

        self.assertEqual(loopdata_pkt['day.wind.min'], '0 mph')
        self.assertEqual(loopdata_pkt['day.wind.mintime.raw'], 1665796967)
        self.assertEqual(loopdata_pkt['day.wind.max'], '200 mph')
        self.assertEqual(loopdata_pkt['day.wind.maxtime.raw'], 1665796969)
        self.assertEqual(loopdata_pkt['day.windSpeed.min'], '0 mph')
        self.assertEqual(loopdata_pkt['day.windSpeed.mintime.raw'], 1665796967)
        self.assertEqual(loopdata_pkt['day.windSpeed.max'], '200 mph')
        self.assertEqual(loopdata_pkt['day.windSpeed.maxtime.raw'], 1665796969)

    def test_ip100_packet_processing(self):

        config_dict = ProcessPacketTests._get_config_dict('us')
        unit_system = weewx.units.unit_constants[config_dict['StdConvert'].get('target_unit', 'US').upper()]

        first_pkt_time, pkts = ip100_packets.IP100Packets._get_packets()
        (alltime_accum, rainyear_accum, rainyear_start, year_accum, month_accum, week_accum,
            week_start, day_accum, hour_accum, twentyfour_hour_accum, ten_min_accum, two_min_accum, trend_accum) = ProcessPacketTests._get_accums(
            config_dict, first_pkt_time)

        converter, formatter = ProcessPacketTests._get_converter_and_formatter(config_dict)
        self.assertEqual(type(converter), weewx.units.Converter)
        self.assertEqual(type(formatter), weewx.units.Formatter)

        (fields_to_include, current_obstypes, trend_obstypes, alltime_obstypes, rainyear_obstypes,
            year_obstypes, month_obstypes, week_obstypes, day_obstypes, hour_obstypes,
            twentyfour_hour_obstypes, ten_min_obstypes, two_min_obstypes) = user.loopdata.LoopData.get_fields_to_include(_get_specified_fields())

        loop_frequency = 2.0
        time_delta = 10800
        baro_trend_descs = user.loopdata.LoopData.construct_baro_trend_descs({})

        for pkt in pkts:
            pkt_time = to_int(pkt['dateTime'])
            (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
                day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
                pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
                fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
                rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
                month_accum, month_obstypes, week_accum, week_start, week_obstypes,
                day_accum, day_obstypes, hour_accum, hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
                baro_trend_descs)

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
        self.assertEqual(loopdata_pkt['trend.barometer'], '-0.180 inHg')
        self.assertTrue(loopdata_pkt['trend.barometer.raw'] > -0.179578851345 and loopdata_pkt['trend.barometer.raw'] < -0.179578851344)
        self.assertEqual(loopdata_pkt['trend.barometer.formatted'], '-0.180')
        self.assertEqual(loopdata_pkt['trend.barometer.code'], -4)
        self.assertEqual(loopdata_pkt['trend.barometer.desc'], 'Falling Very Rapidly')

        # 72.0 - 71.6
        self.assertEqual(loopdata_pkt['trend.outTemp'], '15.5°F')
        self.assertTrue(loopdata_pkt['trend.outTemp.raw'] > 15.5395683453 and loopdata_pkt['trend.outTemp.raw'] < 15.5395683454)
        self.assertEqual(loopdata_pkt['trend.outTemp.formatted'], '15.5')

        # 59.57749595318801 - 60.48739574937819
        self.assertEqual(loopdata_pkt['trend.dewpoint'], '-35.3°F')
        self.assertTrue(loopdata_pkt['trend.dewpoint.raw'] > -35.3486251758 and loopdata_pkt['trend.dewpoint.raw'] < -35.3486251757)
        self.assertEqual(loopdata_pkt['trend.dewpoint.formatted'], '-35.3')

        self.assertEqual(loopdata_pkt['day.rain.sum'], '0.00 in')
        self.assertEqual(loopdata_pkt['day.rain.sum.formatted'], '0.00')
        self.assertEqual(loopdata_pkt['unit.label.rain'], ' in')

        self.assertEqual(loopdata_pkt['day.outTemp.avg'], '72.1°F')
        self.assertEqual(loopdata_pkt['day.barometer.avg'], '30.055 inHg')
        self.assertEqual(loopdata_pkt['day.windSpeed.avg'], '3 mph')
        self.assertEqual(loopdata_pkt['day.windDir.avg'], '88°')
        self.assertEqual(loopdata_pkt['day.wind.vecdir'], '26°')

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
        (alltime_accum, rainyear_accum, rainyear_start, year_accum, month_accum, week_accum,
            week_start, day_accum, hour_accum, twentyfour_hour_accum, ten_min_accum, two_min_accum, trend_accum) = ProcessPacketTests._get_accums(
            config_dict, first_pkt_time)

        converter, formatter = ProcessPacketTests._get_converter_and_formatter(config_dict)
        self.assertEqual(type(converter), weewx.units.Converter)
        self.assertEqual(type(formatter), weewx.units.Formatter)

        (fields_to_include, current_obstypes, trend_obstypes, alltime_obstypes, rainyear_obstypes,
            year_obstypes, month_obstypes, week_obstypes, day_obstypes, hour_obstypes,
            twentyfour_hour_obstypes, ten_min_obstypes, two_min_obstypes) = user.loopdata.LoopData.get_fields_to_include(_get_specified_fields())

        loop_frequency = 2.0
        time_delta = 10800
        baro_trend_descs = user.loopdata.LoopData.construct_baro_trend_descs({})

        for pkt in pkts:
            pkt_time = to_int(pkt['dateTime'])
            (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
                day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
                pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
                fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
                rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
                month_accum, month_obstypes, week_accum, week_start, week_obstypes,
                day_accum, day_obstypes, hour_accum, hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
                baro_trend_descs)

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
        self.assertEqual(loopdata_pkt['trend.barometer'], '-0.180 inHg')
        self.assertTrue(loopdata_pkt['trend.barometer.raw'] > -0.179578851345 and loopdata_pkt['trend.barometer.raw'] < -0.179578851344)
        self.assertEqual(loopdata_pkt['trend.barometer.formatted'], '-0.180')
        self.assertEqual(loopdata_pkt['trend.barometer.code'], -4)
        self.assertEqual(loopdata_pkt['trend.barometer.desc'], 'Falling Very Rapidly')

        # 72.0 - 71.6
        self.assertEqual(loopdata_pkt['trend.outTemp'], '15.5°F')
        self.assertTrue(loopdata_pkt['trend.outTemp.raw'] > 15.5395683453 and loopdata_pkt['trend.outTemp.raw'] < 15.5395683454)
        self.assertEqual(loopdata_pkt['trend.outTemp.formatted'], '15.5')

        # 59.57749595318801 - 60.48739574937819
        self.assertEqual(loopdata_pkt['trend.dewpoint'], '-35.3°F')
        self.assertTrue(loopdata_pkt['trend.dewpoint.raw'] > -35.3486251758 and loopdata_pkt['trend.dewpoint.raw'] < -35.3486251757)
        self.assertEqual(loopdata_pkt['trend.dewpoint.formatted'], '-35.3')

        self.assertEqual(loopdata_pkt['day.rain.sum'], '0.00 in')
        self.assertEqual(loopdata_pkt['day.rain.sum.formatted'], '0.00')
        self.assertEqual(loopdata_pkt['unit.label.rain'], ' in')

        self.assertEqual(loopdata_pkt['day.outTemp.avg'], '72.1°F')
        self.assertEqual(loopdata_pkt['day.barometer.avg'], '30.055 inHg')
        self.assertEqual(loopdata_pkt['day.windSpeed.avg'], '3 mph')
        self.assertEqual(loopdata_pkt['day.windDir.avg'], '88°')
        self.assertEqual(loopdata_pkt['day.wind.vecdir'], '26°')

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

    def test_vantage_pro2_packet_processing(self):

        config_dict = ProcessPacketTests._get_config_dict('us')
        unit_system = weewx.units.unit_constants[config_dict['StdConvert'].get('target_unit', 'US').upper()]

        first_pkt_time, pkts = vantagepro2_packets.VantagePro2Packets._get_batch_one_packets()
        (alltime_accum, rainyear_accum, rainyear_start, year_accum, month_accum, week_accum,
            week_start, day_accum, hour_accum, twentyfour_hour_accum, ten_min_accum, two_min_accum, trend_accum) = ProcessPacketTests._get_accums(
            config_dict, first_pkt_time)

        converter, formatter = ProcessPacketTests._get_converter_and_formatter(config_dict)
        self.assertEqual(type(converter), weewx.units.Converter)
        self.assertEqual(type(formatter), weewx.units.Formatter)

        (fields_to_include, current_obstypes, trend_obstypes, alltime_obstypes, rainyear_obstypes,
            year_obstypes, month_obstypes, week_obstypes, day_obstypes, hour_obstypes,
            twentyfour_hour_obstypes, ten_min_obstypes, two_min_obstypes) = user.loopdata.LoopData.get_fields_to_include(_get_specified_fields())

        loop_frequency = 2.0
        time_delta = 10800
        baro_trend_descs = user.loopdata.LoopData.construct_baro_trend_descs({})

        # Batch One

        for pkt in pkts:
            pkt_time = to_int(pkt['dateTime'])
            (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
                day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
                pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
                fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
                rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
                month_accum, month_obstypes, week_accum, week_start, week_obstypes,
                day_accum, day_obstypes, hour_accum, hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
                baro_trend_descs)

        # {'altimeter': '29.7797704917861', 'appTemp': '63.987293613602574', 'barometer': '29.777', 'cloudbase': '1277.245423375492', 'consBatteryVoltage': '3.78', 'dateTime': '1665797353', 'dayET': '0.087', 'dayRain': '0.0', 'dewpoint': '56.828520137147834', 'ET': 'None', 'extraAlarm1': '0', 'extraAlarm2': '0', 'extraAlarm3': '0', 'extraAlarm4': '0', 'extraAlarm5': '0', 'extraAlarm6': '0', 'extraAlarm7': '0', 'extraAlarm8': '0', 'extraHumid1': '63.0', 'extraHumid2': '50.0', 'extraTemp1': '72.0', 'extraTemp2': '76.9', 'forecastIcon': '6', 'forecastRule': '190', 'heatindex': '62.194', 'humidex': '68.24125795353245', 'inDewpoint': '58.70188105843598', 'inHumidity': '63.0', 'insideAlarm': '0', 'inTemp': '72.0', 'maxSolarRad': '0.004442804340921485', 'monthET': '1.49', 'monthRain': '0.0', 'outHumidity': '82.0', 'outsideAlarm1': '0', 'outsideAlarm2': '0', 'outTemp': '62.4', 'pm1_0': '4.6875', 'pm2_5': '5.080500000000001', 'pm2_5_aqi': '21', 'pm2_5_aqi_color': '32768', 'pm10_0': '8.0', 'pressure': '29.765246956632804', 'radiation': '5.0', 'rain': '0.0', 'rainAlarm': '0', 'rainRate': '0.0', 'soilLeafAlarm1': '0', 'soilLeafAlarm2': '0', 'soilLeafAlarm3': '0', 'soilLeafAlarm4': '0', 'stormRain': '0.0', 'sunrise': '1665756960', 'sunset': '1665797520', 'txBatteryStatus': '0', 'usUnits': '1', 'UV': '0.0', 'windchill': '62.4', 'windDir': '296.0', 'windGust': '3.0', 'windGustDir': '77.0', 'windrun': 'None', 'windSpeed': '1.0', 'windSpeed10': '2.0', 'yearET': '45.86', 'yearRain': '0.0'}])

        self.maxDiff = None

        self.assertEqual(loopdata_pkt['unit.label.outTemp'], '°F')

        self.assertEqual(loopdata_pkt['current.dateTime.raw'], 1665797353)
        self.assertEqual(loopdata_pkt['current.dateTime'], '10/14/22 18:29:13')

        self.assertEqual(loopdata_pkt['2m.windGust.max'], '3 mph')
        self.assertEqual(loopdata_pkt['2m.windGust.max.formatted'], '3')
        self.assertEqual(loopdata_pkt['2m.windGust.max.raw'], 3.0)
        self.assertEqual(loopdata_pkt['2m.windGust.maxtime'], '10/14/22 18:27:15')
        self.assertEqual(loopdata_pkt['2m.windGust.maxtime.raw'], 1665797235)

        self.assertEqual(loopdata_pkt['2m.outTemp.max'], '62.5°F')
        self.assertEqual(loopdata_pkt['2m.outTemp.max.formatted'], '62.5')
        self.assertEqual(loopdata_pkt['2m.outTemp.max.raw'], 62.5)
        self.assertEqual(loopdata_pkt['2m.outTemp.maxtime'], '10/14/22 18:27:15')
        self.assertEqual(loopdata_pkt['2m.outTemp.maxtime.raw'], 1665797235)

        self.assertEqual(loopdata_pkt['10m.windGust.max'], '5 mph')
        self.assertEqual(loopdata_pkt['10m.windGust.max.formatted'], '5')
        self.assertEqual(loopdata_pkt['10m.windGust.max.raw'], 5.0)
        self.assertEqual(loopdata_pkt['10m.windGust.maxtime'], '10/14/22 18:23:53')
        self.assertEqual(loopdata_pkt['10m.windGust.maxtime.raw'], 1665797033)

        self.assertEqual(loopdata_pkt['10m.outTemp.max'], '62.9°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.max.formatted'], '62.9')
        self.assertEqual(loopdata_pkt['10m.outTemp.max.raw'], 62.9)
        self.assertEqual(loopdata_pkt['10m.outTemp.maxtime'], '10/14/22 18:22:47')
        self.assertEqual(loopdata_pkt['10m.outTemp.maxtime.raw'], 1665796967)

        self.assertEqual(loopdata_pkt['10m.outTemp.min'], '62.4°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.min.formatted'], '62.4')
        self.assertEqual(loopdata_pkt['10m.outTemp.min.raw'], 62.4)
        self.assertEqual(loopdata_pkt['10m.outTemp.mintime'], '10/14/22 18:28:37')
        self.assertEqual(loopdata_pkt['10m.outTemp.mintime.raw'], 1665797317)

        self.assertEqual(loopdata_pkt['hour.windGust.max'], '5 mph')
        self.assertEqual(loopdata_pkt['hour.windGust.max.formatted'], '5')
        self.assertEqual(loopdata_pkt['hour.windGust.max.raw'], 5.0)
        self.assertEqual(loopdata_pkt['hour.windGust.maxtime'], '10/14/22 18:23:53')
        self.assertEqual(loopdata_pkt['hour.windGust.maxtime.raw'], 1665797033)

        self.assertEqual(loopdata_pkt['hour.outTemp.max'], '62.9°F')
        self.assertEqual(loopdata_pkt['hour.outTemp.max.formatted'], '62.9')
        self.assertEqual(loopdata_pkt['hour.outTemp.max.raw'], 62.9)
        self.assertEqual(loopdata_pkt['hour.outTemp.maxtime'], '10/14/22 18:22:47')
        self.assertEqual(loopdata_pkt['hour.outTemp.maxtime.raw'], 1665796967)

        self.assertEqual(loopdata_pkt['current.outTemp'], '62.4°F')
        self.assertEqual(loopdata_pkt['current.barometer'], '29.777 inHg')
        self.assertEqual(loopdata_pkt['current.windSpeed'], '1 mph')
        self.assertEqual(loopdata_pkt.get('current.windDir'), '296°')
        self.assertEqual(loopdata_pkt.get('current.windDir.ordinal_compass'), 'WNW')

        self.assertEqual(loopdata_pkt['trend.barometer'], '0.168 inHg')
        self.assertTrue(loopdata_pkt['trend.barometer.raw'] > 0.16787564765 and loopdata_pkt['trend.barometer.raw'] < 0.16787564767)
        self.assertEqual(loopdata_pkt['trend.barometer.formatted'], '0.168')
        self.assertEqual(loopdata_pkt['trend.barometer.code'], 3)
        self.assertEqual(loopdata_pkt['trend.barometer.desc'], 'Rising Quickly')

        #  -0.5               (unajdusted)
        # -13.989637305699482 (adjusted)
        self.assertEqual(loopdata_pkt['trend.outTemp'], '-14.0°F')
        self.assertTrue(loopdata_pkt['trend.outTemp.raw'] > -13.9896373057 and loopdata_pkt['trend.outTemp.raw'] < -13.9896373055)
        self.assertEqual(loopdata_pkt['trend.outTemp.formatted'], '-14.0')

        # seconds = 1665797353 - 1665796967 = 386s
        # 10800 / 386 = 27.9792746114
        # 56.828520137147834 - 56.828520137147834 = 0.19734268078
        # 0.19734268078 * 27.9792746114 = 5.52150505809
        self.assertEqual(loopdata_pkt['trend.dewpoint'], '5.5°F')
        self.assertTrue(loopdata_pkt['trend.dewpoint.raw'] > 5.52150505 and loopdata_pkt['trend.dewpoint.raw'] < 5.52150506)
        self.assertEqual(loopdata_pkt['trend.dewpoint.formatted'], '5.5')

        self.assertEqual(loopdata_pkt['day.rain.sum'], '0.00 in')
        self.assertEqual(loopdata_pkt['day.rain.sum.formatted'], '0.00')
        self.assertEqual(loopdata_pkt['unit.label.rain'], ' in')

        self.assertEqual(loopdata_pkt['day.rain.avg'], '0.00 in')
        self.assertEqual(loopdata_pkt['day.rain.avg.formatted'], '0.00')
        self.assertEqual(loopdata_pkt['day.rain.avg.raw'], 0.0)

        self.assertEqual(loopdata_pkt['day.outTemp.avg'], '62.7°F')
        self.assertEqual(loopdata_pkt['day.barometer.avg'], '29.773 inHg')
        self.assertEqual(loopdata_pkt['day.windSpeed.avg'], '2 mph')
        self.assertEqual(loopdata_pkt.get('day.windDir.avg'), '207°')
        self.assertEqual(loopdata_pkt.get('day.wind.vecdir'), '343°')

        self.assertEqual(loopdata_pkt['day.outTemp.max'], '62.9°F')
        self.assertEqual(loopdata_pkt['day.barometer.max'], '29.777 inHg')
        self.assertEqual(loopdata_pkt['day.windSpeed.max'], '5 mph')
        self.assertEqual(loopdata_pkt.get('day.windDir.max'), '354°')

        self.assertEqual(loopdata_pkt['day.outTemp.min'], '62.4°F')
        self.assertEqual(loopdata_pkt['day.barometer.min'], '29.771 inHg')
        self.assertEqual(loopdata_pkt['day.windSpeed.min'], '0 mph')
        self.assertEqual(loopdata_pkt.get('day.windDir.min'), '1°')

        self.assertEqual(loopdata_pkt['unit.label.outTemp'], '°F')
        self.assertEqual(loopdata_pkt['unit.label.barometer'], ' inHg')
        self.assertEqual(loopdata_pkt['unit.label.windSpeed'], ' mph')
        self.assertEqual(loopdata_pkt['unit.label.windDir'], '°')

        self.assertEqual(loopdata_pkt['unit.label.wind'], ' mph')
        self.assertEqual(loopdata_pkt['day.wind.maxtime'], '10/14/22 18:23:53')
        self.assertEqual(loopdata_pkt['day.wind.max.formatted'], '5')
        self.assertEqual(loopdata_pkt['day.wind.max'], '5 mph')
        self.assertEqual(loopdata_pkt.get('day.wind.gustdir.formatted'), '354')
        self.assertEqual(loopdata_pkt.get('day.wind.gustdir.ordinal_compass'), 'N')
        self.assertEqual(loopdata_pkt.get('day.wind.gustdir'), '354°')

        self.assertEqual(loopdata_pkt['day.wind.mintime'], '10/14/22 18:24:57')
        self.assertEqual(loopdata_pkt['day.wind.min.formatted'], '0')
        self.assertEqual(loopdata_pkt['day.wind.min'], '0 mph')
        self.assertEqual(loopdata_pkt['unit.label.wind'], ' mph')

        self.assertEqual(loopdata_pkt['day.wind.avg.formatted'], '2')
        self.assertEqual(loopdata_pkt['day.wind.avg'], '2 mph')

        self.assertEqual(loopdata_pkt['day.wind.rms.formatted'], '2')
        self.assertEqual(loopdata_pkt['day.wind.rms'], '2 mph')

        self.assertEqual(loopdata_pkt['day.wind.vecavg.formatted'], '1')
        self.assertEqual(loopdata_pkt['day.wind.vecavg'], '1 mph')

        self.assertEqual(loopdata_pkt.get('day.wind.vecdir.formatted'), '343')
        self.assertEqual(loopdata_pkt.get('day.wind.vecdir'), '343°')

        # Batch Two

        first_pkt_time, pkts = vantagepro2_packets.VantagePro2Packets._get_batch_two_packets()

        for pkt in pkts:
            pkt_time = to_int(pkt['dateTime'])
            (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
                day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
                pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
                fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
                rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
                month_accum, month_obstypes, week_accum, week_start, week_obstypes,
                day_accum, day_obstypes, hour_accum, hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
                baro_trend_descs)

        # {'altimeter': '29.77677022521029', 'appTemp': '62.44239660171717', 'barometer': '29.774', 'cloudbase': '1198.7225365853292', 'consBatteryVoltage': '3.78', 'dateTime': '1665797745', 'dayET': '0.087', 'dayRain': '0.0', 'dewpoint': '56.77402083902455', 'ET': 'None', 'extraAlarm1': '0', 'extraAlarm2': '0', 'extraAlarm3': '0', 'extraAlarm4': '0', 'extraAlarm5': '0', 'extraAlarm6': '0', 'extraAlarm7': '0', 'extraAlarm8': '0', 'extraHumid1': '63.0', 'extraHumid2': '50.0', 'extraTemp1': '72.0', 'extraTemp2': '77.0', 'forecastIcon': '6', 'forecastRule': '190', 'heatindex': '61.801', 'humidex': '67.80972824587653', 'inDewpoint': '58.70188105843598', 'inHumidity': '63.0', 'insideAlarm': '0', 'inTemp': '72.0', 'maxSolarRad': '0.0', 'monthET': '1.49', 'monthRain': '0.0', 'outHumidity': '83.0', 'outsideAlarm1': '0', 'outsideAlarm2': '0', 'outTemp': '62.0', 'pm1_0': '5.5', 'pm2_5': '5.0920000000000005', 'pm2_5_aqi': '21', 'pm2_5_aqi_color': '32768', 'pm10_0': '8.5', 'pressure': '29.762248005689198', 'radiation': '0.0', 'rain': '0.0', 'rainAlarm': '0', 'rainRate': '0.0', 'soilLeafAlarm1': '0', 'soilLeafAlarm2': '0', 'soilLeafAlarm3': '0', 'soilLeafAlarm4': '0', 'stormRain': '0.0', 'sunrise': '1665756960', 'sunset': '1665797520', 'txBatteryStatus': '0', 'usUnits': '1', 'UV': '0.0', 'windchill': '62.0', 'windDir': '321.0', 'windGust': '3.0', 'windGustDir': '274.0', 'windrun': 'None', 'windSpeed': '3.0', 'windSpeed10': '1.0', 'yearET': '45.86', 'yearRain': '0.0'}])

        self.maxDiff = None

        self.assertEqual(loopdata_pkt['unit.label.outTemp'], '°F')

        self.assertEqual(loopdata_pkt['current.dateTime.raw'], 1665797745)
        self.assertEqual(loopdata_pkt['current.dateTime'], '10/14/22 18:35:45')

        self.assertEqual(loopdata_pkt['2m.windGust.max'], '4 mph')
        self.assertEqual(loopdata_pkt['2m.windGust.max.formatted'], '4')
        self.assertEqual(loopdata_pkt['2m.windGust.max.raw'], 4.0)
        self.assertEqual(loopdata_pkt['2m.windGust.maxtime'], '10/14/22 18:33:47')
        self.assertEqual(loopdata_pkt['2m.windGust.maxtime.raw'], 1665797627)

        self.assertEqual(loopdata_pkt['2m.outTemp.max'], '62.1°F')
        self.assertEqual(loopdata_pkt['2m.outTemp.max.formatted'], '62.1')
        self.assertEqual(loopdata_pkt['2m.outTemp.max.raw'], 62.1)
        self.assertEqual(loopdata_pkt['2m.outTemp.maxtime'], '10/14/22 18:33:47')
        self.assertEqual(loopdata_pkt['2m.outTemp.maxtime.raw'], 1665797627)

        self.assertEqual(loopdata_pkt['10m.windGust.max'], '4 mph')
        self.assertEqual(loopdata_pkt['10m.windGust.max.formatted'], '4')
        self.assertEqual(loopdata_pkt['10m.windGust.max.raw'], 4.0)
        self.assertEqual(loopdata_pkt['10m.windGust.maxtime'], '10/14/22 18:30:04')
        self.assertEqual(loopdata_pkt['10m.windGust.maxtime.raw'], 1665797404)

        self.assertEqual(loopdata_pkt['10m.outTemp.max'], '62.7°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.max.formatted'], '62.7')
        self.assertEqual(loopdata_pkt['10m.outTemp.max.raw'], 62.7)
        self.assertEqual(loopdata_pkt['10m.outTemp.maxtime'], '10/14/22 18:25:47')
        self.assertEqual(loopdata_pkt['10m.outTemp.maxtime.raw'], 1665797147)

        self.assertEqual(loopdata_pkt['10m.outTemp.min'], '62.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.min.formatted'], '62.0')
        self.assertEqual(loopdata_pkt['10m.outTemp.min.raw'], 62.0)
        self.assertEqual(loopdata_pkt['10m.outTemp.mintime'], '10/14/22 18:34:35')
        self.assertEqual(loopdata_pkt['10m.outTemp.mintime.raw'], 1665797675)

        self.assertEqual(loopdata_pkt['hour.windGust.max'], '5 mph')
        self.assertEqual(loopdata_pkt['hour.windGust.max.formatted'], '5')
        self.assertEqual(loopdata_pkt['hour.windGust.max.raw'], 5.0)
        self.assertEqual(loopdata_pkt['hour.windGust.maxtime'], '10/14/22 18:23:53')
        self.assertEqual(loopdata_pkt['hour.windGust.maxtime.raw'], 1665797033)

        self.assertEqual(loopdata_pkt['hour.outTemp.max'], '62.9°F')
        self.assertEqual(loopdata_pkt['hour.outTemp.max.formatted'], '62.9')
        self.assertEqual(loopdata_pkt['hour.outTemp.max.raw'], 62.9)
        self.assertEqual(loopdata_pkt['hour.outTemp.maxtime'], '10/14/22 18:22:47')
        self.assertEqual(loopdata_pkt['hour.outTemp.maxtime.raw'], 1665796967)

        self.assertEqual(loopdata_pkt['current.outTemp'], '62.0°F')
        self.assertEqual(loopdata_pkt['current.barometer'], '29.774 inHg')
        self.assertEqual(loopdata_pkt['current.windSpeed'], '3 mph')
        self.assertEqual(loopdata_pkt.get('current.windDir'), '321°')
        self.assertEqual(loopdata_pkt.get('current.windDir.ordinal_compass'), 'NW')

        self.assertEqual(loopdata_pkt['trend.barometer'], '0.042 inHg')
        self.assertTrue(loopdata_pkt['trend.barometer.raw'] > 0.041645244215 and loopdata_pkt['trend.barometer.raw'] < 0.041645244216)
        self.assertEqual(loopdata_pkt['trend.barometer.formatted'], '0.042')
        self.assertEqual(loopdata_pkt['trend.barometer.code'], 1)
        self.assertEqual(loopdata_pkt['trend.barometer.desc'], 'Rising Slowly')

        self.assertEqual(loopdata_pkt['trend.outTemp'], '-12.5°F')
        self.assertTrue(loopdata_pkt['trend.outTemp.raw'] > -12.4935732648 and loopdata_pkt['trend.outTemp.raw'] < -12.4935732647)
        self.assertEqual(loopdata_pkt['trend.outTemp.formatted'], '-12.5')

        self.assertEqual(loopdata_pkt['trend.dewpoint'], '2.0°F')
        self.assertTrue(loopdata_pkt['trend.dewpoint.raw'] > 1.98291585182 and loopdata_pkt['trend.dewpoint.raw'] < 1.98291585183)
        self.assertEqual(loopdata_pkt['trend.dewpoint.formatted'], '2.0')

        self.assertEqual(loopdata_pkt['day.rain.sum'], '0.00 in')
        self.assertEqual(loopdata_pkt['day.rain.sum.formatted'], '0.00')
        self.assertEqual(loopdata_pkt['unit.label.rain'], ' in')

        self.assertEqual(loopdata_pkt['day.rain.avg'], '0.00 in')
        self.assertEqual(loopdata_pkt['day.rain.avg.formatted'], '0.00')
        self.assertEqual(loopdata_pkt['day.rain.avg.raw'], 0.0)

        self.assertEqual(loopdata_pkt['day.outTemp.avg'], '62.4°F')
        self.assertEqual(loopdata_pkt['day.barometer.avg'], '29.774 inHg')
        self.assertEqual(loopdata_pkt['day.windSpeed.avg'], '1 mph')
        self.assertEqual(loopdata_pkt.get('day.windDir.avg'), '226°')

        self.assertEqual(loopdata_pkt['day.outTemp.max'], '62.9°F')
        self.assertEqual(loopdata_pkt['day.barometer.max'], '29.777 inHg')
        self.assertEqual(loopdata_pkt['day.windSpeed.max'], '5 mph')
        self.assertEqual(loopdata_pkt.get('day.windDir.max'), '354°')

        self.assertEqual(loopdata_pkt['day.outTemp.min'], '62.0°F')
        self.assertEqual(loopdata_pkt['day.barometer.min'], '29.771 inHg')
        self.assertEqual(loopdata_pkt['day.windSpeed.min'], '0 mph')
        self.assertEqual(loopdata_pkt.get('day.windDir.min'), '1°')

        self.assertEqual(loopdata_pkt['unit.label.outTemp'], '°F')
        self.assertEqual(loopdata_pkt['unit.label.barometer'], ' inHg')
        self.assertEqual(loopdata_pkt['unit.label.windSpeed'], ' mph')
        self.assertEqual(loopdata_pkt['unit.label.windDir'], '°')

        self.assertEqual(loopdata_pkt['unit.label.wind'], ' mph')
        self.assertEqual(loopdata_pkt['day.wind.maxtime'], '10/14/22 18:23:53')
        self.assertEqual(loopdata_pkt['day.wind.max.formatted'], '5')
        self.assertEqual(loopdata_pkt['day.wind.max'], '5 mph')
        self.assertEqual(loopdata_pkt.get('day.wind.gustdir.formatted'), '354')
        self.assertEqual(loopdata_pkt.get('day.wind.gustdir.ordinal_compass'), 'N')
        self.assertEqual(loopdata_pkt.get('day.wind.gustdir'), '354°')

        self.assertEqual(loopdata_pkt['day.wind.mintime'], '10/14/22 18:24:57')
        self.assertEqual(loopdata_pkt['day.wind.min.formatted'], '0')
        self.assertEqual(loopdata_pkt['day.wind.min'], '0 mph')
        self.assertEqual(loopdata_pkt['unit.label.wind'], ' mph')

        self.assertEqual(loopdata_pkt['day.wind.avg.formatted'], '1')
        self.assertEqual(loopdata_pkt['day.wind.avg'], '1 mph')

        self.assertEqual(loopdata_pkt['day.wind.rms.formatted'], '2')
        self.assertEqual(loopdata_pkt['day.wind.rms'], '2 mph')

        self.assertEqual(loopdata_pkt['day.wind.vecavg.formatted'], '1')
        self.assertEqual(loopdata_pkt['day.wind.vecavg'], '1 mph')

        self.assertEqual(loopdata_pkt.get('day.wind.vecdir.formatted'), '332')
        self.assertEqual(loopdata_pkt.get('day.wind.vecdir'), '332°')

    def test_cc3000_packet_processing(self):

        config_dict = ProcessPacketTests._get_config_dict('us')
        unit_system = weewx.units.unit_constants[config_dict['StdConvert'].get('target_unit', 'US').upper()]

        first_pkt_time, pkts = cc3000_packets.CC3000Packets._get_packets()
        (alltime_accum, rainyear_accum, rainyear_start, year_accum, month_accum, week_accum,
            week_start, day_accum, hour_accum, twentyfour_hour_accum, ten_min_accum, two_min_accum, trend_accum) = ProcessPacketTests._get_accums(
            config_dict, first_pkt_time)

        converter, formatter = ProcessPacketTests._get_converter_and_formatter(config_dict)
        self.assertEqual(type(converter), weewx.units.Converter)
        self.assertEqual(type(formatter), weewx.units.Formatter)

        (fields_to_include, current_obstypes, trend_obstypes, alltime_obstypes, rainyear_obstypes,
            year_obstypes, month_obstypes, week_obstypes, day_obstypes, hour_obstypes,
            twentyfour_hour_obstypes, ten_min_obstypes, two_min_obstypes) = user.loopdata.LoopData.get_fields_to_include(_get_specified_fields())

        loop_frequency = 2.0
        time_delta = 10800
        baro_trend_descs = user.loopdata.LoopData.construct_baro_trend_descs({})

        for pkt in pkts:
            pkt_time = to_int(pkt['dateTime'])
            (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
                day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
                pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
                fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
                rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
                month_accum, month_obstypes, week_accum, week_start, week_obstypes,
                day_accum, day_obstypes, hour_accum, hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
                baro_trend_descs)

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

        self.assertEqual(loopdata_pkt['trend.barometer'], '-0.310 inHg')
        self.assertTrue(loopdata_pkt['trend.barometer.raw'] > -0.30970339516 and loopdata_pkt['trend.barometer.raw'] < -0.30970339515)
        self.assertEqual(loopdata_pkt['trend.barometer.formatted'], '-0.310')
        self.assertEqual(loopdata_pkt['trend.barometer.code'], -4)
        self.assertEqual(loopdata_pkt['trend.barometer.desc'], 'Falling Very Rapidly')

        self.assertEqual(loopdata_pkt['trend.outTemp'], '-22.5°F')
        self.assertTrue(loopdata_pkt['trend.outTemp.raw'] > -22.5 and loopdata_pkt['trend.outTemp.raw'] < -22.4999999999)
        self.assertEqual(loopdata_pkt['trend.outTemp.formatted'], '-22.5')

        # 56.53264564000546 - 54.73645937493746
        self.assertEqual(loopdata_pkt['trend.dewpoint'], '57.7°F')
        self.assertTrue(loopdata_pkt['trend.dewpoint.raw'] > 57.7345585200 and loopdata_pkt['trend.dewpoint.raw'] < 57.7345585202)
        self.assertEqual(loopdata_pkt['trend.dewpoint.formatted'], '57.7')

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
        (alltime_accum, rainyear_accum, rainyear_start, year_accum, month_accum, week_accum,
            week_start, day_accum, hour_accum, twentyfour_hour_accum, ten_min_accum, two_min_accum, trend_accum) = ProcessPacketTests._get_accums(
            config_dict, first_pkt_time)

        converter, formatter = ProcessPacketTests._get_converter_and_formatter(config_dict)
        self.assertEqual(type(converter), weewx.units.Converter)
        self.assertEqual(type(formatter), weewx.units.Formatter)

        (fields_to_include, current_obstypes, trend_obstypes, alltime_obstypes, rainyear_obstypes,
            year_obstypes, month_obstypes, week_obstypes, day_obstypes, hour_obstypes,
            twentyfour_hour_obstypes, ten_min_obstypes, two_min_obstypes) = user.loopdata.LoopData.get_fields_to_include(_get_specified_fields())

        loop_frequency = 2.0
        time_delta = 10800
        baro_trend_descs = user.loopdata.LoopData.construct_baro_trend_descs({})

        for pkt in pkts:
            pkt_time = to_int(pkt['dateTime'])
            (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
                day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
                pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
                fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
                rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
                month_accum, month_obstypes, week_accum, week_start, week_obstypes,
                day_accum, day_obstypes, hour_accum, hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
                baro_trend_descs)

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
        self.assertEqual(loopdata_pkt['trend.barometer'], '-10.5 mbar')
        self.assertTrue(loopdata_pkt['trend.barometer.raw'] > -10.4877591012 and loopdata_pkt['trend.barometer.raw'] < -10.4877591010)
        self.assertEqual(loopdata_pkt['trend.barometer.formatted'], '-10.5')
        self.assertEqual(loopdata_pkt['trend.barometer.code'], -4)
        self.assertEqual(loopdata_pkt['trend.barometer.desc'], 'Falling Very Rapidly')

        self.assertEqual(loopdata_pkt['trend.outTemp'], '-12.5°C')
        self.assertTrue(loopdata_pkt['trend.outTemp.raw'] > -12.5 and loopdata_pkt['trend.outTemp.raw'] < -12.4999999999)
        self.assertEqual(loopdata_pkt['trend.outTemp.formatted'], '-12.5')

        # 56.53264564000546 - 54.73645937493746
        self.assertEqual(loopdata_pkt['trend.dewpoint'], '32.1°C')
        self.assertTrue(loopdata_pkt['trend.dewpoint.raw'] > 32.074754733 and loopdata_pkt['trend.dewpoint.raw'] < 32.074754734)
        self.assertEqual(loopdata_pkt['trend.dewpoint.formatted'], '32.1')

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
        (alltime_accum, rainyear_accum, rainyear_start, year_accum, month_accum, week_accum,
            week_start, day_accum, hour_accum, twentyfour_hour_accum, ten_min_accum, two_min_accum, trend_accum) = ProcessPacketTests._get_accums(
            config_dict, first_pkt_time)

        converter, formatter = ProcessPacketTests._get_converter_and_formatter(config_dict)
        self.assertEqual(type(converter), weewx.units.Converter)
        self.assertEqual(type(formatter), weewx.units.Formatter)

        (fields_to_include, current_obstypes, trend_obstypes, alltime_obstypes, rainyear_obstypes,
            year_obstypes, month_obstypes, week_obstypes, day_obstypes, hour_obstypes,
            twentyfour_hour_obstypes, ten_min_obstypes, two_min_obstypes) = user.loopdata.LoopData.get_fields_to_include(_get_specified_fields())

        loop_frequency = 2.0
        time_delta = 10800
        baro_trend_descs = user.loopdata.LoopData.construct_baro_trend_descs({})

        # Pre Midnight

        for pkt in pkts:
            pkt_time = to_int(pkt['dateTime'])
            (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
                day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
                pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
                fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
                rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
                month_accum, month_obstypes, week_accum, week_start, week_obstypes,
                day_accum, day_obstypes, hour_accum, hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
                baro_trend_descs)

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
        # 0.000060348662600517855
        self.assertTrue(loopdata_pkt['trend.barometer.raw'] > 0.000060347 and loopdata_pkt['trend.barometer.raw'] < 0.000060349)
        self.assertEqual(loopdata_pkt['trend.barometer.formatted'], '0.000')
        self.assertEqual(loopdata_pkt['trend.barometer.code'], 0)
        self.assertEqual(loopdata_pkt['trend.barometer.desc'], 'Steady')

        self.assertEqual(loopdata_pkt['trend.outTemp'], '-1.2°F')
        self.assertTrue(loopdata_pkt['trend.outTemp.raw'] > -1.20001 and loopdata_pkt['trend.outTemp.raw'] < -1.19999)
        self.assertEqual(loopdata_pkt['trend.outTemp.formatted'], '-1.2')

        self.assertEqual(loopdata_pkt['trend.dewpoint'], '6.3°F')
        self.assertTrue(loopdata_pkt['trend.dewpoint.raw'] > 6.282497261 and loopdata_pkt['trend.dewpoint.raw'] < 6.282497263)
        self.assertEqual(loopdata_pkt['trend.dewpoint.formatted'], '6.3')

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
            (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
                day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
                pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
                fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
                rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
                month_accum, month_obstypes, week_accum, week_start, week_obstypes,
                day_accum, day_obstypes, hour_accum, hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
                baro_trend_descs)

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
        self.assertTrue(loopdata_pkt['trend.barometer.raw'] > -0.000241and loopdata_pkt['trend.barometer.raw'] < -0.000240)
        self.assertEqual(loopdata_pkt['trend.barometer.formatted'], '-0.000')
        self.assertEqual(loopdata_pkt['trend.barometer.code'], 0)
        self.assertEqual(loopdata_pkt['trend.barometer.desc'], 'Steady')

        self.assertEqual(loopdata_pkt['trend.outTemp'], '4.8°F')
        self.assertTrue(loopdata_pkt['trend.outTemp.raw'] >  4.8000000000 and loopdata_pkt['trend.outTemp.raw'] <  4.8000000001)
        self.assertEqual(loopdata_pkt['trend.outTemp.formatted'], '4.8')

        self.assertEqual(loopdata_pkt['trend.dewpoint'], '10.3°F')
        self.assertTrue(loopdata_pkt['trend.dewpoint.raw'] > 10.3100937860 and loopdata_pkt['trend.dewpoint.raw'] < 10.3100937862)
        self.assertEqual(loopdata_pkt['trend.dewpoint.formatted'], '10.3')

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
        (alltime_accum, rainyear_accum, rainyear_start, year_accum, month_accum, week_accum,
            week_start, day_accum, hour_accum, twentyfour_hour_accum, ten_min_accum, two_min_accum, trend_accum) = ProcessPacketTests._get_accums(
            config_dict, first_pkt_time)

        converter, formatter = ProcessPacketTests._get_converter_and_formatter(config_dict)
        self.assertEqual(type(converter), weewx.units.Converter)
        self.assertEqual(type(formatter), weewx.units.Formatter)

        (fields_to_include, current_obstypes, trend_obstypes, alltime_obstypes, rainyear_obstypes,
            year_obstypes, month_obstypes, week_obstypes, day_obstypes, hour_obstypes,
            twentyfour_hour_obstypes, ten_min_obstypes, two_min_obstypes) = user.loopdata.LoopData.get_fields_to_include(_get_specified_fields())

        loop_frequency = 2.0
        time_delta = 10800
        baro_trend_descs = user.loopdata.LoopData.construct_baro_trend_descs({})

        for pkt in pkts:
            pkt_time = to_int(pkt['dateTime'])
            (loopdata_pkt, alltime_accum, rainyear_accum, year_accum, month_accum, week_accum,
                day_accum, hour_accum) =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
                pkt, pkt_time, unit_system, loop_frequency, converter, formatter,
                fields_to_include, current_obstypes, alltime_accum, alltime_obstypes, rainyear_accum,
                rainyear_start, rainyear_obstypes, year_accum, year_obstypes,
                month_accum, month_obstypes, week_accum, week_start, week_obstypes,
                day_accum, day_obstypes, hour_accum, hour_obstypes, twentyfour_hour_accum, twentyfour_hour_obstypes, ten_min_accum, ten_min_obstypes, two_min_accum, two_min_obstypes, time_delta, trend_accum, trend_obstypes,
                baro_trend_descs)

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
        self.assertEqual(loopdata_pkt['trend.barometer'], '-0.2 mbar')
        self.assertTrue(loopdata_pkt['trend.barometer.raw'] > -0.219507438611 and loopdata_pkt['trend.barometer.raw'] < -0.21950743861)
        self.assertEqual(loopdata_pkt['trend.barometer.formatted'], '-0.2')
        self.assertEqual(loopdata_pkt['trend.barometer.code'], -1)
        self.assertEqual(loopdata_pkt['trend.barometer.desc'], 'Falling Slowly')

        # 0.032246952164187964 - 0.3770915275499615
        self.assertEqual(loopdata_pkt['trend.outTemp'], '-4.1°C')
        self.assertTrue(loopdata_pkt['trend.outTemp.raw'] > -4.1107300378 and loopdata_pkt['trend.outTemp.raw'] < -4.1107300377)
        self.assertEqual(loopdata_pkt['trend.outTemp.formatted'], '-4.1')

        # -3.003421962855377 - -2.6645899102645934
        self.assertEqual(loopdata_pkt['trend.dewpoint'], '-4.0°C')
        self.assertTrue(loopdata_pkt['trend.dewpoint.raw'] > -4.0390575806 and loopdata_pkt['trend.dewpoint.raw'] < -4.0390575805)
        self.assertEqual(loopdata_pkt['trend.dewpoint.formatted'], '-4.0')

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
        Returns alltime_accum, rainyear_accum, rainyear_start, year_accum, month_accum, week_accum, week_start, day_accum, hour_accum, twentyfour_hour_accum, ten_min_accum, two_min_accum, trend_accum.
        """

        unit_system = weewx.units.unit_constants[config_dict['StdConvert'].get('target_unit', 'US').upper()]
        rainyear_start: int = to_int(config_dict['Station']['rain_year_start'])
        week_start: int = to_int(config_dict['Station']['week_start'])

        span = weeutil.weeutil.TimeSpan(86400, 17514144000)
        alltime_accum = weewx.accum.Accum(span, unit_system)

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

        twentyfour_hour_accum = user.loopdata.ContinuousAccum(86400, unit_system)

        ten_min_accum = user.loopdata.ContinuousAccum(600, unit_system)

        two_min_accum = user.loopdata.ContinuousAccum(120, unit_system)

        trend_accum = user.loopdata.ContinuousAccum(10800, unit_system)

        return alltime_accum, rainyear_accum, rainyear_start, year_accum, month_accum, week_accum, week_start, day_accum, hour_accum, twentyfour_hour_accum, ten_min_accum, two_min_accum, trend_accum

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
        '10m.windSpeed.max',
        '10m.windDir.max',
        '10m.outTemp.max',
        '10m.outTemp.max.formatted',
        '10m.outTemp.max.raw',
        '10m.outTemp.maxtime',
        '10m.outTemp.maxtime.raw',
        '10m.outTemp.min',
        '10m.outTemp.min.formatted',
        '10m.outTemp.min.raw',
        '10m.outTemp.mintime',
        '10m.outTemp.mintime.raw',
        '24h.rain.sum',
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
        'day.rain.avg',
        'day.rain.avg.formatted',
        'day.rain.avg.raw',
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
