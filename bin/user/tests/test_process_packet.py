#    Copyright (c) 2020 John A Kline <john@johnkline.com>
#
#    See the file LICENSE.txt for your full rights.
#
"""Test processing packets."""

import configobj
import logging
import time
import unittest

import weewx
import weewx.accum
from weeutil.weeutil import to_int

from typing import Any, Dict

import weeutil.logger

import user.loopdata
import cc3000_packets
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

        cname = user.loopdata.LoopData.parse_cname('day.wind')
        self.assertEqual(cname, None)

        cname = user.loopdata.LoopData.parse_cname('day.wind.max.formatted.foo')
        self.assertEqual(cname, None)

        cname = user.loopdata.LoopData.parse_cname('trend')
        self.assertEqual(cname, None)

        cname = user.loopdata.LoopData.parse_cname('trend.wind.formatted.foo')
        self.assertEqual(cname, None)

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

        cname = user.loopdata.LoopData.parse_cname('day.rain.sum.raw')
        self.assertEqual(cname.field, 'day.rain.sum.raw')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'day')
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

    def test_get_fields_to_include(self):

        specified_fields = [ 'current.dateTime.raw', 'current.outTemp',
            'trend.barometer.desc', '10m.wind.max', '10m.wind.gustdir' ]

        fields_to_include, trend_obstypes, ten_min_obstypes = \
            user.loopdata.LoopData.get_fields_to_include(specified_fields)

        self.assertEqual(len(fields_to_include), 5)
        self.assertTrue(user.loopdata.CheetahName(
            'current.dateTime.raw', None, None, 'current', 'dateTime', None, 'raw') in fields_to_include)
        self.assertTrue(user.loopdata.CheetahName(
            'current.outTemp', None, None, 'current', 'outTemp', None, None) in fields_to_include)
        self.assertTrue(user.loopdata.CheetahName(
            'trend.barometer.desc', None, None, 'trend', 'barometer', None, 'desc') in fields_to_include)
        self.assertTrue(user.loopdata.CheetahName(
            '10m.wind.max', None, None, '10m', 'wind', 'max', None) in fields_to_include)
        self.assertTrue(user.loopdata.CheetahName(
            '10m.wind.gustdir', None, None, '10m', 'wind', 'gustdir', None) in fields_to_include)

        self.assertEqual(len(trend_obstypes), 1)
        self.assertTrue('barometer' in trend_obstypes)

        self.assertEqual(len(ten_min_obstypes), 5)
        self.assertTrue('wind' in ten_min_obstypes)
        self.assertTrue('windDir' in ten_min_obstypes)
        self.assertTrue('windGust' in ten_min_obstypes)
        self.assertTrue('windGustDir' in ten_min_obstypes)
        self.assertTrue('windSpeed' in ten_min_obstypes)

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
        fields_to_include, trend_obstypes, ten_min_obstypes = \
            user.loopdata.LoopData.get_fields_to_include(_get_specified_fields())

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
        # the first packet should be dateTime - 600
        self.assertEqual(ten_min_packets[0].timestamp, dateTime - 600)

    def test_ip100_packet_processing(self):

        config_dict = ProcessPacketTests._get_config_dict('us')
        unit_system = weewx.units.unit_constants[config_dict['StdConvert'].get('target_unit', 'US').upper()]

        first_pkt_time, pkts = ip100_packets.IP100Packets._get_packets()
        day_accum = ProcessPacketTests._get_day_accum(config_dict, first_pkt_time)

        converter, formatter = ProcessPacketTests._get_converter_and_formatter(config_dict)
        self.assertEqual(type(converter), weewx.units.Converter)
        self.assertEqual(type(formatter), weewx.units.Formatter)

        fields_to_include, trend_obstypes, ten_min_obstypes = \
            user.loopdata.LoopData.get_fields_to_include(_get_specified_fields())

        trend_packets = []
        ten_min_packets = []
        time_delta = 10800
        baro_trend_descs = user.loopdata.LoopData.construct_baro_trend_descs({})

        for in_pkt in pkts:
            pkt = weewx.units.StdUnitConverters[unit_system].convertDict(in_pkt)
            pkt['usUnits'] = unit_system
            pkt_time = to_int(pkt['dateTime'])

            loopdata_pkt: Dict[str, Any] =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
                pkt, pkt_time, unit_system, converter, formatter,
                fields_to_include, day_accum, trend_packets, time_delta, trend_obstypes,
                baro_trend_descs, ten_min_packets, ten_min_obstypes)

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

        self.assertEqual(loopdata_pkt['current.outTemp'], '72.0°F')
        self.assertEqual(loopdata_pkt['current.barometer'], '30.055 inHg')
        self.assertEqual(loopdata_pkt['current.windSpeed'], '6 mph')
        self.assertEqual(loopdata_pkt['current.windDir'], '45°')
        self.assertEqual(loopdata_pkt['current.windDir.ordinal_compass'], 'NE')

        # 30.055425865734495 - 30.060048358389471
        self.assertEqual(loopdata_pkt['trend.barometer'], '-0.005 inHg')
        self.assertTrue(loopdata_pkt['trend.barometer.raw'] < -0.0046224926 and loopdata_pkt['trend.barometer.raw'] > -0.0046224927)
        self.assertEqual(loopdata_pkt['trend.barometer.formatted'], '-0.005')
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

        self.assertEqual(loopdata_pkt['day.outTemp.avg'], '72.0°F')
        self.assertEqual(loopdata_pkt['day.barometer.avg'], '30.055 inHg')
        self.assertEqual(loopdata_pkt['day.windSpeed.avg'], '3 mph')
        self.assertEqual(loopdata_pkt['day.windDir.avg'], '87°')

        self.assertEqual(loopdata_pkt['day.outTemp.max'], '72.1°F')
        self.assertEqual(loopdata_pkt['day.barometer.max'], '30.060 inHg')
        self.assertEqual(loopdata_pkt['day.windSpeed.max'], '6 mph')
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
        self.assertEqual(loopdata_pkt['day.wind.maxtime'], '07/04/20 10:18:20')
        self.assertEqual(loopdata_pkt['day.wind.max.formatted'], '6')
        self.assertEqual(loopdata_pkt['day.wind.max'], '6 mph')
        self.assertEqual(loopdata_pkt['day.wind.gustdir.formatted'], '45')
        self.assertEqual(loopdata_pkt['day.wind.gustdir.ordinal_compass'], 'NE')
        self.assertEqual(loopdata_pkt['day.wind.gustdir'], '45°')

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

        self.assertEqual(loopdata_pkt['day.wind.vecdir.formatted'], '28')
        self.assertEqual(loopdata_pkt['day.wind.vecdir'], '28°')

    def test_ip100_us_packets_to_metric_db_to_us_report_processing(self):

        config_dict = ProcessPacketTests._get_config_dict('db-metric.report-us')
        unit_system = weewx.units.unit_constants[config_dict['StdConvert'].get('target_unit', 'US').upper()]

        first_pkt_time, pkts = ip100_packets.IP100Packets._get_packets()
        day_accum = ProcessPacketTests._get_day_accum(config_dict, first_pkt_time)

        converter, formatter = ProcessPacketTests._get_converter_and_formatter(config_dict)
        self.assertEqual(type(converter), weewx.units.Converter)
        self.assertEqual(type(formatter), weewx.units.Formatter)

        fields_to_include, trend_obstypes, ten_min_obstypes = \
            user.loopdata.LoopData.get_fields_to_include(_get_specified_fields())

        trend_packets = []
        ten_min_packets = []
        time_delta = 10800
        baro_trend_descs = user.loopdata.LoopData.construct_baro_trend_descs({})

        for in_pkt in pkts:
            pkt = weewx.units.StdUnitConverters[unit_system].convertDict(in_pkt)
            pkt['usUnits'] = unit_system
            pkt_time = to_int(pkt['dateTime'])

            loopdata_pkt: Dict[str, Any] =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
                pkt, pkt_time, unit_system, converter, formatter,
                fields_to_include, day_accum, trend_packets, time_delta, trend_obstypes,
                baro_trend_descs, ten_min_packets, ten_min_obstypes)

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

        self.assertEqual(loopdata_pkt['day.outTemp.avg'], '72.0°F')
        self.assertEqual(loopdata_pkt['day.barometer.avg'], '30.055 inHg')
        self.assertEqual(loopdata_pkt['day.windSpeed.avg'], '3 mph')
        self.assertEqual(loopdata_pkt['day.windDir.avg'], '87°')

        self.assertEqual(loopdata_pkt['day.outTemp.max'], '72.1°F')
        self.assertEqual(loopdata_pkt['day.barometer.max'], '30.060 inHg')
        self.assertEqual(loopdata_pkt['day.windSpeed.max'], '7 mph')
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
        self.assertEqual(loopdata_pkt['day.wind.maxtime'], '07/04/20 10:18:20')
        self.assertEqual(loopdata_pkt['day.wind.max.formatted'], '7')
        self.assertEqual(loopdata_pkt['day.wind.max'], '7 mph')
        self.assertEqual(loopdata_pkt['day.wind.gustdir.formatted'], '45')
        self.assertEqual(loopdata_pkt['day.wind.gustdir.ordinal_compass'], 'NE')
        self.assertEqual(loopdata_pkt['day.wind.gustdir'], '45°')

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

        self.assertEqual(loopdata_pkt['day.wind.vecdir.formatted'], '28')
        self.assertEqual(loopdata_pkt['day.wind.vecdir'], '28°')

    def test_cc3000_packet_processing(self):

        config_dict = ProcessPacketTests._get_config_dict('us')
        unit_system = weewx.units.unit_constants[config_dict['StdConvert'].get('target_unit', 'US').upper()]

        first_pkt_time, pkts = cc3000_packets.CC3000Packets._get_packets()
        day_accum = ProcessPacketTests._get_day_accum(config_dict, first_pkt_time)

        converter, formatter = ProcessPacketTests._get_converter_and_formatter(config_dict)
        self.assertEqual(type(converter), weewx.units.Converter)
        self.assertEqual(type(formatter), weewx.units.Formatter)

        fields_to_include, trend_obstypes, ten_min_obstypes = \
            user.loopdata.LoopData.get_fields_to_include(_get_specified_fields())

        trend_packets = []
        ten_min_packets = []
        time_delta = 10800
        baro_trend_descs = user.loopdata.LoopData.construct_baro_trend_descs({})

        for in_pkt in pkts:
            pkt = weewx.units.StdUnitConverters[unit_system].convertDict(in_pkt)
            pkt['usUnits'] = unit_system
            pkt_time = to_int(pkt['dateTime'])

            loopdata_pkt: Dict[str, Any] =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
                pkt, pkt_time, unit_system, converter, formatter,
                fields_to_include, day_accum, trend_packets, time_delta, trend_obstypes,
                baro_trend_descs, ten_min_packets, ten_min_obstypes)

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

        self.assertEqual(loopdata_pkt['current.outTemp'], '75.4°F')
        self.assertEqual(loopdata_pkt['current.barometer'], '30.005 inHg')
        self.assertEqual(loopdata_pkt['current.windSpeed'], '2 mph')
        self.assertEqual(loopdata_pkt['current.windDir'], '45°')
        self.assertEqual(loopdata_pkt['current.windDir.ordinal_compass'], 'NE')

        # 30.005222168998216 - 30.014857385736513
        self.assertEqual(loopdata_pkt['trend.barometer'], '-0.010 inHg')
        self.assertTrue(loopdata_pkt['trend.barometer.raw'] > -0.0096352168 and loopdata_pkt['trend.barometer.raw'] < -0.0096352167)
        self.assertEqual(loopdata_pkt['trend.barometer.formatted'], '-0.010')
        self.assertEqual(loopdata_pkt['trend.barometer.desc'], 'Falling Slowly')

        # 75.4 - 76.1
        self.assertEqual(loopdata_pkt['trend.outTemp'], '-0.7°F')
        self.assertTrue(loopdata_pkt['trend.outTemp.raw'] > -0.6999 and loopdata_pkt['trend.outTemp.raw'] < -0.7001)
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
        day_accum = ProcessPacketTests._get_day_accum(config_dict, first_pkt_time)

        converter, formatter = ProcessPacketTests._get_converter_and_formatter(config_dict)
        self.assertEqual(type(converter), weewx.units.Converter)
        self.assertEqual(type(formatter), weewx.units.Formatter)

        fields_to_include, trend_obstypes, ten_min_obstypes = \
            user.loopdata.LoopData.get_fields_to_include(_get_specified_fields())

        trend_packets = []
        ten_min_packets = []
        time_delta = 10800
        baro_trend_descs = user.loopdata.LoopData.construct_baro_trend_descs({})

        for in_pkt in pkts:
            pkt = weewx.units.StdUnitConverters[unit_system].convertDict(in_pkt)
            pkt['usUnits'] = unit_system
            pkt_time = to_int(pkt['dateTime'])

            loopdata_pkt: Dict[str, Any] =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
                pkt, pkt_time, unit_system, converter, formatter,
                fields_to_include, day_accum, trend_packets, time_delta, trend_obstypes,
                baro_trend_descs, ten_min_packets, ten_min_obstypes)

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

    def test_simulator_packet_processing(self):

        config_dict = ProcessPacketTests._get_config_dict('metric')
        unit_system = weewx.units.unit_constants[config_dict['StdConvert'].get('target_unit', 'US').upper()]

        first_pkt_time, pkts = simulator_packets.SimulatorPackets._get_packets()
        day_accum = ProcessPacketTests._get_day_accum(config_dict, first_pkt_time)

        converter, formatter = ProcessPacketTests._get_converter_and_formatter(config_dict)
        self.assertEqual(type(converter), weewx.units.Converter)
        self.assertEqual(type(formatter), weewx.units.Formatter)

        fields_to_include, trend_obstypes, ten_min_obstypes = \
            user.loopdata.LoopData.get_fields_to_include(_get_specified_fields())

        trend_packets = []
        ten_min_packets = []
        time_delta = 10800
        baro_trend_descs = user.loopdata.LoopData.construct_baro_trend_descs({})

        for in_pkt in pkts:
            pkt = weewx.units.StdUnitConverters[unit_system].convertDict(in_pkt)
            pkt['usUnits'] = unit_system
            pkt_time = to_int(pkt['dateTime'])

            loopdata_pkt: Dict[str, Any] =  user.loopdata.LoopProcessor.generate_loopdata_dictionary(
                pkt, pkt_time, unit_system, converter, formatter,
                fields_to_include, day_accum, trend_packets, time_delta, trend_obstypes,
                baro_trend_descs, ten_min_packets, ten_min_obstypes)

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
    def _get_day_accum(config_dict, dateTime):

        timespan = weeutil.weeutil.archiveDaySpan(dateTime)
        unit_system = weewx.units.unit_constants[config_dict['StdConvert'].get('target_unit', 'US').upper()]
        day_accum = weewx.accum.Accum(timespan, unit_system=unit_system)

        return day_accum

    @staticmethod
    def _get_config_dict(kind):
        return configobj.ConfigObj('bin/user/tests/weewx.conf.%s' % kind, encoding='utf-8')

    @staticmethod
    def _get_converter_and_formatter(config_dict):
        target_report_dict = user.loopdata.LoopData.get_target_report_dict(config_dict, 'SeasonsReport')

        converter = weewx.units.Converter.fromSkinDict(target_report_dict)
        formatter = weewx.units.Formatter.fromSkinDict(target_report_dict)

        return converter, formatter

def _get_specified_fields():
    return [
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
