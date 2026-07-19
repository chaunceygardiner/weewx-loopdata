#    Copyright (c) 2022-2026 John A Kline <john@johnkline.com>
#
#    See the file LICENSE.txt for your full rights.
#
#    HOW TO RUN THESE TESTS
#    ----------------------
#    Run from the repository root (~/software/weewx-loopdata), NOT from the
#    tests directory.  The harness loads its config files via paths relative
#    to the current directory (e.g. 'bin/user/tests/weewx.conf.us'), so the
#    working directory must be the repo root or every config-loading test
#    fails with KeyError: 'StdConvert' (an empty config from a missing file).
#
#    Activate the weewx venv first so weewx/weeutil import.
#
#    Both 'bin' and 'bin/user/tests' must be on PYTHONPATH:
#      - bin             -> resolves 'import user.loopdata'
#      - bin/user/tests  -> resolves the packet-data modules
#                           (cc3000_packets, ip100_packets, etc.)
#
#    Command (uses Python's built-in unittest runner; pytest not required):
#
#      cd ~/software/weewx-loopdata
#      PYTHONPATH=bin:bin/user/tests python3 bin/user/tests/test_process_packet.py
#
#    Add -v for per-test names, or append a test name to run just one, e.g.:
#
#      PYTHONPATH=bin:bin/user/tests python3 bin/user/tests/test_process_packet.py -v
#      PYTHONPATH=bin:bin/user/tests python3 bin/user/tests/test_process_packet.py \
#          ProcessPacketTests.test_wind
#
"""Test processing packets."""

import configobj
import logging
import os
import queue
import random
import shutil
import tempfile
import time
import unittest

from datetime import date

import weewx
import weewx.accum
import weewx.almanac
import weewx.manager
import weewx.units
from weewx.schemas.wview_extended import schema as wview_extended_schema
from weeutil.weeutil import to_int
from weeutil.weeutil import timestamp_to_string

from typing import Any, Dict, List, Optional, Set

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

class StubAlmanacBinder:
    """Stands in for a heavenly-body binder (e.g. $almanac.sun)."""
    def __init__(self, stub: 'StubAlmanacType', almanac_obj: Any) -> None:
        self.stub = stub
        self.almanac_obj = almanac_obj
        self.use_center = 0

    def __call__(self, use_center: int = 0) -> 'StubAlmanacBinder':
        self.use_center = use_center
        return self

    @property
    def az(self) -> float:
        self.stub.count('sun.az')
        return 123.4

    @property
    def rise(self) -> weewx.units.ValueHelper:
        # 06:00 local on the almanac's local day, shifted one minute per
        # degree of horizon and ten minutes earlier for use_center, so the
        # test can verify both kwargs reached the computation.
        self.stub.count('sun.rise')
        day_start = time.mktime(date.fromtimestamp(self.almanac_obj.time_ts).timetuple())
        rise_ts = (day_start + 6 * 3600 + int(self.almanac_obj.horizon * 60)
                   - (600 if self.use_center else 0))
        return self.stub.time_vh(self.almanac_obj, rise_ts)

class StubAlmanacType(weewx.almanac.AlmanacType):
    """Serves deterministic values so loopdata's almanac plumbing (parsing,
    evaluation, formatting, caching) can be tested from first principles,
    independent of any ephemeris."""
    def __init__(self) -> None:
        self.counts: Dict[str, int] = {}
        self.next_full_moon_ts: float = 0.0

    def count(self, attr: str) -> None:
        self.counts[attr] = self.counts.get(attr, 0) + 1

    @staticmethod
    def time_vh(almanac_obj: Any, ts: float) -> weewx.units.ValueHelper:
        return weewx.units.ValueHelper(
            weewx.units.ValueTuple(ts, 'unix_epoch', 'group_time'),
            context='ephem_day',
            formatter=almanac_obj.formatter,
            converter=almanac_obj.converter)

    def get_almanac_data(self, almanac_obj: Any, attr: str) -> Any:
        if attr == 'stub_time':
            self.count(attr)
            return almanac_obj.time_ts
        if attr == 'stub_horizon':
            self.count(attr)
            return almanac_obj.horizon
        if attr == 'sunrise':
            self.count(attr)
            day_start = time.mktime(date.fromtimestamp(almanac_obj.time_ts).timetuple())
            return StubAlmanacType.time_vh(almanac_obj, day_start + 6 * 3600)
        if attr == 'moon_index':
            self.count(attr)
            return 4
        if attr == 'next_full_moon':
            self.count(attr)
            return StubAlmanacType.time_vh(almanac_obj, self.next_full_moon_ts)
        if attr == 'sun':
            return StubAlmanacBinder(self, almanac_obj)
        raise weewx.UnknownType(attr)

class ProcessPacketTests(unittest.TestCase):
    maxDiff = None

    def test_parse_cname(self) -> None:
        cname: Optional[user.loopdata.CheetahName] = user.loopdata.LoopData.parse_cname('unit.label.outTemp')
        assert cname is not None
        self.assertEqual(cname.field, 'unit.label.outTemp')
        self.assertEqual(cname.prefix, 'unit')
        self.assertEqual(cname.prefix2, 'label')
        self.assertEqual(cname.period, None)
        self.assertEqual(cname.obstype, 'outTemp')
        self.assertEqual(cname.agg_type, None)
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('2m.windGust.max.raw')
        assert cname is not None
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
        assert cname is not None
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
        assert cname is not None
        self.assertEqual(cname.field, 'week.outTemp.avg')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'week')
        self.assertEqual(cname.obstype, 'outTemp')
        self.assertEqual(cname.agg_type, 'avg')
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('month.outTemp.avg')
        assert cname is not None
        self.assertEqual(cname.field, 'month.outTemp.avg')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'month')
        self.assertEqual(cname.obstype, 'outTemp')
        self.assertEqual(cname.agg_type, 'avg')
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('year.outTemp.avg')
        assert cname is not None
        self.assertEqual(cname.field, 'year.outTemp.avg')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'year')
        self.assertEqual(cname.obstype, 'outTemp')
        self.assertEqual(cname.agg_type, 'avg')
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('rainyear.outTemp.avg')
        assert cname is not None
        self.assertEqual(cname.field, 'rainyear.outTemp.avg')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'rainyear')
        self.assertEqual(cname.obstype, 'outTemp')
        self.assertEqual(cname.agg_type, 'avg')
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('alltime.outTemp.avg')
        assert cname is not None
        self.assertEqual(cname.field, 'alltime.outTemp.avg')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'alltime')
        self.assertEqual(cname.obstype, 'outTemp')
        self.assertEqual(cname.agg_type, 'avg')
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('week.windGust.max.formatted')
        assert cname is not None
        self.assertEqual(cname.field, 'week.windGust.max.formatted')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'week')
        self.assertEqual(cname.obstype, 'windGust')
        self.assertEqual(cname.agg_type, 'max')
        self.assertEqual(cname.format_spec, 'formatted')

        cname = user.loopdata.LoopData.parse_cname('month.windGust.max.formatted')
        assert cname is not None
        self.assertEqual(cname.field, 'month.windGust.max.formatted')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'month')
        self.assertEqual(cname.obstype, 'windGust')
        self.assertEqual(cname.agg_type, 'max')
        self.assertEqual(cname.format_spec, 'formatted')

        cname = user.loopdata.LoopData.parse_cname('year.windGust.max.formatted')
        assert cname is not None
        self.assertEqual(cname.field, 'year.windGust.max.formatted')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'year')
        self.assertEqual(cname.obstype, 'windGust')
        self.assertEqual(cname.agg_type, 'max')
        self.assertEqual(cname.format_spec, 'formatted')

        cname = user.loopdata.LoopData.parse_cname('rainyear.windGust.max.formatted')
        assert cname is not None
        self.assertEqual(cname.field, 'rainyear.windGust.max.formatted')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'rainyear')
        self.assertEqual(cname.obstype, 'windGust')
        self.assertEqual(cname.agg_type, 'max')
        self.assertEqual(cname.format_spec, 'formatted')

        cname = user.loopdata.LoopData.parse_cname('alltime.windGust.max.formatted')
        assert cname is not None
        self.assertEqual(cname.field, 'alltime.windGust.max.formatted')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'alltime')
        self.assertEqual(cname.obstype, 'windGust')
        self.assertEqual(cname.agg_type, 'max')
        self.assertEqual(cname.format_spec, 'formatted')

        cname = user.loopdata.LoopData.parse_cname('2m.windGust.max.formatted')
        assert cname is not None
        self.assertEqual(cname.field, '2m.windGust.max.formatted')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, '2m')
        self.assertEqual(cname.obstype, 'windGust')
        self.assertEqual(cname.agg_type, 'max')
        self.assertEqual(cname.format_spec, 'formatted')

        cname = user.loopdata.LoopData.parse_cname('10m.windGust.max.formatted')
        assert cname is not None
        self.assertEqual(cname.field, '10m.windGust.max.formatted')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, '10m')
        self.assertEqual(cname.obstype, 'windGust')
        self.assertEqual(cname.agg_type, 'max')
        self.assertEqual(cname.format_spec, 'formatted')

        cname = user.loopdata.LoopData.parse_cname('10m.windGust.maxtime')
        assert cname is not None
        self.assertEqual(cname.field, '10m.windGust.maxtime')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, '10m')
        self.assertEqual(cname.obstype, 'windGust')
        self.assertEqual(cname.agg_type, 'maxtime')
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('10m.outTemp.max.raw')
        assert cname is not None
        self.assertEqual(cname.field, '10m.outTemp.max.raw')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, '10m')
        self.assertEqual(cname.obstype, 'outTemp')
        self.assertEqual(cname.agg_type, 'max')
        self.assertEqual(cname.format_spec, 'raw')

        cname = user.loopdata.LoopData.parse_cname('10m.outTemp.max.formatted')
        assert cname is not None
        self.assertEqual(cname.field, '10m.outTemp.max.formatted')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, '10m')
        self.assertEqual(cname.obstype, 'outTemp')
        self.assertEqual(cname.agg_type, 'max')
        self.assertEqual(cname.format_spec, 'formatted')

        cname = user.loopdata.LoopData.parse_cname('10m.outTemp.maxtime')
        assert cname is not None
        self.assertEqual(cname.field, '10m.outTemp.maxtime')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, '10m')
        self.assertEqual(cname.obstype, 'outTemp')
        self.assertEqual(cname.agg_type, 'maxtime')
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('unit.label.wind')
        assert cname is not None
        self.assertEqual(cname.field, 'unit.label.wind')
        self.assertEqual(cname.prefix, 'unit')
        self.assertEqual(cname.prefix2, 'label')
        self.assertEqual(cname.period, None)
        self.assertEqual(cname.obstype, 'wind')
        self.assertEqual(cname.agg_type, None)
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('trend.barometer')
        assert cname is not None
        self.assertEqual(cname.field, 'trend.barometer')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'trend')
        self.assertEqual(cname.obstype, 'barometer')
        self.assertEqual(cname.agg_type, None)
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('trend.barometer.formatted')
        assert cname is not None
        self.assertEqual(cname.field, 'trend.barometer.formatted')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'trend')
        self.assertEqual(cname.obstype, 'barometer')
        self.assertEqual(cname.agg_type, None)
        self.assertEqual(cname.format_spec, 'formatted')

        cname = user.loopdata.LoopData.parse_cname('trend.barometer.code')
        assert cname is not None
        self.assertEqual(cname.field, 'trend.barometer.code')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'trend')
        self.assertEqual(cname.obstype, 'barometer')
        self.assertEqual(cname.agg_type, None)
        self.assertEqual(cname.format_spec, 'code')

        cname = user.loopdata.LoopData.parse_cname('trend.barometer.desc')
        assert cname is not None
        self.assertEqual(cname.field, 'trend.barometer.desc')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'trend')
        self.assertEqual(cname.obstype, 'barometer')
        self.assertEqual(cname.agg_type, None)
        self.assertEqual(cname.format_spec, 'desc')

        cname = user.loopdata.LoopData.parse_cname('trend.outTemp')
        assert cname is not None
        self.assertEqual(cname.field, 'trend.outTemp')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'trend')
        self.assertEqual(cname.obstype, 'outTemp')
        self.assertEqual(cname.agg_type, None)
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('trend.outTemp.formatted')
        assert cname is not None
        self.assertEqual(cname.field, 'trend.outTemp.formatted')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'trend')
        self.assertEqual(cname.obstype, 'outTemp')
        self.assertEqual(cname.agg_type, None)
        self.assertEqual(cname.format_spec, 'formatted')

        cname = user.loopdata.LoopData.parse_cname('trend.dewpoint')
        assert cname is not None
        self.assertEqual(cname.field, 'trend.dewpoint')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'trend')
        self.assertEqual(cname.obstype, 'dewpoint')
        self.assertEqual(cname.agg_type, None)
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('trend.dewpoint.formatted')
        assert cname is not None
        self.assertEqual(cname.field, 'trend.dewpoint.formatted')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'trend')
        self.assertEqual(cname.obstype, 'dewpoint')
        self.assertEqual(cname.agg_type, None)
        self.assertEqual(cname.format_spec, 'formatted')

        cname = user.loopdata.LoopData.parse_cname('current.outTemp')
        assert cname is not None
        self.assertEqual(cname.field, 'current.outTemp')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'current')
        self.assertEqual(cname.obstype, 'outTemp')
        self.assertEqual(cname.agg_type, None)
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('current.dateTime')
        assert cname is not None
        self.assertEqual(cname.field, 'current.dateTime')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'current')
        self.assertEqual(cname.obstype, 'dateTime')
        self.assertEqual(cname.agg_type, None)
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('current.dateTime.raw')
        assert cname is not None
        self.assertEqual(cname.field, 'current.dateTime.raw')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'current')
        self.assertEqual(cname.obstype, 'dateTime')
        self.assertEqual(cname.agg_type, None)
        self.assertEqual(cname.format_spec, 'raw')

        cname = user.loopdata.LoopData.parse_cname('current.windSpeed')
        assert cname is not None
        self.assertEqual(cname.field, 'current.windSpeed')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'current')
        self.assertEqual(cname.obstype, 'windSpeed')
        self.assertEqual(cname.agg_type, None)
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('current.windSpeed.ordinal_compass')
        assert cname is not None
        self.assertEqual(cname.field, 'current.windSpeed.ordinal_compass')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'current')
        self.assertEqual(cname.obstype, 'windSpeed')
        self.assertEqual(cname.agg_type, None)
        self.assertEqual(cname.format_spec, 'ordinal_compass')

        cname = user.loopdata.LoopData.parse_cname('day.rain.sum')
        assert cname is not None
        self.assertEqual(cname.field, 'day.rain.sum')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'day')
        self.assertEqual(cname.obstype, 'rain')
        self.assertEqual(cname.agg_type, 'sum')
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('hour.rain.sum')
        assert cname is not None
        self.assertEqual(cname.field, 'hour.rain.sum')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'hour')
        self.assertEqual(cname.obstype, 'rain')
        self.assertEqual(cname.agg_type, 'sum')
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('day.rain.sum.raw')
        assert cname is not None
        self.assertEqual(cname.field, 'day.rain.sum.raw')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'day')
        self.assertEqual(cname.obstype, 'rain')
        self.assertEqual(cname.agg_type, 'sum')
        self.assertEqual(cname.format_spec, 'raw')

        cname = user.loopdata.LoopData.parse_cname('hour.rain.sum.raw')
        assert cname is not None
        self.assertEqual(cname.field, 'hour.rain.sum.raw')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'hour')
        self.assertEqual(cname.obstype, 'rain')
        self.assertEqual(cname.agg_type, 'sum')
        self.assertEqual(cname.format_spec, 'raw')

        cname = user.loopdata.LoopData.parse_cname('24h.rain.sum.raw')
        assert cname is not None
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
        assert cname is not None
        self.assertEqual(cname.field, 'day.windGust.max')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'day')
        self.assertEqual(cname.obstype, 'windGust')
        self.assertEqual(cname.agg_type, 'max')
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('day.windDir.max')
        assert cname is not None
        self.assertEqual(cname.field, 'day.windDir.max')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'day')
        self.assertEqual(cname.obstype, 'windDir')
        self.assertEqual(cname.agg_type, 'max')
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('day.wind.maxtime')
        assert cname is not None
        self.assertEqual(cname.field, 'day.wind.maxtime')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'day')
        self.assertEqual(cname.obstype, 'wind')
        self.assertEqual(cname.agg_type, 'maxtime')
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('day.wind.max')
        assert cname is not None
        self.assertEqual(cname.field, 'day.wind.max')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'day')
        self.assertEqual(cname.obstype, 'wind')
        self.assertEqual(cname.agg_type, 'max')
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('day.wind.gustdir')
        assert cname is not None
        self.assertEqual(cname.field, 'day.wind.gustdir')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'day')
        self.assertEqual(cname.obstype, 'wind')
        self.assertEqual(cname.agg_type, 'gustdir')
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('day.wind.vecavg')
        assert cname is not None
        self.assertEqual(cname.field, 'day.wind.vecavg')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'day')
        self.assertEqual(cname.obstype, 'wind')
        self.assertEqual(cname.agg_type, 'vecavg')
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('day.wind.vecdir')
        assert cname is not None
        self.assertEqual(cname.field, 'day.wind.vecdir')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'day')
        self.assertEqual(cname.obstype, 'wind')
        self.assertEqual(cname.agg_type, 'vecdir')
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('day.wind.rms')
        assert cname is not None
        self.assertEqual(cname.field, 'day.wind.rms')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'day')
        self.assertEqual(cname.obstype, 'wind')
        self.assertEqual(cname.agg_type, 'rms')
        self.assertEqual(cname.format_spec, None)


        cname = user.loopdata.LoopData.parse_cname('day.wind.avg')
        assert cname is not None
        self.assertEqual(cname.field, 'day.wind.avg')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'day')
        self.assertEqual(cname.obstype, 'wind')
        self.assertEqual(cname.agg_type, 'avg')
        self.assertEqual(cname.format_spec, None)

        cname = user.loopdata.LoopData.parse_cname('year.windrun.sum.formatted')
        assert cname is not None
        self.assertEqual(cname.field, 'year.windrun.sum.formatted')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'year')
        self.assertEqual(cname.obstype, 'windrun')
        self.assertEqual(cname.agg_type, 'sum')
        self.assertEqual(cname.format_spec, 'formatted')

        cname = user.loopdata.LoopData.parse_cname('hour.windrun_ENE.sum.formatted')
        assert cname is not None
        self.assertEqual(cname.field, 'hour.windrun_ENE.sum.formatted')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'hour')
        self.assertEqual(cname.obstype, 'windrun_ENE')
        self.assertEqual(cname.agg_type, 'sum')
        self.assertEqual(cname.format_spec, 'formatted')

        cname = user.loopdata.LoopData.parse_cname('day.windrun_W.sum')
        assert cname is not None
        self.assertEqual(cname.field, 'day.windrun_W.sum')
        self.assertEqual(cname.prefix, None)
        self.assertEqual(cname.prefix2, None)
        self.assertEqual(cname.period, 'day')
        self.assertEqual(cname.obstype, 'windrun_W')
        self.assertEqual(cname.agg_type, 'sum')
        self.assertEqual(cname.format_spec, None)

        # --- segment-guard branch (mutation-test target) ---
        # 'unit' prefix but the second segment is not 'label' -> the prefix2
        # else-branch returns None.
        self.assertEqual(
            user.loopdata.LoopData.parse_cname('unit.notlabel.outTemp'), None)

    def test_compose_loop_data_dir(self) -> None:
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

    def test_period_classification(self) -> None:
        # Pure-function coverage for the period-classification helpers:
        # is_minute_period, is_hour_period, is_continuous_period, is_valid_period.
        L = user.loopdata.LoopData

        # --- is_minute_period: valid range is 1m..1440m inclusive. ---
        self.assertTrue(L.is_minute_period('1m'))
        self.assertTrue(L.is_minute_period('2m'))
        self.assertTrue(L.is_minute_period('10m'))
        self.assertTrue(L.is_minute_period('1440m'))     # upper bound
        self.assertFalse(L.is_minute_period('0m'))       # below lower bound
        self.assertFalse(L.is_minute_period('1441m'))    # above upper bound
        self.assertFalse(L.is_minute_period('m'))        # no digits
        self.assertFalse(L.is_minute_period('10'))       # no 'm'
        self.assertFalse(L.is_minute_period('10h'))      # wrong unit
        self.assertFalse(L.is_minute_period('1.5m'))     # non-integer
        self.assertFalse(L.is_minute_period('-5m'))      # isdigit() rejects sign
        self.assertFalse(L.is_minute_period('day'))

        # --- is_hour_period: valid range is 1h..24h inclusive. ---
        self.assertTrue(L.is_hour_period('1h'))
        self.assertTrue(L.is_hour_period('2h'))
        self.assertTrue(L.is_hour_period('24h'))         # upper bound
        self.assertFalse(L.is_hour_period('0h'))         # below lower bound
        self.assertFalse(L.is_hour_period('25h'))        # above upper bound
        self.assertFalse(L.is_hour_period('h'))          # no digits
        self.assertFalse(L.is_hour_period('24'))         # no 'h'
        self.assertFalse(L.is_hour_period('2m'))         # wrong unit
        self.assertFalse(L.is_hour_period('1.5h'))       # non-integer
        self.assertFalse(L.is_hour_period('day'))

        # --- is_continuous_period: 'trend' or any valid minute/hour period. ---
        self.assertTrue(L.is_continuous_period('trend'))
        self.assertTrue(L.is_continuous_period('2m'))
        self.assertTrue(L.is_continuous_period('24h'))
        self.assertFalse(L.is_continuous_period('day'))
        self.assertFalse(L.is_continuous_period('current'))
        self.assertFalse(L.is_continuous_period('0m'))
        self.assertFalse(L.is_continuous_period('25h'))

        # --- is_valid_period: fixed periods OR continuous periods. ---
        for p in ['alltime', 'rainyear', 'year', 'month', 'week', 'current', 'hour', 'day']:
            self.assertTrue(L.is_valid_period(p), msg='%s should be valid' % p)
        self.assertTrue(L.is_valid_period('trend'))
        self.assertTrue(L.is_valid_period('1m'))
        self.assertTrue(L.is_valid_period('1440m'))
        self.assertTrue(L.is_valid_period('1h'))
        self.assertTrue(L.is_valid_period('24h'))
        self.assertFalse(L.is_valid_period('decade'))
        self.assertFalse(L.is_valid_period('0m'))
        self.assertFalse(L.is_valid_period('1441m'))
        self.assertFalse(L.is_valid_period('25h'))
        self.assertFalse(L.is_valid_period(''))

    def test_get_windrun_bucket(self) -> None:
        # Pure-function coverage for get_windrun_bucket: maps a wind direction
        # to one of 16 compass buckets (0=N, 1=NNE, ... 15=NNW).  Each bucket
        # is centered on its compass point (i * 22.5 deg) and spans +/-11.25.
        # Directions in [348.75, 360) wrap back to bucket 0 (N).
        L = user.loopdata.LoopProcessor

        # Each of the 16 compass-point centers maps to its own bucket, in order.
        for i in range(16):
            center = i * 22.5
            self.assertEqual(L.get_windrun_bucket(center), i,
                             msg='center %.1f should be bucket %d' % (center, i))

        # Just past each lower edge rounds up into the next bucket.
        self.assertEqual(L.get_windrun_bucket(11.25), 1)    # N/NNE edge -> NNE
        self.assertEqual(L.get_windrun_bucket(33.75), 2)    # NNE/NE edge -> NE
        self.assertEqual(L.get_windrun_bucket(326.25), 15)  # NW/NNW edge -> NNW

        # Wraparound: the top edge and everything up to 360 folds back to N(0).
        self.assertEqual(L.get_windrun_bucket(348.75), 0)
        self.assertEqual(L.get_windrun_bucket(355.0), 0)
        self.assertEqual(L.get_windrun_bucket(359.9), 0)
        self.assertEqual(L.get_windrun_bucket(360.0), 0)
        self.assertEqual(L.get_windrun_bucket(0.0), 0)

    def test_massage_near_zero(self) -> None:
        # Values within +/-1e-10 of zero are clamped to exactly 0.0; everything
        # else passes through unchanged.  Guards against -0.0-ish float dust in
        # the vector sums producing tiny non-zero artifacts.
        L = user.loopdata.LoopData
        self.assertEqual(L.massage_near_zero(0.0), 0.0)
        self.assertEqual(L.massage_near_zero(1e-11), 0.0)
        self.assertEqual(L.massage_near_zero(-1e-11), 0.0)
        # Just outside the window: unchanged.
        self.assertEqual(L.massage_near_zero(1e-9), 1e-9)
        self.assertEqual(L.massage_near_zero(-1e-9), -1e-9)
        self.assertEqual(L.massage_near_zero(5.0), 5.0)
        self.assertEqual(L.massage_near_zero(-273.15), -273.15)
        # Exact boundary: the window is STRICT (val > -1e-10 AND val < 1e-10),
        # so exactly +/-1e-10 is OUTSIDE and passes through unchanged.  This
        # distinguishes '<' from a mutated '<=' (and '>' from '>=').
        self.assertEqual(L.massage_near_zero(1e-10), 1e-10)
        self.assertEqual(L.massage_near_zero(-1e-10), -1e-10)
        # Just inside the strict window -> clamped.
        self.assertEqual(L.massage_near_zero(9.99e-11), 0.0)
        self.assertEqual(L.massage_near_zero(-9.99e-11), 0.0)

    def test_construct_baro_trend_descs(self) -> None:
        # Builds a BarometerTrend -> description map.  Supplied translations
        # override; missing keys fall back to the English defaults.
        L = user.loopdata.LoopData
        BT = user.loopdata.BarometerTrend

        # Empty translation dict -> all defaults present for all nine trends.
        descs = L.construct_baro_trend_descs({})
        self.assertEqual(len(descs), 9)
        # Assert ALL nine mappings (each is a distinct line in the function;
        # asserting only a few lets mutations to the others survive).
        self.assertEqual(descs[BT.RISING_VERY_RAPIDLY], 'Rising Very Rapidly')
        self.assertEqual(descs[BT.RISING_QUICKLY], 'Rising Quickly')
        self.assertEqual(descs[BT.RISING], 'Rising')
        self.assertEqual(descs[BT.RISING_SLOWLY], 'Rising Slowly')
        self.assertEqual(descs[BT.STEADY], 'Steady')
        self.assertEqual(descs[BT.FALLING_SLOWLY], 'Falling Slowly')
        self.assertEqual(descs[BT.FALLING], 'Falling')
        self.assertEqual(descs[BT.FALLING_QUICKLY], 'Falling Quickly')
        self.assertEqual(descs[BT.FALLING_VERY_RAPIDLY], 'Falling Very Rapidly')

        # Partial override: supplied keys win, the rest keep defaults.
        descs = L.construct_baro_trend_descs({
            'STEADY': 'Holding',
            'RISING': 'Going Up'})
        self.assertEqual(descs[BT.STEADY], 'Holding')
        self.assertEqual(descs[BT.RISING], 'Going Up')
        self.assertEqual(descs[BT.FALLING], 'Falling')  # untouched default

        # To kill mutations of the lookup KEYS (e.g. 'RISING_QUICKLY' ->
        # corrupted), supply a translation dict containing EVERY key with a
        # distinct sentinel value.  A corrupted key would miss the dict and fall
        # back to the English default, producing a different string -- so each
        # mapping line is independently pinned.
        keys = ['RISING_VERY_RAPIDLY', 'RISING_QUICKLY', 'RISING',
                'RISING_SLOWLY', 'STEADY', 'FALLING_SLOWLY', 'FALLING',
                'FALLING_QUICKLY', 'FALLING_VERY_RAPIDLY']
        full = {k: 'X_' + k for k in keys}
        d2 = L.construct_baro_trend_descs(full)
        self.assertEqual(d2[BT.RISING_VERY_RAPIDLY], 'X_RISING_VERY_RAPIDLY')
        self.assertEqual(d2[BT.RISING_QUICKLY], 'X_RISING_QUICKLY')
        self.assertEqual(d2[BT.RISING], 'X_RISING')
        self.assertEqual(d2[BT.RISING_SLOWLY], 'X_RISING_SLOWLY')
        self.assertEqual(d2[BT.STEADY], 'X_STEADY')
        self.assertEqual(d2[BT.FALLING_SLOWLY], 'X_FALLING_SLOWLY')
        self.assertEqual(d2[BT.FALLING], 'X_FALLING')
        self.assertEqual(d2[BT.FALLING_QUICKLY], 'X_FALLING_QUICKLY')
        self.assertEqual(d2[BT.FALLING_VERY_RAPIDLY], 'X_FALLING_VERY_RAPIDLY')

    def test_compute_period_obstypes(self) -> None:
        # For a given period, collect the obstypes of fields in that period and
        # auto-add the dependency obstypes for composite types (wind, appTemp,
        # windrun_*, beaufort).  Fields in other periods are ignored.
        L = user.loopdata.LoopData

        def cn(field):
            c = L.parse_cname(field)
            self.assertIsNotNone(c, msg='parse_cname failed for %s' % field)
            return c

        fields = {
            cn('day.outTemp.avg'),         # plain, no expansion
            cn('day.wind.vecavg'),         # wind -> +windSpeed/windDir/windGust/windGustDir
            cn('day.appTemp.avg'),         # appTemp -> +outTemp/outHumidity/windSpeed
            cn('day.windrun_N.sum'),       # windrun* -> +windSpeed/windDir
            cn('day.beaufort.max'),        # beaufort -> +windSpeed
            cn('2m.outTemp.avg')}          # different period: must be excluded

        result = L.compute_period_obstypes(fields, 'day')

        # Base obstypes for the 'day' fields.
        self.assertIn('outTemp', result)
        self.assertIn('wind', result)
        self.assertIn('appTemp', result)
        self.assertIn('windrun_N', result)
        self.assertIn('beaufort', result)
        # Auto-added dependencies.
        self.assertIn('windSpeed', result)
        self.assertIn('windDir', result)
        self.assertIn('windGust', result)
        self.assertIn('windGustDir', result)
        self.assertIn('outHumidity', result)
        # The 2m field's obstype must NOT leak into the 'day' result set
        # (outTemp is already present from day.outTemp, so assert the period
        # filter via a period that has only the excluded field).
        result_2m = L.compute_period_obstypes(fields, '2m')
        self.assertEqual(result_2m, {'outTemp'})

    def test_compute_period_obstypes_isolated_composites(self) -> None:
        # The shared test asserts presence, but several composites add the SAME
        # dependency (e.g. both 'wind' and 'windrun' add windDir), so removing
        # one composite's contribution is masked.  Here each composite is tested
        # ALONE with exact set equality, so dropping any single .add() is caught.
        L = user.loopdata.LoopData

        def cn(field):
            c = L.parse_cname(field)
            self.assertIsNotNone(c, msg='parse_cname failed for %s' % field)
            return c

        # wind -> itself + windSpeed, windDir, windGust, windGustDir
        self.assertEqual(
            L.compute_period_obstypes({cn('day.wind.vecavg')}, 'day'),
            {'wind', 'windSpeed', 'windDir', 'windGust', 'windGustDir'})
        # appTemp -> itself + outTemp, outHumidity, windSpeed
        self.assertEqual(
            L.compute_period_obstypes({cn('day.appTemp.avg')}, 'day'),
            {'appTemp', 'outTemp', 'outHumidity', 'windSpeed'})
        # windrun_* -> itself + windSpeed, windDir
        self.assertEqual(
            L.compute_period_obstypes({cn('day.windrun_N.sum')}, 'day'),
            {'windrun_N', 'windSpeed', 'windDir'})
        # beaufort -> itself + windSpeed
        self.assertEqual(
            L.compute_period_obstypes({cn('day.beaufort.max')}, 'day'),
            {'beaufort', 'windSpeed'})
        # plain obstype -> itself only (no expansion)
        self.assertEqual(
            L.compute_period_obstypes({cn('day.outTemp.avg')}, 'day'),
            {'outTemp'})

    def test_get_fields_to_include(self) -> None:

        specified_fields: Set[str] = {'current.dateTime.raw', 'current.outTemp', 'trend.outTemp', 'trend.barometer.code',
            'trend.barometer.desc', '2m.wind.max', '2m.wind.gustdir', '10m.wind.max', '10m.wind.gustdir',
            '10m.windSpeed.max', '10m.windDir.max', '24h.rain.sum', 'hour.inTemp.min', 'hour.inTemp.mintime',
            'day.barometer.min', 'day.barometer.max', 'day.wind.max', 'day.wind.gustdir', 'day.wind.maxtime'}

        (fields_to_include, obstypes) = user.loopdata.LoopData.get_fields_to_include(specified_fields)

        self.assertEqual(len(fields_to_include), 19)
        self.assertTrue(user.loopdata.CheetahName(
            'current.dateTime.raw', None, None, 'current', 'dateTime', None, 'raw') in fields_to_include)
        self.assertTrue(user.loopdata.CheetahName(
            'current.outTemp', None, None, 'current', 'outTemp', None, None) in fields_to_include)
        self.assertTrue(user.loopdata.CheetahName(
            'trend.outTemp', None, None, 'trend', 'outTemp', None, None) in fields_to_include)
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

        self.assertEqual(len(obstypes.current), 10)
        self.assertTrue('inTemp' in obstypes.current)
        self.assertTrue('outTemp' in obstypes.current)
        self.assertTrue('barometer' in obstypes.current)
        self.assertTrue('wind' in obstypes.current)
        self.assertTrue('windDir' in obstypes.current)
        self.assertTrue('windGust' in obstypes.current)
        self.assertTrue('windGustDir' in obstypes.current)
        self.assertTrue('windSpeed' in obstypes.current)

        self.assertEqual(len(obstypes.continuous['trend']), 2)
        self.assertTrue('barometer' in obstypes.continuous['trend'])
        self.assertTrue('outTemp' in obstypes.continuous['trend'])

        self.assertEqual(len(obstypes.continuous['10m']), 5)
        self.assertTrue('wind' in obstypes.continuous['10m'])
        self.assertTrue('windDir' in obstypes.continuous['10m'])
        self.assertTrue('windGust' in obstypes.continuous['10m'])
        self.assertTrue('windGustDir' in obstypes.continuous['10m'])
        self.assertTrue('windSpeed' in obstypes.continuous['10m'])

        self.assertEqual(len(obstypes.continuous['24h']), 1)
        self.assertTrue('rain' in obstypes.continuous['24h'])

        self.assertEqual(len(obstypes.hour), 1)
        self.assertTrue('inTemp' in obstypes.hour)

        self.assertEqual(len(obstypes.day), 6)
        self.assertTrue('barometer' in obstypes.day)
        self.assertTrue('wind' in obstypes.day)
        self.assertTrue('windDir' in obstypes.day)
        self.assertTrue('windGust' in obstypes.day)
        self.assertTrue('windGustDir' in obstypes.day)
        self.assertTrue('windSpeed' in obstypes.day)

    def test_parse_almanac_field(self) -> None:
        parse = user.loopdata.LoopData.parse_almanac_field

        af = parse('almanac.sunrise')
        assert af is not None
        self.assertEqual(af.field, 'almanac.sunrise')
        self.assertEqual(af.almanac_kwargs, {})
        self.assertEqual(af.days, 0)
        self.assertEqual(af.chain, [user.loopdata.AlmanacSegment('sunrise', None)])
        self.assertEqual(af.format_spec, None)
        self.assertEqual(af.tier, 'day')

        af = parse('almanac.sunrise.raw')
        assert af is not None
        self.assertEqual(af.chain, [user.loopdata.AlmanacSegment('sunrise', None)])
        self.assertEqual(af.format_spec, 'raw')
        self.assertEqual(af.tier, 'day')

        af = parse('almanac.moon_index')
        assert af is not None
        self.assertEqual(af.tier, 'continuous')

        af = parse('almanac.sun.az')
        assert af is not None
        self.assertEqual(af.chain, [user.loopdata.AlmanacSegment('sun', None),
                                    user.loopdata.AlmanacSegment('az', None)])
        self.assertEqual(af.tier, 'continuous')

        af = parse('almanac(horizon=-6).sun(use_center=1).rise.raw')
        assert af is not None
        self.assertEqual(af.almanac_kwargs, {'horizon': -6})
        self.assertEqual(af.days, 0)
        self.assertEqual(af.chain, [user.loopdata.AlmanacSegment('sun', {'use_center': 1}),
                                    user.loopdata.AlmanacSegment('rise', None)])
        self.assertEqual(af.format_spec, 'raw')
        self.assertEqual(af.tier, 'day')

        af = parse('almanac(days=+1).sunset.raw')
        assert af is not None
        self.assertEqual(af.almanac_kwargs, {})
        self.assertEqual(af.days, 1)
        self.assertEqual(af.tier, 'day')

        af = parse('almanac(days=-1).sun.visible.raw')
        assert af is not None
        self.assertEqual(af.days, -1)
        self.assertEqual(af.tier, 'day')

        af = parse('almanac.next_solstice.raw')
        assert af is not None
        self.assertEqual(af.tier, 'event')

        af = parse('almanac.previous_equinox')
        assert af is not None
        self.assertEqual(af.tier, 'event')

        af = parse('almanac.mars.earth_distance')
        assert af is not None
        self.assertEqual(af.tier, 'continuous')

        af = parse('almanac(pressure=0, horizon=-8.5).sun.rise')
        assert af is not None
        self.assertEqual(af.almanac_kwargs, {'pressure': 0, 'horizon': -8.5})
        self.assertEqual(af.tier, 'day')

        af = parse('almanac.moon.phase.formatted')
        assert af is not None
        self.assertEqual(af.chain, [user.loopdata.AlmanacSegment('moon', None),
                                    user.loopdata.AlmanacSegment('phase', None)])
        self.assertEqual(af.format_spec, 'formatted')

        # Malformed entries.
        self.assertIsNone(parse('almanac'))
        self.assertIsNone(parse('almanac.'))
        self.assertIsNone(parse('almanac..sunrise'))
        self.assertIsNone(parse('almanac(days=1.5).sunrise'))
        self.assertIsNone(parse('almanac(horizon=abc).sun.rise'))
        self.assertIsNone(parse('almanac(horizon-6).sun.rise'))
        self.assertIsNone(parse('almanac(horizon=-6.sun.rise'))
        self.assertIsNone(parse('almanac.sun(rise'))
        self.assertIsNone(parse('almanac.9sun.rise'))
        self.assertIsNone(parse('almanac(horizon=-6)(use_center=1).sun.rise'))

    def test_get_almanac_fields(self) -> None:
        specified_fields = [
            'current.outTemp',
            'almanac.sunrise.raw',
            'day.outTemp.max',
            'almanac(horizon=-6).sun(use_center=1).rise.raw',
            'almanac.sunrise.raw',            # duplicate: dropped
            'almanac(horizon=abc).sun.rise',  # malformed: dropped
            'trend.barometer.desc',
        ]
        almanac_fields = user.loopdata.LoopData.get_almanac_fields(specified_fields)
        self.assertEqual([ f.field for f in almanac_fields ],
            ['almanac.sunrise.raw', 'almanac(horizon=-6).sun(use_center=1).rise.raw'])

        # Almanac entries must not leak into the observation-field parse.
        (fields_to_include, _) = user.loopdata.LoopData.get_fields_to_include(set(specified_fields))
        self.assertEqual({ cname.field for cname in fields_to_include },
            {'current.outTemp', 'day.outTemp.max', 'trend.barometer.desc'})

    def test_almanac_field_evaluator(self) -> None:
        specified_fields = [
            'almanac.stub_time',                                # continuous, plain float
            'almanac(horizon=-6).stub_horizon',                 # continuous, almanac kwargs
            'almanac.sunrise.raw',                              # day tier
            'almanac.sunrise',                                  # formatted like the report tag
            'almanac(days=1).sunrise.raw',                      # local calendar-day shift
            'almanac(days=-1).sunrise.raw',
            'almanac(horizon=-6).sun(use_center=1).rise.raw',   # chain call kwargs
            'almanac.moon_index',                               # plain int
            'almanac.moon_index.raw',                           # .raw identity on plain value
            'almanac.moon_index.formatted',                     # invalid: skipped
            'almanac.next_full_moon.raw',                       # event tier
            'almanac.no_such_attr',                             # unknown: skipped
        ]
        cfg: user.loopdata.Configuration = ProcessPacketTests._get_config('us', 10800, 10, 6, ['current.outTemp'])
        cfg.almanac_fields = user.loopdata.LoopData.get_almanac_fields(specified_fields)
        self.assertEqual(len(cfg.almanac_fields), len(specified_fields))
        cfg.latitude, cfg.longitude, cfg.altitude_m = 37.4, -122.1, 20.0

        stub = StubAlmanacType()
        weewx.almanac.almanacs.insert(0, stub)
        try:
            evaluator = user.loopdata.AlmanacFieldEvaluator(cfg)

            # 2020-07-01 (PDT) noon; day boundaries computed the same way the stub does.
            day1_noon = 1593630000
            def six_am(day_offset: int) -> float:
                day = date.fromtimestamp(day1_noon + day_offset * 86400)
                return time.mktime(day.timetuple()) + 6 * 3600
            stub.next_full_moon_ts = six_am(2) + 12 * 3600   # day 3, 18:00

            pkt: Dict[str, Any] = {'dateTime': day1_noon, 'usUnits': 1}
            loopdata_pkt: Dict[str, Any] = {}
            evaluator.insert_fields(loopdata_pkt, pkt)

            self.assertEqual(loopdata_pkt['almanac.stub_time'], day1_noon)
            self.assertEqual(loopdata_pkt['almanac(horizon=-6).stub_horizon'], -6)
            self.assertEqual(loopdata_pkt['almanac.sunrise.raw'], six_am(0))
            expected_vh = weewx.units.ValueHelper(
                weewx.units.ValueTuple(six_am(0), 'unix_epoch', 'group_time'),
                context='ephem_day', formatter=cfg.formatter, converter=cfg.converter)
            self.assertEqual(loopdata_pkt['almanac.sunrise'], str(expected_vh))
            self.assertEqual(loopdata_pkt['almanac(days=1).sunrise.raw'], six_am(1))
            self.assertEqual(loopdata_pkt['almanac(days=-1).sunrise.raw'], six_am(-1))
            # 06:00 less 6 degrees of horizon (one minute per degree) less ten minutes for use_center.
            self.assertEqual(loopdata_pkt['almanac(horizon=-6).sun(use_center=1).rise.raw'],
                six_am(0) - 360 - 600)
            self.assertEqual(loopdata_pkt['almanac.moon_index'], 4)
            self.assertEqual(loopdata_pkt['almanac.moon_index.raw'], 4)
            self.assertNotIn('almanac.moon_index.formatted', loopdata_pkt)
            self.assertEqual(loopdata_pkt['almanac.next_full_moon.raw'], stub.next_full_moon_ts)
            self.assertNotIn('almanac.no_such_attr', loopdata_pkt)

            # Four fields walk the sunrise attribute; moon_index is walked
            # three times (the .formatted variant evaluates, then fails to format).
            self.assertEqual(stub.counts['sunrise'], 4)
            self.assertEqual(stub.counts['sun.rise'], 1)
            self.assertEqual(stub.counts['moon_index'], 3)
            self.assertEqual(stub.counts['next_full_moon'], 1)

            # Same day, an hour later: continuous fields recompute, day and
            # event fields are served from cache.
            pkt = {'dateTime': day1_noon + 3600, 'usUnits': 1}
            loopdata_pkt = {}
            evaluator.insert_fields(loopdata_pkt, pkt)
            self.assertEqual(loopdata_pkt['almanac.stub_time'], day1_noon + 3600)
            self.assertEqual(loopdata_pkt['almanac.sunrise.raw'], six_am(0))
            self.assertEqual(stub.counts['stub_time'], 2)
            self.assertEqual(stub.counts['sunrise'], 4)
            self.assertEqual(stub.counts['sun.rise'], 1)
            self.assertEqual(stub.counts['moon_index'], 6)
            self.assertEqual(stub.counts['next_full_moon'], 1)

            # Day 2: day-tier fields recompute; the cached full moon (day 3
            # evening) is still ahead, so the event field is kept.
            pkt = {'dateTime': day1_noon + 86400, 'usUnits': 1}
            loopdata_pkt = {}
            evaluator.insert_fields(loopdata_pkt, pkt)
            self.assertEqual(loopdata_pkt['almanac.sunrise.raw'], six_am(1))
            self.assertEqual(loopdata_pkt['almanac(days=1).sunrise.raw'], six_am(2))
            self.assertEqual(stub.counts['sunrise'], 8)
            self.assertEqual(stub.counts['next_full_moon'], 1)

            # Day 3, the day of the full moon: the event is deliberately kept
            # for the rest of its day.
            pkt = {'dateTime': day1_noon + 2 * 86400, 'usUnits': 1}
            evaluator.insert_fields({}, pkt)
            self.assertEqual(stub.counts['next_full_moon'], 1)

            # Day 4: the local day advanced past the cached event; recompute.
            stub.next_full_moon_ts = six_am(31)
            pkt = {'dateTime': day1_noon + 3 * 86400, 'usUnits': 1}
            loopdata_pkt = {}
            evaluator.insert_fields(loopdata_pkt, pkt)
            self.assertEqual(stub.counts['next_full_moon'], 2)
            self.assertEqual(loopdata_pkt['almanac.next_full_moon.raw'], six_am(31))

            # Backfilled packet from day 1: equality compare, so the old day
            # gets its own values, never a newer cache.
            pkt = {'dateTime': day1_noon, 'usUnits': 1}
            loopdata_pkt = {}
            evaluator.insert_fields(loopdata_pkt, pkt)
            self.assertEqual(loopdata_pkt['almanac.sunrise.raw'], six_am(0))
            # Four sunrise fields recomputed on each of the five day changes.
            self.assertEqual(stub.counts['sunrise'], 20)
            self.assertEqual(stub.counts['next_full_moon'], 3)
        finally:
            weewx.almanac.almanacs.remove(stub)

    def test_almanac_field_end_to_end(self) -> None:
        """Almanac fields through generate_loopdata_dictionary, against the
        real weewx.almanac (PyEphem or the built-in fallback) as oracle."""
        specified_fields = [
            'current.outTemp',
            'almanac.sunrise',
            'almanac.sunrise.raw',
            'almanac.sunset.raw',
            'almanac.moon_phase',
            'almanac.moon_index.raw',
            'almanac(days=1).sunrise.raw',
            'almanac(horizon=-6).sun(use_center=1).rise.raw',
        ]
        cfg: user.loopdata.Configuration = ProcessPacketTests._get_config('us', 10800, 10, 6, specified_fields)
        cfg.almanac_fields = user.loopdata.LoopData.get_almanac_fields(specified_fields)
        cfg.latitude, cfg.longitude, cfg.altitude_m = 37.4, -122.1, 20.0
        evaluator = user.loopdata.AlmanacFieldEvaluator(cfg)

        # July 1, 2020 Noon PDT
        pkt: Dict[str, Any] = {'dateTime': 1593630000, 'usUnits': 1, 'outTemp': 77.4}
        accums: user.loopdata.Accumulators = ProcessPacketTests._get_accums(cfg, pkt['dateTime'])
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums, evaluator)

        # The oracle: the Almanac exactly as a report would build it (same
        # location, temperature from the packet, formatter/converter from the
        # target report).
        temperature_c = weewx.units.convert(
            weewx.units.as_value_tuple(pkt, 'outTemp'), 'degree_C')[0]
        oracle = weewx.almanac.Almanac(pkt['dateTime'], 37.4, -122.1, altitude=20.0,
            temperature=temperature_c, texts={}, formatter=cfg.formatter, converter=cfg.converter)

        self.assertEqual(loopdata_pkt['current.outTemp'], '77.4°F')
        self.assertEqual(loopdata_pkt['almanac.sunrise'], str(oracle.sunrise))
        self.assertEqual(loopdata_pkt['almanac.sunrise.raw'], oracle.sunrise.raw)
        self.assertEqual(loopdata_pkt['almanac.sunset.raw'], oracle.sunset.raw)
        self.assertEqual(loopdata_pkt['almanac.moon_phase'], str(oracle.moon_phase))
        self.assertEqual(loopdata_pkt['almanac.moon_index.raw'], oracle.moon_index)
        tomorrow_oracle = oracle(almanac_time=user.loopdata.AlmanacFieldEvaluator.shift_days(
            pkt['dateTime'], 1))
        self.assertEqual(loopdata_pkt['almanac(days=1).sunrise.raw'], tomorrow_oracle.sunrise.raw)
        if oracle.hasExtras:
            self.assertEqual(loopdata_pkt['almanac(horizon=-6).sun(use_center=1).rise.raw'],
                oracle(horizon=-6).sun(use_center=1).rise.raw)
            # Civil dawn precedes sunrise.
            self.assertLess(loopdata_pkt['almanac(horizon=-6).sun(use_center=1).rise.raw'],
                loopdata_pkt['almanac.sunrise.raw'])

    def test_get_barometer_trend_mbar(self) -> None:
        # Forecast descriptions for the 3 hour change in barometer readings.
        # Falling (or rising) slowly: 0.1 - 1.5mb in 3 hours
        # Falling (or rising): 1.6 - 3.5mb in 3 hours
        # Falling (or rising) quickly: 3.6 - 6.0mb in 3 hours
        # Falling (or rising) very rapidly: More than 6.0mb in 3 hours

        baroTrend: user.loopdata.BarometerTrend = user.loopdata.LoopProcessor.get_barometer_trend(9.0, 'mbar', 'group_pressure', 10800)
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

    def test_get_barometer_trend_inHg(self) -> None:
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

        # Normalization-to-3-hours: the thresholds are per-3-hours, so a
        # time_delta != 10800 must rescale the delta (delta /= time_delta/10800).
        # All cases above use 10800 (identity), so they don't exercise the
        # division.  Here a ~1.0 mbar rise over 1 hour (time_delta=3600)
        # normalizes to ~3.0 mbar/3h -> RISING; without the normalization it
        # would read ~1.0 -> RISING_SLOWLY, so the division is pinned.  The
        # value sits mid-bucket, robust to small conversion-factor differences.
        baroTrend = user.loopdata.LoopProcessor.get_barometer_trend(
            0.029530, 'inHg', 'group_pressure', 3600)
        self.assertEqual(baroTrend, user.loopdata.BarometerTrend.RISING)
        # Same delta over 6 hours normalizes DOWN (~0.5 mbar/3h) -> RISING_SLOWLY.
        baroTrend = user.loopdata.LoopProcessor.get_barometer_trend(
            0.029530, 'inHg', 'group_pressure', 21600)
        self.assertEqual(baroTrend, user.loopdata.BarometerTrend.RISING_SLOWLY)

    def test_prune_period_packet(self) -> None:
        """ test that packet is pruned to just the observations needed. """

        pkt: Dict[str, Any] = { 'dateTime': 123456789, 'usUnits': 1, 'windSpeed': 10, 'windDir': 27 }
        in_use_obstypes = {'barometer'}
        new_pkt: Dict[str, Any] = user.loopdata.LoopProcessor.prune_period_packet(pkt, in_use_obstypes)
        self.assertEqual(len(new_pkt), 2)
        self.assertEqual(new_pkt['dateTime'], 123456789)
        self.assertEqual(new_pkt['usUnits'], 1)

        pkt = { 'dateTime': 123456789, 'usUnits': 1, 'windSpeed': 10, 'windDir': 27 }
        in_use_obstypes = {'windSpeed'}
        new_pkt = user.loopdata.LoopProcessor.prune_period_packet(pkt, in_use_obstypes)
        self.assertEqual(len(new_pkt), 3)
        self.assertEqual(new_pkt['dateTime'], 123456789)
        self.assertEqual(new_pkt['usUnits'], 1)
        self.assertEqual(new_pkt['windSpeed'], 10)

        pkt = { 'dateTime': 123456789, 'usUnits': 1, 'windSpeed': 10, 'windDir': 27, 'barometer': 1035.01 }
        in_use_obstypes = {'windSpeed', 'barometer', 'windDir'}
        new_pkt = user.loopdata.LoopProcessor.prune_period_packet(pkt, in_use_obstypes)
        self.assertEqual(len(new_pkt), 5)
        self.assertEqual(new_pkt['dateTime'], 123456789)
        self.assertEqual(new_pkt['usUnits'], 1)
        self.assertEqual(new_pkt['windSpeed'], 10)
        self.assertEqual(new_pkt['barometer'], 1035.01)

    def test_changing_periods(self) -> None:
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
        cfg: user.loopdata.Configuration = ProcessPacketTests._get_config('us', 10800, 10, 6, specified_fields)

        # July 1, 2020 Noon PDT
        pkt: Dict[str, Any] = {'dateTime': 1593630000, 'usUnits': 1, 'outTemp': 77.4, 'rain': 0.01}

        accums: user.loopdata.Accumulators = ProcessPacketTests._get_accums(cfg, pkt['dateTime'])

        # First packet.
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

        # Next packet 1 minute later
        pkt = {'dateTime': 1593630060, 'usUnits': 1, 'outTemp': 77.3}
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)
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
        self.assertEqual(loopdata_pkt['trend.outTemp'], '-17.4°F')

        # Next packet 9 minute later
        pkt = {'dateTime': 1593630600, 'usUnits': 1, 'outTemp': 77.2, 'rain': 0.01}
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

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
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

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
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

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
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

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
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

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
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

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
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

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
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

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
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

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
        self.assertEqual(loopdata_pkt['trend.outTemp'],         '-348.4°F')

        # Jump to October 15 (new rain year)
        # Friday, October 15, 2021 12:00:00 PM GMT-07:00 DST
        pkt = {'dateTime': 1634324400, 'usUnits': 1, 'outTemp': 41.0, 'rain': 0.00}
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

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
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

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
        self.assertEqual(loopdata_pkt['trend.outTemp'],         '3600.0°F')

        # About 2 days later, Sunday, October 17, 2021 04:24:27 PM PDT
        pkt = {'dateTime': 1634513067, 'usUnits': 1, 'outTemp': 88.0, 'rain': 0.00}
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

        # Next day, Monday, October 18, 2021 04:24:27 PM PDT
        pkt = {'dateTime': 1634599467, 'usUnits': 1, 'outTemp': 87.5, 'rain': 0.01}
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

        # 6 days later, Saturday, October 23, 2021 04:24:27 PM PDT
        pkt = {'dateTime': 1635031467, 'usUnits': 1, 'outTemp': 87.0, 'rain': 0.04}
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

        # Next day, starts a new week, the high should be 85 (from today)
        # Sunday, October 24, 2021 04:24:27 PM PDT
        pkt = {'dateTime': 1635117867, 'usUnits': 1, 'outTemp': 85.0, 'rain': 0.01}
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

        # Make sure we have a new week accumulator that starts this Sunday 2021-10-24.
        assert accums.week_accum is not None
        self.assertEqual(timestamp_to_string(accums.week_accum.timespan.start), '2021-10-24 00:00:00 PDT (1635058800)')
        self.assertEqual(accums.week_accum['outTemp'].max, 85.0)
        self.assertEqual(loopdata_pkt['week.outTemp.max'], '85.0°F')

    def test_changing_periods_rainyear_start_1(self) -> None:
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
        cfg: user.loopdata.Configuration = ProcessPacketTests._get_config('us', 10800, 1, 6, specified_fields)

        # July 1, 2020 Noon PDT
        pkt: Dict[str, Any] = {'dateTime': 1593630000, 'usUnits': 1, 'outTemp': 77.4, 'rain': 0.01}

        accums: user.loopdata.Accumulators = ProcessPacketTests._get_accums(cfg, pkt['dateTime'])

        # First packet.
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

        # Next packet 1 minute later
        pkt = {'dateTime': 1593630060, 'usUnits': 1, 'outTemp': 77.3}
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

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
        self.assertEqual(loopdata_pkt['trend.outTemp'],    '-17.4°F')

        # Next packet 9 minute later
        pkt = {'dateTime': 1593630600, 'usUnits': 1, 'outTemp': 77.2, 'rain': 0.01}
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

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
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

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
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

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
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

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
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

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
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

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
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

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
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

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
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

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
        self.assertEqual(loopdata_pkt['trend.outTemp'],         '-348.4°F')

        # Jump to October 15 (NOT a new rain year)
        # Friday, October 15, 2021 12:00:00 PM GMT-07:00 DST
        pkt = {'dateTime': 1634324400, 'usUnits': 1, 'outTemp': 41.0, 'rain': 0.00}
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

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
        self.assertEqual(loopdata_pkt['rainyear.outTemp.max'],  '99.0°F')
        self.assertEqual(loopdata_pkt['alltime.outTemp.min'],   '41.0°F')
        self.assertEqual(loopdata_pkt['alltime.outTemp.max'],   '99.0°F')
        self.assertEqual(loopdata_pkt.get('trend.outTemp'), None)

        # 1s later
        pkt = {'dateTime': 1634324401, 'usUnits': 1, 'outTemp': 42.0, 'rain': 0.01}
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

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
        self.assertEqual(loopdata_pkt['rainyear.outTemp.max'],  '99.0°F')
        self.assertEqual(loopdata_pkt['rainyear.outTemp.avg'],  '69.8°F')
        self.assertEqual(loopdata_pkt['alltime.outTemp.min'],   '41.0°F')
        self.assertEqual(loopdata_pkt['alltime.outTemp.max'],   '99.0°F')
        self.assertEqual(loopdata_pkt['alltime.outTemp.avg'],   '73.6°F')
        self.assertEqual(loopdata_pkt['trend.outTemp'],         '3600.0°F')

        # About 2 days later, Sunday, October 17, 2021 04:24:27 PM PDT
        pkt = {'dateTime': 1634513067, 'usUnits': 1, 'outTemp': 88.0, 'rain': 0.00}
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

        # Next day, Monday, October 18, 2021 04:24:27 PM PDT
        pkt = {'dateTime': 1634599467, 'usUnits': 1, 'outTemp': 87.5, 'rain': 0.01}
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

        # 6 days later, Saturday, October 23, 2021 04:24:27 PM PDT
        pkt = {'dateTime': 1635031467, 'usUnits': 1, 'outTemp': 87.0, 'rain': 0.04}
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

        # Next day, starts a new week, the high should be 85 (from today)
        # Sunday, October 24, 2021 04:24:27 PM PDT
        pkt = {'dateTime': 1635117867, 'usUnits': 1, 'outTemp': 85.0, 'rain': 0.01}
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

        # Make sure we have a new week accumulator that starts this Sunday 2021-10-24.
        assert accums.week_accum is not None
        self.assertEqual(timestamp_to_string(accums.week_accum.timespan.start), '2021-10-24 00:00:00 PDT (1635058800)')
        self.assertEqual(accums.week_accum['outTemp'].max, 85.0)
        self.assertEqual(loopdata_pkt['week.outTemp.max'], '85.0°F')

        # Jump to January 2 (new rain year)
        # Sunday, January 2, 2022 1:00:00 PM GMT-08:00
        pkt = {'dateTime': 1641157200, 'usUnits': 1, 'outTemp': 55.0, 'rain': 0.00}
        # And 1 minute later.
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)
        pkt = {'dateTime': 1641157260, 'usUnits': 1, 'outTemp': 60.0, 'rain': 0.00}
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)
        self.assertEqual(loopdata_pkt['year.outTemp.min'],      '55.0°F')
        self.assertEqual(loopdata_pkt['year.outTemp.max'],      '60.0°F')
        self.assertEqual(loopdata_pkt['year.outTemp.avg'],      '57.5°F')
        self.assertEqual(loopdata_pkt['rainyear.outTemp.min'],  '55.0°F')
        self.assertEqual(loopdata_pkt['rainyear.outTemp.max'],  '60.0°F')
        self.assertEqual(loopdata_pkt['rainyear.outTemp.avg'],  '57.5°F')

    def test_changing_periods_week_start_0(self) -> None:
        specified_fields = [ 'current.outTemp', 'trend.outTemp',
                             '10m.outTemp.max', '10m.outTemp.min', '10m.outTemp.avg',
                             'day.outTemp.max', 'day.outTemp.min', 'day.outTemp.avg',
                             'week.outTemp.max', 'week.outTemp.min', 'week.outTemp.avg',
                             'month.outTemp.max', 'month.outTemp.min', 'month.outTemp.avg',
                             'year.outTemp.max', 'year.outTemp.min', 'year.outTemp.avg',
                             'rainyear.outTemp.max', 'rainyear.outTemp.min', 'rainyear.outTemp.avg',
                             'alltime.outTemp.max', 'alltime.outTemp.min', 'alltime.outTemp.avg']
        cfg: user.loopdata.Configuration = ProcessPacketTests._get_config('us', 10800, 10, 0, specified_fields)
        # July 1, 2020 Noon PDT
        pkt: Dict[str, Any] = {'dateTime': 1593630000, 'usUnits': 1, 'outTemp': 77.4}
        accums: user.loopdata.Accumulators = ProcessPacketTests._get_accums(cfg, pkt['dateTime'])
        self.assertEqual(cfg.week_start, 0)

        # First packet.
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

        # Next packet 1 minute later
        pkt = {'dateTime': 1593630060, 'usUnits': 1, 'outTemp': 77.3}
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

        self.assertEqual(loopdata_pkt['current.outTemp'], '77.3°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.max'], '77.4°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.min'], '77.3°F')
        self.assertEqual(loopdata_pkt['trend.outTemp']  , '-17.4°F')

        # Next packet 9 minute later
        pkt = {'dateTime': 1593630600, 'usUnits': 1, 'outTemp': 77.2}
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

        self.assertEqual(loopdata_pkt['current.outTemp'], '77.2°F')
        # Previous max should have dropped off of 10m.
        self.assertEqual(loopdata_pkt['10m.outTemp.max'], '77.3°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.min'], '77.2°F')
        self.assertEqual(loopdata_pkt['trend.outTemp'], '-3.6°F')

        # Next packet 2:51 later
        pkt = {'dateTime': 1593640860, 'usUnits': 1, 'outTemp': 76.9}
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

        self.assertEqual(loopdata_pkt['current.outTemp'], '76.9°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.max'], '76.9°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.min'], '76.9°F')
        self.assertEqual(loopdata_pkt['trend.outTemp'], '-0.3°F')

        # Next packet 4:00 later
        pkt = {'dateTime': 1593655260, 'usUnits': 1, 'outTemp': 75.0}
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

        self.assertEqual(loopdata_pkt['current.outTemp'], '75.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.max'], '75.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.min'], '75.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.min'], '75.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.max'], '77.4°F')
        self.assertEqual(loopdata_pkt.get('trend.outTemp'), None)

        # Next packet 20:00 later
        pkt = {'dateTime': 1593727260, 'usUnits': 1, 'outTemp': 70.0}
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

        self.assertEqual(loopdata_pkt['current.outTemp'], '70.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.max'], '70.0°F')
        self.assertEqual(loopdata_pkt['10m.outTemp.min'], '70.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.min'], '70.0°F')
        self.assertEqual(loopdata_pkt['day.outTemp.max'], '70.0°F')
        self.assertEqual(loopdata_pkt.get('trend.outTemp'), None)

        # Add another temp a minute later so we get a trend
        pkt = {'dateTime': 1593727320, 'usUnits': 1, 'outTemp': 70.0}
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

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
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

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
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

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
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

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
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

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
        self.assertEqual(loopdata_pkt['trend.outTemp'],         '-348.4°F')

        # Jump to October 15 (new rain year)
        pkt = {'dateTime': 1634324400, 'usUnits': 1, 'outTemp': 41.0}
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

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
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

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
        self.assertEqual(loopdata_pkt['trend.outTemp'],         '3600.0°F')

        # About 2 days later, Sunday, October 17, 2021 04:24:27 PM PDT
        pkt = {'dateTime': 1634513067, 'usUnits': 1, 'outTemp': 88.0}
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

        # Make sure we still have the old accumulator that started Monday 2021-10-11.
        assert accums.week_accum is not None
        self.assertEqual(timestamp_to_string(accums.week_accum.timespan.start), '2021-10-11 00:00:00 PDT (1633935600)')

        # Next day, Monday, October 18, 2021 04:24:27 PM PDT
        pkt = {'dateTime': 1634599467, 'usUnits': 1, 'outTemp': 87.5}
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

        # Make sure we still have a new accumulator that started Monday 2021-10-18.
        assert accums.week_accum is not None
        self.assertEqual(timestamp_to_string(accums.week_accum.timespan.start), '2021-10-18 00:00:00 PDT (1634540400)')

        # 6 days later, Saturday, October 23, 2021 04:24:27 PM PDT
        pkt = {'dateTime': 1635031467, 'usUnits': 1, 'outTemp': 87.0}
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

        # Next day DOES NOT START a new week since week_start is 0, high should be 87.5 (last Monday)
        # Sunday, October 24, 2021 04:24:27 PM PDT
        pkt = {'dateTime': 1635117867, 'usUnits': 1, 'outTemp': 85.0}
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

        # Make sure we still have the accumulator that started Monday 2021-10-18.
        assert accums.week_accum is not None
        self.assertEqual(timestamp_to_string(accums.week_accum.timespan.start), '2021-10-18 00:00:00 PDT (1634540400)')
        self.assertEqual(accums.week_accum['outTemp'].max, 87.5)
        self.assertEqual(loopdata_pkt['week.outTemp.max'], '87.5°F')

    def test_new_db_startup(self) -> None:
        pkts: List[Dict[str, Any]] = [ {'dateTime': 1665796967, 'usUnits': 1, 'windDir': 355.0, 'windSpeed': 4.0, 'outTemp': 69.1},
                 {'dateTime': 1665796969, 'usUnits': 1, 'windDir':   5.0, 'windSpeed': 3.0, 'outTemp': 69.2},
                 {'dateTime': 1665796971, 'usUnits': 1, 'windDir':  10.0, 'windSpeed': 2.0, 'outTemp': 69.3}]

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

        cfg: user.loopdata.Configuration = ProcessPacketTests._get_config('us', 10800, 10, 6, wind_fields)

        # Test when adding very first packet.
        pkt: Dict[str, Any] = pkts[0]
        accums: user.loopdata.Accumulators = ProcessPacketTests._get_accums(cfg, pkt['dateTime'])
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

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
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

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
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

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

    def test_wind(self) -> None:
        pkts: List[Dict[str, Any]] = [ {'dateTime': 1665796967, 'usUnits': 1, 'windDir': 355.0, 'windGust': 4.0, 'windGustDir': 355.0, 'windrun': None, 'windSpeed': 4.0},
                 {'dateTime': 1665796969, 'usUnits': 1, 'windDir':   5.0, 'windGust': 3.0, 'windGustDir':   5.0, 'windrun': None, 'windSpeed': 3.0},
                 {'dateTime': 1665796971, 'usUnits': 1, 'windDir':  10.0, 'windGust': 2.0, 'windGustDir':  10.0, 'windrun': None, 'windSpeed': 2.0}]

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

        cfg: user.loopdata.Configuration = ProcessPacketTests._get_config('us', 10800, 10, 6, wind_fields)

        accums: user.loopdata.Accumulators = ProcessPacketTests._get_accums(cfg, pkts[0]['dateTime'])
        for pkt in pkts:
            loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

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

    def test_wind2(self) -> None:

        pkts: List[Dict[str, Any]] = [ {'dateTime': 1665796967, 'usUnits': 1, 'windDir': 355.0, 'windGust': 4.0, 'windGustDir': 355.0, 'windrun': None, 'windSpeed': 4.0},
                 {'dateTime': 1665796969, 'usUnits': 1, 'windDir':   5.0, 'windGust': 100.0, 'windGustDir':   5.0, 'windrun': None, 'windSpeed': 100.0}]

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

        cfg: user.loopdata.Configuration = ProcessPacketTests._get_config('us', 10800, 10, 6, wind_fields)

        accums: user.loopdata.Accumulators = ProcessPacketTests._get_accums(cfg, pkts[0]['dateTime'])
        for pkt in pkts:
            loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

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

    def test_wind_rms(self) -> None:

        pkts: List[Dict[str, Any]] = [ {'dateTime': 1665796967, 'usUnits': 1, 'windGust': 0.0, 'windSpeed': 0.0},
                 {'dateTime': 1665796969, 'usUnits': 1, 'windDir':   5.0, 'windGust': 200.0, 'windGustDir':   5.0, 'windrun': None, 'windSpeed': 200.0}]

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

        cfg: user.loopdata.Configuration = ProcessPacketTests._get_config('us', 10800, 10, 6, wind_fields)

        pkt: Dict[str, Any] = pkts[0]
        accums: user.loopdata.Accumulators = ProcessPacketTests._get_accums(cfg, pkt['dateTime'])
        loopdata_pkt: Dict[str, Any] = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

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
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

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

    def test_day_wind_vecdir_vecavg(self) -> None:
        # Validate that the day accumulator's vector direction and vector
        # average are computed correctly (true vector math), independent of
        # any continuous/rolling accumulator.  The expected values are computed
        # by hand from the textbook formula and asserted against loopdata's
        # output, so this test catches any regression in the vector sum:
        #
        #   For each obs (speed, dirN), with math angle theta = radians(90 - dirN):
        #       xsum += weight * speed * cos(theta)
        #       ysum += weight * speed * sin(theta)
        #   vec_dir = (90 - degrees(atan2(ysum, xsum))) mod 360
        #   vec_avg = sqrt(xsum^2 + ysum^2) / sumtime
        #
        # The chosen sequence has a vec_dir (~10.29 deg) that is wildly
        # different from the naive scalar average of the directions (144 deg),
        # and a vec_avg (~13.89 mph) different from the scalar speed average
        # (15 mph), so a broken implementation cannot pass by coincidence.
        #
        # NOTE: weight (loop_frequency) cancels out of both results -- it
        # scales xsum/ysum equally (vec_dir is the angle, scale-invariant) and
        # is divided back out of vec_avg via sumtime -- so these expectations
        # hold regardless of the configured loop_frequency.
        import math

        pkts: List[Dict[str, Any]] = [
            {'dateTime': 1665796961, 'usUnits': 1, 'windDir': 350.0, 'windGust': 10.0, 'windGustDir': 350.0, 'windrun': None, 'windSpeed': 10.0},
            {'dateTime': 1665796963, 'usUnits': 1, 'windDir':  20.0, 'windGust': 20.0, 'windGustDir':  20.0, 'windrun': None, 'windSpeed': 20.0},
            {'dateTime': 1665796965, 'usUnits': 1, 'windDir':  40.0, 'windGust': 10.0, 'windGustDir':  40.0, 'windrun': None, 'windSpeed': 10.0},
            {'dateTime': 1665796967, 'usUnits': 1, 'windDir':  10.0, 'windGust': 30.0, 'windGustDir':  10.0, 'windrun': None, 'windSpeed': 30.0},
            {'dateTime': 1665796969, 'usUnits': 1, 'windDir': 300.0, 'windGust':  5.0, 'windGustDir': 300.0, 'windrun': None, 'windSpeed':  5.0}]

        wind_fields = [
            'day.wind.vecdir',
            'day.wind.vecdir.raw',
            'day.wind.vecavg',
            'day.wind.vecavg.raw',
            'day.windDir.avg']

        cfg: user.loopdata.Configuration = ProcessPacketTests._get_config('us', 10800, 10, 6, wind_fields)

        accums: user.loopdata.Accumulators = ProcessPacketTests._get_accums(cfg, pkts[0]['dateTime'])
        for pkt in pkts:
            loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

        # --- Compute the expected values by hand from the same inputs. ---
        weight = cfg.loop_frequency  # cancels out, but use the real value for fidelity
        xsum = ysum = sumtime = 0.0
        for pkt in pkts:
            speed = pkt['windSpeed']
            dirN = pkt['windDir']
            theta = math.radians(90.0 - dirN)
            xsum += weight * speed * math.cos(theta)
            ysum += weight * speed * math.sin(theta)
            sumtime += weight
        expected_vecdir = 90.0 - math.degrees(math.atan2(ysum, xsum))
        if expected_vecdir < 0.0:
            expected_vecdir += 360.0
        expected_vecavg = math.sqrt(xsum ** 2 + ysum ** 2) / sumtime

        # Sanity: these expectations are the ones computed offline (~10.29, ~13.89).
        self.assertAlmostEqual(expected_vecdir, 10.2922352167, places=6)
        self.assertAlmostEqual(expected_vecavg, 13.8928679139, places=6)

        # --- Assert loopdata's day accumulator matches the hand calculation. ---
        self.assertAlmostEqual(loopdata_pkt['day.wind.vecdir.raw'], expected_vecdir, places=4)
        self.assertAlmostEqual(loopdata_pkt['day.wind.vecavg.raw'], expected_vecavg, places=4)

        # The vector direction (~10 deg) must NOT collapse to the bogus scalar
        # average of directions (144 deg); windDir.avg is the scalar mean and
        # is exactly why wind.vecdir exists.  Confirm they differ markedly.
        self.assertEqual(loopdata_pkt['day.wind.vecdir'], '10°')
        self.assertNotEqual(loopdata_pkt['day.wind.vecdir'], loopdata_pkt['day.windDir.avg'])

        # Formatted vector average rounds to 14 mph (13.89 -> 14).
        self.assertEqual(loopdata_pkt['day.wind.vecavg'], '14 mph')

    def test_continuous_wind_vecdir_expiry(self) -> None:
        # Validate the CONTINUOUS (rolling) wind accumulator's vector math
        # across window expiry.  This is the path that the sign fix in
        # ContinuousVecStats.trimExpiredEntries (xsum/ysum debit) and the
        # dirsumtime debit live in: when an observation ages out of the
        # rolling window, its full vector contribution must be SUBTRACTED.
        #
        # A '2m' tag has timelength = 120s.  A packet added at ts expires once
        # a later packet arrives at ts + 120 or beyond (trim condition is
        # debit.expiration <= current_ts, expiration = ts + timelength).
        #
        # Timeline (ts, windSpeed, windDir):
        #     1000  10  200   <- expires by ts=1160 (1000+120=1120 <= 1160)
        #     1030  10  250   <- expires by ts=1160 (1030+120=1150 <= 1160)
        #     1160  10  350   <- survivor
        #     1180  10   10   <- survivor
        #     1200  10   30   <- survivor (last packet)
        #
        # The window is walked through fill -> expire -> refill, and vecdir is
        # asserted at three stages.  The progression 225 -> 350 -> 10 degrees
        # is only producible if expired contributions are correctly removed.
        # (With the old '+=' trim bug, the final vecdir would be ~277.5 deg,
        # a 267-degree error -- so this test discriminates strongly.)
        import math

        pkts: List[Dict[str, Any]] = [
            {'dateTime': 1000, 'usUnits': 1, 'windDir': 200.0, 'windGust': 10.0, 'windGustDir': 200.0, 'windrun': None, 'windSpeed': 10.0},
            {'dateTime': 1030, 'usUnits': 1, 'windDir': 250.0, 'windGust': 10.0, 'windGustDir': 250.0, 'windrun': None, 'windSpeed': 10.0},
            {'dateTime': 1160, 'usUnits': 1, 'windDir': 350.0, 'windGust': 10.0, 'windGustDir': 350.0, 'windrun': None, 'windSpeed': 10.0},
            {'dateTime': 1180, 'usUnits': 1, 'windDir':  10.0, 'windGust': 10.0, 'windGustDir':  10.0, 'windrun': None, 'windSpeed': 10.0},
            {'dateTime': 1200, 'usUnits': 1, 'windDir':  30.0, 'windGust': 10.0, 'windGustDir':  30.0, 'windrun': None, 'windSpeed': 10.0}]

        wind_fields = [
            '2m.wind.vecdir',
            '2m.wind.vecdir.raw',
            '2m.wind.vecavg',
            '2m.wind.vecavg.raw']

        cfg: user.loopdata.Configuration = ProcessPacketTests._get_config('us', 10800, 10, 6, wind_fields)
        weight = cfg.loop_frequency

        def hand_vec(survivors):
            # survivors: list of (windSpeed, windDir)
            xsum = ysum = sumtime = 0.0
            for speed, dirN in survivors:
                theta = math.radians(90.0 - dirN)
                xsum += weight * speed * math.cos(theta)
                ysum += weight * speed * math.sin(theta)
                sumtime += weight
            vec_dir = 90.0 - math.degrees(math.atan2(ysum, xsum))
            if vec_dir < 0.0:
                vec_dir += 360.0
            vec_avg = math.sqrt(xsum ** 2 + ysum ** 2) / sumtime
            return vec_dir, vec_avg

        accums: user.loopdata.Accumulators = ProcessPacketTests._get_accums(cfg, pkts[0]['dateTime'])

        # Packet 1 (ts=1000): only the 200-degree obs is present.
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkts[0], cfg, accums)

        # Packet 2 (ts=1030): both 200 and 250 live; nothing expired yet.
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkts[1], cfg, accums)
        vd, va = hand_vec([(10.0, 200.0), (10.0, 250.0)])
        self.assertAlmostEqual(vd, 225.0, places=4)  # offline-computed checkpoint
        self.assertAlmostEqual(loopdata_pkt['2m.wind.vecdir.raw'], vd, places=4)
        self.assertAlmostEqual(loopdata_pkt['2m.wind.vecavg.raw'], va, places=4)

        # Packet 3 (ts=1160): 1000 (exp 1120) and 1030 (exp 1150) have both
        # expired (<= 1160); only the 350-degree obs survives.
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkts[2], cfg, accums)
        vd, va = hand_vec([(10.0, 350.0)])
        self.assertAlmostEqual(vd, 350.0, places=4)
        self.assertAlmostEqual(loopdata_pkt['2m.wind.vecdir.raw'], vd, places=4)
        self.assertAlmostEqual(loopdata_pkt['2m.wind.vecavg.raw'], va, places=4)

        # Packets 4 and 5 (ts=1180, 1200): window now holds 350, 10, 30.
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkts[3], cfg, accums)
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkts[4], cfg, accums)
        vd, va = hand_vec([(10.0, 350.0), (10.0, 10.0), (10.0, 30.0)])
        self.assertAlmostEqual(vd, 10.0, places=4)  # offline-computed checkpoint
        self.assertAlmostEqual(va, 9.5979508052, places=6)
        self.assertAlmostEqual(loopdata_pkt['2m.wind.vecdir.raw'], vd, places=4)
        self.assertAlmostEqual(loopdata_pkt['2m.wind.vecavg.raw'], va, places=4)

    def test_min_max_dict(self) -> None:
        # Direct unit tests for MinMaxDict, the two-heap lazy-deletion mapping
        # behind the continuous accumulators.  Expected values are computed
        # from first principles (a shadow dict with min()/max() as the
        # oracle), not from loopdata output.
        MMD = user.loopdata.MinMaxDict

        # --- Empty: peekitem raises IndexError, like any empty container. ---
        d = MMD()
        self.assertEqual(len(d), 0)
        self.assertNotIn(1.0, d)
        with self.assertRaises(IndexError):
            d.peekitem(0)
        with self.assertRaises(IndexError):
            d.peekitem(-1)

        # --- Only the two end indexes are supported. ---
        d[1.0] = 'one'
        with self.assertRaises(IndexError):
            d.peekitem(1)
        with self.assertRaises(IndexError):
            d.peekitem(-2)

        # --- Basic get/set/pop; overwriting an existing key is not a new key. ---
        self.assertIn(1.0, d)
        self.assertEqual(d[1.0], 'one')
        d[1.0] = 'uno'
        self.assertEqual(d[1.0], 'uno')
        self.assertEqual(len(d), 1)
        self.assertEqual(d.peekitem(0), (1.0, 'uno'))
        self.assertEqual(d.peekitem(-1), (1.0, 'uno'))
        self.assertEqual(d.pop(1.0), 'uno')
        self.assertEqual(len(d), 0)
        with self.assertRaises(IndexError):
            d.peekitem(0)

        # --- Long stale chains: peekitem must skim dead heap entries.  Kill
        #     the 999 smallest keys, so the min-heap top is 999 dead entries
        #     deep; then the symmetric case for the max-heap. ---
        d = MMD()
        for i in range(1000):
            d[float(i)] = i
        for i in range(999):
            d.pop(float(i))
        self.assertEqual(d.peekitem(0), (999.0, 999))
        self.assertEqual(d.peekitem(-1), (999.0, 999))
        d = MMD()
        for i in range(1000):
            d[float(i)] = i
        for i in range(1, 1000):
            d.pop(float(i))
        self.assertEqual(d.peekitem(-1), (0.0, 0))
        self.assertEqual(d.peekitem(0), (0.0, 0))

        # --- Re-adding a dead key leaves duplicate heap entries; peeks must
        #     stay correct through kill/revive cycles and after final death. ---
        d = MMD()
        d[5.0] = 'a'
        for marker in ('b', 'c', 'd'):
            d.pop(5.0)
            d[5.0] = marker
            self.assertEqual(d.peekitem(0), (5.0, marker))
            self.assertEqual(d.peekitem(-1), (5.0, marker))
        d[7.0] = 'hi'
        d.pop(5.0)
        self.assertEqual(d.peekitem(0), (7.0, 'hi'))
        self.assertEqual(d.peekitem(-1), (7.0, 'hi'))

        # --- Randomized differential fuzz against the shadow dict.  Narrow
        #     key ranges force constant key death and revival (duplicate heap
        #     entries); wide ranges force churn and compaction.  Values are
        #     mutable lists appended to in place, mirroring how the
        #     accumulators use the timestamp deques. ---
        rand = random.Random(20260711)
        for key_range, n_ops in ((5, 20000), (40, 20000), (1000, 12000)):
            d = MMD()
            shadow: Dict[float, List[int]] = {}
            for op in range(n_ops):
                key = float(rand.randrange(key_range))
                r = rand.random()
                if r < 0.50:
                    if key not in d:
                        self.assertNotIn(key, shadow)
                        d[key] = []
                        shadow[key] = d[key]  # same object in both
                    d[key].append(op)
                elif shadow:
                    victim = rand.choice(list(shadow))
                    self.assertIs(d.pop(victim), shadow.pop(victim))
                self.assertEqual(len(d), len(shadow))
                if shadow:
                    mn, mx = min(shadow), max(shadow)
                    self.assertEqual(d.peekitem(0), (mn, shadow[mn]))
                    self.assertEqual(d.peekitem(-1), (mx, shadow[mx]))
            # The heaps are rebuilt whenever an insert leaves them holding
            # more than twice the live keys, so right after an insert they
            # are bounded.  (Pops don't shrink them, so insert one key to
            # re-establish the bound before checking.)
            d[float(key_range)] = []
            self.assertLessEqual(len(d._min_heap), 2 * len(d) + 16)
            self.assertLessEqual(len(d._max_heap), 2 * len(d) + 16)

    def test_continuous_scalar_stats_edge_cases(self) -> None:
        # Direct unit tests for ContinuousScalarStats accessors, focused on the
        # empty-accumulator branches and the None/NaN rejection path in addSum.
        CS = user.loopdata.ContinuousScalarStats

        # --- Empty accumulator: every accessor degrades gracefully. ---
        s = CS(timelength=120)
        self.assertIsNone(s.first)
        self.assertIsNone(s.firsttime)
        self.assertIsNone(s.last)
        self.assertIsNone(s.lasttime)
        self.assertIsNone(s.avg)            # count is 0 -> None
        # getStatsTuple on empty: min/mintime/max/maxtime are None; the numeric
        # fields are zero (sum/wsum massaged to 0.0, count 0, sumtime 0.0).
        self.assertEqual(s.getStatsTuple(), (None, None, None, None, 0.0, 0, 0.0, 0.0))

        # --- One value (ts=100, val=5.0, weight=2). ---
        s.addSum(100, 5.0, weight=2)
        self.assertEqual(s.first, 5.0)
        self.assertEqual(s.firsttime, 100)
        self.assertEqual(s.last, 5.0)
        self.assertEqual(s.lasttime, 100)
        self.assertAlmostEqual(s.avg, 5.0)  # wsum/sumtime = 10/2
        self.assertEqual(s.getStatsTuple(), (5.0, 100, 5.0, 100, 5.0, 1, 10.0, 2))

        # --- None / NaN / non-numeric are rejected by addSum (no state change). ---
        s.addSum(110, None, weight=2)
        s.addSum(120, float('nan'), weight=2)
        s.addSum(130, 'not-a-number', weight=2)
        # Still exactly the single value from before.
        self.assertEqual(s.getStatsTuple(), (5.0, 100, 5.0, 100, 5.0, 1, 10.0, 2))
        self.assertEqual(s.lasttime, 100)

        # --- A second, larger value updates min/max ordering and last. ---
        s.addSum(140, 9.0, weight=2)
        mn, mntime, mx, mxtime, ssum, scount, swsum, ssumtime = s.getStatsTuple()
        self.assertEqual(mn, 5.0)
        self.assertEqual(mx, 9.0)
        self.assertEqual(scount, 2)
        self.assertEqual(s.last, 9.0)
        self.assertEqual(s.lasttime, 140)

    def test_continuous_vec_stats_edge_cases(self) -> None:
        # Direct unit tests for ContinuousVecStats accessors, focused on the
        # empty-accumulator branches (including the maxdir slot that must be
        # None when empty), the calm-wind (dirN is None, speed 0) path in
        # addSum, and that 'first' reports the FIRST observation's direction.
        CV = user.loopdata.ContinuousVecStats

        # --- Empty accumulator. ---
        v = CV(timelength=120)
        self.assertIsNone(v.first)
        self.assertIsNone(v.firsttime)
        self.assertIsNone(v.last)
        self.assertIsNone(v.lasttime)
        self.assertIsNone(v.avg)
        self.assertIsNone(v.rms)
        self.assertIsNone(v.vec_avg)
        self.assertIsNone(v.vec_dir)        # empty -> last is None -> None
        # getStatsTuple on empty must not raise; the maxdir slot (index 8) is
        # None (regression guard: it was previously an unbound local).
        st = v.getStatsTuple()
        self.assertEqual(len(st), 14)
        self.assertEqual(st[0:4], (None, None, None, None))  # min,mintime,max,maxtime
        self.assertEqual(st[5], 0)                            # count
        self.assertIsNone(st[8])                              # maxdir

        # --- Calm wind: speed 0 with dirN None is accepted (dirsumtime path). ---
        v.addSum(100, (0.0, None), weight=2)
        self.assertEqual(v.count, 1)
        self.assertEqual(v.first, (0.0, None))
        self.assertEqual(v.last, (0.0, None))

        # --- Two observations with DIFFERENT directions: 'first' must report
        # the first observation's direction, 'last' the last's. ---
        v2 = CV(timelength=120)
        v2.addSum(200, (10.0, 90.0), weight=2)    # first: East
        v2.addSum(210, (10.0, 270.0), weight=2)   # last: West
        self.assertEqual(v2.first, (10.0, 90.0))  # regression guard for the
                                                  # first-direction index fix
        self.assertEqual(v2.last, (10.0, 270.0))
        self.assertEqual(v2.firsttime, 200)
        self.assertEqual(v2.lasttime, 210)

        # --- None speed is rejected by addSum (no state change). ---
        before = v2.count
        v2.addSum(220, (None, 45.0), weight=2)
        self.assertEqual(v2.count, before)

        # --- Non-numeric speed: to_float raises -> speed becomes None ->
        # the whole observation is rejected (covers the except ValueError path
        # for speed). ---
        v3 = CV(timelength=120)
        v3.addSum(300, ('not-a-number', 45.0), weight=2)
        self.assertEqual(v3.count, 0)

        # --- Non-numeric dirN with valid speed: to_float(dirN) raises -> dirN
        # becomes None, but the speed is still recorded (covers the except
        # ValueError path for dirN, and the dirN-is-None branch with nonzero
        # speed where xsum/ysum are NOT updated). ---
        v4 = CV(timelength=120)
        v4.addSum(310, (10.0, 'bad-dir'), weight=2)
        self.assertEqual(v4.count, 1)
        self.assertEqual(v4.xsum, 0.0)   # no direction -> no vector components
        self.assertEqual(v4.ysum, 0.0)
        self.assertEqual(v4.last, (10.0, None))

    def test_continuous_vec_stats_trim_debits_every_field(self) -> None:
        # trimExpiredEntries must DEBIT every running sum by exactly the
        # contribution of each expired observation -- it is the precise inverse
        # of addSum.  This test pins each debited field to an independently
        # hand-computed post-trim value, so that flipping any '-=' to '+=' (or
        # otherwise corrupting a debit) is detected.  Mutation testing showed
        # these debits were previously unasserted (the original vecdir bug lived
        # in exactly this trim arithmetic).
        CV = user.loopdata.ContinuousVecStats

        v = CV(timelength=100)
        # Three observations, weight 2 each, at 90 deg apart so x/y components
        # are clean: (10, E), (20, S), (30, W).  Expirations: 200, 250, 300.
        v.addSum(100, (10.0, 90.0), weight=2)    # East
        v.addSum(150, (20.0, 180.0), weight=2)   # South
        v.addSum(200, (30.0, 270.0), weight=2)   # West

        # Sanity: full state before trimming (independently computed).
        self.assertEqual(v.sum, 60.0)
        self.assertEqual(v.count, 3)
        self.assertEqual(v.wsum, 120.0)
        self.assertEqual(v.sumtime, 6.0)
        self.assertEqual(v.squaresum, 1400.0)
        self.assertEqual(v.wsquaresum, 2800.0)
        self.assertEqual(v.dirsumtime, 6)

        # Trim at ts=205: only the first debit (expiration 200 <= 205) matures.
        v.trimExpiredEntries(205)

        # Every field must be debited by exactly the East observation's
        # contribution.  Values hand-computed, independent of the implementation.
        self.assertEqual(v.sum, 50.0)          # 60 - 10
        self.assertEqual(v.count, 2)           # 3 - 1
        self.assertEqual(v.wsum, 100.0)        # 120 - 2*10
        self.assertEqual(v.sumtime, 4.0)       # 6 - 2
        self.assertEqual(v.squaresum, 1300.0)  # 1400 - 10**2
        self.assertEqual(v.wsquaresum, 2600.0) # 2800 - 2*10**2
        self.assertEqual(v.dirsumtime, 4)      # 6 - 2  (the dirsumtime debit fix)
        # East (90 deg) contributes only to xsum (cos), nothing to ysum (sin=0).
        # So trimming it changes xsum but leaves ysum unchanged -- this pins the
        # CONDITIONAL x/y debits independently.
        self.assertAlmostEqual(v.xsum, -60.0, places=6)
        self.assertAlmostEqual(v.ysum, -40.0, places=6)

        # The expired entry must also be removed from speed_dict (speed 10 gone).
        self.assertNotIn(10.0, v.speed_dict)
        self.assertIn(20.0, v.speed_dict)
        self.assertIn(30.0, v.speed_dict)

    def test_continuous_vec_stats_trim_boundary_and_calm(self) -> None:
        # Companion to the trim-debits test, targeting three branches the first
        # one cannot reach:
        #  (1) the expiration boundary -- a debit at EXACTLY its expiration ts
        #      must mature (expiration <= ts is inclusive);
        #  (2) the ysum debit -- trimming an observation with a non-zero
        #      y-component must change ysum (the East obs used elsewhere has
        #      zero y, so it cannot pin this line);
        #  (3) the calm-wind branch (dirN is None and speed 0) -- dirsumtime is
        #      credited on add and must be debited on trim.
        CV = user.loopdata.ContinuousVecStats

        # --- (1) + (2): trim a SOUTH observation (pure -y) at exactly its
        # expiration. ---
        v = CV(timelength=100)
        v.addSum(100, (20.0, 180.0), weight=2)   # South: ysum=-40, xsum~0
        v.addSum(150, (30.0, 270.0), weight=2)   # West:  xsum=-60, ysum~0
        self.assertAlmostEqual(v.ysum, -40.0, places=6)

        # Trim at EXACTLY the first debit's expiration (100 + 100 = 200).  The
        # inclusive boundary (expiration <= ts) means it matures.
        v.trimExpiredEntries(200)
        self.assertEqual(v.count, 1)             # boundary debit did mature
        self.assertAlmostEqual(v.ysum, 0.0, places=6)   # South's -y removed
        self.assertAlmostEqual(v.xsum, -60.0, places=6) # West's x remains

        # --- (3): calm wind (speed 0, dirN None) credits then debits
        # dirsumtime via the 'or speed == 0' branch. ---
        c = CV(timelength=100)
        c.addSum(100, (0.0, None), weight=2)
        self.assertEqual(c.dirsumtime, 2)        # credited despite dirN None
        self.assertEqual(c.count, 1)
        c.trimExpiredEntries(200)                # expiration 200 <= 200 -> trim
        self.assertEqual(c.dirsumtime, 0)        # debited back to zero
        self.assertEqual(c.count, 0)

    def test_continuous_scalar_stats_sums_and_trim(self) -> None:
        # Pins every running-sum field of ContinuousScalarStats across addSum
        # (credits) and trimExpiredEntries (debits), with independently
        # hand-computed values, so a flipped +=/-= or wrong factor is detected.
        CS = user.loopdata.ContinuousScalarStats

        s = CS(timelength=100)
        s.addSum(100, 10.0, weight=2)
        s.addSum(150, 20.0, weight=2)
        s.addSum(200, 30.0, weight=2)
        # After three adds (independently computed).
        self.assertEqual(s.sum, 60.0)
        self.assertEqual(s.count, 3)
        self.assertEqual(s.wsum, 120.0)
        self.assertEqual(s.sumtime, 6.0)
        self.assertAlmostEqual(s.avg, 20.0)        # wsum/sumtime = 120/6

        # Trim at 205: only the first debit (expiration 200) matures.
        s.trimExpiredEntries(205)
        self.assertEqual(s.sum, 50.0)              # 60 - 10
        self.assertEqual(s.count, 2)               # 3 - 1
        self.assertEqual(s.wsum, 100.0)            # 120 - 10*2
        self.assertEqual(s.sumtime, 4.0)           # 6 - 2
        self.assertAlmostEqual(s.avg, 25.0)        # 100/4
        # values_dict cleanup: the trimmed value's key is removed.
        self.assertNotIn(10.0, s.values_dict)
        self.assertIn(20.0, s.values_dict)

    def test_continuous_vec_stats_addsum_credits(self) -> None:
        # Pins every running-sum field credited by ContinuousVecStats.addSum
        # (the inverse of the trim debits), with hand-computed values.  Uses
        # three observations 90 deg apart so x and y components are cleanly
        # separable -- which also pins the xsum (cos) and ysum (sin) lines
        # independently (a mutation to either must change a distinct field).
        CV = user.loopdata.ContinuousVecStats

        v = CV(timelength=100)
        v.addSum(100, (10.0, 90.0), weight=2)    # East:  +x only
        v.addSum(150, (20.0, 180.0), weight=2)   # South: -y only
        v.addSum(200, (30.0, 270.0), weight=2)   # West:  -x only

        self.assertEqual(v.sum, 60.0)
        self.assertEqual(v.count, 3)
        self.assertEqual(v.wsum, 120.0)
        self.assertEqual(v.sumtime, 6.0)
        self.assertEqual(v.squaresum, 1400.0)     # 10^2+20^2+30^2
        self.assertEqual(v.wsquaresum, 2800.0)    # 2*(above)
        self.assertEqual(v.dirsumtime, 6)
        # East(+x 20) + West(-x 60) = -40; South contributes -y 40.
        self.assertAlmostEqual(v.xsum, -40.0, places=6)
        self.assertAlmostEqual(v.ysum, -40.0, places=6)

    def test_continuous_vec_stats_getstatstuple_and_accessors(self) -> None:
        # Pins the getStatsTuple massage/return slots and the derived accessors
        # (avg, rms, vec_avg, vec_dir, first, last, firsttime, lasttime) with
        # independently computed values.
        CV = user.loopdata.ContinuousVecStats

        v = CV(timelength=100)
        v.addSum(100, (10.0, 90.0), weight=2)
        v.addSum(150, (20.0, 180.0), weight=2)
        v.addSum(200, (30.0, 270.0), weight=2)

        # Accessors (hand-computed).
        self.assertAlmostEqual(v.avg, 20.0)                      # wsum/sumtime
        self.assertAlmostEqual(v.rms, 21.602468994692867, places=6)   # sqrt(2800/6)
        self.assertAlmostEqual(v.vec_avg, 9.428090415820636, places=6)
        self.assertAlmostEqual(v.vec_dir, 225.0, places=6)       # atan2(-40,-40)
        # first = first observation's (speed, dir); last = last observation's.
        self.assertEqual(v.first, (10.0, 90.0))
        self.assertEqual(v.last, (30.0, 270.0))
        self.assertEqual(v.firsttime, 100)
        self.assertEqual(v.lasttime, 200)

        # getStatsTuple slots that feed sum/wsum/sumtime/squaresum/etc.
        st = v.getStatsTuple()
        # Indices: 0 min,1 mintime,2 max,3 maxtime,4 sum,5 count,6 wsum,
        # 7 sumtime,8 maxdir,9 xsum,10 ysum,11 dirsumtime,12 squaresum,13 wsquaresum
        self.assertEqual(st[4], 60.0)    # sum
        self.assertEqual(st[5], 3)       # count
        self.assertEqual(st[6], 120.0)   # wsum
        self.assertEqual(st[7], 6.0)     # sumtime
        self.assertAlmostEqual(st[9], -40.0, places=6)   # xsum
        self.assertAlmostEqual(st[10], -40.0, places=6)  # ysum
        self.assertEqual(st[11], 6)      # dirsumtime
        self.assertEqual(st[12], 1400.0) # squaresum
        self.assertEqual(st[13], 2800.0) # wsquaresum

    def test_add_period_obstype_scalar_agg_dispatch(self) -> None:
        # Pins the scalar agg_type dispatch in add_period_obstype (each
        # agg_type must route to the CORRECT stat).  Uses a US accum and a US
        # converter so conversion is identity for outTemp (degree_F), making the
        # '.raw' values exact and independent of any skin config.  Distinct
        # min/max/sum/avg values mean swapping any two agg branches is caught.
        import weewx.units
        US = weewx.units.unit_constants['US']

        # Build a day-period scalar accum for outTemp with known records.
        span = weeutil.weeutil.TimeSpan(0, 100000000000)
        accum = weewx.accum.Accum(span, US)
        for ts, t in ((1000, 40.0), (1300, 60.0), (1600, 50.0)):
            accum.addRecord({'dateTime': ts, 'usUnits': 1, 'outTemp': t}, weight=300)

        formatter = weewx.units.Formatter()                 # default
        converter = weewx.units.Converter(weewx.units.USUnits)

        # Conversion-identity precondition: a degree_F value must pass through
        # unchanged, so the asserted '.raw' values below are exact.
        vt = weewx.units.ValueTuple(40.0, 'degree_F', 'group_temperature')
        self.assertAlmostEqual(converter.convert(vt)[0], 40.0)

        def raw_value(agg):
            cname = user.loopdata.LoopData.parse_cname('day.outTemp.%s.raw' % agg)
            self.assertIsNotNone(cname, msg='parse failed for agg %s' % agg)
            pkt = {}
            user.loopdata.LoopProcessor.add_period_obstype(
                cname, accum, pkt, converter, formatter)
            return pkt.get(cname.field)

        # Each agg_type routes its specific stat (independently hand-computed).
        self.assertAlmostEqual(raw_value('min'), 40.0)
        self.assertAlmostEqual(raw_value('max'), 60.0)
        self.assertAlmostEqual(raw_value('sum'), 150.0)
        self.assertAlmostEqual(raw_value('avg'), 50.0)
        self.assertEqual(raw_value('mintime'), 1000)
        self.assertEqual(raw_value('maxtime'), 1300)
        # count is a valid ScalarStats agg per weewx accum.py (getStatsTuple
        # slot 5); 3 records -> count 3.
        self.assertEqual(raw_value('count'), 3)
        # Distinctness guard: min and max must differ (catches min<->max swap).
        self.assertNotEqual(raw_value('min'), raw_value('max'))

    def test_add_period_obstype_vec_agg_dispatch(self) -> None:
        # Pins the VECTOR agg_type dispatch in add_period_obstype.  Builds a
        # 'wind' VecStats accum with distinct per-agg values so each agg_type
        # routes its specific slot; a swapped branch produces a different value.
        import weewx.units
        US = weewx.units.unit_constants['US']

        span = weeutil.weeutil.TimeSpan(0, 100000000000)
        accum = weewx.accum.Accum(span, US)
        # windSpeed + windDir records (no separate gust -> gust tracks speed).
        for ts, spd, d in ((1000, 3.0, 90.0), (1300, 9.0, 180.0), (1600, 6.0, 270.0)):
            accum.addRecord({'dateTime': ts, 'usUnits': 1,
                             'windSpeed': spd, 'windDir': d, 'windGust': spd,
                             'windGustDir': d}, weight=300)

        formatter = weewx.units.Formatter()
        converter = weewx.units.Converter(weewx.units.USUnits)

        def raw_value(agg):
            cname = user.loopdata.LoopData.parse_cname('day.wind.%s.raw' % agg)
            self.assertIsNotNone(cname, msg='parse failed for agg %s' % agg)
            pkt = {}
            user.loopdata.LoopProcessor.add_period_obstype(
                cname, accum, pkt, converter, formatter)
            return pkt.get(cname.field)

        # Independently computed from the records.
        self.assertAlmostEqual(raw_value('min'), 3.0)
        self.assertAlmostEqual(raw_value('max'), 9.0)
        self.assertEqual(raw_value('mintime'), 1000)
        self.assertEqual(raw_value('maxtime'), 1300)
        # count is a valid VecStats agg per weewx accum.py; 3 records -> 3.
        # (This previously could not be requested -- 'count' was missing from
        # parse_cname's valid_agg_types -- which was a bug, now fixed.)
        self.assertEqual(raw_value('count'), 3)
        self.assertAlmostEqual(raw_value('sum'), 18.0)
        self.assertAlmostEqual(raw_value('avg'), 6.0)
        self.assertAlmostEqual(raw_value('rms'), 6.480741, places=4)
        self.assertAlmostEqual(raw_value('vecavg'), 3.162278, places=4)
        self.assertAlmostEqual(raw_value('vecdir'), 198.434949, places=4)
        # gustdir == max_dir: per weewx accum.py VecStats.addHiLo, max_dir is
        # the direction recorded at the maximum speed.  Max speed 9.0 occurred
        # at dir 180.0.
        self.assertAlmostEqual(raw_value('gustdir'), 180.0)
        # Routing-distinctness guards (catch branch swaps even without pinning
        # the exact gustdir value, whose weewx semantics we don't re-derive).
        self.assertNotEqual(raw_value('min'), raw_value('max'))
        self.assertNotEqual(raw_value('avg'), raw_value('vecavg'))
        self.assertNotEqual(raw_value('mintime'), raw_value('maxtime'))

        # Empty vec accum (count == 0): the dispatch guard 'and stats.count != 0'
        # must be false, so NO value is produced for any agg.  This pins the
        # count-guard on the VecStats branch.
        empty_accum = weewx.accum.Accum(span, US)
        # Touch 'wind' so the obstype exists but has no observations.
        empty_accum.addRecord({'dateTime': 1000, 'usUnits': 1,
                               'windDir': 90.0}, weight=300)  # dir only, no speed
        cname_e = user.loopdata.LoopData.parse_cname('day.wind.max.raw')
        pkt_e = {}
        if 'wind' in empty_accum:
            user.loopdata.LoopProcessor.add_period_obstype(
                cname_e, empty_accum, pkt_e, converter, formatter)
            self.assertNotIn('day.wind.max.raw', pkt_e)  # count==0 -> no output

    def test_add_current_obstype_format_spec_dispatch(self) -> None:
        # Pins the format_spec branches of add_current_obstype: 'raw' returns
        # the numeric value; 'formatted' returns the bare formatted string;
        # 'ordinal_compass' returns a compass label; default appends the unit
        # label.  Uses the real SeasonsReport formatter (via _get_config) so
        # 'formatted' (e.g. '72.5') genuinely differs from the default
        # toString (e.g. '72.5°F') -- with a bare Formatter() they coincide and
        # the branch cannot be distinguished.
        specified_fields = ['current.outTemp', 'current.outTemp.raw',
                            'current.outTemp.formatted',
                            'current.windDir.ordinal_compass']
        cfg = ProcessPacketTests._get_config('us', 10800, 1, 6, specified_fields)
        converter = cfg.converter
        formatter = cfg.formatter

        pkt = {'dateTime': 1000, 'usUnits': 1, 'outTemp': 72.5, 'windDir': 90.0}

        def field_value(spec, obstype='outTemp'):
            field = ('current.%s' % obstype) if spec is None else (
                'current.%s.%s' % (obstype, spec))
            cname = user.loopdata.LoopData.parse_cname(field)
            self.assertIsNotNone(cname, msg='parse failed %s %s' % (obstype, spec))
            out = {}
            user.loopdata.LoopProcessor.add_current_obstype(
                cname, pkt, out, converter, formatter)
            return out.get(cname.field)

        # 'raw' -> exact numeric (US identity conversion for degree_F).
        self.assertAlmostEqual(field_value('raw'), 72.5)
        # 'formatted' -> bare number string; default -> appends unit label.
        # These differ under the real skin formatter, pinning the 'formatted'
        # branch against the fall-through default.
        t_formatted = field_value('formatted', obstype='outTemp')
        t_default = field_value(None, obstype='outTemp')
        self.assertIsInstance(t_formatted, str)
        self.assertNotEqual(t_formatted, t_default)
        # 'ordinal_compass' on windDir -> a compass label, distinct from the
        # default rendering, pinning the ordinal_compass branch.
        d_compass = field_value('ordinal_compass', obstype='windDir')
        d_default = field_value(None, obstype='windDir')
        self.assertIsInstance(d_compass, str)
        self.assertNotEqual(d_compass, d_default)

    def test_create_loopdata_packet_period_routing(self) -> None:
        # Pins create_loopdata_packet's period routing: each cname.period must
        # be dispatched to its OWN accumulator.  day_accum and hour_accum hold
        # the SAME obstype (outTemp) with DIFFERENT values, so a mis-routed
        # period (e.g. a 'day'->'hour' comparison swap) reads the wrong accum
        # and produces the wrong value.  Uses '.raw' so values are exact
        # (target_unit=US -> identity conversion).
        specified_fields = ['current.outTemp.raw',
                            'day.outTemp.max.raw', 'day.outTemp.min.raw',
                            'hour.outTemp.max.raw', 'hour.outTemp.min.raw']
        cfg = ProcessPacketTests._get_config('us', 10800, 1, 6, specified_fields)

        # Noon PDT, July 1 2020 (same anchor the other harness tests use).
        pkt_time = 1593630000
        accums = ProcessPacketTests._get_accums(cfg, pkt_time)
        self.assertIsNotNone(accums.hour_accum)   # requested -> built

        # Distinct values per period.  NOTE: archiveHoursAgoSpan(pkt_time) is
        # the PREVIOUS completed hour [11:00, 12:00], so hour records must fall
        # inside it (not at/after noon).  The day span contains noon.
        accums.day_accum.addRecord(
            {'dateTime': pkt_time - 1, 'usUnits': 1, 'outTemp': 50.0}, weight=300)
        accums.day_accum.addRecord(
            {'dateTime': pkt_time, 'usUnits': 1, 'outTemp': 80.0}, weight=300)
        accums.hour_accum.addRecord(
            {'dateTime': pkt_time - 1800, 'usUnits': 1, 'outTemp': 60.0}, weight=300)
        accums.hour_accum.addRecord(
            {'dateTime': pkt_time - 900, 'usUnits': 1, 'outTemp': 70.0}, weight=300)

        pkt = {'dateTime': pkt_time, 'usUnits': 1, 'outTemp': 65.0}
        loopdata_pkt = user.loopdata.LoopProcessor.create_loopdata_packet(
            pkt, cfg, accums)

        # Each period routed to its own accum (cross-checks routing).
        self.assertAlmostEqual(loopdata_pkt['day.outTemp.max.raw'], 80.0)
        self.assertAlmostEqual(loopdata_pkt['day.outTemp.min.raw'], 50.0)
        self.assertAlmostEqual(loopdata_pkt['hour.outTemp.max.raw'], 70.0)
        self.assertAlmostEqual(loopdata_pkt['hour.outTemp.min.raw'], 60.0)
        # 'current' routed to the live packet, not an accum.
        self.assertAlmostEqual(loopdata_pkt['current.outTemp.raw'], 65.0)

    def test_add_trend_obstype_barometer_code_desc(self) -> None:
        # Pins add_trend_obstype's barometer code/desc routing: for
        # trend.barometer.code the field gets the BarometerTrend enum VALUE;
        # for .desc it gets the description string.  A barometer rise of
        # 0.07 inHg over ~time_delta -> ~2.37 mbar/3h -> RISING (code 2).
        import weewx.units
        US = weewx.units.unit_constants['US']
        converter = weewx.units.Converter(weewx.units.USUnits)
        formatter = weewx.units.Formatter()
        baro_descs = user.loopdata.LoopData.construct_baro_trend_descs({})

        time_delta = 10800
        loop_frequency = 2.0
        # Span the accum across ~time_delta so the trend adjustment factor is ~1.
        t0 = 1000
        t1 = t0 + (time_delta - loop_frequency)  # actual_time_delta == time_delta
        accum = user.loopdata.ContinuousAccum(100000, US)
        accum.addRecord({'dateTime': t0, 'usUnits': 1, 'barometer': 30.00})
        accum.addRecord({'dateTime': t1, 'usUnits': 1, 'barometer': 30.07})

        pkt = {'dateTime': t1, 'usUnits': 1, 'barometer': 30.07}

        def trend_field(spec):
            cname = user.loopdata.LoopData.parse_cname('trend.barometer.%s' % spec)
            self.assertIsNotNone(cname, msg='parse failed for %s' % spec)
            out = {}
            user.loopdata.LoopProcessor.add_trend_obstype(
                cname, accum, pkt, out, time_delta, loop_frequency,
                baro_descs, converter, formatter)
            return out.get(cname.field)

        # code -> the enum value (RISING == 2); desc -> the description string.
        self.assertEqual(trend_field('code'), user.loopdata.BarometerTrend.RISING.value)
        self.assertEqual(trend_field('desc'), 'Rising')

    def test_get_trend_computation(self) -> None:
        # Pins the trend math in get_trend: trend = last - first, adjusted to
        # spread over time_delta:  adj = time_delta / (lasttime - firsttime +
        # loop_frequency) * trend.  Value is independently hand-computed.  US
        # config -> identity conversion for barometer (inHg).
        import weewx.units
        US = weewx.units.unit_constants['US']
        converter = weewx.units.Converter(weewx.units.USUnits)

        # Continuous accum holding two barometer readings.
        accum = user.loopdata.ContinuousAccum(100000, US)
        accum.addRecord({'dateTime': 1000, 'usUnits': 1, 'barometer': 30.00})
        accum.addRecord({'dateTime': 1900, 'usUnits': 1, 'barometer': 30.06})
        self.assertIn('barometer', accum)
        self.assertAlmostEqual(accum['barometer'].first, 30.00)
        self.assertAlmostEqual(accum['barometer'].last, 30.06)

        cname = user.loopdata.LoopData.parse_cname('trend.barometer.raw')
        self.assertIsNotNone(cname)
        pkt = {'dateTime': 1900, 'usUnits': 1, 'barometer': 30.06}
        time_delta = 10800
        loop_frequency = 2.0

        value, unit_type, group_type = user.loopdata.LoopProcessor.get_trend(
            cname, pkt, accum, converter, time_delta, loop_frequency)

        # trend = 30.06 - 30.00 = 0.06
        # actual_time_delta = 1900 - 1000 + 2.0 = 902.0
        # adj = 10800 / 902.0 * 0.06 = 0.718404...
        self.assertAlmostEqual(value, 0.7184035476718404, places=6)

        # Guard branches: identical first/last time -> None (need two readings).
        accum2 = user.loopdata.ContinuousAccum(100000, US)
        accum2.addRecord({'dateTime': 1000, 'usUnits': 1, 'barometer': 30.00})
        v2, _, _ = user.loopdata.LoopProcessor.get_trend(
            cname, pkt, accum2, converter, time_delta, loop_frequency)
        self.assertIsNone(v2)  # only one reading -> firsttime == lasttime

        # Obstype absent from accum -> None.
        cname_missing = user.loopdata.LoopData.parse_cname('trend.outTemp.raw')
        v3, _, _ = user.loopdata.LoopProcessor.get_trend(
            cname_missing, pkt, accum, converter, time_delta, loop_frequency)
        self.assertIsNone(v3)

    def test_period_accum_wrappers_use_correct_spans(self) -> None:
        # Pins the span construction in each create_<period>_accum wrapper: the
        # returned accumulator's timespan must equal the corresponding weeutil
        # span function for the same pkt_time.  weeutil is the spec for what
        # each period's span IS; a wrapper calling the wrong span function (or
        # wrong args) produces a mismatched timespan and is caught.
        import weewx.units, weewx.manager
        US = weewx.units.unit_constants['US']

        # pkt_time: 2020-07-01 12:00:00 PDT (a fixed, unambiguous instant).
        pkt_time = 1593630000
        week_start = 6
        rainyear_start = 1
        archive_interval = 5

        tmpdir = tempfile.mkdtemp()
        dbm = None
        try:
            db_dict = {'database_name': os.path.join(tmpdir, 'test.sdb'),
                       'driver': 'weedb.sqlite'}
            dbm = weewx.manager.DaySummaryManager.open_with_create(
                db_dict, table_name='archive', schema=wview_extended_schema)

            day_accum = weewx.accum.Accum(
                weeutil.weeutil.archiveDaySpan(pkt_time), US)
            day_accum.addRecord(
                {'dateTime': pkt_time, 'usUnits': 1, 'outTemp': 70.0}, weight=300)
            obstypes = {'outTemp'}

            def span_of(accum):
                return (accum.timespan.start, accum.timespan.stop)

            # year
            accum, _ = user.loopdata.LoopData.create_year_accum(
                US, archive_interval, obstypes, pkt_time, day_accum, dbm)
            self.assertEqual(span_of(accum),
                (weeutil.weeutil.archiveYearSpan(pkt_time).start,
                 weeutil.weeutil.archiveYearSpan(pkt_time).stop))
            # month
            accum, _ = user.loopdata.LoopData.create_month_accum(
                US, archive_interval, obstypes, pkt_time, day_accum, dbm)
            self.assertEqual(span_of(accum),
                (weeutil.weeutil.archiveMonthSpan(pkt_time).start,
                 weeutil.weeutil.archiveMonthSpan(pkt_time).stop))
            # week
            accum, _ = user.loopdata.LoopData.create_week_accum(
                US, archive_interval, obstypes, pkt_time, week_start, day_accum, dbm)
            self.assertEqual(span_of(accum),
                (weeutil.weeutil.archiveWeekSpan(pkt_time, week_start).start,
                 weeutil.weeutil.archiveWeekSpan(pkt_time, week_start).stop))
            # rainyear
            accum, _ = user.loopdata.LoopData.create_rainyear_accum(
                US, archive_interval, obstypes, pkt_time, rainyear_start, day_accum, dbm)
            self.assertEqual(span_of(accum),
                (weeutil.weeutil.archiveRainYearSpan(pkt_time, rainyear_start).start,
                 weeutil.weeutil.archiveRainYearSpan(pkt_time, rainyear_start).stop))
            # hour
            accum, _ = user.loopdata.LoopData.create_hour_accum(
                US, archive_interval, obstypes, pkt_time, day_accum, dbm)
            self.assertEqual(span_of(accum),
                (weeutil.weeutil.archiveHoursAgoSpan(pkt_time).start,
                 weeutil.weeutil.archiveHoursAgoSpan(pkt_time).stop))
            # alltime (fixed literal span)
            accum, _ = user.loopdata.LoopData.create_alltime_accum(
                US, archive_interval, obstypes, day_accum, dbm)
            self.assertEqual(span_of(accum), (86400, 17514144000))
        finally:
            if dbm is not None:
                dbm.close()
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_period_accum_wrappers_distinct_spans(self) -> None:
        # Reinforces the above: the five dated periods must produce DIFFERENT
        # spans from each other for the same pkt_time, so a wrapper calling
        # another period's span function (e.g. year calling month) is caught
        # even if the absolute-equality check above were somehow satisfied.
        pkt_time = 1593630000
        spans = {
            'year': weeutil.weeutil.archiveYearSpan(pkt_time),
            'month': weeutil.weeutil.archiveMonthSpan(pkt_time),
            'week': weeutil.weeutil.archiveWeekSpan(pkt_time, 6),
            'hour': weeutil.weeutil.archiveHoursAgoSpan(pkt_time),
        }
        pairs = [(a.start, a.stop) for a in spans.values()]
        self.assertEqual(len(set(pairs)), len(pairs), msg='period spans not all distinct')

    def test_continuous_firstlast_accum_empty(self) -> None:
        # A fresh (empty) ContinuousFirstLastAccum must return None from every
        # accessor rather than indexing an empty list.  This pins the empty
        # guards on first/firsttime/last/lasttime and getStatsTuple -- the
        # guards added specifically to prevent the IndexError that an unguarded
        # getStatsTuple would raise.
        fl = user.loopdata.ContinuousFirstLastAccum(timelength=100)
        self.assertIsNone(fl.first)
        self.assertIsNone(fl.firsttime)
        self.assertIsNone(fl.last)
        self.assertIsNone(fl.lasttime)
        self.assertEqual(fl.getStatsTuple(), (None, None, None, None))

    def test_firstlast_obstype_end_to_end(self) -> None:
        # Exercises the newly-implemented firstlast support end to end, mirroring
        # weewx's own test_Accum_with_string approach: register a string obstype
        # as a firstlast accumulator, feed it through a ContinuousAccum, then
        # dispatch first/last/firsttime/lasttime through add_period_obstype.
        # Verifies (1) type preservation (no str() coercion) and (2) correct
        # first/last selection.
        import weewx.units, weewx.accum
        US = weewx.units.unit_constants['US']
        converter = weewx.units.Converter(weewx.units.USUnits)
        formatter = weewx.units.Formatter()

        # Register a firstlast string obstype (cleaned up in finally).
        weewx.accum.accum_dict.extend(
            {'stringType': {'accumulator': 'firstlast', 'extractor': 'last'}})
        try:
            accum = user.loopdata.ContinuousAccum(100000, US)
            accum.addRecord({'dateTime': 1000, 'usUnits': 1, 'stringType': 'alpha'})
            accum.addRecord({'dateTime': 1500, 'usUnits': 1, 'stringType': 'beta'})
            accum.addRecord({'dateTime': 2000, 'usUnits': 1, 'stringType': 'gamma'})

            self.assertIn('stringType', accum)
            stats = accum['stringType']
            self.assertIsInstance(stats, user.loopdata.ContinuousFirstLastAccum)
            # Type preserved (strings stored as-is, not via str() of something).
            self.assertEqual(stats.first, 'alpha')
            self.assertEqual(stats.last, 'gamma')
            self.assertEqual(stats.firsttime, 1000)
            self.assertEqual(stats.lasttime, 2000)

            def field(agg):
                cname = user.loopdata.LoopData.parse_cname('day.stringType.%s' % agg)
                self.assertIsNotNone(cname, msg='parse failed for %s' % agg)
                out = {}
                user.loopdata.LoopProcessor.add_period_obstype(
                    cname, accum, out, converter, formatter)
                return out.get(cname.field)

            # first/last emit the string value as-is (string bypass).
            self.assertEqual(field('first'), 'alpha')
            self.assertEqual(field('last'), 'gamma')
            # firsttime/lasttime are timestamps (numeric, routed/formatted).
            # The raw form pins the exact value.
            cname_ft = user.loopdata.LoopData.parse_cname('day.stringType.firsttime.raw')
            out_ft = {}
            user.loopdata.LoopProcessor.add_period_obstype(
                cname_ft, accum, out_ft, converter, formatter)
            self.assertEqual(out_ft.get('day.stringType.firsttime.raw'), 1000)

            cname_lt = user.loopdata.LoopData.parse_cname('day.stringType.lasttime.raw')
            out_lt = {}
            user.loopdata.LoopProcessor.add_period_obstype(
                cname_lt, accum, out_lt, converter, formatter)
            self.assertEqual(out_lt.get('day.stringType.lasttime.raw'), 2000)

            # Rolling-window correctness: as the oldest entries expire off the
            # front, 'first' must advance to the next survivor (this is why the
            # full values_list is kept, not just two endpoints).
            # Nothing expired yet at a ts well within the window:
            stats.trimExpiredEntries(50000)
            self.assertEqual(stats.first, 'alpha')
            self.assertEqual(stats.last, 'gamma')
            # Expire alpha (added at 1000, timelength 100000): the FirstLast trim
            # condition is dateTime + timelength <= ts, so ts = 1000 + 100000
            # ages alpha out and 'first' advances to beta.
            stats.trimExpiredEntries(1000 + 100000)
            self.assertEqual(stats.first, 'beta')
            self.assertEqual(stats.firsttime, 1500)
            self.assertEqual(stats.last, 'gamma')   # last unchanged
        finally:
            # Remove the synthetic obstype so other tests are unaffected.
            # accum_dict is a ChainMap; the entry added by extend() may live in
            # a layer that does not support del by key, so guard it.
            try:
                maps = getattr(weewx.accum.accum_dict, 'maps', [weewx.accum.accum_dict])
                for m in maps:
                    if 'stringType' in m:
                        del m['stringType']
            except Exception:
                pass

    def test_continuous_firstlast_accum_basic(self) -> None:
        # ContinuousFirstLastAccum: collects first/last string observations over
        # a rolling window.  (Note: this accumulator type is registered but the
        # 'firstlast' agg is not currently surfaced through add_period_obstype;
        # this exercises the class directly.)
        FL = user.loopdata.ContinuousFirstLastAccum

        fl = FL(timelength=120)
        # None is skipped by addSum.
        fl.addSum(100, None)
        # First real value.
        fl.addSum(110, 'alpha')
        fl.addSum(120, 'omega')
        # getStatsTuple -> (first_value, first_time, last_value, last_time).
        self.assertEqual(fl.getStatsTuple(), ('alpha', 110, 'omega', 120))

        # trimExpiredEntries removes entries whose dateTime + timelength <= ts.
        # 'alpha' (110) expires once ts >= 230; 'omega' (120) once ts >= 240.
        fl.trimExpiredEntries(235)
        self.assertEqual(fl.getStatsTuple(), ('omega', 120, 'omega', 120))

        # Exact-boundary: trimming at exactly omega's expiration (120+120=240)
        # must remove it (the condition is dateTime + timelength <= ts), leaving
        # the values_list empty.  (getStatsTuple has no empty guard and the
        # 'firstlast' agg is not surfaced in production, so we check the list
        # directly rather than calling getStatsTuple on an empty accumulator.)
        fl.trimExpiredEntries(240)
        self.assertEqual(len(fl.values_list), 0)

    def test_continuous_vec_dir_wraparound_and_zero_vector(self) -> None:
        # Pins two vec_dir branches:
        #  (1) the negative-angle wraparound (_result < 0 -> += 360);
        #  (2) the zero-vector fallback: when xsum == ysum == 0 (vectors cancel)
        #      but dirsumtime > 0, vec_dir returns the LAST known direction.
        CV = user.loopdata.ContinuousVecStats

        # (1) A vector pointing up-and-left (NW quadrant in math axes) yields
        # 90 - atan2 > 90 deg negative, triggering the += 360 wraparound.
        # Two NW observations so the resultant is unambiguous.
        v = CV(timelength=1000)
        v.addSum(100, (10.0, 315.0), weight=2)   # NW
        v.addSum(200, (10.0, 315.0), weight=2)   # NW again
        # 315 deg compass -> vec_dir should report ~315 (wraparound applied).
        self.assertAlmostEqual(v.vec_dir, 315.0, places=4)

        # (2) Calm wind (speed 0 with a direction) credits dirsumtime but
        # contributes EXACTLY zero to xsum/ysum (unlike opposing non-zero
        # vectors, which leave floating-point dust and miss this branch).  With
        # xsum == ysum == 0 and dirsumtime > 0, vec_dir falls back to the last
        # known direction.
        v2 = CV(timelength=1000)
        v2.addSum(100, (0.0, 123.0), weight=2)   # calm, direction 123
        self.assertEqual(v2.xsum, 0.0)           # exactly zero
        self.assertEqual(v2.ysum, 0.0)
        self.assertEqual(v2.vec_dir, 123.0)      # last known direction

        # (3) A due-NORTH vector pins two more vec_dir branches:
        #   - line 485 (_result < 0): North gives _result == 0.0 exactly, where
        #     '<' yields no wraparound (vec_dir 0.0) but a mutated '<=' would
        #     wrap to 360.0;
        #   - line 483 (ysum or xsum): North has ysum != 0 but xsum ~ 0, so
        #     'ysum or xsum' is truthy while a mutated 'ysum and xsum' is falsy
        #     (which would wrongly take the fallback).
        v3 = CV(timelength=1000)
        v3.addSum(100, (10.0, 0.0), weight=2)    # North
        v3.addSum(200, (10.0, 0.0), weight=2)    # North
        self.assertEqual(v3.vec_dir, 0.0)        # NOT 360.0, NOT the fallback

        # (4) line 483: 'if dirsumtime and (ysum or xsum)'.  To distinguish the
        # 'or' from a mutated 'and', exactly ONE of xsum/ysum must be zero and
        # the other non-zero.  Real compass vectors leave cos/sin dust, so set
        # the internal state directly: xsum exactly 0, ysum non-zero.  With
        # 'or' the guard is truthy (compute a direction); with 'and' it would be
        # falsy (wrongly take the fallback).
        v4 = CV(timelength=1000)
        v4.addSum(100, (10.0, 45.0), weight=2)   # establish a 'last' direction
        v4.xsum = 0.0
        v4.ysum = 40.0                           # points due North in math axes
        # dirsumtime is already > 0 from the addSum above.
        self.assertEqual(v4.vec_dir, 0.0)        # 90 - atan2(40,0)=90 -> 0; not fallback (45)

    def test_continuous_vec_ysum_separation(self) -> None:
        # Pins the ysum credit (addSum) and debit (trim) independently of xsum,
        # using a SE vector (dir 135) whose x and y components are equal in
        # magnitude but OPPOSITE in sign -- so a mutation that corrupts only the
        # sin (y) term produces a ysum distinguishable from xsum.
        CV = user.loopdata.ContinuousVecStats

        v = CV(timelength=100)
        v.addSum(100, (10.0, 135.0), weight=2)   # SE: xsum=+14.142, ysum=-14.142
        self.assertAlmostEqual(v.xsum, 14.142135623730951, places=6)
        self.assertAlmostEqual(v.ysum, -14.142135623730951, places=6)

        # Trim it (expiration 200) -> both return to zero.
        v.trimExpiredEntries(200)
        self.assertAlmostEqual(v.xsum, 0.0, places=6)
        self.assertAlmostEqual(v.ysum, 0.0, places=6)

    def test_continuous_accum_units_and_wind(self) -> None:
        # Pins ContinuousAccum unit-system handling and the wind-component skip.
        CA = user.loopdata.ContinuousAccum

        # __init__ with no unit system -> isEmpty True; _check_units adopts the
        # first system, then raises on a mismatch.
        acc = CA(timelength=120)
        self.assertTrue(acc.isEmpty)             # unit_system is None
        acc._check_units(1)                      # adopt US
        self.assertFalse(acc.isEmpty)
        self.assertEqual(acc.unit_system, 1)
        acc._check_units(1)                      # matching -> no raise
        with self.assertRaises(ValueError):
            acc._check_units(16)                 # mismatch (METRIC) -> raise

        # add_wind_value: windDir/windGust/windGustDir are ALL skipped (return
        # early); only windSpeed creates the 'wind' vector accumulator.  Testing
        # all three pins the membership list (removing any element would let
        # that component through).
        acc2 = CA(timelength=120)
        rec = {'dateTime': 1000, 'usUnits': 1, 'windSpeed': 5.0, 'windDir': 90.0,
               'windGust': 8.0, 'windGustDir': 95.0}
        for skipped in ('windDir', 'windGust', 'windGustDir'):
            acc2.add_wind_value(rec, skipped, 1)
            self.assertNotIn('wind', acc2)       # none of these create 'wind'
            self.assertNotIn(skipped, acc2)
        acc2.add_wind_value(rec, 'windSpeed', 1) # processed -> 'wind' + 'windSpeed'
        self.assertIn('wind', acc2)
        self.assertIn('windSpeed', acc2)

    def test_get_trend_guard_branches(self) -> None:
        # get_trend has three early-return guards that fire BEFORE any unit
        # conversion (so converter can be None here):
        #   1. obstype not present in the accumulator
        #   2. first/last is None (empty accumulator)
        #   3. firsttime == lasttime (only one reading -> no trend)
        # Each must return (None, None, None).
        LP = user.loopdata.LoopProcessor
        CA = user.loopdata.ContinuousAccum
        CS = user.loopdata.ContinuousScalarStats

        cname = user.loopdata.LoopData.parse_cname('trend.outTemp')
        self.assertIsNotNone(cname)
        pkt = {'dateTime': 1000, 'usUnits': 1, 'outTemp': 50.0}

        # --- Guard 1: obstype absent from the accumulator. ---
        empty_accum = CA(timelength=10800)
        result = LP.get_trend(cname, pkt, empty_accum, None, 10800, 2.0)
        self.assertEqual(result, (None, None, None))

        # --- Guard 2: obstype present but accumulator empty (first is None). ---
        accum = CA(timelength=10800)
        accum['outTemp'] = CS(timelength=10800)
        result = LP.get_trend(cname, pkt, accum, None, 10800, 2.0)
        self.assertEqual(result, (None, None, None))

        # --- Guard 3: exactly one reading -> firsttime == lasttime. ---
        accum2 = CA(timelength=10800)
        stats = CS(timelength=10800)
        stats.addSum(1000, 50.0, weight=2)
        accum2['outTemp'] = stats
        result = LP.get_trend(cname, pkt, accum2, None, 10800, 2.0)
        self.assertEqual(result, (None, None, None))

    def test_day_wind_vecdir_loop_vs_quantized_archive(self) -> None:
        # Document WHY loopdata's day.wind.vecdir legitimately differs from the
        # WeeWX report's day wind direction on a Davis VP2 using hardware
        # record generation.
        #
        # loopdata vector-averages full-resolution LOOP packets.  The report
        # aggregates ARCHIVE records whose windDir is a single value per
        # interval, QUANTIZED to one of 16 compass points (22.5 deg) -- a
        # documented property of the Davis archive record (Davis spec: wind
        # direction display resolution is 16 points / 22.5 deg on the compass
        # rose).  These are different inputs, so the two day-level vecdir
        # values legitimately differ; neither is "wrong".  loopdata's is the
        # higher-resolution vector direction.
        #
        # IMPORTANT (scope): This test models ONLY the 22.5-degree quantization
        # of the archive direction, which is documented fact.  It does NOT
        # replicate the console's bin-SELECTION algorithm, which Davis has
        # never published and for which community descriptions conflict
        # (sample-count "mode" vs speed-weighted).  To stay independent of that
        # unresolved question, every archive interval below contains samples of
        # a SINGLE true direction, so count-mode and speed-weighted selection
        # necessarily pick the same bin -- the snapped archive direction is
        # unambiguous under either theory.
        #
        # Construction: four intervals whose true directions each sit 10 deg
        # clockwise of a compass point, so each snaps the same rotational way
        # (-10 deg).  The bias therefore accumulates rather than cancels, and
        # the day-level divergence is a clean 10 deg:
        #     true 10.0 -> archive 0.0    (N)
        #     true 32.5 -> archive 22.5   (NNE)
        #     true 55.0 -> archive 45.0   (NE)
        #     true 77.5 -> archive 67.5   (ENE)
        # loopdata (full-res vector avg) -> 43.75 deg
        # archive  (quantized per-interval) -> 33.75 deg
        # vecavg is identical in both paths (a control: this is a direction
        # effect, not a speed effect).
        import math

        speed = 10.0
        true_dirs = [10.0, 32.5, 55.0, 77.5]
        compass_points = [i * 22.5 for i in range(16)]

        def snap_to_compass(deg):
            deg = deg % 360.0
            return min(compass_points,
                       key=lambda c: min(abs(deg - c), 360.0 - abs(deg - c)))

        def vecdir_vecavg(samples, weight):
            # samples: list of (speed, dirN)
            xsum = ysum = sumtime = 0.0
            for s, d in samples:
                theta = math.radians(90.0 - d)
                xsum += weight * s * math.cos(theta)
                ysum += weight * s * math.sin(theta)
                sumtime += weight
            vd = 90.0 - math.degrees(math.atan2(ysum, xsum))
            if vd < 0.0:
                vd += 360.0
            return vd, math.sqrt(xsum ** 2 + ysum ** 2) / sumtime

        # Build loop packets: 3 per interval, all within a single local day,
        # spaced 2s apart (base chosen at midday so no timezone straddles a day
        # boundary over the 22-second span).
        base = 1665838800  # 2022-10-15 13:00:00 UTC -> daytime across US zones
        pkts: List[Dict[str, Any]] = []
        ts = base
        for d in true_dirs:
            for _ in range(3):
                pkts.append({'dateTime': ts, 'usUnits': 1,
                             'windDir': d, 'windGust': speed,
                             'windGustDir': d, 'windrun': None,
                             'windSpeed': speed})
                ts += 2

        wind_fields = ['day.wind.vecdir', 'day.wind.vecdir.raw',
                       'day.wind.vecavg', 'day.wind.vecavg.raw']

        cfg: user.loopdata.Configuration = ProcessPacketTests._get_config('us', 10800, 10, 6, wind_fields)
        weight = cfg.loop_frequency

        accums: user.loopdata.Accumulators = ProcessPacketTests._get_accums(cfg, pkts[0]['dateTime'])
        for pkt in pkts:
            loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

        # (1) loopdata's day vecdir = full-resolution vector average of all
        # loop samples.
        loop_samples = [(speed, d) for d in true_dirs for _ in range(3)]
        expected_loop_vecdir, expected_loop_vecavg = vecdir_vecavg(loop_samples, weight)
        self.assertAlmostEqual(expected_loop_vecdir, 43.75, places=4)
        self.assertAlmostEqual(loopdata_pkt['day.wind.vecdir.raw'], expected_loop_vecdir, places=4)
        self.assertAlmostEqual(loopdata_pkt['day.wind.vecavg.raw'], expected_loop_vecavg, places=4)

        # (2) The report's path: one quantized record per interval.  Computed
        # here as the reference -- loopdata does not produce this; it is what
        # the archive-based report aggregates.
        archive_records = [(speed, snap_to_compass(d)) for d in true_dirs]
        archive_vecdir, archive_vecavg = vecdir_vecavg(archive_records, weight)
        self.assertAlmostEqual(archive_vecdir, 33.75, places=4)

        # (3) The point of the test: the two legitimately diverge in direction
        # (here by a full 10 deg), while vecavg is identical -- confirming the
        # divergence is purely the direction-quantization effect.
        divergence = abs(((expected_loop_vecdir - archive_vecdir) + 180.0) % 360.0 - 180.0)
        self.assertAlmostEqual(divergence, 10.0, places=4)
        self.assertAlmostEqual(expected_loop_vecavg, archive_vecavg, places=6)

    def test_ip100_packet_processing(self) -> None:
        pkts: List[Dict[str, Any]] = ip100_packets.IP100Packets._get_packets()

        cfg: user.loopdata.Configuration = ProcessPacketTests._get_config('us', 10800, 10, 6, ProcessPacketTests._get_specified_fields())

        accums: user.loopdata.Accumulators = ProcessPacketTests._get_accums(cfg, pkts[0]['dateTime'])
        for pkt in pkts:
            loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

        # {'dateTime': 1593883054, 'usUnits': 1, 'outTemp': 71.6, 'barometer': 30.060048358389471, 'dewpoint': 60.48739574937819
        # {'dateTime': 1593883332, 'usUnits': 1, 'outTemp': 72.0, 'barometer': 30.055425865734495, 'dewpoint': 59.57749595318801

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
        self.assertEqual(loopdata_pkt['trend.barometer'], '-0.178 inHg')
        self.assertAlmostEqual(loopdata_pkt['trend.barometer.raw'], -0.1782961, 7)
        self.assertEqual(loopdata_pkt['trend.barometer.formatted'], '-0.178')
        self.assertEqual(loopdata_pkt['trend.barometer.code'], -4)
        self.assertEqual(loopdata_pkt['trend.barometer.desc'], 'Falling Very Rapidly')

        # 72.0 - 71.6
        self.assertEqual(loopdata_pkt['trend.outTemp'], '15.4°F')
        self.assertAlmostEqual(loopdata_pkt['trend.outTemp.raw'],  15.4285714, 7)
        self.assertEqual(loopdata_pkt['trend.outTemp.formatted'], '15.4')

        # 59.57749595318801 - 60.48739574937819
        self.assertEqual(loopdata_pkt['trend.dewpoint'], '-35.1°F')
        self.assertAlmostEqual(loopdata_pkt['trend.dewpoint.raw'], -35.0961350, 7)
        self.assertEqual(loopdata_pkt['trend.dewpoint.formatted'], '-35.1')

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

    def test_ip100_us_packets_to_metric_db_to_us_report_processing(self) -> None:
        pkts: List[Dict[str, Any]] = ip100_packets.IP100Packets._get_packets()

        cfg: user.loopdata.Configuration = ProcessPacketTests._get_config('db-metric.report-us', 10800, 10, 6, ProcessPacketTests._get_specified_fields())

        accums: user.loopdata.Accumulators = ProcessPacketTests._get_accums(cfg, pkts[0]['dateTime'])
        for pkt in pkts:
            loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

        # {'dateTime': 1593883054, 'usUnits': 1, 'outTemp': 71.6, 'barometer': 30.060048358389471, 'dewpoint': 60.48739574937819
        # {'dateTime': 1593883332, 'usUnits': 1, 'outTemp': 72.0, 'barometer': 30.055425865734495, 'dewpoint': 59.57749595318801

        self.assertEqual(loopdata_pkt['unit.label.outTemp'], '°F')

        self.assertEqual(loopdata_pkt['current.dateTime.raw'], 1593883332)
        self.assertEqual(loopdata_pkt['current.dateTime'], '07/04/20 10:22:12')

        self.assertEqual(loopdata_pkt['10m.windGust.max'], '7 mph')
        self.assertEqual(loopdata_pkt['10m.windGust.max.formatted'], '7')
        self.assertAlmostEqual(loopdata_pkt['10m.windGust.max.raw'], 6.5000162, 7)
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
        self.assertEqual(loopdata_pkt['trend.barometer'], '-0.178 inHg')
        self.assertAlmostEqual(loopdata_pkt['trend.barometer.raw'], -0.1782961, 7)
        self.assertEqual(loopdata_pkt['trend.barometer.formatted'], '-0.178')
        self.assertEqual(loopdata_pkt['trend.barometer.code'], -4)
        self.assertEqual(loopdata_pkt['trend.barometer.desc'], 'Falling Very Rapidly')

        # 72.0 - 71.6
        self.assertEqual(loopdata_pkt['trend.outTemp'], '15.4°F')
        self.assertAlmostEqual(loopdata_pkt['trend.outTemp.raw'], 15.4285714, 7)
        self.assertEqual(loopdata_pkt['trend.outTemp.formatted'], '15.4')

        # 59.57749595318801 - 60.48739574937819
        self.assertEqual(loopdata_pkt['trend.dewpoint'], '-35.1°F')
        self.assertAlmostEqual(loopdata_pkt['trend.dewpoint.raw'], -35.0961350, 7)
        self.assertEqual(loopdata_pkt['trend.dewpoint.formatted'], '-35.1')

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

    def test_vantage_pro2_packet_processing(self) -> None:
        pkts: List[Dict[str, Any]] = vantagepro2_packets.VantagePro2Packets._get_batch_one_packets()

        cfg: user.loopdata.Configuration = ProcessPacketTests._get_config('us', 10800, 10, 6, ProcessPacketTests._get_specified_fields())

        accums: user.loopdata.Accumulators = ProcessPacketTests._get_accums(cfg, pkts[0]['dateTime'])

        # Batch One
        for pkt in pkts:
            loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

        # {'altimeter': '29.7797704917861', 'appTemp': '63.987293613602574', 'barometer': '29.777', 'cloudbase': '1277.245423375492', 'consBatteryVoltage': '3.78', 'dateTime': '1665797353', 'dayET': '0.087', 'dayRain': '0.0', 'dewpoint': '56.828520137147834', 'ET': 'None', 'extraAlarm1': '0', 'extraAlarm2': '0', 'extraAlarm3': '0', 'extraAlarm4': '0', 'extraAlarm5': '0', 'extraAlarm6': '0', 'extraAlarm7': '0', 'extraAlarm8': '0', 'extraHumid1': '63.0', 'extraHumid2': '50.0', 'extraTemp1': '72.0', 'extraTemp2': '76.9', 'forecastIcon': '6', 'forecastRule': '190', 'heatindex': '62.194', 'humidex': '68.24125795353245', 'inDewpoint': '58.70188105843598', 'inHumidity': '63.0', 'insideAlarm': '0', 'inTemp': '72.0', 'maxSolarRad': '0.004442804340921485', 'monthET': '1.49', 'monthRain': '0.0', 'outHumidity': '82.0', 'outsideAlarm1': '0', 'outsideAlarm2': '0', 'outTemp': '62.4', 'pm1_0': '4.6875', 'pm2_5': '5.080500000000001', 'pm2_5_aqi': '21', 'pm2_5_aqi_color': '32768', 'pm10_0': '8.0', 'pressure': '29.765246956632804', 'radiation': '5.0', 'rain': '0.0', 'rainAlarm': '0', 'rainRate': '0.0', 'soilLeafAlarm1': '0', 'soilLeafAlarm2': '0', 'soilLeafAlarm3': '0', 'soilLeafAlarm4': '0', 'stormRain': '0.0', 'sunrise': '1665756960', 'sunset': '1665797520', 'txBatteryStatus': '0', 'usUnits': '1', 'UV': '0.0', 'windchill': '62.4', 'windDir': '296.0', 'windGust': '3.0', 'windGustDir': '77.0', 'windrun': 'None', 'windSpeed': '1.0', 'windSpeed10': '2.0', 'yearET': '45.86', 'yearRain': '0.0'}])

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

        self.assertEqual(loopdata_pkt['trend.barometer'], '0.167 inHg')
        self.assertAlmostEqual(loopdata_pkt['trend.barometer.raw'], 0.1670103, 7)
        self.assertEqual(loopdata_pkt['trend.barometer.formatted'], '0.167')
        self.assertEqual(loopdata_pkt['trend.barometer.code'], 3)
        self.assertEqual(loopdata_pkt['trend.barometer.desc'], 'Rising Quickly')

        #  -0.5               (unajdusted)
        # -13.989637305699482 (adjusted)
        self.assertEqual(loopdata_pkt['trend.outTemp'], '-13.9°F')
        self.assertAlmostEqual(loopdata_pkt['trend.outTemp.raw'], -13.9175258, 7)
        self.assertEqual(loopdata_pkt['trend.outTemp.formatted'], '-13.9')

        # seconds = 1665797353 - 1665796967 = 386s
        # 10800 / 386 = 27.9792746114
        # 56.828520137147834 - 56.828520137147834 = 0.19734268078
        # 0.19734268078 * 27.9792746114 = 5.52150505809
        self.assertEqual(loopdata_pkt['trend.dewpoint'], '5.5°F')
        self.assertAlmostEqual(loopdata_pkt['trend.dewpoint.raw'], 5.4930437, 7)
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

        pkts = vantagepro2_packets.VantagePro2Packets._get_batch_two_packets()

        for pkt in pkts:
            loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

        # {'altimeter': '29.77677022521029', 'appTemp': '62.44239660171717', 'barometer': '29.774', 'cloudbase': '1198.7225365853292', 'consBatteryVoltage': '3.78', 'dateTime': '1665797745', 'dayET': '0.087', 'dayRain': '0.0', 'dewpoint': '56.77402083902455', 'ET': 'None', 'extraAlarm1': '0', 'extraAlarm2': '0', 'extraAlarm3': '0', 'extraAlarm4': '0', 'extraAlarm5': '0', 'extraAlarm6': '0', 'extraAlarm7': '0', 'extraAlarm8': '0', 'extraHumid1': '63.0', 'extraHumid2': '50.0', 'extraTemp1': '72.0', 'extraTemp2': '77.0', 'forecastIcon': '6', 'forecastRule': '190', 'heatindex': '61.801', 'humidex': '67.80972824587653', 'inDewpoint': '58.70188105843598', 'inHumidity': '63.0', 'insideAlarm': '0', 'inTemp': '72.0', 'maxSolarRad': '0.0', 'monthET': '1.49', 'monthRain': '0.0', 'outHumidity': '83.0', 'outsideAlarm1': '0', 'outsideAlarm2': '0', 'outTemp': '62.0', 'pm1_0': '5.5', 'pm2_5': '5.0920000000000005', 'pm2_5_aqi': '21', 'pm2_5_aqi_color': '32768', 'pm10_0': '8.5', 'pressure': '29.762248005689198', 'radiation': '0.0', 'rain': '0.0', 'rainAlarm': '0', 'rainRate': '0.0', 'soilLeafAlarm1': '0', 'soilLeafAlarm2': '0', 'soilLeafAlarm3': '0', 'soilLeafAlarm4': '0', 'stormRain': '0.0', 'sunrise': '1665756960', 'sunset': '1665797520', 'txBatteryStatus': '0', 'usUnits': '1', 'UV': '0.0', 'windchill': '62.0', 'windDir': '321.0', 'windGust': '3.0', 'windGustDir': '274.0', 'windrun': 'None', 'windSpeed': '3.0', 'windSpeed10': '1.0', 'yearET': '45.86', 'yearRain': '0.0'}])

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
        self.assertAlmostEqual(loopdata_pkt['trend.barometer.raw'], 0.0415385, 7)
        self.assertEqual(loopdata_pkt['trend.barometer.formatted'], '0.042')
        self.assertEqual(loopdata_pkt['trend.barometer.code'], 1)
        self.assertEqual(loopdata_pkt['trend.barometer.desc'], 'Rising Slowly')

        self.assertEqual(loopdata_pkt['trend.outTemp'], '-12.5°F')
        self.assertAlmostEqual(loopdata_pkt['trend.outTemp.raw'], -12.4615385, 7)
        self.assertEqual(loopdata_pkt['trend.outTemp.formatted'], '-12.5')

        self.assertEqual(loopdata_pkt['trend.dewpoint'], '2.0°F')
        self.assertAlmostEqual(loopdata_pkt['trend.dewpoint.raw'], 1.9778315, 7)
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

    def test_custom_time_delta(self) -> None:
        pkts: List[Dict[str, Any]] = vantagepro2_packets.VantagePro2Packets._get_batch_one_packets()

        cfg: user.loopdata.Configuration = ProcessPacketTests._get_config('us', 600, 10, 6, ProcessPacketTests._get_specified_fields())

        accums: user.loopdata.Accumulators = ProcessPacketTests._get_accums(cfg, pkts[0]['dateTime'])

        # Batch One
        for pkt in pkts:
            loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

        self.assertEqual(loopdata_pkt['trend.barometer'], '0.009 inHg')
        self.assertAlmostEqual(loopdata_pkt['trend.barometer.raw'], 0.0092784, 7)
        self.assertEqual(loopdata_pkt['trend.barometer.formatted'], '0.009')
        self.assertEqual(loopdata_pkt['trend.barometer.code'], 3)
        self.assertEqual(loopdata_pkt['trend.barometer.desc'], 'Rising Quickly')

        self.assertEqual(loopdata_pkt['trend.outTemp'], '-0.8°F')
        self.assertAlmostEqual(loopdata_pkt['trend.outTemp.raw'], -0.7731959, 7)
        self.assertEqual(loopdata_pkt['trend.outTemp.formatted'], '-0.8')

        # seconds = 1665797353 - 1665796967 = 386s
        # 10800 / 386 = 27.9792746114
        # 56.828520137147834 - 56.828520137147834 = 0.19734268078
        # 0.19734268078 * 27.9792746114 = 5.52150505809
        self.assertEqual(loopdata_pkt['trend.dewpoint'], '0.3°F')
        self.assertAlmostEqual(loopdata_pkt['trend.dewpoint.raw'], 0.3051691, 7)
        self.assertEqual(loopdata_pkt['trend.dewpoint.formatted'], '0.3')

        # Batch Two

        pkts = vantagepro2_packets.VantagePro2Packets._get_batch_two_packets()

        for pkt in pkts:
            loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

        self.assertEqual(loopdata_pkt['trend.barometer'], '0.002 inHg')
        self.assertAlmostEqual(loopdata_pkt['trend.barometer.raw'], 0.002, 7)
        self.assertEqual(loopdata_pkt['trend.barometer.formatted'], '0.002')
        self.assertEqual(loopdata_pkt['trend.barometer.code'], 1)
        self.assertEqual(loopdata_pkt['trend.barometer.desc'], 'Rising Slowly')

        self.assertEqual(loopdata_pkt['trend.outTemp'], '-0.7°F')
        self.assertAlmostEqual(loopdata_pkt['trend.outTemp.raw'], -0.7, 7)
        self.assertEqual(loopdata_pkt['trend.outTemp.formatted'], '-0.7')

        self.assertEqual(loopdata_pkt['trend.dewpoint'], '0.3°F')
        self.assertAlmostEqual(loopdata_pkt['trend.dewpoint.raw'], 0.33741600247, 7)
        self.assertEqual(loopdata_pkt['trend.dewpoint.formatted'], '0.3')

    def test_cc3000_packet_processing(self) -> None:
        pkts: List[Dict[str, Any]] = cc3000_packets.CC3000Packets._get_packets()

        cfg: user.loopdata.Configuration = ProcessPacketTests._get_config('us', 10800, 10, 6, ProcessPacketTests._get_specified_fields())

        accums: user.loopdata.Accumulators = ProcessPacketTests._get_accums(cfg, pkts[0]['dateTime'])

        for pkt in pkts:
            loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

        # {'dateTime': 1593975030, 'outTemp': 76.1, 'barometer': 30.014857385736513, 'dewpoint': 54.73645937493746
        # {'dateTime': 1593975366, 'outTemp': 75.4, 'barometer': 30.005222168998216, 'dewpoint': 56.53264564000546

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

        self.assertEqual(loopdata_pkt['trend.barometer'], '-0.308 inHg')
        self.assertAlmostEqual(loopdata_pkt['trend.barometer.raw'], -0.3078708, 7)
        self.assertEqual(loopdata_pkt['trend.barometer.formatted'], '-0.308')
        self.assertEqual(loopdata_pkt['trend.barometer.code'], -4)
        self.assertEqual(loopdata_pkt['trend.barometer.desc'], 'Falling Very Rapidly')

        self.assertEqual(loopdata_pkt['trend.outTemp'], '-22.4°F')
        self.assertAlmostEqual(loopdata_pkt['trend.outTemp.raw'], -22.3668639, 7)
        self.assertEqual(loopdata_pkt['trend.outTemp.formatted'], '-22.4')

        # 56.53264564000546 - 54.73645937493746
        self.assertEqual(loopdata_pkt['trend.dewpoint'], '57.4°F')
        self.assertAlmostEqual(loopdata_pkt['trend.dewpoint.raw'], 57.3929339, 7)
        self.assertEqual(loopdata_pkt['trend.dewpoint.formatted'], '57.4')

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

    def test_cc3000_packet_processing_us_device_us_database_metric_report(self) -> None:

        pkts: List[Dict[str, Any]] = cc3000_packets.CC3000Packets._get_packets()

        cfg: user.loopdata.Configuration = ProcessPacketTests._get_config('db-us.report-metric', 10800, 10, 6, ProcessPacketTests._get_specified_fields())

        accums: user.loopdata.Accumulators = ProcessPacketTests._get_accums(cfg, pkts[0]['dateTime'])

        for pkt in pkts:
            loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

        # {'dateTime': 1593975030, 'outTemp': 76.1, 'barometer': 30.014857385736513, 'dewpoint': 54.73645937493746
        # {'dateTime': 1593975366, 'outTemp': 75.4, 'barometer': 30.005222168998216, 'dewpoint': 56.53264564000546

        self.assertEqual(loopdata_pkt['unit.label.outTemp'], '°C')

        self.assertEqual(loopdata_pkt['current.dateTime.raw'], 1593975366)
        self.assertEqual(loopdata_pkt['current.dateTime'], '07/05/20 11:56:06')

        self.assertEqual(loopdata_pkt['10m.windGust.max'], '12 km/h')
        self.assertEqual(loopdata_pkt['10m.windGust.max.formatted'], '12')
        self.assertAlmostEqual(loopdata_pkt['10m.windGust.max.raw'], 11.5872768, 7)
        self.assertEqual(loopdata_pkt['10m.windGust.maxtime'], '07/05/20 11:50:30')
        self.assertEqual(loopdata_pkt['10m.windGust.maxtime.raw'], 1593975030)

        self.assertEqual(loopdata_pkt['10m.outTemp.max'], '24.6°C')
        self.assertEqual(loopdata_pkt['10m.outTemp.max.formatted'], '24.6')
        self.assertAlmostEqual(loopdata_pkt['10m.outTemp.max.raw'], 24.6111111, 7)
        self.assertEqual(loopdata_pkt['10m.outTemp.maxtime'], '07/05/20 11:50:34')
        self.assertEqual(loopdata_pkt['10m.outTemp.maxtime.raw'], 1593975034)

        self.assertEqual(loopdata_pkt['current.outTemp'], '24.1°C')
        self.assertEqual(loopdata_pkt['current.barometer'], '1016.1 mbar')
        self.assertEqual(loopdata_pkt['current.windSpeed'], '3 km/h')
        self.assertEqual(loopdata_pkt['current.windDir'], '45°')
        self.assertEqual(loopdata_pkt['current.windDir.ordinal_compass'], 'NE')

        # 30.005222168998216 - 30.014857385736513
        self.assertEqual(loopdata_pkt['trend.barometer'], '-10.4 mbar')
        self.assertAlmostEqual(loopdata_pkt['trend.barometer.raw'], -10.4257014, 7)
        self.assertEqual(loopdata_pkt['trend.barometer.formatted'], '-10.4')
        self.assertEqual(loopdata_pkt['trend.barometer.code'], -4)
        self.assertEqual(loopdata_pkt['trend.barometer.desc'], 'Falling Very Rapidly')

        self.assertEqual(loopdata_pkt['trend.outTemp'], '-12.4°C')
        self.assertAlmostEqual(loopdata_pkt['trend.outTemp.raw'], -12.4260355, 7)
        self.assertEqual(loopdata_pkt['trend.outTemp.formatted'], '-12.4')

        # 56.53264564000546 - 54.73645937493746
        self.assertEqual(loopdata_pkt['trend.dewpoint'], '31.9°C')
        self.assertAlmostEqual(loopdata_pkt['trend.dewpoint.raw'], 31.8849633, 7)
        self.assertEqual(loopdata_pkt['trend.dewpoint.formatted'], '31.9')

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

    def test_cc3000_cross_midnight_packet_processing(self) -> None:
        pkts: List[Dict[str, Any]] = cc3000_cross_midnight_packets.CC3000CrossMidnightPackets._get_pre_midnight_packets()

        cfg: user.loopdata.Configuration = ProcessPacketTests._get_config('us', 10800, 10, 6, ProcessPacketTests._get_specified_fields())

        accums: user.loopdata.Accumulators = ProcessPacketTests._get_accums(cfg, pkts[0]['dateTime'])

        # Pre Midnight
        for pkt in pkts:
            loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

        # {'dateTime': 1595487600, 'outTemp': 57.3, 'outHumidity': 89.0, 'pressure': 29.85,

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
        self.assertAlmostEqual(loopdata_pkt['trend.barometer.raw'], 0.0000602, 7)
        self.assertEqual(loopdata_pkt['trend.barometer.formatted'], '0.000')
        self.assertEqual(loopdata_pkt['trend.barometer.code'], 0)
        self.assertEqual(loopdata_pkt['trend.barometer.desc'], 'Steady')

        self.assertEqual(loopdata_pkt['trend.outTemp'], '-1.2°F')
        self.assertAlmostEqual(loopdata_pkt['trend.outTemp.raw'], -1.1973392, 7)
        self.assertEqual(loopdata_pkt['trend.outTemp.formatted'], '-1.2')

        self.assertEqual(loopdata_pkt['trend.dewpoint'], '6.3°F')
        self.assertAlmostEqual(loopdata_pkt['trend.dewpoint.raw'], 6.2685671, 7)
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

        pkts = cc3000_cross_midnight_packets.CC3000CrossMidnightPackets._get_post_midnight_packets()

        for pkt in pkts:
            loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

        # {'dateTime': 1595488500, 'outTemp': 58.2, 'outHumidity': 90.0, 'pressure': 29.85,

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
        self.assertAlmostEqual(loopdata_pkt['trend.barometer.raw'], -0.0002407, 7)
        self.assertEqual(loopdata_pkt['trend.barometer.formatted'], '-0.000')
        self.assertEqual(loopdata_pkt['trend.barometer.code'], 0)
        self.assertEqual(loopdata_pkt['trend.barometer.desc'], 'Steady')

        self.assertEqual(loopdata_pkt['trend.outTemp'], '4.8°F')
        self.assertAlmostEqual(loopdata_pkt['trend.outTemp.raw'], 4.7946726, 7)
        self.assertEqual(loopdata_pkt['trend.outTemp.formatted'], '4.8')

        self.assertEqual(loopdata_pkt['trend.dewpoint'], '10.3°F')
        self.assertAlmostEqual(loopdata_pkt['trend.dewpoint.raw'], 10.2986508, 7)
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

    def test_simulator_packet_processing(self) -> None:
        cfg: user.loopdata.Configuration = ProcessPacketTests._get_config('metric', 10800, 10, 6, ProcessPacketTests._get_specified_fields())

        pkts: List[Dict[str, Any]] = simulator_packets.SimulatorPackets._get_packets()

        accums: user.loopdata.Accumulators = ProcessPacketTests._get_accums(cfg, pkts[0]['dateTime'])

        for pkt in pkts:
            loopdata_pkt: Dict[str, Any] = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

        # {'dateTime': 1593976709, 'outTemp': 0.3770915275499615,  'barometer': 1053.1667173695532, 'dewpoint': -2.6645899102645934
        # {'dateTime': 1593977615, 'outTemp': 0.032246952164187964,'barometer': 1053.1483031344253, 'dewpoint': -3.003421962855377

        self.assertEqual(loopdata_pkt['unit.label.outTemp'], '°C')

        self.assertEqual(loopdata_pkt['current.dateTime.raw'], 1593977615)
        self.assertEqual(loopdata_pkt['current.dateTime'], '07/05/20 12:33:35')

        self.assertEqual(loopdata_pkt['10m.windGust.max'], '0 km/h')
        self.assertEqual(loopdata_pkt['10m.windGust.max.formatted'], '0')
        self.assertAlmostEqual(loopdata_pkt['10m.windGust.max.raw'], 0.0052507, 7)
        self.assertEqual(loopdata_pkt['10m.windGust.maxtime'], '07/05/20 12:33:35')
        self.assertEqual(loopdata_pkt['10m.windGust.maxtime.raw'], 1593977615)

        self.assertEqual(loopdata_pkt['10m.outTemp.max'], '1.4°C')
        self.assertEqual(loopdata_pkt['10m.outTemp.max.formatted'], '1.4')
        self.assertAlmostEqual(loopdata_pkt['10m.outTemp.max.raw'], 1.3578938, 7)
        self.assertEqual(loopdata_pkt['10m.outTemp.maxtime'], '07/05/20 12:33:19')
        self.assertEqual(loopdata_pkt['10m.outTemp.maxtime.raw'], 1593977599)

        self.assertEqual(loopdata_pkt['current.outTemp'], '0.0°C')
        self.assertEqual(loopdata_pkt['current.barometer'], '1053.1 mbar')
        self.assertEqual(loopdata_pkt['current.windSpeed'], '0 km/h')
        self.assertEqual(loopdata_pkt['current.windDir'], '360°')
        self.assertEqual(loopdata_pkt['current.windDir.ordinal_compass'], 'N')

        # 1053.1483031344253 - 1053.1667173695532
        self.assertEqual(loopdata_pkt['trend.barometer'], '-0.2 mbar')
        self.assertAlmostEqual(loopdata_pkt['trend.barometer.raw'], -0.2190239, 7)
        self.assertEqual(loopdata_pkt['trend.barometer.formatted'], '-0.2')
        self.assertEqual(loopdata_pkt['trend.barometer.code'], -1)
        self.assertEqual(loopdata_pkt['trend.barometer.desc'], 'Falling Slowly')

        # 0.032246952164187964 - 0.3770915275499615
        self.assertEqual(loopdata_pkt['trend.outTemp'], '-4.1°C')
        self.assertAlmostEqual(loopdata_pkt['trend.outTemp.raw'], -4.1016756, 7)
        self.assertEqual(loopdata_pkt['trend.outTemp.formatted'], '-4.1')

        # -3.003421962855377 - -2.6645899102645934
        self.assertEqual(loopdata_pkt['trend.dewpoint'], '-4.0°C')
        self.assertAlmostEqual(loopdata_pkt['trend.dewpoint.raw'], -4.0301610, 7)
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

    def test_various_minute_and_hour_tags(self) -> None:
        specified_fields = [ 'current.outTemp', 'trend.outTemp',
                             '1m.rain.sum', '1m.outTemp.max', '1m.outTemp.min', '1m.outTemp.avg',
                             '7m.rain.sum', '7m.outTemp.max', '7m.outTemp.min', '7m.outTemp.avg',
                             '13h.rain.sum', '13h.outTemp.max', '13h.outTemp.min', '13h.outTemp.avg']
        cfg: user.loopdata.Configuration = ProcessPacketTests._get_config('us', 10800, 1, 6, specified_fields)

        # July 1, 2020 Noon PDT
        pkt: Dict[str, Any] = {'dateTime': 1593630000, 'usUnits': 1, 'outTemp': 77.4, 'rain': 0.01}

        accums: user.loopdata.Accumulators = ProcessPacketTests._get_accums(cfg, pkt['dateTime'])

        # First packet.
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

        # Next packet 1 minute later
        pkt = {'dateTime': 1593630060, 'usUnits': 1, 'outTemp': 77.3, 'rain': 0.0}
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

        self.assertEqual(loopdata_pkt['current.outTemp'], '77.3°F')

        # first packet dropped off for 1m.
        self.assertEqual(loopdata_pkt['1m.outTemp.max'], '77.3°F')
        self.assertEqual(loopdata_pkt['1m.outTemp.min'], '77.3°F')
        self.assertEqual(loopdata_pkt['1m.outTemp.avg'], '77.3°F')
        self.assertEqual(loopdata_pkt['1m.rain.sum'], '0.00 in')
        self.assertEqual(loopdata_pkt['1m.outTemp.avg'], '77.3°F')

        self.assertEqual(loopdata_pkt['7m.outTemp.max'], '77.4°F')
        self.assertEqual(loopdata_pkt['7m.outTemp.min'], '77.3°F')
        self.assertEqual(loopdata_pkt['7m.outTemp.avg'], '77.3°F')
        self.assertEqual(loopdata_pkt['7m.rain.sum'], '0.01 in')

        self.assertEqual(loopdata_pkt['13h.rain.sum'], '0.01 in')
        self.assertEqual(loopdata_pkt['13h.outTemp.max'], '77.4°F')
        self.assertEqual(loopdata_pkt['13h.outTemp.min'], '77.3°F')
        self.assertEqual(loopdata_pkt['13h.outTemp.avg'], '77.3°F')

        # Next packet 58s later
        pkt = {'dateTime': 1593630118, 'usUnits': 1, 'outTemp': 77.2, 'rain': 0.00}
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

        self.assertEqual(loopdata_pkt['1m.outTemp.max'], '77.3°F')
        self.assertEqual(loopdata_pkt['1m.outTemp.min'], '77.2°F')
        self.assertEqual(loopdata_pkt['1m.outTemp.avg'], '77.2°F')
        self.assertEqual(loopdata_pkt['1m.rain.sum'], '0.00 in')

        self.assertEqual(loopdata_pkt['7m.outTemp.max'], '77.4°F')
        self.assertEqual(loopdata_pkt['7m.outTemp.min'], '77.2°F')
        self.assertEqual(loopdata_pkt['7m.outTemp.avg'], '77.3°F')
        self.assertEqual(loopdata_pkt['7m.rain.sum'], '0.01 in')

        self.assertEqual(loopdata_pkt['13h.rain.sum'], '0.01 in')
        self.assertEqual(loopdata_pkt['13h.outTemp.max'], '77.4°F')
        self.assertEqual(loopdata_pkt['13h.outTemp.min'], '77.2°F')
        self.assertEqual(loopdata_pkt['13h.outTemp.avg'], '77.3°F')

        # Next packet 2s later
        pkt = {'dateTime': 1593630120, 'usUnits': 1, 'outTemp': 77.2, 'rain': 0.00}
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

        self.assertEqual(loopdata_pkt['1m.outTemp.max'], '77.2°F')
        self.assertEqual(loopdata_pkt['1m.outTemp.min'], '77.2°F')
        self.assertEqual(loopdata_pkt['1m.outTemp.avg'], '77.2°F')
        self.assertEqual(loopdata_pkt['1m.rain.sum'], '0.00 in')

        self.assertEqual(loopdata_pkt['7m.outTemp.max'], '77.4°F')
        self.assertEqual(loopdata_pkt['7m.outTemp.min'], '77.2°F')
        self.assertEqual(loopdata_pkt['7m.outTemp.avg'], '77.3°F')
        self.assertEqual(loopdata_pkt['7m.rain.sum'], '0.01 in')

        self.assertEqual(loopdata_pkt['13h.rain.sum'], '0.01 in')
        self.assertEqual(loopdata_pkt['13h.outTemp.max'], '77.4°F')
        self.assertEqual(loopdata_pkt['13h.outTemp.min'], '77.2°F')
        self.assertEqual(loopdata_pkt['13h.outTemp.avg'], '77.3°F')

        # Next packet 9 minutes later
        pkt = {'dateTime': 1593630600, 'usUnits': 1, 'outTemp': 77.2, 'rain': 0.01}
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

        self.assertEqual(loopdata_pkt['current.outTemp'], '77.2°F')

        self.assertEqual(loopdata_pkt['1m.outTemp.max'], '77.2°F')
        self.assertEqual(loopdata_pkt['1m.outTemp.min'], '77.2°F')
        self.assertEqual(loopdata_pkt['1m.outTemp.avg'], '77.2°F')
        self.assertEqual(loopdata_pkt['1m.rain.sum'], '0.01 in')

        # Everything but this packet has dropped of for 7m tag.
        # Previous max should have dropped off of 7m.
        self.assertEqual(loopdata_pkt['7m.outTemp.max'], '77.2°F')
        self.assertEqual(loopdata_pkt['7m.outTemp.min'], '77.2°F')
        self.assertEqual(loopdata_pkt['7m.outTemp.avg'], '77.2°F')
        self.assertEqual(loopdata_pkt['7m.rain.sum'], '0.01 in')

        # 13h
        self.assertEqual(loopdata_pkt['13h.outTemp.max'], '77.4°F')
        self.assertEqual(loopdata_pkt['13h.outTemp.min'], '77.2°F')
        self.assertEqual(loopdata_pkt['13h.outTemp.avg'], '77.3°F')
        self.assertEqual(loopdata_pkt['13h.rain.sum'], '0.02 in')

        # Next packet 13 hours later
        pkt = {'dateTime': 1593677400, 'usUnits': 1, 'outTemp': 75.0, 'rain': 0.01}
        loopdata_pkt = user.loopdata.LoopProcessor.generate_loopdata_dictionary(pkt, cfg, accums)

        self.assertEqual(loopdata_pkt['current.outTemp'], '75.0°F')
        self.assertEqual(loopdata_pkt['7m.outTemp.max'], '75.0°F')
        self.assertEqual(loopdata_pkt['7m.outTemp.min'], '75.0°F')
        self.assertEqual(loopdata_pkt['13h.outTemp.max'], '75.0°F')
        self.assertEqual(loopdata_pkt['13h.outTemp.min'], '75.0°F')
        self.assertEqual(loopdata_pkt['13h.rain.sum'], '0.01 in')

    @staticmethod
    def _get_accums(cfg: user.loopdata.Configuration, pkt_time) -> user.loopdata.Accumulators:
        """
        Returns an Accumulators instance.
        """
        accums = user.loopdata.Accumulators(
            alltime_accum         = None,
            rainyear_accum        = None,
            year_accum            = None,
            month_accum           = None,
            week_accum            = None,
            day_accum             = weewx.accum.Accum(weeutil.weeutil.archiveDaySpan(pkt_time), cfg.unit_system),
            hour_accum            = None,
            continuous            = {})
        # Only make accums if there are obstypes for the period.
        if cfg.obstypes.alltime is not None:
            accums.alltime_accum = weewx.accum.Accum(weeutil.weeutil.TimeSpan(86400, 17514144000), cfg.unit_system)
        if cfg.obstypes.rainyear is not None:
            accums.rainyear_accum        = weewx.accum.Accum(weeutil.weeutil.archiveRainYearSpan(pkt_time, cfg.rainyear_start), cfg.unit_system)
        if cfg.obstypes.year is not None:
            accums.year_accum            = weewx.accum.Accum(weeutil.weeutil.archiveYearSpan(pkt_time), cfg.unit_system)
        if cfg.obstypes.month is not None:
            accums.month_accum           = weewx.accum.Accum(weeutil.weeutil.archiveMonthSpan(pkt_time), cfg.unit_system)
        if cfg.obstypes.week is not None:
            accums.week_accum            = weewx.accum.Accum(weeutil.weeutil.archiveWeekSpan(pkt_time, cfg.week_start), cfg.unit_system)
        if cfg.obstypes.hour is not None:
            accums.hour_accum            = weewx.accum.Accum(weeutil.weeutil.archiveHoursAgoSpan(pkt_time), cfg.unit_system)
        # Make continous accums if there are matching continous obs types.
        for per in cfg.obstypes.continuous:
            if per == 'trend':
                timelength = cfg.time_delta
            elif user.loopdata.LoopData.is_hour_period(per):
                timelength = int(per[:-1])*3600
            elif user.loopdata.LoopData.is_minute_period(per):
                timelength = int(per[:-1])*60
            accums.continuous[per]   = user.loopdata.ContinuousAccum(timelength, cfg.unit_system)

        return accums

    @staticmethod
    def _get_config(config_dict_kind, time_delta, rainyear_start, week_start, specified_fields) -> user.loopdata.Configuration:
        os.environ['TZ'] = 'America/Los_Angeles'
        config_dict: Dict[str, Any] = configobj.ConfigObj('bin/user/tests/weewx.conf.%s' % config_dict_kind, encoding='utf-8')
        unit_system: int = weewx.units.unit_constants[config_dict['StdConvert'].get('target_unit', 'US').upper()]
        std_archive_dict: Dict[str, Any] = config_dict.get('StdArchive', {})
        (fields_to_include, obstypes) = user.loopdata.LoopData.get_fields_to_include(specified_fields)

        # Get converter and formatter from SeasonsReport.
        target_report_dict: Dict[str, Any] = user.loopdata.LoopData.get_target_report_dict(config_dict, 'SeasonsReport')
        converter: weewx.units.Converter = weewx.units.Converter.fromSkinDict(target_report_dict)
        assert type(converter) == weewx.units.Converter
        formatter: weewx.units.Formatter = weewx.units.Formatter.fromSkinDict(target_report_dict)
        assert type(formatter) == weewx.units.Formatter

        return user.loopdata.Configuration(
            queue                    = queue.SimpleQueue(), # dummy
            config_dict              = config_dict,
            unit_system              = unit_system,
            archive_interval         = to_int(std_archive_dict.get('archive_interval')),
            archive_delay            = to_int(std_archive_dict.get('archive_delay', 15)),
            loop_data_dir            = '', # dummy
            filename                 = '', # dummy
            target_report            = '', # dummy
            loop_frequency           = 2.0,
            specified_fields         = specified_fields,
            fields_to_include        = fields_to_include,
            formatter                = formatter,
            converter                = converter,
            tmpname                  = '', # dummy
            enable                   = True, # dummy
            remote_server            = '', # dummy
            remote_port              = 22, # dummy
            remote_user              = '', # dummy
            remote_dir               = '', # dummy
            compress                 = True, # dummy
            log_success              = False, # dummy
            ssh_options              = '', # dummy
            timeout                  = 1, # dummy
            skip_if_older_than       = 3, # dummy
            time_delta               = time_delta,
            week_start               = week_start,
            rainyear_start           = rainyear_start,
            obstypes                 = obstypes,
            baro_trend_descs         = user.loopdata.LoopData.construct_baro_trend_descs({}))

    @staticmethod
    def _get_specified_fields() -> List[str]:
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

    def test_create_period_accum_from_database(self) -> None:
        # Category 2: exercise create_period_accum against a REAL (temporary)
        # SQLite weewx database -- not a mock.  This covers the day-summary
        # priming path: create_period_accum reads archive_day_<obstype> rows via
        # day_summary_records_generator and merges them with today's day_accum.
        #
        # Fixture: a temp database with archive records spanning two UTC days.
        # The earlier day's records become a day-summary row that the 'week'
        # period accumulator must merge; today's records arrive via day_accum.
        unit_system = weewx.units.unit_constants['US']  # 1

        tmpdir = tempfile.mkdtemp()
        dbm = None
        try:
            db_dict = {
                'database_name': os.path.join(tmpdir, 'test.sdb'),
                'driver': 'weedb.sqlite'}
            dbm = weewx.manager.DaySummaryManager.open_with_create(
                db_dict, table_name='archive', schema=wview_extended_schema)

            # Day 1: 2022-10-14, three records.  Day 2 (today): 2022-10-15.
            day1 = 1665750000   # 2022-10-14 12:20:00 UTC (mid-day)
            day2 = 1665838800   # 2022-10-15 13:00:00 UTC (mid-day)

            day1_temps = [40.0, 50.0, 60.0]
            archive_recs = []
            for i, t in enumerate(day1_temps):
                archive_recs.append({
                    'dateTime': day1 + i * 300, 'usUnits': 1, 'interval': 5,
                    'outTemp': t,
                    'windSpeed': 4.0 + i, 'windDir': 80.0 + i * 10,
                    'windGust': 6.0 + i, 'windGustDir': 85.0 + i * 10})
            dbm.addRecord(archive_recs)

            # Build today's day_accum and populate it with two readings each of
            # a scalar (outTemp) and a vector (wind) obstype.
            day_accum = weewx.accum.Accum(
                weeutil.weeutil.archiveDaySpan(day2), unit_system)
            today_temps = [70.0, 80.0]
            for i, t in enumerate(today_temps):
                day_accum.addRecord(
                    {'dateTime': day2 + i * 300, 'usUnits': 1, 'outTemp': t,
                     'windSpeed': 10.0 + i, 'windDir': 200.0 + i * 10,
                     'windGust': 12.0 + i, 'windGustDir': 205.0 + i * 10},
                    weight=300)

            # Day-summary inclusion follows weewx's DailySummaries spec:
            # start <= dateTime < stop.  The span brackets day1's day-start
            # (included) and stops well after day2, so day1's summary row is in
            # range.  day2 is only in day_accum (not the db), so it is not a
            # day-summary row; it is merged separately below.
            week_span = weeutil.weeutil.TimeSpan(day1 - 86400, day2 + 86400)

            # Request both a scalar and a vector obstype: this exercises the
            # ScalarStats AND VecStats type-dispatch and merge paths.
            accum, valid_obstypes = user.loopdata.LoopData.create_period_accum(
                'week', unit_system, 5, {'outTemp', 'wind'}, week_span, day_accum, dbm)

            self.assertIsNotNone(accum)
            self.assertIn('outTemp', valid_obstypes)
            self.assertIn('wind', valid_obstypes)
            self.assertIn('outTemp', accum)
            self.assertIn('wind', accum)

            # Scalar merge: min from day1 (40), max from today (80), count 3+2=5.
            stats = accum['outTemp']
            self.assertEqual(stats.min, 40.0)   # day1 low
            self.assertEqual(stats.max, 80.0)   # today's high
            self.assertEqual(stats.count, 5)

            # Vector merge: the wind accumulator must be a VecStats spanning both
            # days.  For weewx VecStats, 'min' tracks the lowest windSpeed and
            # 'max' tracks the highest windGust.  Day1 speeds 4,5,6 / gusts 6,7,8;
            # today speeds 10,11 / gusts 12,13.  So min=4 (day1), max=13 (today).
            wind_stats = accum['wind']
            self.assertEqual(type(wind_stats), weewx.accum.VecStats)
            self.assertEqual(wind_stats.count, 5)       # 3 + 2 observations
            self.assertEqual(wind_stats.max, 13.0)      # max windGust (today)
            self.assertEqual(wind_stats.min, 4.0)       # min windSpeed (day1)

        finally:
            if dbm is not None:
                dbm.close()
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_create_period_accum_empty_obstypes(self) -> None:
        # The early-return guard: no obstypes -> (None, empty set), no DB needed.
        accum, valid = user.loopdata.LoopData.create_period_accum(
            'week', 1, 5, set(),
            weeutil.weeutil.TimeSpan(0, 100), None, None)
        self.assertIsNone(accum)
        self.assertEqual(valid, set())

    def test_create_period_accum_day_summary_upper_bound(self) -> None:
        # Concern-#2 / spec test: day-summary aggregation must follow weewx's
        # DailySummaries convention (weewx.xtypes): dateTime >= start AND
        # dateTime < stop -- inclusive left, EXCLUSIVE right.  This test proves
        # the exclusive-right bound by placing the span.stop exactly on a
        # day-summary key: the day-summary row AT span.stop must be EXCLUDED,
        # while the prior day's row is INCLUDED.  The expected inclusion is
        # derived from the spec, NOT from loopdata's SQL.
        unit_system = weewx.units.unit_constants['US']

        tmpdir = tempfile.mkdtemp()
        dbm = None
        try:
            db_dict = {'database_name': os.path.join(tmpdir, 'test.sdb'),
                       'driver': 'weedb.sqlite'}
            dbm = weewx.manager.DaySummaryManager.open_with_create(
                db_dict, table_name='archive', schema=wview_extended_schema)

            # Two days of records.  Day A: 2022-10-13.  Day B: 2022-10-14.
            dayA = 1665663600   # 2022-10-13 12:20:00 UTC
            dayB = 1665750000   # 2022-10-14 12:20:00 UTC
            for d, temp in ((dayA, 40.0), (dayB, 80.0)):
                dbm.addRecord([{'dateTime': d, 'usUnits': 1, 'interval': 5,
                                'outTemp': temp}])

            # Day-summary keys use startOfArchiveDay (the same function
            # DaySummaryManager uses to key archive_day_* rows).
            dayB_key = weeutil.weeutil.startOfArchiveDay(dayB)

            # Set span.stop EXACTLY on dayB's summary key.  Per the spec
            # (dateTime < stop), dayB's row must be excluded; dayA's included.
            span = weeutil.weeutil.TimeSpan(weeutil.weeutil.startOfArchiveDay(dayA), dayB_key)

            # day_accum must CONTAIN outTemp, or create_period_accum skips the
            # obstype entirely (it only processes obstypes present in day_accum).
            # Give it one reading at value 50 -- distinct from dayA (40) and
            # dayB (80) -- so the asserted max proves which day-summary rows
            # were merged.  'today' here is a day AFTER dayB so it is not itself
            # a day-summary row in the db.
            today = dayB + 86400
            day_accum = weewx.accum.Accum(
                weeutil.weeutil.archiveDaySpan(today), unit_system)
            day_accum.addRecord(
                {'dateTime': today, 'usUnits': 1, 'outTemp': 50.0}, weight=300)

            accum, valid = user.loopdata.LoopData.create_period_accum(
                'week', unit_system, 5, {'outTemp'}, span, day_accum, dbm)

            self.assertIsNotNone(accum)
            self.assertIn('outTemp', accum)
            # dayA's summary (1 obs, value 40) is within [start, stop).  dayB's
            # row (value 80), keyed exactly at stop, is EXCLUDED by the
            # exclusive-right bound.  day_accum adds 1 obs (value 50).  So:
            #   count = 1 (dayA) + 1 (day_accum) = 2
            #   max   = 50 (day_accum); NOT 80 -- proving dayB was excluded.
            self.assertEqual(accum['outTemp'].count, 2)
            self.assertEqual(accum['outTemp'].max, 50.0)
            self.assertEqual(accum['outTemp'].min, 40.0)  # dayA's value
        finally:
            if dbm is not None:
                dbm.close()
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_create_hour_accum_from_database(self) -> None:
        # Covers the name=='hour' path of create_period_accum, which primes the
        # hour accumulator from ARCHIVE records.
        #
        # Inclusion spec (derived independently of loopdata's SQL): the hour
        # accumulator is a weewx.accum.Accum(span), so it accepts records per
        # TimeSpan.includesArchiveTime -- start < t <= stop (exclusive left,
        # inclusive right).  archiveHoursAgoSpan(now) returns the current clock
        # hour, so span.stop is in the FUTURE relative to now; the inclusive-
        # right edge is therefore never exercised by real data (nothing is
        # newer than now).  The testable boundary is the EXCLUSIVE LEFT edge: a
        # record at exactly span.start must be EXCLUDED.  The hour path does NOT
        # merge day_accum (unlike longer periods), so count reflects archive
        # records only.
        import time as _time
        unit_system = weewx.units.unit_constants['US']

        tmpdir = tempfile.mkdtemp()
        dbm = None
        try:
            db_dict = {'database_name': os.path.join(tmpdir, 'test.sdb'),
                       'driver': 'weedb.sqlite'}
            dbm = weewx.manager.DaySummaryManager.open_with_create(
                db_dict, table_name='archive', schema=wview_extended_schema)

            now = int(_time.time())
            hour_span = weeutil.weeutil.archiveHoursAgoSpan(now)

            # Records must be both inside the span (dateTime > hour_span.start)
            # AND not future-dated relative to now (the create_period_accum hour
            # path applies the archive_delay future-record rejection).  Because
            # archiveHoursAgoSpan(now) is the CURRENT clock hour, span.stop is in
            # the future and 'now' may be anywhere within the hour -- so anchor
            # the included records to 'now' (always in the past) rather than to
            # span.start (which risks placing them after 'now' early in the
            # hour).  They remain > span.start as long as the test runs at least
            # a few seconds into the clock hour, which is effectively always.
            # (reuse the now sampled above for hour_span)
            # One record EXACTLY at span.start: excluded by the exclusive-left
            # rule (get_archive_packets uses dateTime > earliest_time).
            recs = [{'dateTime': hour_span.start, 'usUnits': 1, 'interval': 5,
                     'outTemp': 99.0}]
            # Three included records a few seconds in the past, distinct and
            # strictly inside (span.start, now].
            for i, off in enumerate((3, 2, 1)):
                recs.append({'dateTime': now - off, 'usUnits': 1,
                             'interval': 5, 'outTemp': 60.0 + i})
            dbm.addRecord(recs)

            # day_accum for today, populated so 'outTemp' dispatches as ScalarStats.
            day_accum = weewx.accum.Accum(
                weeutil.weeutil.archiveDaySpan(now), unit_system)
            day_accum.addRecord(
                {'dateTime': now, 'usUnits': 1, 'outTemp': 65.0}, weight=300)

            accum, valid = user.loopdata.LoopData.create_period_accum(
                'hour', unit_system, 5, {'outTemp'}, hour_span, day_accum, dbm)

            self.assertIsNotNone(accum)
            self.assertIn('outTemp', valid)
            self.assertIn('outTemp', accum)
            # Exactly the 3 in-span records (values 60, 61, 62).  The boundary
            # record at span.start (99.0) is EXCLUDED by the exclusive-left rule;
            # if it leaked in, count would be 4 and max would be 99.
            self.assertEqual(accum['outTemp'].count, 3)
            self.assertEqual(accum['outTemp'].min, 60.0)
            self.assertEqual(accum['outTemp'].max, 62.0)  # NOT 99 -> start excluded

        finally:
            if dbm is not None:
                dbm.close()
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_create_continuous_accum_from_database(self) -> None:
        # Covers create_continuous_accum, which primes a ContinuousAccum from
        # archive records.
        #
        # Inclusion spec (the continuous accumulator's own rolling-window rule,
        # stated independently of loopdata's SQL): at priming, the window is
        # anchored to wall-clock time (there is no current observation yet).  A
        # record survives iff ts > now - timelength; a record at or before
        # now - timelength is trimmed.  This test verifies both sides of that
        # boundary.
        #
        # To avoid a sub-second race between the test's 'now' and the code's
        # internal time.time(), the boundary records are placed a few seconds
        # inside and outside the edge rather than exactly on it.
        import time as _time
        unit_system = weewx.units.unit_constants['US']

        tmpdir = tempfile.mkdtemp()
        dbm = None
        try:
            db_dict = {'database_name': os.path.join(tmpdir, 'test.sdb'),
                       'driver': 'weedb.sqlite'}
            dbm = weewx.manager.DaySummaryManager.open_with_create(
                db_dict, table_name='archive', schema=wview_extended_schema)

            timelength = 3600   # 1 hour rolling window ('1h')
            now = int(_time.time())
            # Records strictly inside the window (ts > now - timelength).
            inside = [now - 900, now - 600, now - 300]
            # Just OUTSIDE the left edge: a few seconds older than the boundary
            # (now - timelength - 30) -> must be trimmed.
            just_outside = now - timelength - 30
            # Far outside, to be unambiguous as well.
            far_outside = now - 7200
            recs = [
                {'dateTime': far_outside, 'usUnits': 1, 'interval': 5, 'outTemp': 99.0},
                {'dateTime': just_outside, 'usUnits': 1, 'interval': 5, 'outTemp': 88.0}]
            for i, ts in enumerate(inside):
                recs.append({'dateTime': ts, 'usUnits': 1, 'interval': 5,
                             'outTemp': 50.0 + i})
            dbm.addRecord(recs)

            day_accum = weewx.accum.Accum(
                weeutil.weeutil.archiveDaySpan(now), unit_system)
            day_accum.addRecord(
                {'dateTime': now - 60, 'usUnits': 1, 'outTemp': 55.0}, weight=300)

            accum, valid = user.loopdata.LoopData.create_continuous_accum(
                '1h', unit_system, 5, {'outTemp'}, timelength, day_accum, dbm)

            self.assertIsNotNone(accum)
            self.assertIn('outTemp', valid)
            self.assertIn('outTemp', accum)
            self.assertEqual(type(accum), user.loopdata.ContinuousAccum)
            self.assertEqual(type(accum['outTemp']), user.loopdata.ContinuousScalarStats)
            # Only the 3 in-window records prime the accumulator.  Both the
            # far-outside (99.0) and just-outside (88.0) records are older than
            # now - timelength and must be trimmed.  count == 3 verifies the
            # left-edge exclusion; if either out-of-window record leaked in,
            # count would be 4 or 5.
            self.assertEqual(accum['outTemp'].count, 3)

        finally:
            if dbm is not None:
                dbm.close()
            shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == '__main__':
    unittest.main()
