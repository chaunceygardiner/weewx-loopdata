"""
loopdata.py

Copyright (C)2020 by John A Kline (john@johnkline.com)
Distributed under the terms of the GNU Public License (GPLv3)

LoopData is a WeeWX service that generates a json file (loop-data.txt)
containing values for the observations in the loop packet; along with
today's high, low, sum, average and weighted averages for each observation
in the packet.
"""

import copy
import configobj
import itertools
import json
import logging
import math
import os
import queue
import shutil
import sys
import tempfile
import threading
import time

from dataclasses import dataclass
from typing import Any, Dict, Generator, List, Optional, Set, Tuple, Union
from enum import Enum
from sortedcontainers import SortedDict

import weewx
import weewx.defaults
import weewx.manager
import weewx.reportengine
import weewx.units
import weewx.wxxtypes
import weeutil.config
import weeutil.logger
import weeutil.rsyncupload
import weeutil.weeutil


from weeutil.weeutil import timestamp_to_string
from weeutil.weeutil import to_bool
from weeutil.weeutil import to_float
from weeutil.weeutil import to_int
from weewx.engine import StdService

# get a logger object
log = logging.getLogger(__name__)

LOOP_DATA_VERSION = '3.2'

if sys.version_info[0] < 3 or (sys.version_info[0] == 3 and sys.version_info[1] < 7):
    raise weewx.UnsupportedFeature(
        "weewx-loopdata requires Python 3.7 or later, found %s.%s" % (sys.version_info[0], sys.version_info[1]))

if weewx.__version__ < "4":
    raise weewx.UnsupportedFeature(
        "weewx-loopdata requires WeeWX, found %s" % weewx.__version__)

windrun_bucket_suffixes: List[str] = [ 'N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE',
                                       'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW' ]

# Set up windrun_<dir> observation types.
for suffix in windrun_bucket_suffixes:
    weewx.units.obs_group_dict['windrun_%s' % suffix] = 'group_distance'

@dataclass
class CheetahName:
    field      : str           # $day.outTemp.avg.formatted
    prefix     : Optional[str] # unit or None
    prefix2    : Optional[str] # label or None
    period     : Optional[str] # 2m, 10m, 24h, hour, day, week, month, year, rainyear, alltime, current, trend
    obstype    : str           # e.g,. outTemp
    agg_type   : Optional[str] # avg, sum, etc. (required if period, other than current, is specified, else None)
    format_spec: Optional[str] # formatted (formatted value sans label), raw or ordinal_compass (could be on direction), or None
    def __hash__(self):
        return hash(self.field)

@dataclass
class ObsTypes:
    current         : Set[str]
    trend           : Set[str]
    alltime         : Set[str]
    rainyear        : Set[str]
    year            : Set[str]
    month           : Set[str]
    week            : Set[str]
    day             : Set[str]
    hour            : Set[str]
    twentyfour_hour : Set[str]
    ten_min         : Set[str]
    two_min         : Set[str]

@dataclass
class Configuration:
    queue                    : queue.SimpleQueue
    config_dict              : Dict[str, Any]
    unit_system              : int
    archive_interval         : int
    loop_data_dir            : str
    filename                 : str
    target_report            : str
    loop_frequency           : float
    specified_fields         : Set[str]
    fields_to_include        : Set[CheetahName]
    formatter                : weewx.units.Formatter
    converter                : weewx.units.Converter
    tmpname                  : str
    enable                   : bool
    remote_server            : str
    remote_port              : int
    remote_user              : str
    remote_dir               : str
    compress                 : bool
    log_success              : bool
    ssh_options              : str
    skip_if_older_than       : int
    timeout                  : int
    time_delta               : int # Used for trend.
    week_start               : int
    rainyear_start           : int
    obstypes                 : ObsTypes
    baro_trend_descs         : Any # Dict[BarometerTrend, str]

# ===============================================================================
#                             ContinuousScalarStats
# ===============================================================================

@dataclass
class ScalarDebit:
    timestamp : int
    expiration: int
    value     : float
    weight    : float

class ContinuousScalarStats(object):
    """Accumulates statistics (min, max, average, etc.) for a scalar value.

    Property 'first' is the first non-None value seen. Property 'firsttime' is
    the time it was seen.

    Property 'last' is the last non-None value seen. Property 'lasttime' is
    the time it was seen.

    The accumulator collects a rolling number of observations spanning timelength
    seconds.

    addSum(ts, val, weight)
              |                          future_debits (List)
              |                          --------------------
              '------------------------> ts|expiration(ts+timelength)|value|weight
              |
              |
              v
        values_dict (Sorted Dict)
        key         value
        ----------- ------------------------
        val         timestamp_list (List)
                    --------------
                    ts

    Every time an observation is added (with addSum), a future
    debit is created with the same information and an expiration of ts + timelength.
    In the continous accumulator addRecord function, after addSum is called on all
    continous stats instances, trimExpiredEntries(ts) is called on
    all continous stats instances.

    The list of future debits is stored in a List.  Each time trimExpiredEntries is
    called, the top of the list is iterated on looking for any entries where
    the expiration is <= the current dateTime.

    In addition to the future debit list, a values_dict (SortedDict) is maintained where:
    key  : the value specified in the call to addSum
    value: timestamp_list, a list of timestamps (as specified in an addSum call)
           for the particular value of the key
    When addSum is called:
    1. If the value does not already exist in values_dict, it is created as the key and an
       empty timestamp_list is created for the value part of the key/value pair.
    2. a new ts is added to the end of the time_stamp list.
    When trimExpiredEntries is called,
    1. The timestamp_list is retrieved in values_dict by looking up the value.
    2. The creation timestamp is removed from the timestamp_list (it will be the first)
    3. If the timestamp_list is now empty, the key/value pair is removed from values_dict.
    As the values_dict is sorted by value, it is used to efficiently find the min and max
    values when getStatsTuple is called.  For max, maxtime is the first entry in the
    timestamp_list for that value.  As expected, for min, mintime is the first entry in the
    timestamp_list for that value.
    """

    def __init__(self, timelength: int):
        self.timelength: int = timelength
        self.future_debits: List[ScalarDebit] = []
        self.values_dict: SortedDict[float, List[int]] = SortedDict()
        self.sum = 0.0
        self.count = 0
        self.wsum = 0.0
        self.sumtime = 0.0

    def getStatsTuple(self):
        # min is key of first element in values_dict
        # mintime is first element of the timestamp list contained in the value of the first element in values_dict
        # max is key of last element in dict
        # maxtime is first element of the timestamp list contained in the value of the last element in values_dict
        min, timelist = self.values_dict.peekitem(0)
        mintime: int = timelist[0]
        max, timelist = self.values_dict.peekitem(-1)
        maxtime: int = timelist[0]
        sum = LoopData.massage_near_zero(self.sum)
        wsum = LoopData.massage_near_zero(self.wsum)
        return (min, mintime, max, maxtime,
                sum, self.count, wsum, self.sumtime)

    def addSum(self, ts, val, weight=1):
        """Add a scalar value to my running sum and count.
           Also add debit to be deducted self.timelength seconds in the future.
        """

        # If necessary, convert to float. Be prepared to catch an exception if not possible.
        try:
            val = to_float(val)
        except ValueError:
            val = None

        # Check for None and NaN:
        if val is not None and val == val:
            self.sum += val
            self.count += 1
            self.wsum += val * weight
            self.sumtime += weight
            # Add to values_dict
            if not val in self.values_dict:
                self.values_dict[val] = []
            timestamp_list: List[int] = self.values_dict[val]
            timestamp_list.append(ts)
            # Add future debit
            debit= ScalarDebit(
                timestamp  = ts,
                expiration = ts + self.timelength,
                value    = val,
                weight   = weight)
            self.future_debits.append(debit)

    def trimExpiredEntries(self, ts):
        # Remove any debits that may have matured.
        while len(self.future_debits) > 0 and self.future_debits[0].expiration <= ts:
            # Apply this debit.
            debit = self.future_debits.pop(0)
            log.debug('Applying debit: %s value: %f, weight: %f' % (timestamp_to_string(debit.timestamp), debit.value, debit.weight))
            self.sum -= debit.value
            self.count -= 1
            self.wsum -= debit.value * debit.weight
            self.sumtime -= debit.weight
            # Remove the debit entry in the values_dict.
            timestamp_list: List[int] = self.values_dict[debit.value]
            first_timestamp = timestamp_list.pop(0)
            assert first_timestamp == debit.timestamp
            if len(timestamp_list) == 0:
                self.values_dict.pop(debit.value)

    @property
    def avg(self):
        return self.wsum / self.sumtime if self.count else None

    @property
    def first(self):
        if len(self.future_debits) != 0:
          return self.future_debits[0].value
        else:
            return None

    @property
    def firsttime(self):
        if len(self.future_debits) != 0:
            return self.future_debits[0].timestamp
        else:
            return None

    @property
    def last(self):
        if len(self.future_debits) != 0:
          return self.future_debits[-1].value
        else:
            return None

    @property
    def lasttime(self):
        if len(self.future_debits) != 0:
            return self.future_debits[-1].timestamp
        else:
            return None

# ===============================================================================
#                             ContinuousVecStats
# ===============================================================================

@dataclass
class VecDebit:
    timestamp : int
    expiration: int
    speed     : float
    dirN      : float
    weight    : float

class ContinuousVecStats(object):
    """Accumulates statistics for a vector value.
    The accumulator collects a rolling number of observations spanning timelength
    seconds.

    addSum(ts, val(speed,dirN), weight)
              |                          future_debits (List)
              |                          --------------------
              '------------------------> ts|expiration(ts+timelength)|value|weight
              |
              |
              v
        speed_dict (Sorted Dict)
        key         value
        ----------- ------------------------
        speed       timestamp_dirn_list (List)
                    -------------------------
                    tuple(ts, dirN)

    Every time an observation is added (with addSum), a future
    debit is created with the same information and an expiration of ts + timelength.
    In the continous accumulator addRecord function, after addSum is called on all
    continous stats instances, trimExpiredEntries(ts) is called on
    all continous stats instances.

    The list of future debits is stored in a List.  Each time trimExpiredEntries is
    called, the top of the list is iterated on looking for any entries where
    the expiration is <= the current dateTime.

    In addition to the future debit list, a speed_dict (SortedDict) is maintained where:
    key  : the value specified in the call to addSum
    value: timestamp_dirn_list, a List of (ts, dirN) tuples
    When addSum is called:
    1. If the speed does not already exist in speed_dict, it is created as the key and an
       empty timestamp_dirn_list is created for the value part of the key/value pair.
    2. a new (ts, dirN) tuple is added to the timestamp_dirn_list.

    When trimExpiredEntries is called,
    1. The timestamp_dirn_list is retrieved in speed_dict by looking up the speed.
    2. The timestamp, dirN tuple (which is the first) entry is removed from the timestamp_dirn_list.
    3. If the timestamp_dirn_lisat is now empty, the speed entry is removed from speed_dict.
    As the speed_dict is sorted by value, it is used to efficiently find the min and max
    values when getStatsTuple is called.  For max, maxtime is the first entry in the
    timestamp_dirn_list for that value (with dirN being the dirN that is paired with that
    first timestamp.  As expected, for min, mintime is the first entry in the
    timestamp_dirn_list with dirN being the value paired with the mintime.
    """

    def __init__(self, timelength: int):
        self.timelength: int = timelength
        self.future_debits: List[VecDebit] = []
        self.speed_dict: SortedDict[float, List[Tuple[int, float]]] = SortedDict()
        self.sum = 0.0
        self.count = 0
        self.wsum = 0.0
        self.sumtime = 0
        self.xsum = 0.0
        self.ysum = 0.0
        self.dirsumtime = 0
        self.squaresum = 0.0
        self.wsquaresum = 0.0

    def getStatsTuple(self):
        # min is key of first key in speed_dict
        # mintime is first entry of the timestamp_dirn_list contained in the value of the first element in speed_dict
        # max is key of last key in speed_dict
        # max is key of last element in speed_dict
        # maxtime is first entry of the timestamp_dirn_list contained in the value of the last element in speed_dict
        if len(self.speed_dict) != 0:
            min, time_dirn_list_min = self.speed_dict.peekitem(0)
            mintime, dummy = time_dirn_list_min[0]
            max, time_dirn_list_max = self.speed_dict.peekitem(-1)
            maxtime, maxdir = time_dirn_list_max[-1]
        else:
            min, mintime, max, maxtime = None, None, None, None

        sum  = LoopData.massage_near_zero(self.sum)
        wsum = LoopData.massage_near_zero(self.wsum)
        sumtime = LoopData.massage_near_zero(self.sumtime)
        dirsumtime = LoopData.massage_near_zero(self.dirsumtime)
        squaresum = LoopData.massage_near_zero(self.squaresum)
        wsquaresum = LoopData.massage_near_zero(self.wsquaresum)

        return (min, mintime,
                max, maxtime,
                sum, self.count,
                wsum, sumtime,
                maxdir, self.xsum, self.ysum,
                dirsumtime, squaresum, wsquaresum)


    def addSum(self, ts, val, weight=1):
        """Add a vector value to my sum and squaresum.
        val: A vector value. It is a 2-way tuple (mag, dir)
        """
        speed, dirN = val


        # If necessary, convert to float. Be prepared to catch an exception if not possible.
        try:
            speed = to_float(speed)
        except ValueError:
            speed = None
        try:
            dirN = to_float(dirN)
        except ValueError:
            dirN = None

        # Check for None and NaN:
        if speed is not None and speed == speed:
            self.sum += speed
            self.count += 1
            self.wsum += weight * speed
            self.sumtime += weight
            self.squaresum += speed ** 2
            self.wsquaresum += weight * speed ** 2
            if dirN is not None:
                self.xsum += weight * speed * math.cos(math.radians(90.0 - dirN))
                self.ysum += weight * speed * math.sin(math.radians(90.0 - dirN))
            # It's OK for direction to be None, provided speed is zero:
            if dirN is not None or speed == 0:
                self.dirsumtime += weight
            # Add to speed_dict
            if not speed in self.speed_dict:
                self.speed_dict[speed] = []
            timestamp_dirn_list: List[Tuple[int, float]] = self.speed_dict[speed]
            timestamp_dirn_list.append((ts, dirN))
            # Add future debit
            debit = VecDebit(
                timestamp  = ts,
                expiration = ts + self.timelength,
                speed      = speed,
                dirN       = dirN,
                weight     = weight)
            self.future_debits.append(debit)

    def trimExpiredEntries(self, ts):
        # Remove any debits that may have matured.
        while len(self.future_debits) > 0 and self.future_debits[0].expiration <= ts:
            debit = self.future_debits.pop(0)
            log.debug('Applying ContinuousVecStats debit: %s speed: %f, dirN: %r, weight: %f' % (timestamp_to_string(debit.timestamp), debit.speed, debit.dirN, debit.weight))
            # Apply this debit.
            self.sum -= debit.speed
            self.count -= 1
            self.wsum -= debit.weight * debit.speed
            self.sumtime -= debit.weight
            self.squaresum -= debit.speed ** 2
            self.wsquaresum -= debit.weight * debit.speed ** 2
            if debit.dirN is not None:
                self.xsum += debit.weight * debit.speed * math.cos(math.radians(90.0 - debit.dirN))
                self.ysum += debit.weight * debit.speed * math.sin(math.radians(90.0 - debit.dirN))
            # Remove the debit entry in the speed_dict.
            timestamp_dirn_list: List[Tuple[int, float]] = self.speed_dict[debit.speed]
            timestamp, dirN = timestamp_dirn_list.pop(0)
            assert timestamp == debit.timestamp
            if len(timestamp_dirn_list) == 0:
                self.speed_dict.pop(debit.speed)

    @property
    def avg(self):
        return self.wsum / self.sumtime if self.count else None

    @property
    def rms(self):
        return math.sqrt(self.wsquaresum / self.sumtime) if self.count else None

    @property
    def vec_avg(self):
        if self.count:
            return math.sqrt((self.xsum ** 2 + self.ysum ** 2) / self.sumtime ** 2)

    @property
    def vec_dir(self):
        if self.dirsumtime and (self.ysum or self.xsum):
            _result = 90.0 - math.degrees(math.atan2(self.ysum, self.xsum))
            if _result < 0.0:
                _result += 360.0
            return _result
        # Return the last known direction when our vector sum is 0
        return self.last[1]

    @property
    def first(self):
        if len(self.future_debits) != 0:
            return self.future_debits[0].speed, self.future_debits[-1].dirN
        else:
            return None

    @property
    def firsttime(self):
        if len(self.future_debits) != 0:
            return self.future_debits[0].timestamp
        else:
            return None

    @property
    def last(self):
        if len(self.future_debits) != 0:
            return self.future_debits[-1].speed, self.future_debits[-1].dirN
        else:
            return None

    @property
    def lasttime(self):
        if len(self.future_debits) != 0:
            return self.future_debits[-1].timestamp
        else:
            return None


# ===============================================================================
#                             ContinuousFirstLastAccum
# ===============================================================================

@dataclass
class FirstLastEntry:
    dateTime: int
    value   : str

class ContinuousFirstLastAccum(object):
    """Minimal accumulator, suitable for strings.
    It can only return the first and last strings it has seen, along with their timestamps.

    The accumulator collects a rolling number of observations spanning timelength
    seconds.

    addSum(ts, val, weight)
              |
              v
        values_list (List)
        FirstLastEntry
        --------------
        dateTime|value

    In the continous accumulator addRecord function, after addSum is called on all
    continous stats instances, trimExpiredEntries(ts) is called on
    all continous stats instances.

    When addSum is called, FirstLastEntry is added to values_list.

    When trimExpiredEntries is called,
    1. the values_list is iterated over while FirstLastEntry.dateTime <= ts
    2.     the FirstLastEntry is deleted

    first/firsttime is the dateTime value and dateTime of the first entry in values_list
    last/lasttime is the dateTime value and dateTime of the last entry in values_list
    """

    def __init__(self, timelength: int):
        self.timelength = timelength
        self.values_list: List[FirstLastEntry] = []

    def getStatsTuple(self):
        """Return a stats-tuple. That is, a tuple containing the gathered statistics."""
        return self.values_list[0].value, self.values_list[0].dateTime, self.values_list[-1].value, self.values_list[-1].dateTime,

    def addSum(self, ts, val, weight=1):
        """Add a scalar value to my running count."""
        if val is not None:
            string_val = str(val)
            self.values_list.append(FirstLastEntry(
                dateTime = ts,
                value = string_val))

    def trimExpiredEntries(self, ts):
        # Remove any expired entries
        while len(self.values_list) > 0 and self.values_list[0].dateTime + self.timelength <= ts:
            self.values_list.pop(0)


# ===============================================================================
#                             Class ContinuousAccum
# ===============================================================================

class ContinuousAccum(dict):
    """Accumulates statistics for a set of observation types.

    ContinousAccum is a lot like WeeWX's accum, but a timelength (rather than
    a timespan) is specified and the ContinousAccum gives stats on a rolling
    timelength number of seconds.

    ContinuousAccums never expire.  In their steady state, for every loop packet,
    they add the new packet and drop the olest packet.
    """

    def __init__(self, timelength: int, unit_system=None):
        """Initialize a Accum.

        timelength: The length of time the accumulator will keep data for (rolling).
        unit_system: The unit system used by the accumulator"""

        self.timelength = timelength
        # Set the accumulator's unit system. Usually left unspecified until the
        # first observation comes in for normal operation or pre-set if
        # obtaining a historical accumulator.
        self.unit_system = unit_system

    def addRecord(self, record, weight=1):
        """Add a record to running statistics.

        The record must have keys 'dateTime' and 'usUnits'."""

        for obs_type in record:
            # Get the proper function ...
            func = get_add_function(obs_type)
            # ... then call it.
            func(self, record, obs_type, weight)

        # Trim the expired entries.
        for stats in self.keys():
            self[stats].trimExpiredEntries(record['dateTime'])

    def getRecord(self):
        """Extract a record out of the results in the accumulator."""

        # All records have a timestamp and unit type
        record = {'dateTime': self.timespan.stop,
                  'usUnits': self.unit_system}

        return self.augmentRecord(record)

    def augmentRecord(self, record):

        # Go through all observation types.
        for obs_type in self:
            # If the type does not appear in the record, then add it:
            if obs_type not in record:
                # Get the proper extraction function...
                func = weewx.accum.get_extract_function(obs_type)
                # ... then call it
                func(self, record, obs_type)

        return record

    #
    # Begin add functions. These add a record to the accumulator.
    #

    def add_value(self, record, obs_type, weight):
        """Add a single observation to myself."""

        val = record[obs_type]

        # If the type has not been seen before, initialize it
        self._init_type(self.timelength, obs_type)
        self[obs_type].addSum(record['dateTime'], val, weight=weight)

    def add_wind_value(self, record, obs_type, weight):
        """Add a single observation of type wind to myself."""

        if obs_type in ['windDir', 'windGust', 'windGustDir']:
            return
        if weewx.debug:
            assert obs_type == 'windSpeed'

        # First add it to regular old 'windSpeed', then
        # treat it like a vector.
        self.add_value(record, obs_type, weight)

        # If the type has not been seen before, initialize it.
        self._init_type(self.timelength, 'wind')

        # Add to the running sum.
        self['wind'].addSum(record['dateTime'], (record['windSpeed'], record.get('windDir')), weight=weight)

    def check_units(self, record, obs_type, weight):
        if weewx.debug:
            assert obs_type == 'usUnits'
        self._check_units(record['usUnits'])

    def noop(self, record, obs_type, weight=1):
        pass

    #
    # Begin extraction functions. These extract a record out of the accumulator.
    #

    def extract_wind(self, record, obs_type):
        """Extract wind values from myself, and put in a record."""
        # Wind records must be flattened into the separate categories:
        if 'windSpeed' not in record:
            record['windSpeed'] = self[obs_type].avg
        if 'windDir' not in record:
            record['windDir'] = self[obs_type].vec_dir
        if 'windGust' not in record:
            record['windGust'] = self[obs_type].max
        if 'windGustDir' not in record:
            record['windGustDir'] = self[obs_type].max_dir

    def extract_sum(self, record, obs_type):
        record[obs_type] = self[obs_type].sum if self[obs_type].count else None

    def extract_last(self, record, obs_type):
        record[obs_type] = self[obs_type].last

    def extract_avg(self, record, obs_type):
        record[obs_type] = self[obs_type].avg

    def extract_min(self, record, obs_type):
        record[obs_type] = self[obs_type].min

    def extract_max(self, record, obs_type):
        record[obs_type] = self[obs_type].max

    def extract_count(self, record, obs_type):
        record[obs_type] = self[obs_type].count

    #
    # Miscellaneous, utility functions
    #

    def _init_type(self, timelength: int, obs_type):
        """Add a given observation type to my dictionary."""
        # Do nothing if this type has already been initialized:
        if obs_type in self:
            return

        # Get a new accumulator of the proper type
        self[obs_type] = new_continuous_accumulator(timelength, obs_type)

    def _check_units(self, new_unit_system):
        # If no unit system has been specified for me yet, adopt the incoming
        # system
        if self.unit_system is None:
            self.unit_system = new_unit_system
        else:
            # Otherwise, make sure they match
            if self.unit_system != new_unit_system:
                raise ValueError("Unit system mismatch %d v. %d" % (self.unit_system,
                                                                    new_unit_system))

    @property
    def isEmpty(self):
        return self.unit_system is None

def new_continuous_accumulator(timelength, obs_type):
    """Instantiate an accumulator, appropriate for type 'obs_type'."""
    # global accum_dict
    # Get the options for this type. Substitute the defaults if they have not been specified
    obs_options = weewx.accum.accum_dict.get(obs_type, weewx.accum.OBS_DEFAULTS)
    # Get the nickname of the accumulator. Default is 'scalar'
    accum_nickname = obs_options.get('accumulator', 'scalar')
    # Instantiate and return the accumulator.
    # If we don't know this nickname, then fail hard with a KeyError
    return ACCUM_TYPES[accum_nickname](timelength)

ACCUM_TYPES = {
    'scalar': ContinuousScalarStats,
    'vector': ContinuousVecStats,
    'firstlast': ContinuousFirstLastAccum
}

ADD_FUNCTIONS = {
    'add': ContinuousAccum.add_value,
    'add_wind': ContinuousAccum.add_wind_value,
    'check_units': ContinuousAccum.check_units,
    'noop': ContinuousAccum.noop
}

def get_add_function(obs_type):
    """Get an adder function appropriate for type 'obs_type'."""
    # global accum_dict
    # Get the options for this type. Substitute the defaults if they have not been specified
    obs_options = weewx.accum.accum_dict.get(obs_type, weewx.accum.OBS_DEFAULTS)
    # Get the nickname of the adder. Default is 'add'
    add_nickname = obs_options.get('adder', 'add')
    # If we don't know this nickname, then fail hard with a KeyError
    return ADD_FUNCTIONS[add_nickname]

@dataclass
class Accumulators:
    alltime_accum        : Optional[weewx.accum.Accum]
    rainyear_accum       : Optional[weewx.accum.Accum]
    year_accum           : Optional[weewx.accum.Accum]
    month_accum          : Optional[weewx.accum.Accum]
    week_accum           : Optional[weewx.accum.Accum]
    day_accum            : weewx.accum.Accum
    hour_accum           : Optional[weewx.accum.Accum]
    twentyfour_hour_accum: Optional[ContinuousAccum]
    ten_min_accum        : Optional[ContinuousAccum]
    two_min_accum        : Optional[ContinuousAccum]
    trend_accum          : Optional[ContinuousAccum]

class BarometerTrend(Enum):
    RISING_VERY_RAPIDLY  =  4
    RISING_QUICKLY       =  3
    RISING               =  2
    RISING_SLOWLY        =  1
    STEADY               =  0
    FALLING_SLOWLY       = -1
    FALLING              = -2
    FALLING_QUICKLY      = -3
    FALLING_VERY_RAPIDLY = -4

@dataclass
class Reading:
    dateTime: int
    value   : Any

@dataclass
class PeriodPacket:
    timestamp: int
    packet   : Dict[str, Any]

class LoopData(StdService):
    def __init__(self, engine, config_dict):
        super(LoopData, self).__init__(engine, config_dict)
        log.info("Service version is %s." % LOOP_DATA_VERSION)

        if sys.version_info[0] < 3:
            raise Exception("Python 3 is required for the loopdata plugin.")

        self.loop_processor_started = False
        self.day_packets: List[Dict[str, Any]] = []

        station_dict             = config_dict.get('Station', {})
        std_archive_dict         = config_dict.get('StdArchive', {})
        loop_config_dict         = config_dict.get('LoopData', {})
        file_spec_dict           = loop_config_dict.get('FileSpec', {})
        formatting_spec_dict     = loop_config_dict.get('Formatting', {})
        loop_frequency_spec_dict = loop_config_dict.get('LoopFrequency', {})
        rsync_spec_dict          = loop_config_dict.get('RsyncSpec', {})
        include_spec_dict        = loop_config_dict.get('Include', {})
        baro_trend_trans_dict    = loop_config_dict.get('BarometerTrendDescriptions', {})

        # Get the unit_system as specified by StdConvert->target_unit.
        # Note: this value will be overwritten if the day accumulator has a a unit_system.
        db_binder = weewx.manager.DBBinder(config_dict)
        default_binding = config_dict.get('StdReport')['data_binding']
        dbm = db_binder.get_manager(default_binding)
        unit_system = dbm.std_unit_system
        if unit_system is None:
            unit_system = weewx.units.unit_constants[self.config_dict['StdConvert'].get('target_unit', 'US').upper()]
        # Get the column names of the archive table.
        self.archive_columns: List[str] = dbm.connection.columnsOf('archive')

        # Get a temporay file in which to write data before renaming.
        tmp = tempfile.NamedTemporaryFile(prefix='LoopData', delete=False)
        tmp.close()

        # Get a target report dictionary we can use for converting units and formatting.
        target_report = formatting_spec_dict.get('target_report', 'LoopDataReport')
        try:
            target_report_dict = LoopData.get_target_report_dict(
                config_dict, target_report)
        except Exception as e:
            log.error('Could not find target_report: %s.  LoopData is exiting. Exception: %s' % (target_report, e))
            return

        loop_data_dir = LoopData.compose_loop_data_dir(config_dict, target_report_dict, file_spec_dict)

        # Get the loop frequency seconds to be passed as the weight to accumulators.
        loop_frequency = to_float(loop_frequency_spec_dict.get('seconds', '2.0'))

        # Get [possibly localized] strings for trend.barometer.desc
        baro_trend_descs = LoopData.construct_baro_trend_descs(baro_trend_trans_dict)

        # Process fields line of LoopData section.
        specified_fields = include_spec_dict.get('fields', [])
        (fields_to_include, obstypes) = LoopData.get_fields_to_include(specified_fields)

        # Get the time_delta (number of seconds) to use for trend_accum.
        try:
            time_delta: int = to_int(target_report_dict['Units']['Trend']['time_delta'])
            if time_delta > 259200:
                log.info('time_delta of %d specified, LoopData will use max value of 259200.' % time_delta)
                time_delta = 259200
        except KeyError:
            time_delta = 10800

        # Get week_start
        try:
            week_start: int = to_int(station_dict['week_start'])
        except KeyError:
            week_start = 6

        # Get rainyear_start (in weewx.conf, it is rain_year_start)
        try:
            rainyear_start: int = to_int(station_dict['rain_year_start'])
        except KeyError:
            rainyear_start = 1

        self.cfg: Configuration = Configuration(
            queue                    = queue.SimpleQueue(),
            config_dict              = config_dict,
            unit_system              = unit_system,
            archive_interval         = to_int(std_archive_dict.get('archive_interval')),
            loop_data_dir            = loop_data_dir,
            filename                 = file_spec_dict.get('filename', 'loop-data.txt'),
            target_report            = target_report,
            loop_frequency           = loop_frequency,
            specified_fields         = specified_fields,
            fields_to_include        = fields_to_include,
            formatter                = weewx.units.Formatter.fromSkinDict(target_report_dict),
            converter                = weewx.units.Converter.fromSkinDict(target_report_dict),
            tmpname                  = tmp.name,
            enable                   = to_bool(rsync_spec_dict.get('enable')),
            remote_server            = rsync_spec_dict.get('remote_server'),
            remote_port              = to_int(rsync_spec_dict.get('remote_port')) if rsync_spec_dict.get(
                                      'remote_port') is not None else None,
            remote_user              = rsync_spec_dict.get('remote_user'),
            remote_dir               = rsync_spec_dict.get('remote_dir'),
            compress                 = to_bool(rsync_spec_dict.get('compress')),
            log_success              = to_bool(rsync_spec_dict.get('log_success')),
            ssh_options              = rsync_spec_dict.get('ssh_options', '-o ConnectTimeout     =1'),
            timeout                  = to_int(rsync_spec_dict.get('timeout', 1)),
            skip_if_older_than       = to_int(rsync_spec_dict.get('skip_if_older_than', 3)),
            time_delta               = time_delta,
            week_start               = week_start,
            rainyear_start           = rainyear_start,
            obstypes                 = obstypes,
            baro_trend_descs         = baro_trend_descs)

        if not os.path.exists(self.cfg.loop_data_dir):
            os.makedirs(self.cfg.loop_data_dir)

        log.info('LoopData file is: %s' % os.path.join(self.cfg.loop_data_dir, self.cfg.filename))

        self.bind(weewx.PRE_LOOP, self.pre_loop)
        self.bind(weewx.NEW_LOOP_PACKET, self.new_loop)

    @staticmethod
    def massage_near_zero(val: float)-> float:
        if val > -0.0000000001 and val < 0.0000000001:
            return 0.0
        else:
            return val

    @staticmethod
    def compose_loop_data_dir(config_dict: Dict[str, Any],
            target_report_dict: Dict[str, Any], file_spec_dict: Dict[str, Any]
            ) -> str:
        # Compose the directory in which to write the file (if
        # relative it is relative to the target_report_directory).
        weewx_root   : str = str(config_dict.get('WEEWX_ROOT'))
        html_root    : str = str(target_report_dict.get('HTML_ROOT'))
        loop_data_dir: str = str(file_spec_dict.get('loop_data_dir', '.'))
        return os.path.join(weewx_root, html_root, loop_data_dir)

    @staticmethod
    def construct_baro_trend_descs(baro_trend_trans_dict: Dict[str, str]) -> Dict[BarometerTrend, str]:
        baro_trend_descs: Dict[BarometerTrend, str] = {}
        baro_trend_descs[BarometerTrend.RISING_VERY_RAPIDLY]  = baro_trend_trans_dict.get('RISING_VERY_RAPIDLY', 'Rising Very Rapidly')
        baro_trend_descs[BarometerTrend.RISING_QUICKLY]       = baro_trend_trans_dict.get('RISING_QUICKLY',       'Rising Quickly')
        baro_trend_descs[BarometerTrend.RISING]               = baro_trend_trans_dict.get('RISING',               'Rising')
        baro_trend_descs[BarometerTrend.RISING_SLOWLY]        = baro_trend_trans_dict.get('RISING_SLOWLY',        'Rising Slowly')
        baro_trend_descs[BarometerTrend.STEADY]               = baro_trend_trans_dict.get('STEADY',               'Steady')
        baro_trend_descs[BarometerTrend.FALLING_SLOWLY]       = baro_trend_trans_dict.get('FALLING_SLOWLY',       'Falling Slowly')
        baro_trend_descs[BarometerTrend.FALLING]              = baro_trend_trans_dict.get('FALLING',              'Falling')
        baro_trend_descs[BarometerTrend.FALLING_QUICKLY]      = baro_trend_trans_dict.get('FALLING_QUICKLY',      'Falling Quickly')
        baro_trend_descs[BarometerTrend.FALLING_VERY_RAPIDLY] = baro_trend_trans_dict.get('FALLING_VERY_RAPIDLY', 'Falling Very Rapidly')
        return baro_trend_descs

    @staticmethod
    def get_fields_to_include(specified_fields: Set[str]) -> Tuple[Set[CheetahName], ObsTypes]:
        """
        Return ObsTypes (fields_to_include and obstypes)
        """
        fields_to_include: Set[CheetahName] = set()
        for field in specified_fields:
            cname: Optional[CheetahName] = LoopData.parse_cname(field)
            if cname is not None:
                fields_to_include.add(cname)
        current_obstypes  : Set[str] = LoopData.compute_period_obstypes(
            fields_to_include, 'current')
        trend_obstypes  : Set[str] = LoopData.compute_period_obstypes(
            fields_to_include, 'trend')
        alltime_obstypes    : Set[str] = LoopData.compute_period_obstypes(
            fields_to_include, 'alltime')
        rainyear_obstypes    : Set[str] = LoopData.compute_period_obstypes(
            fields_to_include, 'rainyear')
        year_obstypes    : Set[str] = LoopData.compute_period_obstypes(
            fields_to_include, 'year')
        month_obstypes    : Set[str] = LoopData.compute_period_obstypes(
            fields_to_include, 'month')
        week_obstypes    : Set[str] = LoopData.compute_period_obstypes(
            fields_to_include, 'week')
        day_obstypes    : Set[str] = LoopData.compute_period_obstypes(
            fields_to_include, 'day')
        hour_obstypes    : Set[str] = LoopData.compute_period_obstypes(
            fields_to_include, 'hour')
        twentyfour_hour_obstypes: Set[str] = LoopData.compute_period_obstypes(
            fields_to_include, '24h')
        ten_min_obstypes: Set[str] = LoopData.compute_period_obstypes(
            fields_to_include, '10m')
        two_min_obstypes: Set[str] = LoopData.compute_period_obstypes(
            fields_to_include, '2m')

        # current_obstypes is special because current observations are
        # needed to feed all the others.  As such, take the union of all.
        current_obstypes = set(itertools.chain(current_obstypes, trend_obstypes,
            alltime_obstypes, rainyear_obstypes, year_obstypes, month_obstypes,
            week_obstypes, day_obstypes, hour_obstypes, twentyfour_hour_obstypes,
            ten_min_obstypes, two_min_obstypes))

        return (fields_to_include, 
                ObsTypes(
                    current         = current_obstypes,
                    trend           = trend_obstypes,
                    alltime         = alltime_obstypes,
                    rainyear        = rainyear_obstypes,
                    year            = year_obstypes,
                    month           = month_obstypes,
                    week            = week_obstypes,
                    day             = day_obstypes,
                    hour            = hour_obstypes,
                    twentyfour_hour = twentyfour_hour_obstypes,
                    ten_min         = ten_min_obstypes,
                    two_min         = two_min_obstypes))

    @staticmethod
    def compute_period_obstypes(fields_to_include: Set[CheetahName], period: str) -> Set[str]:
        period_obstypes: Set[str] = set()
        for cname in fields_to_include:
            if cname.period == period:
                period_obstypes.add(cname.obstype)
                if cname.obstype == 'wind':
                    period_obstypes.add('windSpeed')
                    period_obstypes.add('windDir')
                    period_obstypes.add('windGust')
                    period_obstypes.add('windGustDir')
                if cname.obstype == 'appTemp':
                    period_obstypes.add('outTemp')
                    period_obstypes.add('outHumidity')
                    period_obstypes.add('windSpeed')
                if cname.obstype.startswith('windrun'):
                    period_obstypes.add('windSpeed')
                    period_obstypes.add('windDir')
                if cname.obstype == 'beaufort':
                    period_obstypes.add('windSpeed')
        return period_obstypes

    @staticmethod
    def get_target_report_dict(config_dict, report) -> Dict[str, Any]:
        try:
            return weewx.reportengine._build_skin_dict(config_dict, report)
        except AttributeError:
            pass # Load the report dict the old fashioned way below
        try:
            skin_dict = weeutil.config.deep_copy(weewx.defaults.defaults)
        except Exception:
            # Fall back to copy.deepcopy for earlier than weewx 4.1.2 installs.
            skin_dict = copy.deepcopy(weewx.defaults.defaults)
        skin_dict['REPORT_NAME'] = report
        skin_config_path = os.path.join(
            config_dict['WEEWX_ROOT'],
            config_dict['StdReport']['SKIN_ROOT'],
            config_dict['StdReport'][report].get('skin', ''),
            'skin.conf')
        try:
            merge_dict = configobj.ConfigObj(skin_config_path, file_error=True, encoding='utf-8')
            log.debug("Found configuration file %s for report '%s'", skin_config_path, report)
            # Merge the skin config file in:
            weeutil.config.merge_config(skin_dict, merge_dict)
        except IOError as e:
            log.debug("Cannot read skin configuration file %s for report '%s': %s",
                      skin_config_path, report, e)
        except SyntaxError as e:
            log.error("Failed to read skin configuration file %s for report '%s': %s",
                      skin_config_path, report, e)
            raise

        # Now add on the [StdReport][[Defaults]] section, if present:
        if 'Defaults' in config_dict['StdReport']:
            # Because we will be modifying the results, make a deep copy of the [[Defaults]]
            # section.
            try:
                merge_dict = weeutil.config.deep_copy(config_dict['StdReport']['Defaults'])
            except Exception:
                # Fall back to copy.deepcopy for earlier weewx 4 installs.
                merge_dict = copy.deepcopy(config_dict['StdReport']['Defaults'])
            weeutil.config.merge_config(skin_dict, merge_dict)

        # Inject any scalar overrides. This is for backwards compatibility. These options should now go
        # under [StdReport][[Defaults]].
        for scalar in config_dict['StdReport'].scalars:
            skin_dict[scalar] = config_dict['StdReport'][scalar]

        # Finally, inject any overrides for this specific report. Because this is the last merge, it will have the
        # final say.
        weeutil.config.merge_config(skin_dict, config_dict['StdReport'][report])

        return skin_dict

    def pre_loop(self, event):
        if self.loop_processor_started:
            return
        # Start the loop processor thread.
        self.loop_processor_started = True

        try:
            binder = weewx.manager.DBBinder(self.config_dict)
            binding = self.config_dict.get('StdReport')['data_binding']
            dbm = binder.get_manager(binding)

            # Get archive packets to prime accumulators.  First find earliest
            # record we need to fetch.

            # Fetch them just once with the greatest time period.
            now = time.time()

            # We want the earliest time needed.
            start_of_day: int = weeutil.weeutil.startOfDay(now)
            log.debug('Earliest time selected is %s' % timestamp_to_string(start_of_day))

            # Fetch the records.
            start = time.time()
            archive_pkts: List[Dict[str, Any]] = LoopData.get_archive_packets(
                dbm, self.archive_columns, start_of_day)

            # Save packets as appropriate.
            pkt_count: int = 0
            for pkt in archive_pkts:
                pkt_time = pkt['dateTime']
                if 'windrun' in pkt and 'windDir' in pkt and pkt['windDir'] is not None:
                    bkt = LoopProcessor.get_windrun_bucket(pkt['windDir'])
                    pkt['windrun_%s' % windrun_bucket_suffixes[bkt]] = pkt['windrun']
                if len(self.cfg.obstypes.day) > 0 and pkt_time >= start_of_day:
                    self.day_packets.append(pkt)
                pkt_count += 1
            log.debug('Collected %d archive packets in %f seconds.' % (pkt_count, time.time() - start))

            # accumulator_payload_sent is used to only create accumulators on first new_loop packet
            self.accumulator_payload_sent = False
            lp: LoopProcessor = LoopProcessor(self.cfg)
            t: threading.Thread = threading.Thread(target=lp.process_queue)
            t.setName('LoopData')
            t.setDaemon(True)
            t.start()
        except Exception as e:
            # Print problem to log and give up.
            log.error('Error in LoopData setup.  LoopData is exiting. Exception: %s' % e)
            weeutil.logger.log_traceback(log.error, "    ****  ")

    @staticmethod
    def day_summary_records_generator(dbm, obstype: str, earliest_time: int
            ) -> Generator[Dict[str, Any], None, None]:
        table_name = 'archive_day_%s' % obstype
        cols: List[str] = dbm.connection.columnsOf(table_name)
        for row in dbm.genSql('SELECT * FROM %s' \
                ' WHERE dateTime >= %d ORDER BY dateTime ASC' % (table_name, earliest_time)):
            record: Dict[str, Any] = {}
            for i in range(len(cols)):
                record[cols[i]] = row[i]
            log.debug('get_day_summary_records: record(%s): %s' % (
                timestamp_to_string(record['dateTime']), record))
            yield record

    @staticmethod
    def get_archive_packets(dbm, archive_columns: List[str],
            earliest_time: int) -> List[Dict[str, Any]]:
        packets = []
        for cols in dbm.genSql('SELECT * FROM archive' \
                ' WHERE dateTime > %d ORDER BY dateTime ASC' % earliest_time):
            pkt: Dict[str, Any] = {}
            for i in range(len(cols)):
                pkt[archive_columns[i]] = cols[i]
            packets.append(pkt)
            log.debug('get_archive_packets: pkt(%s): %s' % (
                timestamp_to_string(pkt['dateTime']), pkt))
        return packets

    def new_loop(self, event):
        log.debug('new_loop: event: %s' % event)
        if not self.accumulator_payload_sent:
            self.accumulator_payload_sent = True
            binder = weewx.manager.DBBinder(self.config_dict)
            binding = self.config_dict.get('StdReport')['data_binding']
            dbm = binder.get_manager(binding)
            pkt_time = to_int(event.packet['dateTime'])

            # Init day accumulator from day_summary
            day_summary = dbm._get_day_summary(time.time())
            # Init an accumulator
            timespan = weeutil.weeutil.archiveDaySpan(pkt_time)
            unit_system = day_summary.unit_system
            if unit_system is not None:
                # Database has a unit_system already (true unless the db just got intialized.)
                self.cfg.unit_system = unit_system
            day_accum = weewx.accum.Accum(timespan, unit_system=self.cfg.unit_system)
            for k in day_summary:
                day_accum.set_stats(k, day_summary[k].getStatsTuple())
            # Need to add the windrun_<bucket> accumulators.
            for pkt in self.day_packets:
                if day_accum.timespan.includesArchiveTime(pkt['dateTime']):
                    for suffix in windrun_bucket_suffixes:
                        obs = 'windrun_%s' % suffix
                        if obs in pkt:
                            day_accum.add_value(pkt, obs, True, pkt['interval'] * 60)
                            continue
            self.day_packets = []

            alltime_accum, self.cfg.obstypes.alltime = LoopData.create_alltime_accum(
                self.cfg.unit_system, self.cfg.archive_interval, self.cfg.obstypes.alltime, day_accum, dbm)
            rainyear_accum, self.cfg.obstypes.rainyear = LoopData.create_rainyear_accum(
                self.cfg.unit_system, self.cfg.archive_interval, self.cfg.obstypes.rainyear, pkt_time, self.cfg.rainyear_start, day_accum, dbm)
            year_accum, self.cfg.obstypes.year = LoopData.create_year_accum(
                self.cfg.unit_system, self.cfg.archive_interval, self.cfg.obstypes.year, pkt_time, day_accum, dbm)
            month_accum, self.cfg.obstypes.month = LoopData.create_month_accum(
                self.cfg.unit_system, self.cfg.archive_interval, self.cfg.obstypes.month, pkt_time, day_accum, dbm)
            week_accum, self.cfg.obstypes.week = LoopData.create_week_accum(
                self.cfg.unit_system, self.cfg.archive_interval, self.cfg.obstypes.week, pkt_time, self.cfg.week_start, day_accum, dbm)
            hour_accum, self.cfg.obstypes.hour = LoopData.create_hour_accum(
                self.cfg.unit_system, self.cfg.archive_interval, self.cfg.obstypes.hour, pkt_time, day_accum, dbm)
            twentyfour_hour_accum, self.cfg.obstypes.twentyfour_hour = LoopData.create_continuous_accum(
                '24h', self.cfg.unit_system, self.cfg.archive_interval, self.cfg.obstypes.twentyfour_hour, 86400, day_accum, dbm)
            ten_min_accum, self.cfg.obstypes.ten_min = LoopData.create_continuous_accum(
                '10m', self.cfg.unit_system, self.cfg.archive_interval, self.cfg.obstypes.ten_min, 600, day_accum, dbm)
            two_min_accum, self.cfg.obstypes.two_min = LoopData.create_continuous_accum(
                '2m', self.cfg.unit_system, self.cfg.archive_interval, self.cfg.obstypes.two_min, 120, day_accum, dbm)
            trend_accum, self.cfg.obstypes.trend = LoopData.create_continuous_accum(
                'trend', self.cfg.unit_system, self.cfg.archive_interval, self.cfg.obstypes.trend, self.cfg.time_delta, day_accum, dbm)
            self.cfg.queue.put(Accumulators(
                alltime_accum         = alltime_accum,
                rainyear_accum        = rainyear_accum,
                year_accum            = year_accum,
                month_accum           = month_accum,
                week_accum            = week_accum,
                day_accum             = day_accum,
                hour_accum            = hour_accum,
                twentyfour_hour_accum = twentyfour_hour_accum,
                ten_min_accum         = ten_min_accum,
                two_min_accum         = two_min_accum,
                trend_accum           = trend_accum))
        self.cfg.queue.put(event)

    @staticmethod
    def create_alltime_accum(unit_system: int, archive_interval: int, obstypes: Set[str], 
            day_accum: weewx.accum.Accum, dbm) -> Tuple[Optional[weewx.accum.Accum], Set[str]]:
        log.debug('Creating alltime_accum')
        # Pick a timespan such that all observations will be included
        # Span from Friday, January 2, 1970 12:00:00 AM UTC to January 1, 2525 12:00:00 AM UTC
        span = weeutil.weeutil.TimeSpan(86400, 17514144000)
        return LoopData.create_period_accum('alltime', unit_system, archive_interval, obstypes, span, day_accum, dbm)

    @staticmethod
    def create_rainyear_accum(unit_system: int, archive_interval: int, obstypes: Set[str], pkt_time: int,
            rainyear_start: int, day_accum: weewx.accum.Accum, dbm) -> Tuple[Optional[weewx.accum.Accum], Set[str]]:
        log.debug('Creating initial rainyear_accum')
        span = weeutil.weeutil.archiveRainYearSpan(pkt_time, rainyear_start)
        return LoopData.create_period_accum('rainyear', unit_system, archive_interval, obstypes, span, day_accum, dbm)

    @staticmethod
    def create_year_accum(unit_system: int, archive_interval: int, obstypes: Set[str], pkt_time: int, day_accum: weewx.accum.Accum, dbm
            ) -> Tuple[Optional[weewx.accum.Accum], Set[str]]:
        log.debug('Creating initial year_accum')
        span = weeutil.weeutil.archiveYearSpan(pkt_time)
        return LoopData.create_period_accum('year', unit_system, archive_interval, obstypes, span, day_accum, dbm)

    @staticmethod
    def create_month_accum(unit_system: int, archive_interval: int, obstypes: Set[str], pkt_time: int, day_accum: weewx.accum.Accum, dbm
            ) -> Tuple[Optional[weewx.accum.Accum], Set[str]]:
        log.debug('Creating initial month_accum')
        span = weeutil.weeutil.archiveMonthSpan(pkt_time)
        return LoopData.create_period_accum('month', unit_system, archive_interval, obstypes, span, day_accum, dbm)

    @staticmethod
    def create_week_accum(unit_system: int, archive_interval: int, obstypes: Set[str], pkt_time: int,
            week_start: int, day_accum: weewx.accum.Accum, dbm) -> Tuple[Optional[weewx.accum.Accum], Set[str]]:
        log.debug('Creating initial week_accum')
        span = weeutil.weeutil.archiveWeekSpan(pkt_time, week_start)
        return LoopData.create_period_accum('week', unit_system, archive_interval, obstypes, span, day_accum, dbm)

    @staticmethod
    def create_hour_accum(unit_system: int, archive_interval: int, obstypes: Set[str], pkt_time: int, day_accum: weewx.accum.Accum, dbm
            ) -> Tuple[Optional[weewx.accum.Accum], Set[str]]:
        log.debug('Creating initial hour_accum')
        span = weeutil.weeutil.archiveHoursAgoSpan(pkt_time)
        return LoopData.create_period_accum('hour', unit_system, archive_interval, obstypes, span, day_accum, dbm)

    @staticmethod
    def create_period_accum(name: str, unit_system: int, archive_interval: int, obstypes: Set[str],
            span: weeutil.weeutil.TimeSpan, day_accum: weewx.accum.Accum, dbm) -> Tuple[Optional[weewx.accum.Accum], Set[str]]:
        """return period accumulator and (possibly trimmed) obstypes"""

        if len(obstypes) == 0:
            return None, set()

        start = time.time()
        accum = weewx.accum.Accum(span, unit_system)

        # valid observation types will be returned
        valid_obstypes: Set[str] = set()

        # for each obstype, create the appropriate stats.
        for obstype in obstypes:
            stats: Optional[Any] = None
            if obstype not in day_accum:
                # Obstypes implemented with xtypes will fall out here.
                # As well as typos or any obstype that is not in day_accum.
                log.info('Ignoring %s for %s time period as this observation has no day accumulator.'
                    % (obstype, name))
                continue
            valid_obstypes.add(obstype)
            if type(day_accum[obstype]) == weewx.accum.ScalarStats:
                stats = weewx.accum.ScalarStats()
            elif type(day_accum[obstype]) == weewx.accum.VecStats:
                stats = weewx.accum.VecStats()
            elif type(day_accum[obstype]) == weewx.accum.FirstLastAccum:
                stats = weewx.accum.FirstLastAccum()
            else:
                return None, set()
            record_count = 0
            # For periods > day, accumulate from day summary records.
            # hour accumulator is handled by reading archive records (see below).
            if  name != 'hour':
                for record in LoopData.day_summary_records_generator(dbm, obstype, span.start):
                    record_count += 1
                    # TODO(jkline): From above, it appears that stats cannot be None.
                    if stats is None:
                        # Figure out the stats type
                        if 'squaresum' in record:
                            stats = weewx.accum.VecStats()
                        elif 'wsum' in record:
                            stats = weewx.accum.ScalarStats()
                        elif 'last' in record:
                            stats = weewx.accum.FirstLastAccum()
                        else:
                            return None, set()
                    if type(stats) == weewx.accum.ScalarStats:
                        sstat = weewx.accum.ScalarStats((record['min'], record['mintime'],
                            record['max'], record['maxtime'],
                            record['sum'], record['count'],
                            record['wsum'], record['sumtime']))
                        stats.mergeHiLo(sstat)
                        stats.mergeSum(sstat)
                    elif type(stats) == weewx.accum.VecStats:
                        vstat = weewx.accum.VecStats((record['min'], record['mintime'],
                            record['max'], record['maxtime'],
                            record['sum'], record['count'],
                            record['wsum'], record['sumtime'],
                            record['max_dir'], record['xsum'], record['ysum'],
                            record['dirsumtime'], record['squaresum'], record['wsquaresum']))
                        stats.mergeHiLo(vstat)
                        stats.mergeSum(vstat)
                    else:  # FirstLastAccum():
                        fstat = weewx.accum.FirstLastAccum((record['first'], record['firsttime'],
                            record['last'], record['lasttime']))
                        stats.mergeHiLo(fstat)
                        stats.mergeSum(fstat)
                # Add in today's stats
                stats.mergeHiLo(day_accum[obstype])
                stats.mergeSum(day_accum[obstype])
            accum[obstype] = stats

        if  name == 'hour':
            # Fetch archive records to prime the hour accumulator.
            earliest_time = span[0]
            start = time.time()
            pkt_count: int = 0
            archive_columns: List[str] = dbm.connection.columnsOf('archive')
            archive_pkts: List[Dict[str, Any]] = LoopData.get_archive_packets(
                dbm, archive_columns, earliest_time)
            for pkt in archive_pkts:
                pkt['usUnits'] = unit_system
                pruned_pkt = LoopProcessor.prune_period_packet(pkt, obstypes)
                accum.addRecord(pruned_pkt, weight=archive_interval * 60)
                pkt_count += 1
            log.debug('Primed hour_accum with %d archive packets in %f seconds.' % (pkt_count, time.time() - start))

        log.debug('Created %s accum in %f seconds (read %d records).' % (name, time.time() - start, record_count))
        return accum, valid_obstypes

    @staticmethod
    def create_continuous_accum(name: str, unit_system: int, archive_interval: int, obstypes: Set[str],
            timelength, day_accum: weewx.accum.Accum, dbm) -> Tuple[Optional[ContinuousAccum], Set[str]]:
        """return continously accumulator and (possibly trimmed) obstypes"""

        if len(obstypes) == 0:
            return None, set()

        accum = ContinuousAccum(timelength, unit_system)

        # valid observation types will be returned
        valid_obstypes: Set[str] = set()

        # for each obstype, create the appropriate stats.
        for obstype in obstypes:
            stats: Optional[Any] = None
            if obstype not in day_accum:
                # Obstypes implemented with xtypes will fall out here.
                # As well as typos or any obstype that is not in day_accum.
                log.info('Ignoring %s for %s time period as this observation has no day accumulator.'
                    % (obstype, name))
                continue
            valid_obstypes.add(obstype)
            if type(day_accum[obstype]) == weewx.accum.ScalarStats:
                stats = ContinuousScalarStats(timelength)
            elif type(day_accum[obstype]) == weewx.accum.VecStats:
                stats = ContinuousVecStats(timelength)
            elif type(day_accum[obstype]) == weewx.accum.FirstLastAccum:
                stats = ContinuousFirstLastAccum(timelength)
            else:
                return None, set()
            accum[obstype] = stats

        # Fetch archive records to prime the accumulator.
        start = time.time()
        earliest_time = start - timelength
        pkt_count: int = 0
        archive_columns: List[str] = dbm.connection.columnsOf('archive')
        archive_pkts: List[Dict[str, Any]] = LoopData.get_archive_packets(
            dbm, archive_columns, earliest_time)
        for pkt in archive_pkts:
            pkt['usUnits'] = unit_system
            pruned_pkt = LoopProcessor.prune_period_packet(pkt, obstypes)
            accum.addRecord(pruned_pkt, weight=archive_interval * 60)
            pkt_count += 1
        log.debug('Primed ContinousAccum(%s) with %d archive packets in %f seconds.' % (name, pkt_count, time.time() - start))

        log.debug('Created %s accum in %f seconds (read %d records).' % (name, time.time() - start, pkt_count))
        return accum, valid_obstypes

    @staticmethod
    def parse_cname(field: str) -> Optional[CheetahName]:
        valid_prefixes    : List[str] = [ 'unit' ]
        valid_prefixes2   : List[str] = [ 'label' ]
        valid_periods     : List[str] = [ 'alltime', 'rainyear', 'year', 'month', 'week',
                                          'current', 'hour', '2m', '10m', '24h', 'day',
                                          'trend' ]
        valid_agg_types   : List[str] = [ 'max', 'min', 'maxtime', 'mintime',
                                          'gustdir', 'avg', 'sum', 'vecavg',
                                          'vecdir', 'rms' ]
        valid_format_specs: List[str] = [ 'formatted', 'raw', 'ordinal_compass',
                                          'desc', 'code' ]

        segment: List[str] = field.split('.')
        if len(segment) < 2:
            return None

        next_seg = 0

        prefix = None
        prefix2 = None
        if segment[next_seg] in valid_prefixes:
            prefix = segment[next_seg]
            next_seg += 1
            if segment[next_seg] in valid_prefixes2:
                prefix2 = segment[next_seg]
                next_seg += 1
            else:
                return None

        period = None
        if prefix is None: # All but $unit must have a period.
            if len(segment) < next_seg:
                return None
            if segment[next_seg] in valid_periods:
                period = segment[next_seg]
                next_seg += 1
            else:
                return  None

        if len(segment) < next_seg:
            # need an obstype, but none there
            return None
        obstype = segment[next_seg]
        next_seg += 1

        agg_type = None
        # 2m/10m/24h/hour/day/week/month/year/rainyear/alltime must have an agg_type
        if period in [ '2m', '10m', '24h', 'hour', 'day', 'week','month', 'year', 'rainyear', 'alltime' ]:
            if len(segment) <= next_seg:
                return None
            if segment[next_seg] not in valid_agg_types:
                return None
            agg_type = segment[next_seg]
            next_seg += 1

        format_spec = None
        # check for a format spec
        if prefix is None and len(segment) > next_seg:
            if segment[next_seg] in valid_format_specs:
                format_spec = segment[next_seg]
                next_seg += 1

        # windrun_<dir> is not supported for week, month, year, rainyear and alltime
        if obstype.startswith('windrun_') and (
                period == 'week' or period == 'month' or period == 'year' or period == 'rainyear' or period == 'alltime'):
            return None

        if len(segment) > next_seg:
            # There is more.  This is unexpected.
            return None

        return CheetahName(
            field       = field,
            prefix      = prefix,
            prefix2     = prefix2,
            period      = period,
            obstype     = obstype,
            agg_type    = agg_type,
            format_spec = format_spec)

class LoopProcessor:
    def __init__(self, cfg: Configuration):
        self.cfg = cfg
        self.archive_start: float = time.time()

    def process_queue(self) -> None:
        try:
            while True:
                event               = self.cfg.queue.get()

                if type(event) == Accumulators:
                    LoopProcessor.log_configuration(self.cfg)
                    self.accumulators: Accumulators = event
                    continue

                # This is a loop packet.
                assert event.event_type == weewx.NEW_LOOP_PACKET

                pkt: Dict[str, Any] = event.packet
                pkt_time: int       = to_int(pkt['dateTime'])
                pkt['interval']     = self.cfg.loop_frequency / 60.0

                log.debug('Dequeued loop event(%s): %s' % (event, timestamp_to_string(pkt_time)))
                log.debug(pkt)

                try:
                    windrun_val = weewx.wxxtypes.WXXTypes.calc_windrun('windrun', pkt)
                    pkt['windrun'] = windrun_val[0]
                    if windrun_val[0] > 0.00 and 'windDir' in pkt and pkt['windDir'] is not None:
                        bkt = LoopProcessor.get_windrun_bucket(pkt['windDir'])
                        pkt['windrun_%s' % windrun_bucket_suffixes[bkt]] = windrun_val[0]
                except weewx.CannotCalculate:
                    log.info('Cannot calculate windrun.')
                    pass

                try:
                    beaufort_val = weewx.wxxtypes.WXXTypes.calc_beaufort('beaufort', pkt)
                    pkt['beaufort'] = beaufort_val[0]
                except weewx.CannotCalculate:
                    log.info('Cannot calculate beaufort.')
                    pass

                # Process new packet.
                loopdata_pkt = LoopProcessor.generate_loopdata_dictionary(pkt, self.cfg, self.accumulators)
                # Write the loop-data.txt file.
                LoopProcessor.write_packet_to_file(loopdata_pkt,
                    self.cfg.tmpname, self.cfg.loop_data_dir, self.cfg.filename)
                if self.cfg.enable:
                    # Rsync the loop-data.txt file.
                    LoopProcessor.rsync_data(pkt_time,
                        self.cfg.skip_if_older_than, self.cfg.loop_data_dir,
                        self.cfg.filename, self.cfg.remote_dir,
                        self.cfg.remote_server, self.cfg.remote_port,
                        self.cfg.timeout, self.cfg.remote_user,
                        self.cfg.ssh_options, self.cfg.compress,
                        self.cfg.log_success)
        except Exception:
            weeutil.logger.log_traceback(log.critical, "    ****  ")
            raise
        finally:
            os.unlink(self.cfg.tmpname)

    @staticmethod
    def generate_loopdata_dictionary(in_pkt: Dict[str, Any], cfg: Configuration, accums: Accumulators) -> Dict[str, Any]:

        # pkt needs to be in the units that the accumulators are expecting.
        pruned_pkt = LoopProcessor.prune_period_packet(in_pkt, cfg.obstypes.current)
        pkt = weewx.units.StdUnitConverters[cfg.unit_system].convertDict(pruned_pkt)
        pkt['usUnits'] = cfg.unit_system

        # Add packet to alltime accumulator.
        # There will never be an OutOfSpan exception.
        if len(cfg.obstypes.alltime) > 0 and accums.alltime_accum is not None:
            pruned_pkt = LoopProcessor.prune_period_packet(pkt, cfg.obstypes.alltime)
            accums.alltime_accum.addRecord(pruned_pkt, weight=cfg.loop_frequency)

        # Add packet to rainyear accumulator.
        try:
            if len(cfg.obstypes.rainyear) > 0 and accums.rainyear_accum is not None:
                pruned_pkt = LoopProcessor.prune_period_packet(pkt, cfg.obstypes.rainyear)
                accums.rainyear_accum.addRecord(pruned_pkt, weight=cfg.loop_frequency)
        except weewx.accum.OutOfSpan:
            timespan = weeutil.weeutil.archiveRainYearSpan(pkt['dateTime'], cfg.rainyear_start)
            accums.rainyear_accum = weewx.accum.Accum(timespan, unit_system=cfg.unit_system)
            # Try again:
            accums.rainyear_accum.addRecord(pkt, weight=cfg.loop_frequency)

        # Add packet to year accumulator.
        try:
            if len(cfg.obstypes.year) > 0 and accums.year_accum is not None:
                pruned_pkt = LoopProcessor.prune_period_packet(pkt, cfg.obstypes.year)
                accums.year_accum.addRecord(pruned_pkt, weight=cfg.loop_frequency)
        except weewx.accum.OutOfSpan:
            timespan = weeutil.weeutil.archiveYearSpan(pkt['dateTime'])
            accums.year_accum = weewx.accum.Accum(timespan, unit_system=cfg.unit_system)
            # Try again:
            accums.year_accum.addRecord(pkt, weight=cfg.loop_frequency)

        # Add packet to month accumulator.
        try:
            if len(cfg.obstypes.month) > 0 and accums.month_accum is not None:
                pruned_pkt = LoopProcessor.prune_period_packet(pkt, cfg.obstypes.month)
                accums.month_accum.addRecord(pruned_pkt, weight=cfg.loop_frequency)
        except weewx.accum.OutOfSpan:
            timespan = weeutil.weeutil.archiveMonthSpan(pkt['dateTime'])
            accums.month_accum = weewx.accum.Accum(timespan, unit_system=cfg.unit_system)
            # Try again:
            accums.month_accum.addRecord(pkt, weight=cfg.loop_frequency)

        # Add packet to week accumulator.
        try:
            if len(cfg.obstypes.week) > 0 and accums.week_accum is not None:
                pruned_pkt = LoopProcessor.prune_period_packet(pkt, cfg.obstypes.week)
                accums.week_accum.addRecord(pruned_pkt, weight=cfg.loop_frequency)
        except weewx.accum.OutOfSpan:
            timespan = weeutil.weeutil.archiveWeekSpan(pkt['dateTime'], cfg.week_start)
            accums.week_accum = weewx.accum.Accum(timespan, unit_system=cfg.unit_system)
            # Try again:
            accums.week_accum.addRecord(pkt, weight=cfg.loop_frequency)

        # Add packet to day accumulator.
        try:
            if len(cfg.obstypes.day) > 0:
                pruned_pkt = LoopProcessor.prune_period_packet(pkt, cfg.obstypes.day)
                accums.day_accum.addRecord(pruned_pkt, weight=cfg.loop_frequency)
        except weewx.accum.OutOfSpan:
            timespan = weeutil.weeutil.archiveDaySpan(pkt['dateTime'])
            accums.day_accum = weewx.accum.Accum(timespan, unit_system=cfg.unit_system)
            # Try again:
            accums.day_accum.addRecord(pkt, weight=cfg.loop_frequency)

        # Add packet to hour accumulator.
        try:
            if accums.hour_accum is not None:
                pruned_pkt = LoopProcessor.prune_period_packet(pkt, cfg.obstypes.hour)
                accums.hour_accum.addRecord(pruned_pkt, weight=cfg.loop_frequency)
        except weewx.accum.OutOfSpan:
            timespan = weeutil.weeutil.archiveHoursAgoSpan(pkt['dateTime'])
            accums.hour_accum = weewx.accum.Accum(timespan, unit_system=cfg.unit_system)
            # Try again:
            accums.hour_accum.addRecord(pkt, weight=cfg.loop_frequency)

        # Add packet to 24h accumulator.
        if accums.twentyfour_hour_accum is not None:
            pruned_pkt = LoopProcessor.prune_period_packet(pkt, cfg.obstypes.twentyfour_hour)
            accums.twentyfour_hour_accum.addRecord(pruned_pkt, weight=cfg.loop_frequency)

        # Add packet to 10m accumulator.
        if accums.ten_min_accum is not None:
            pruned_pkt = LoopProcessor.prune_period_packet(pkt, cfg.obstypes.ten_min)
            accums.ten_min_accum.addRecord(pruned_pkt, weight=cfg.loop_frequency)

        # Add packet to 2m accumulator.
        if accums.two_min_accum is not None:
            pruned_pkt = LoopProcessor.prune_period_packet(pkt, cfg.obstypes.two_min)
            accums.two_min_accum.addRecord(pruned_pkt, weight=cfg.loop_frequency)

        # Add packet to trend accumulator.
        if accums.trend_accum is not None:
            pruned_pkt = LoopProcessor.prune_period_packet(pkt, cfg.obstypes.trend)
            accums.trend_accum.addRecord(pruned_pkt, weight=cfg.loop_frequency)

        # Create the loopdata dictionary.
        return LoopProcessor.create_loopdata_packet(pkt, cfg, accums)

    @staticmethod
    def add_unit_obstype(cname: CheetahName, loopdata_pkt: Dict[str, Any],
            converter: weewx.units.Converter,
            formatter: weewx.units.Formatter) -> None:

        if cname.prefix2 == 'label':
            # agg_type not allowed
            # tgt_type, tgt_group = converter.getTargetUnit(cname.obstype, agg_type=cname.agg_type)
            tgt_type, tgt_group = converter.getTargetUnit(cname.obstype)
            loopdata_pkt[cname.field] = formatter.get_label_string(tgt_type)

    @staticmethod
    def add_current_obstype(cname: CheetahName, pkt: Dict[str, Any],
            loopdata_pkt: Dict[str, Any], converter: weewx.units.Converter,
            formatter: weewx.units.Formatter) -> None:

        if cname.obstype not in pkt:
            log.debug('%s not found in packet, skipping %s' % (cname.obstype, cname.field))
            return

        value, unit_type, group_type = LoopProcessor.convert_current_obs(
                converter, cname.obstype, pkt)

        if value is None:
            log.debug('%s not found in loop packet.' % cname.field)
            return

        if cname.format_spec == 'ordinal_compass':
            loopdata_pkt[cname.field] = formatter.to_ordinal_compass(
                (value, unit_type, group_type))
            return

        if cname.format_spec == 'formatted':
            fmt_str = formatter.get_format_string(unit_type)
            try:
                loopdata_pkt[cname.field] = fmt_str % value
            except Exception as e:
                log.debug('%s: %s, %s, %s' % (e, cname.field, fmt_str, value))
            return

        if cname.format_spec == 'raw':
            loopdata_pkt[cname.field] = value
            return

        loopdata_pkt[cname.field] = formatter.toString((value, unit_type, group_type))

    @staticmethod
    def add_period_obstype(cname: CheetahName, period_accum: Union[weewx.accum.Accum, ContinuousAccum],
            loopdata_pkt: Dict[str, Any], converter: weewx.units.Converter,
            formatter: weewx.units.Formatter) -> None:
        if cname.obstype not in period_accum:
            log.debug('No %s stats for %s, skipping %s' % (cname.period, cname.obstype, cname.field))
            return

        stats = period_accum[cname.obstype]

        if (isinstance(stats, weewx.accum.ScalarStats) or isinstance(stats, ContinuousScalarStats))  and stats.lasttime is not None:
            min, mintime, max, maxtime, sum, count, wsum, sumtime = stats.getStatsTuple()
            if cname.agg_type == 'min':
                src_value = min
            elif cname.agg_type == 'mintime':
                src_value = mintime
            elif cname.agg_type == 'max':
                src_value = max
            elif cname.agg_type == 'maxtime':
                src_value = maxtime
            elif cname.agg_type == 'sum':
                src_value = sum
            elif cname.agg_type == 'avg':
                src_value = stats.avg
            else:
                return

        elif (isinstance(stats, weewx.accum.VecStats) or isinstance(stats, ContinuousVecStats)) and stats.count != 0:
            min, mintime, max, maxtime, sum, count, wsum, sumtime, max_dir, xsum, ysum, dirsumtime, squaresum, wsquaresum = stats.getStatsTuple()
            if cname.agg_type == 'maxtime':
                src_value = maxtime
            elif cname.agg_type == 'max':
                src_value = max
            elif cname.agg_type == 'gustdir':
                src_value = max_dir
            elif cname.agg_type == 'mintime':
                src_value = mintime
            elif cname.agg_type == 'min':
                src_value = min
            elif cname.agg_type == 'count':
                src_value = count
            elif cname.agg_type == 'avg':
                src_value = stats.avg
            elif cname.agg_type == 'sum':
                src_value = stats.sum
            elif cname.agg_type == 'rms':
                src_value = stats.rms
            elif cname.agg_type == 'vecavg':
                src_value = stats.vec_avg
            elif cname.agg_type == 'vecdir':
                src_value = stats.vec_dir
            else:
                return

        else:
            # firstlast not currently supported
            return

        if src_value is None:
            log.debug('Currently no %s stats for %s.' % (cname.period, cname.field))
            return

        src_type, src_group = weewx.units.getStandardUnitType(period_accum.unit_system, cname.obstype, agg_type=cname.agg_type)

        tgt_value, tgt_type, tgt_group = converter.convert((src_value, src_type, src_group))

        if cname.format_spec == 'ordinal_compass':
            loopdata_pkt[cname.field] = formatter.to_ordinal_compass(
                (tgt_value, tgt_type, tgt_group))
            return

        if cname.format_spec == 'formatted':
            fmt_str = formatter.get_format_string(tgt_type)
            try:
                loopdata_pkt[cname.field] = fmt_str % tgt_value
            except Exception as e:
                log.debug('%s: %s, %s, %s' % (e, cname.field, fmt_str, tgt_value))
            return

        if cname.format_spec == 'raw':
            loopdata_pkt[cname.field] = tgt_value
            return

        loopdata_pkt[cname.field] = formatter.toString((tgt_value, tgt_type, tgt_group))

    @staticmethod
    def add_trend_obstype(cname: CheetahName, trend_accum: ContinuousAccum,
            pkt: Dict[str, Any], loopdata_pkt: Dict[str, Any], time_delta: int,
            baro_trend_descs, converter: weewx.units.Converter,
            formatter: weewx.units.Formatter) -> None:

        value, unit_type, group_type = LoopProcessor.get_trend(cname, pkt, trend_accum, converter, time_delta)
        if value is None:
            log.debug('add_trend_obstype: %s: get_trend returned None.' % cname.field)
            return

        if cname.obstype == 'barometer' and (cname.format_spec == 'code' or cname.format_spec == 'desc'):
            baroTrend: BarometerTrend = LoopProcessor.get_barometer_trend(value, unit_type, group_type, time_delta)
            if cname.format_spec == 'code':
                loopdata_pkt[cname.field] = baroTrend.value
            else: # cname.format_spec == 'desc':
                loopdata_pkt[cname.field] = baro_trend_descs[baroTrend]
            return
        elif cname.format_spec == 'code' or cname.format_spec == 'desc':
            # code and desc are only supported for trend.barometer
            return

        if cname.format_spec == 'formatted':
            fmt_str = formatter.get_format_string(unit_type)
            try:
                loopdata_pkt[cname.field] = fmt_str % value
            except Exception as e:
                log.debug('%s: %s, %s, %s' % (e, cname.field, fmt_str, value))
            return

        if cname.format_spec == 'raw':
            loopdata_pkt[cname.field] = value
            return

        loopdata_pkt[cname.field] = formatter.toString((value, unit_type, group_type))


    @staticmethod
    def convert_current_obs(converter: weewx.units.Converter, obstype: str,
            pkt: Dict[str, Any]) -> Tuple[Any, Any, Any]:
        """ Returns value, format_str, label_str """

        v_t = weewx.units.as_value_tuple(pkt, obstype)
        _, original_unit_type, original_group_type = v_t
        value, unit_type, group_type = converter.convert(v_t)

        return value, unit_type, group_type

    @staticmethod
    def create_loopdata_packet(pkt: Dict[str, Any], cfg: Configuration, accums: Accumulators) -> Dict[str, Any]:

        loopdata_pkt: Dict[str, Any] = {}

        # Iterate through fields.
        for cname in cfg.fields_to_include:
            if cname is None:
                continue
            if cname.prefix == 'unit':
                LoopProcessor.add_unit_obstype(cname, loopdata_pkt, cfg.converter, cfg.formatter)
                continue
            if cname.period == 'current':
                LoopProcessor.add_current_obstype(cname, pkt, loopdata_pkt, cfg.converter, cfg.formatter)
                continue
            if cname.period == 'trend' and accums.trend_accum is not None:
                LoopProcessor.add_trend_obstype(cname, accums.trend_accum, pkt,
                    loopdata_pkt, cfg.time_delta, cfg.baro_trend_descs, cfg.converter, cfg.formatter)
                continue
            if cname.period == 'alltime' and accums.alltime_accum is not None:
                LoopProcessor.add_period_obstype(cname, accums.alltime_accum, loopdata_pkt, cfg.converter, cfg.formatter)
                continue
            if cname.period == 'rainyear' and accums.rainyear_accum is not None:
                LoopProcessor.add_period_obstype(cname, accums.rainyear_accum, loopdata_pkt, cfg.converter, cfg.formatter)
                continue
            if cname.period == 'year' and accums.year_accum is not None:
                LoopProcessor.add_period_obstype(cname, accums.year_accum, loopdata_pkt, cfg.converter, cfg.formatter)
                continue
            if cname.period == 'month' and accums.month_accum is not None:
                LoopProcessor.add_period_obstype(cname, accums.month_accum, loopdata_pkt, cfg.converter, cfg.formatter)
                continue
            if cname.period == 'week' and accums.week_accum is not None:
                LoopProcessor.add_period_obstype(cname, accums.week_accum, loopdata_pkt, cfg.converter, cfg.formatter)
                continue
            if cname.period == 'day':
                LoopProcessor.add_period_obstype(cname, accums.day_accum, loopdata_pkt, cfg.converter, cfg.formatter)
                continue
            if cname.period == 'hour' and accums.hour_accum is not None:
                LoopProcessor.add_period_obstype(cname, accums.hour_accum, loopdata_pkt, cfg.converter, cfg.formatter)
                continue
            if cname.period == '24h' and accums.twentyfour_hour_accum is not None:
                LoopProcessor.add_period_obstype(cname,  accums.twentyfour_hour_accum, loopdata_pkt, cfg.converter, cfg.formatter)
                continue
            if cname.period == '10m' and accums.ten_min_accum is not None:
                LoopProcessor.add_period_obstype(cname,  accums.ten_min_accum, loopdata_pkt, cfg.converter, cfg.formatter)
                continue
            if cname.period == '2m' and accums.two_min_accum is not None:
                LoopProcessor.add_period_obstype(cname, accums.two_min_accum, loopdata_pkt, cfg.converter, cfg.formatter)
                continue

        return loopdata_pkt

    @staticmethod
    def write_packet_to_file(selective_pkt: Dict[str, Any], tmpname: str,
            loop_data_dir: str, filename: str) -> None:
        log.debug('Writing packet to %s' % tmpname)
        with open(tmpname, "w") as f:
            f.write(json.dumps(selective_pkt))
            f.flush()
            os.fsync(f.fileno())
        log.debug('Wrote to %s' % tmpname)
        # move it to filename
        shutil.move(tmpname, os.path.join(loop_data_dir, filename))
        log.debug('Moved to %s' % os.path.join(loop_data_dir, filename))

    @staticmethod
    def log_configuration(cfg: Configuration) -> None:
        # queue
        # config_dict
        log.info('unit_system             : %d' % cfg.unit_system)
        log.info('archive_interval        : %d' % cfg.archive_interval)
        log.info('loop_data_dir           : %s' % cfg.loop_data_dir)
        log.info('filename                : %s' % cfg.filename)
        log.info('target_report           : %s' % cfg.target_report)
        log.info('loop_frequency          : %s' % cfg.loop_frequency)
        log.info('specified_fields        : %s' % cfg.specified_fields)
        # fields_to_include
        # formatter
        # converter
        log.info('tmpname                 : %s' % cfg.tmpname)
        log.info('enable                  : %d' % cfg.enable)
        log.info('remote_server           : %s' % cfg.remote_server)
        log.info('remote_port             : %r' % cfg.remote_port)
        log.info('remote_user             : %s' % cfg.remote_user)
        log.info('remote_dir              : %s' % cfg.remote_dir)
        log.info('compress                : %d' % cfg.compress)
        log.info('log_success             : %d' % cfg.log_success)
        log.info('ssh_options             : %s' % cfg.ssh_options)
        log.info('timeout                 : %d' % cfg.timeout)
        log.info('skip_if_older_than      : %d' % cfg.skip_if_older_than)
        log.info('time_delta              : %d' % cfg.time_delta)
        log.info('week_start              : %d' % cfg.week_start)
        log.info('rainyear_start          : %d' % cfg.rainyear_start)
        log.info('obstypes.trend          : %s' % cfg.obstypes.trend)
        log.info('obstypes.alltime        : %s' % cfg.obstypes.alltime)
        log.info('obstypes.rainyear       : %s' % cfg.obstypes.rainyear)
        log.info('obstypes.year           : %s' % cfg.obstypes.year)
        log.info('obstypes.month          : %s' % cfg.obstypes.month)
        log.info('obstypes.week           : %s' % cfg.obstypes.week)
        log.info('obstypes.day            : %s' % cfg.obstypes.day)
        log.info('obstypes.hour           : %s' % cfg.obstypes.hour)
        log.info('obstypes.twentyfour_hour: %s' % cfg.obstypes.twentyfour_hour)
        log.info('obstypes.ten_min        : %s' % cfg.obstypes.ten_min)
        log.info('obstypes.two_min        : %s' % cfg.obstypes.two_min)
        log.info('baro_trend_descs        : %s' % cfg.baro_trend_descs)

    @staticmethod
    def rsync_data(pktTime: int, skip_if_older_than: int, loop_data_dir: str,
            filename: str, remote_dir: str, remote_server: str,
            remote_port: int, timeout: int, remote_user: str, ssh_options: str,
            compress: bool, log_success: bool) -> None:
        log.debug('rsync_data(%d) start' % pktTime)
        # Don't upload if more than skip_if_older_than seconds behind.
        if skip_if_older_than != 0:
            age = time.time() - pktTime
            if age > skip_if_older_than:
                log.info('skipping packet (%s) with age: %f' % (timestamp_to_string(pktTime), age))
                return
        rsync_upload = weeutil.rsyncupload.RsyncUpload(
            local_root= os.path.join(loop_data_dir, filename),
            remote_root = os.path.join(remote_dir, filename),
            server=remote_server,
            user=remote_user,
            port=str(remote_port) if remote_port is not None else None,
            ssh_options=ssh_options,
            compress=compress,
            delete=False,
            log_success=log_success,
            timeout=timeout)
        try:
            rsync_upload.run()
        except IOError as e:
            (cl, unused_ob, unused_tr) = sys.exc_info()
            log.error("rsync_data: Caught exception %s: %s" % (cl, e))

    @staticmethod
    def get_barometer_trend(value, unit_type, group_type, time_delta: int) -> BarometerTrend:

        # Forecast descriptions for the 3 hour change in barometer readings.
        # Falling (or rising) slowly: 0.1 - 1.5mb in 3 hours
        # Falling (or rising): 1.6 - 3.5mb in 3 hours
        # Falling (or rising) quickly: 3.6 - 6.0mb in 3 hours
        # Falling (or rising) very rapidly: More than 6.0mb in 3 hours

        # Convert to mbars as that is the standard we have for descriptions.
        converter = weewx.units.Converter(weewx.units.MetricUnits)
        delta_mbar, _, _ = converter.convert((value, unit_type, group_type))
        log.debug('Converted to mbar/h: %f' % delta_mbar)

        # Normalize to three hours.
        delta_three_hours = time_delta / 10800.0
        delta_mbar = delta_mbar / delta_three_hours

        if delta_mbar > 6.0:
            baroTrend = BarometerTrend.RISING_VERY_RAPIDLY
        elif delta_mbar > 3.5:
            baroTrend = BarometerTrend.RISING_QUICKLY
        elif delta_mbar > 1.5:
            baroTrend = BarometerTrend.RISING
        elif delta_mbar >= 0.1:
            baroTrend = BarometerTrend.RISING_SLOWLY
        elif delta_mbar > -0.1:
            baroTrend = BarometerTrend.STEADY
        elif delta_mbar >= -1.5:
            baroTrend = BarometerTrend.FALLING_SLOWLY
        elif delta_mbar >= -3.5:
            baroTrend = BarometerTrend.FALLING
        elif delta_mbar >= -6.0:
            baroTrend = BarometerTrend.FALLING_QUICKLY
        else:
            baroTrend = BarometerTrend.FALLING_VERY_RAPIDLY

        return baroTrend

    @staticmethod
    def get_trend(cname: CheetahName, pkt: Dict[str, Any], trend_accum: ContinuousAccum,
            converter, time_delta: int) -> Tuple[Optional[Any], Optional[str], Optional[str]]:
        first = trend_accum[cname.obstype].first
        firsttime = trend_accum[cname.obstype].firsttime
        last = trend_accum[cname.obstype].last
        lasttime = trend_accum[cname.obstype].lasttime
        if first is None or last is None:
            return None, None, None
        if firsttime == lasttime:
            # Need atleast two readings to get a trend.
            return None, None, None
        try:
            # Trend needs to be in report target units.
            start_value, unit_type, group_type = LoopProcessor.convert_current_obs(
                converter, cname.obstype, { 'dateTime': firsttime, 'usUnits': pkt['usUnits'], cname.obstype: first })
            end_value, unit_type, group_type = LoopProcessor.convert_current_obs(
                converter, cname.obstype, { 'dateTime': lasttime, 'usUnits': pkt['usUnits'], cname.obstype: last })

            log.debug('get_trend: %s: start_value: %s' % (cname.obstype, start_value))
            log.debug('get_trend: %s: end_value: %s' % (cname.obstype, end_value))
            if start_value is not None and end_value is not None:
                trend = end_value - start_value
                # This may not be over the entire range of time_delta (e.g., new station startup)
                # Adjust to spread over entire range.
                actual_time_delta = lasttime - firsttime
                adj_trend = time_delta / actual_time_delta * trend
                log.debug('get_trend: %s: %s unadjusted(%s)' % (cname.obstype, adj_trend, trend))
                return adj_trend, unit_type, group_type
        except:
            # Perhaps not a scalar value
            log.debug('Could not compute trend for %s' % cname.obstype)

        return None, None, None

    @staticmethod
    def prune_period_packet(pkt: Dict[str, Any], in_use_obstypes: Set[str]
            ) -> Dict[str, Any]:
        # Prune to only the observations needed.
        new_pkt: Dict[str, Any] = {}
        new_pkt['dateTime'] = pkt['dateTime']
        new_pkt['usUnits'] = pkt['usUnits']
        if 'interval' in pkt:
            # Probably not needed.
            new_pkt['interval'] = pkt['interval']
        for obstype in in_use_obstypes:
            if obstype in pkt:
                new_pkt[obstype] = pkt[obstype]
        return new_pkt

    @staticmethod
    def get_windrun_bucket(wind_dir: float) -> int:
        bucket_count = len(windrun_bucket_suffixes)
        slice_size: float = 360.0 / bucket_count
        bucket: int = to_int((wind_dir + slice_size / 2.0) / slice_size)
        if bucket >= bucket_count:
            bucket = 0
        log.debug('get_windrun_bucket: wind_dir: %d, bucket: %d' % (wind_dir, bucket))
        return bucket
