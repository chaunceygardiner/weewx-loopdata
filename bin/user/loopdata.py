"""
loopdata.py

Copyright (C)2022-2026 by John A Kline (john@johnkline.com)
Distributed under the terms of the GNU Public License (GPLv3)

LoopData is a WeeWX service that generates a json file (loop-data.txt)
containing values for the observations in the loop packet; along with
today's high, low, sum, average and weighted averages for each observation
in the packet.
"""

import ast
import copy
import configobj
import itertools
import json
import logging
import math
import os
import queue
import re
import sys
import tempfile
import threading
import time

from collections import deque, namedtuple
from datetime import date, datetime, timedelta
from heapq import heapify, heappop, heappush
from dataclasses import dataclass, field as dataclass_field
from typing import Any, Callable, Deque, Dict, FrozenSet, Generator, Generic, List, Optional, Set, Tuple, TypeVar, Union
from enum import Enum

import weewx
import weewx.almanac
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

LOOP_DATA_VERSION = '5.1'

if sys.version_info[0] < 3 or (sys.version_info[0] == 3 and sys.version_info[1] < 7):
    raise weewx.UnsupportedFeature(
        "weewx-loopdata requires Python 3.7 or later, found %s.%s" % (sys.version_info[0], sys.version_info[1]))

if weewx.__version__ < "4":
    raise weewx.UnsupportedFeature(
        "weewx-loopdata requires WeeWX 4, found %s" % weewx.__version__)

windrun_bucket_suffixes: List[str] = [ 'N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE',
                                       'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW' ]

# Set up windrun_<dir> observation types.
for suffix in windrun_bucket_suffixes:
    weewx.units.obs_group_dict['windrun_%s' % suffix] = 'group_distance'

def reraise_if_terminate(e: BaseException) -> None:
    """weewxd stops by raising Terminate from its SIGTERM signal handler --
    inside whatever the main thread is executing at that instant.  Every
    broad exception handler on a main-thread path must call this first and
    hand the exception back, or weewx cannot shut down.  weewxd runs as
    __main__, so its Terminate class cannot be imported here and is
    recognized by name."""
    if type(e).__name__ == 'Terminate':
        raise e

@dataclass
class CheetahName:
    field      : str           # $day.outTemp.avg.formatted
    prefix     : Optional[str] # unit or None
    prefix2    : Optional[str] # label or None
    period     : Optional[str] # current, 1m-1440m, 1h-24h, trend, hour, day, week, month, year, rainyear, alltime
    obstype    : str           # e.g,. outTemp
    agg_type   : Optional[str] # avg, sum, etc. (required if period, other than current, is specified, else None)
    unit       : Optional[str] # unit override (e.g. degree_C, beaufort); None means the target report's unit for the obstype's group.  Grammar-ordered between agg_type and format_spec.  Value fields only -- never on the unit.label prefix form (WeeWX parity: $unit.label has no override).
    format_spec: Optional[str] # formatted (formatted value sans label), raw or ordinal_compass (could be on direction), a call spec (format/nolabel/string/long_form), or None
    format_kwargs: Optional[Dict[str, Any]] = None # call-syntax specs only: the call's arguments, positionals bound to the ValueHelper method's parameter names; None for bare specs
    def __hash__(self):
        return hash(self.field)

@dataclass
class AlmanacSegment:
    """One dotted segment of an almanac field's attribute chain, e.g. the
    'sun(use_center=1)' in almanac(horizon=-6).sun(use_center=1).rise.
    kwargs is None for a plain attribute, a (possibly empty) dict when the
    segment carries a call suffix."""
    name  : str
    kwargs: Optional[Dict[str, float]]

@dataclass
class AlmanacField:
    """A parsed almanac entry from the fields line.  The grammar is a WeeWX
    report almanac tag with the $ removed (almanac.sunrise.raw,
    almanac(horizon=-6).sun(use_center=1).rise, ...), plus the loopdata
    extension almanac(days=±N) meaning "same wall-clock time N local calendar
    days away".  tier drives the evaluator's caching: 'continuous' fields are
    recomputed every packet, 'day' fields once per local day, 'event' fields
    are kept until the local day advances past the cached event."""
    field         : str                  # almanac(horizon=-6).sun(use_center=1).rise.raw
    almanac_kwargs: Dict[str, float]     # kwargs of the leading almanac segment (days removed)
    days          : int                  # local calendar-day shift (almanac(days=±N))
    chain         : List[AlmanacSegment] # attribute chain after the leading almanac segment
    format_spec   : Optional[str]        # formatted, raw, ordinal_compass, a call spec (format/nolabel/string/long_form), or None
    tier          : str                  # continuous, day or event
    format_kwargs : Optional[Dict[str, Any]] = None # call-syntax specs only (see CheetahName.format_kwargs)
    def __hash__(self):
        return hash(self.field)

@dataclass
class ObsTypes:
    current         : Set[str]
    alltime         : Set[str]
    rainyear        : Set[str]
    year            : Set[str]
    month           : Set[str]
    week            : Set[str]
    day             : Set[str]
    hour            : Set[str]
    continuous      : Dict[str, Set[str]] # e.g., continuous['24h'], or ['trend']

@dataclass
class Configuration:
    queue                    : queue.SimpleQueue
    config_dict              : Dict[str, Any]
    unit_system              : int
    archive_interval         : int
    archive_delay            : int
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
    almanac_fields           : List[AlmanacField] = dataclass_field(default_factory=list)
    latitude                 : float = 0.0 # station latitude in decimal degrees
    longitude                : float = 0.0 # station longitude in decimal degrees
    altitude_m               : float = 0.0 # station altitude in meters
    almanac_texts            : Dict[str, Any] = dataclass_field(default_factory=dict) # target report's [Almanac] section (moon_phases, ...)

# ===============================================================================
#                        Aggregate dispatch tables
# ===============================================================================

# getStatsTuple() is the one interface shared by weewx.accum.ScalarStats/VecStats
# and their Continuous* counterparts: on the Continuous classes, min/mintime/max/
# maxtime/count/max_dir are computed inside getStatsTuple() (from the MinMaxDict),
# not stored as attributes, while avg/rms/vec_avg/vec_dir exist only as properties
# on the objects.  Naming the tuple slots lets extractors read t.max instead of a
# positional index (and avoids shadowing the builtins min/max/sum).
ScalarStatsTuple = namedtuple('ScalarStatsTuple',
    ['min', 'mintime', 'max', 'maxtime', 'sum', 'count', 'wsum', 'sumtime'])
VecStatsTuple = namedtuple('VecStatsTuple',
    ['min', 'mintime', 'max', 'maxtime', 'sum', 'count', 'wsum', 'sumtime',
     'max_dir', 'xsum', 'ysum', 'dirsumtime', 'squaresum', 'wsquaresum'])

# agg_type -> extractor, one table per stats kind.  These tables are the single
# source of truth for which aggregate types exist: the grammar's accepted set
# (AGG_TYPES, below) is their union, so an aggregate cannot parse unless a table
# implements it.  Each extractor takes (s, t): s is the stats object (for the
# computed properties), t is its ScalarStatsTuple/VecStatsTuple (for the
# positional slots).
SCALAR_AGGS: Dict[str, Callable[[Any, Any], Any]] = {
    'min':     lambda s, t: t.min,
    'mintime': lambda s, t: t.mintime,
    'max':     lambda s, t: t.max,
    'maxtime': lambda s, t: t.maxtime,
    'sum':     lambda s, t: t.sum,
    'count':   lambda s, t: t.count,
    'avg':     lambda s, t: s.avg,
}
VEC_AGGS: Dict[str, Callable[[Any, Any], Any]] = {
    'min':     lambda s, t: t.min,
    'mintime': lambda s, t: t.mintime,
    'max':     lambda s, t: t.max,
    'maxtime': lambda s, t: t.maxtime,
    'gustdir': lambda s, t: t.max_dir,
    'count':   lambda s, t: t.count,
    'avg':     lambda s, t: s.avg,
    # NB: vec sum reads the OBJECT attribute (raw), while scalar sum reads the
    # TUPLE slot (massage_near_zero'd on Continuous accums).  This asymmetry is
    # longstanding shipped behavior -- do not "fix" it into consistency.
    'sum':     lambda s, t: s.sum,
    'rms':     lambda s, t: s.rms,
    'vecavg':  lambda s, t: s.vec_avg,
    'vecdir':  lambda s, t: s.vec_dir,
}
FIRSTLAST_AGGS: Dict[str, Callable[[Any, Any], Any]] = {
    'first':     lambda s, t: s.first,
    'last':      lambda s, t: s.last,
    'firsttime': lambda s, t: s.firsttime,
    'lasttime':  lambda s, t: s.lasttime,
}

# The grammar's valid aggregate types ARE the dispatch's -- derived, never
# hand-listed.  parse_cname validates against this set.
AGG_TYPES: FrozenSet[str] = (
    frozenset(SCALAR_AGGS) | frozenset(VEC_AGGS) | frozenset(FIRSTLAST_AGGS))

# ===============================================================================
#                          Format-spec renderers
# ===============================================================================

# Unit types that hold a point in time.  Times have no numeric format string;
# they render through Formatter.toString with a [Units][TimeFormats] context.
TIME_UNIT_TYPES: FrozenSet[str] = frozenset(
    ('unix_epoch', 'unix_epoch_ms', 'unix_epoch_ns'))

# The renderers behind FORMAT_SPECS (below).  Each takes the field's
# CheetahName, the converted value tuple (value, unit_type, group_type), the
# output packet, the target report's formatter, the [Units][TimeFormats]
# context for time values, and is_delta (see LoopProcessor.render_field), and
# writes the finished json value into loopdata_pkt[cname.field] -- or, on a
# formatting error, logs and writes nothing, omitting the field.

def _render_ordinal_compass(cname: CheetahName, value_t: Tuple[Any, Any, Any],
        loopdata_pkt: Dict[str, Any], formatter: weewx.units.Formatter,
        time_context: str, is_delta: bool) -> None:
    loopdata_pkt[cname.field] = formatter.to_ordinal_compass(value_t)

def _render_formatted(cname: CheetahName, value_t: Tuple[Any, Any, Any],
        loopdata_pkt: Dict[str, Any], formatter: weewx.units.Formatter,
        time_context: str, is_delta: bool) -> None:
    value, unit_type, _ = value_t
    if not is_delta and unit_type in TIME_UNIT_TYPES:
        # Times have no numeric format string; render via the time context,
        # as a report tag's .formatted does (times never carry a label, so
        # this equals the unadorned rendering).
        loopdata_pkt[cname.field] = formatter.toString(value_t,
            context=time_context, addLabel=False)
        return
    fmt_str = formatter.get_format_string(unit_type)
    try:
        loopdata_pkt[cname.field] = fmt_str % value
    except Exception as e:
        log.debug('%s: %s, %s, %s' % (e, cname.field, fmt_str, value))

def _render_raw(cname: CheetahName, value_t: Tuple[Any, Any, Any],
        loopdata_pkt: Dict[str, Any], formatter: weewx.units.Formatter,
        time_context: str, is_delta: bool) -> None:
    loopdata_pkt[cname.field] = value_t[0]

def _render_default(cname: CheetahName, value_t: Tuple[Any, Any, Any],
        loopdata_pkt: Dict[str, Any], formatter: weewx.units.Formatter,
        time_context: str, is_delta: bool) -> None:
    """The no-format_spec rendering: WeeWX's formatted-with-label string."""
    if type(value_t[0]) == str:
        # String values (e.g. a firstlast string obstype) are emitted as-is;
        # they have no numeric format.
        loopdata_pkt[cname.field] = value_t[0]
    else:
        loopdata_pkt[cname.field] = formatter.toString(value_t,
            context=time_context)

# format_spec -> renderer, the single render path for current, period and trend
# fields (LoopProcessor.render_field dispatches here).  This table is the
# single source of truth for which format specs exist: the grammar's accepted
# sets (FORMAT_SPEC_NAMES below; parse_almanac_field uses this table directly)
# are derived from it, so a spec cannot parse unless a renderer implements it.
FORMAT_SPECS: Dict[str, Callable[[CheetahName, Tuple[Any, Any, Any],
        Dict[str, Any], weewx.units.Formatter, str, bool], None]] = {
    'ordinal_compass': _render_ordinal_compass,
    'formatted':       _render_formatted,
    'raw':             _render_raw,
}

# The grammar's valid format specs ARE the renderers' -- derived, never
# hand-listed -- plus code/desc, which are not value renderings but
# trend.barometer classifications, handled in add_trend_obstype before the
# renderer is reached.  parse_cname validates against this set.
FORMAT_SPEC_NAMES: FrozenSet[str] = (
    frozenset(FORMAT_SPECS) | frozenset(('code', 'desc')))

# Call-syntax format specs: the ValueHelper formatting methods a report tag
# can call, e.g. $day.outTemp.maxtime.format("%H:%M"),
# $current.outTemp.format(add_label=False), $day.windGust.max.nolabel("%.1f"),
# $day.rain.sum.string("--"), $day.sunshineDur.sum.long_form().  Each entry
# mirrors the ValueHelper method of the same name: params lists its
# parameters in positional order (LoopData.parse_call_spec binds a field's
# positional arguments to these names), required counts the leading ones a
# call must supply (nolabel's format_string), and render applies the bound
# kwargs through the target report's Formatter -- the exact calls ValueHelper
# makes, so the output matches the report tag's.  A bare spec name (no
# parens) is a zero-argument call, as Cheetah's auto-call renders it.
@dataclass(frozen=True)
class CallFormatSpec:
    params  : Tuple[str, ...]
    required: int
    render  : Callable[[weewx.units.Formatter, Tuple[Any, Any, Any], str,
                        Dict[str, Any]], str]

CALL_FORMAT_SPECS: Dict[str, CallFormatSpec] = {
    'format': CallFormatSpec(
        ('format_string', 'None_string', 'add_label', 'localize'), 0,
        lambda f, v, ctx, kw: f.toString(v, context=ctx,
            useThisFormat=kw.get('format_string'),
            None_string=kw.get('None_string'),
            addLabel=kw.get('add_label', True),
            localize=kw.get('localize', True))),
    'nolabel': CallFormatSpec(
        ('format_string', 'None_string'), 1,
        lambda f, v, ctx, kw: f.toString(v, context=ctx, addLabel=False,
            useThisFormat=kw['format_string'],
            None_string=kw.get('None_string'))),
    'string': CallFormatSpec(
        ('None_string',), 0,
        lambda f, v, ctx, kw: f.toString(v, context=ctx,
            None_string=kw.get('None_string'))),
    'long_form': CallFormatSpec(
        ('format_string', 'None_string'), 0,
        lambda f, v, ctx, kw: f.long_form(v, context=ctx,
            format_string=kw.get('format_string'),
            None_string=kw.get('None_string'))),
}

def _render_call_spec(cname: CheetahName, value_t: Tuple[Any, Any, Any],
        loopdata_pkt: Dict[str, Any], formatter: weewx.units.Formatter,
        time_context: str, is_delta: bool) -> None:
    """The renderer for every call-syntax spec: look the spec up in
    CALL_FORMAT_SPECS and apply the field's bound kwargs.  As with the other
    renderers, a formatting error (bad format string, unit with no 'second'
    conversion under long_form, ...) logs and omits the field."""
    assert cname.format_spec is not None
    call_spec = CALL_FORMAT_SPECS[cname.format_spec]
    try:
        loopdata_pkt[cname.field] = call_spec.render(
            formatter, value_t, time_context, cname.format_kwargs or {})
    except Exception as e:
        log.debug('%s: %s' % (cname.field, e))

def spec_emits_none(cname: CheetahName) -> bool:
    """True when the field's format spec carries explicit None handling -- a
    string() call, or an explicit None_string argument to
    format/nolabel/long_form.  Such a field is EMITTED with its None
    rendering when data is missing (a report tag always renders something),
    overriding loopdata's default of omitting fields with no data."""
    if cname.format_kwargs is None:
        return False
    return cname.format_spec == 'string' \
        or cname.format_kwargs.get('None_string') is not None

# ===============================================================================
#                                  MinMaxDict
# ===============================================================================

V = TypeVar('V')

class MinMaxDict(Generic[V]):
    """A dict with float keys that also tracks the smallest and largest key,
    fetched with peekitem(0) and peekitem(-1) — the only indexes supported.

    Only the operations the continuous accumulators use are provided: in, [],
    pop, len and peekitem.

    The keys are the distinct observation values currently in the accumulator's
    window (duplicate values share a key) — a handful for real sensor data,
    but potentially one per packet for an obstype whose value never repeats.

    Candidate keys live in two heaps (a min-heap, and a max-heap of negated
    keys) with lazy deletion: pop() only removes the key from the dict, and
    peekitem() discards heap entries whose key is no longer live as they
    surface at the top.  Re-adding a key leaves a duplicate heap entry;
    duplicates compare equal, so peeks stay correct and the stale copy is
    discarded once the key dies.  Every live key always has at least one
    entry in each heap, so peekitem always terminates on a live key when the
    dict is non-empty.  When the heaps outgrow twice the live key count they
    are rebuilt, bounding memory and keeping all operations O(log n)
    amortized — there is no pathological workload: even the worst case the
    field grammar permits (a 72h trend window, 1s loop interval, every
    packet's value unique -> ~260k keys) costs single-digit microseconds per
    packet, while for a handful of keys it matches a plain sorted list.
    """

    def __init__(self) -> None:
        self._data: Dict[float, V] = {}
        self._min_heap: List[float] = []
        self._max_heap: List[float] = []  # negated keys

    def __contains__(self, key: float) -> bool:
        return key in self._data

    def __len__(self) -> int:
        return len(self._data)

    def __getitem__(self, key: float) -> V:
        return self._data[key]

    def __setitem__(self, key: float, value: V) -> None:
        if key not in self._data:
            self._data[key] = value
            heappush(self._min_heap, key)
            heappush(self._max_heap, -key)
            if len(self._min_heap) > 2 * len(self._data) + 16:
                self._compact()
        else:
            self._data[key] = value

    def pop(self, key: float) -> V:
        # The key's heap entries go stale; peekitem/_compact discard them.
        return self._data.pop(key)

    def peekitem(self, index: int = -1) -> Tuple[float, V]:
        if index == 0:
            heap = self._min_heap
            while heap[0] not in self._data:
                heappop(heap)
            key = heap[0]
        elif index == -1:
            heap = self._max_heap
            while -heap[0] not in self._data:
                heappop(heap)
            key = -heap[0]
        else:
            raise IndexError('MinMaxDict.peekitem supports only index 0 or -1')
        return key, self._data[key]

    def _compact(self) -> None:
        self._min_heap = list(self._data)
        heapify(self._min_heap)
        self._max_heap = [-key for key in self._data]
        heapify(self._max_heap)

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
              |                          future_debits (deque)
              |                          --------------------
              '------------------------> ts|expiration(ts+timelength)|value|weight
              |
              |
              v
        values_dict (MinMaxDict)
        key         value
        ----------- ------------------------
        val         timestamp_list (deque)
                    --------------
                    ts

    Every time an observation is added (with addSum), a future
    debit is created with the same information and an expiration of ts + timelength.
    In the continuous accumulator addRecord function, after addSum is called on all
    continuous stats instances, trimExpiredEntries(ts) is called on
    all continuous stats instances.

    The future debits are stored in a deque.  Each time trimExpiredEntries is
    called, the top of the list is iterated on looking for any entries where
    the expiration is <= the current dateTime.

    In addition to the future debit list, a values_dict (MinMaxDict) is maintained where:
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
        self.future_debits: Deque[ScalarDebit] = deque()
        self.values_dict: MinMaxDict[Deque[int]] = MinMaxDict()
        self.sum = 0.0
        self.count = 0
        self.wsum = 0.0
        self.sumtime = 0.0

    def getStatsTuple(self):
        # min is key of first element in values_dict
        # mintime is first element of the timestamp list contained in the value of the first element in values_dict
        # max is key of last element in dict
        # maxtime is first element of the timestamp list contained in the value of the last element in values_dict
        if len(self.values_dict) != 0:
            min, timelist = self.values_dict.peekitem(0)
            mintime: int = timelist[0]
            max, timelist = self.values_dict.peekitem(-1)
            maxtime: int = timelist[0]
        else:
            min, mintime, max, maxtime = None, None, None, None
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
                self.values_dict[val] = deque()
            timestamp_list: Deque[int] = self.values_dict[val]
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
            debit = self.future_debits.popleft()
            log.debug('Applying debit: %s value: %f, weight: %f' % (timestamp_to_string(debit.timestamp), debit.value, debit.weight))
            self.sum -= debit.value
            self.count -= 1
            self.wsum -= debit.value * debit.weight
            self.sumtime -= debit.weight
            # Remove the debit entry in the values_dict.
            timestamp_list: Deque[int] = self.values_dict[debit.value]
            first_timestamp = timestamp_list.popleft()
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
              |                          future_debits (deque)
              |                          --------------------
              '------------------------> ts|expiration(ts+timelength)|value|weight
              |
              |
              v
        speed_dict (MinMaxDict)
        key         value
        ----------- ------------------------
        speed       timestamp_dirn_list (deque)
                    -------------------------
                    tuple(ts, dirN)

    Every time an observation is added (with addSum), a future
    debit is created with the same information and an expiration of ts + timelength.
    In the continuous accumulator addRecord function, after addSum is called on all
    continuous stats instances, trimExpiredEntries(ts) is called on
    all continuous stats instances.

    The future debits are stored in a deque.  Each time trimExpiredEntries is
    called, the top of the list is iterated on looking for any entries where
    the expiration is <= the current dateTime.

    In addition to the future debit list, a speed_dict (MinMaxDict) is maintained where:
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
        self.future_debits: Deque[VecDebit] = deque()
        self.speed_dict: MinMaxDict[Deque[Tuple[int, float]]] = MinMaxDict()
        self.sum = 0.0
        self.count = 0
        self.wsum = 0.0
        self.sumtime = 0.0
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
            min, mintime, max, maxtime, maxdir = None, None, None, None, None

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
                self.speed_dict[speed] = deque()
            timestamp_dirn_list: Deque[Tuple[int, float]] = self.speed_dict[speed]
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
            debit = self.future_debits.popleft()
            log.debug('Applying ContinuousVecStats debit: %s speed: %f, dirN: %r, weight: %f' % (timestamp_to_string(debit.timestamp), debit.speed, debit.dirN, debit.weight))
            # Apply this debit.
            self.sum -= debit.speed
            self.count -= 1
            self.wsum -= debit.weight * debit.speed
            self.sumtime -= debit.weight
            self.squaresum -= debit.speed ** 2
            self.wsquaresum -= debit.weight * debit.speed ** 2
            if debit.dirN is not None:
                self.xsum -= debit.weight * debit.speed * math.cos(math.radians(90.0 - debit.dirN))
                self.ysum -= debit.weight * debit.speed * math.sin(math.radians(90.0 - debit.dirN))
            # Mirror the addSum credit condition (dirN present, or calm).
            if debit.dirN is not None or debit.speed == 0:
                self.dirsumtime -= debit.weight
            # Remove the debit entry in the speed_dict.
            timestamp_dirn_list: Deque[Tuple[int, float]] = self.speed_dict[debit.speed]
            timestamp, dirN = timestamp_dirn_list.popleft()
            assert timestamp == debit.timestamp
            if len(timestamp_dirn_list) == 0:
                self.speed_dict.pop(debit.speed)

    @property
    def avg(self):
        return self.wsum / self.sumtime if self.count else None

    @property
    def rms(self):
        return math.sqrt(abs(self.wsquaresum / self.sumtime)) if self.count else None

    @property
    def vec_avg(self):
        if self.count:
            return math.sqrt(abs((self.xsum ** 2 + self.ysum ** 2) / self.sumtime ** 2))

    @property
    def vec_dir(self):
        if self.dirsumtime and (self.ysum or self.xsum):
            _result = 90.0 - math.degrees(math.atan2(self.ysum, self.xsum))
            if _result < 0.0:
                _result += 360.0
            return _result
        # Return the last known direction when our vector sum is 0
        last = self.last
        return last[1] if last is not None else None

    @property
    def first(self):
        if len(self.future_debits) != 0:
            return self.future_debits[0].speed, self.future_debits[0].dirN
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

    In the continuous accumulator addRecord function, after addSum is called on all
    continuous stats instances, trimExpiredEntries(ts) is called on
    all continuous stats instances.

    When addSum is called, FirstLastEntry is added to values_list.

    When trimExpiredEntries is called,
    1. the values_list is iterated over while FirstLastEntry.dateTime <= ts
    2.     the FirstLastEntry is deleted

    first/firsttime is the dateTime value and dateTime of the first entry in values_list
    last/lasttime is the dateTime value and dateTime of the last entry in values_list
    """

    def __init__(self, timelength: int):
        self.timelength = timelength
        self.values_list: Deque[FirstLastEntry] = deque()

    def getStatsTuple(self):
        """Return a stats-tuple. That is, a tuple containing the gathered statistics."""
        if len(self.values_list) == 0:
            return (None, None, None, None)
        return (self.values_list[0].value, self.values_list[0].dateTime,
                self.values_list[-1].value, self.values_list[-1].dateTime)

    @property
    def first(self):
        """The first value seen (None if empty)."""
        if len(self.values_list) == 0:
            return None
        return self.values_list[0].value

    @property
    def firsttime(self):
        """The timestamp of the first value seen (None if empty)."""
        if len(self.values_list) == 0:
            return None
        return self.values_list[0].dateTime

    @property
    def last(self):
        """The last value seen (None if empty)."""
        if len(self.values_list) == 0:
            return None
        return self.values_list[-1].value

    @property
    def lasttime(self):
        """The timestamp of the last value seen (None if empty)."""
        if len(self.values_list) == 0:
            return None
        return self.values_list[-1].dateTime

    def addSum(self, ts, val, weight=1):
        """Add a value, preserving its type.  weewx's FirstLastAccum stores the
        value as-is (it may be of almost any type), so we do NOT coerce to str."""
        if val is not None:
            self.values_list.append(FirstLastEntry(
                dateTime = ts,
                value = val))

    def trimExpiredEntries(self, ts):
        # Remove any expired entries
        while len(self.values_list) > 0 and self.values_list[0].dateTime + self.timelength <= ts:
            self.values_list.popleft()


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
    continuous           : Dict[str, ContinuousAccum] # e.g., continuous_accums['24h'], or ['trend']

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

        # Get a target report dictionary we can use for converting units and formatting.
        target_report = formatting_spec_dict.get('target_report', 'LoopDataReport')
        try:
            target_report_dict = LoopData.get_target_report_dict(
                config_dict, target_report)
        except Exception as e:
            reraise_if_terminate(e)
            log.error('Could not find target_report: %s.  LoopData is exiting. Exception: %s' % (target_report, e))
            return

        loop_data_dir = LoopData.compose_loop_data_dir(config_dict, target_report_dict, file_spec_dict)
        os.makedirs(loop_data_dir, exist_ok=True)

        # Get a temporay file in which to write data before renaming.
        tmp = tempfile.NamedTemporaryFile(prefix='LoopData', dir=loop_data_dir, delete=False)
        tmp.close()

        # Get the loop frequency seconds to be passed as the weight to accumulators.
        loop_frequency = to_float(loop_frequency_spec_dict.get('seconds', '2.0'))

        # Get [possibly localized] strings for trend.barometer.desc
        baro_trend_descs = LoopData.construct_baro_trend_descs(baro_trend_trans_dict)

        # Process fields line of LoopData section.
        specified_fields = include_spec_dict.get('fields', [])
        (fields_to_include, obstypes) = LoopData.get_fields_to_include(specified_fields)

        # Almanac fields (almanac.sunrise, almanac(horizon=-6).sun(use_center=1).rise, ...)
        # are evaluated against weewx.almanac rather than the loop packet.
        almanac_fields = LoopData.get_almanac_fields(specified_fields)
        altitude_m = weewx.units.convert(engine.stn_info.altitude_vt, 'meter')[0]

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
            archive_delay            = to_int(std_archive_dict.get('archive_delay', 15)),
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
            ssh_options              = rsync_spec_dict.get('ssh_options', '-o ConnectTimeout=1'),
            timeout                  = to_int(rsync_spec_dict.get('timeout', 1)),
            skip_if_older_than       = to_int(rsync_spec_dict.get('skip_if_older_than', 3)),
            time_delta               = time_delta,
            week_start               = week_start,
            rainyear_start           = rainyear_start,
            obstypes                 = obstypes,
            baro_trend_descs         = baro_trend_descs,
            almanac_fields           = almanac_fields,
            latitude                 = engine.stn_info.latitude_f,
            longitude                = engine.stn_info.longitude_f,
            altitude_m               = altitude_m if altitude_m is not None else 0.0,
            almanac_texts            = dict(target_report_dict.get('Almanac', {})))

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
    def is_valid_period(period: str)-> bool:
        valid_fixed_periods     : List[str] = [ 'alltime', 'rainyear', 'year', 'month', 'week', 'current', 'hour', 'day' ]
        if period in valid_fixed_periods or LoopData.is_continuous_period(period):
            return True
        return False

    # Set of every unit name WeeWX knows how to convert to/from, populated lazily
    # on first use (after weewx.wxxtypes -- imported at module load -- has
    # registered beaufort and friends into weewx.units.conversionDict).
    _known_units: Optional[Set[str]] = None

    @staticmethod
    def is_valid_unit(unit: str) -> bool:
        """Is unit a unit WeeWX recognizes (a valid override target)?  Drawn from
        the conversion table (source and target units) plus the standard unit
        systems, so e.g. degree_C, degree_F, knot, mile_per_hour and beaufort all
        qualify."""
        if LoopData._known_units is None:
            units: Set[str] = set(weewx.units.conversionDict.keys())
            for targets in weewx.units.conversionDict.values():
                units |= set(targets.keys())
            for unit_system in (weewx.units.USUnits, weewx.units.MetricUnits, weewx.units.MetricWXUnits):
                units |= set(unit_system.values())
            LoopData._known_units = units
        return unit in LoopData._known_units

    @staticmethod
    def is_continuous_period(period: str)-> bool:
        if period == 'trend' or LoopData.is_minute_period(period) or LoopData.is_hour_period(period):
            return True
        return False

    @staticmethod
    def is_minute_period(period: str)-> bool:
        """ Check for 1m-1440m tags. """
        if period.endswith('m'):
            char_part = period[-1]
            digit_part = period[:-1]
            if digit_part.isdigit():
                val = int(digit_part)
                if char_part == 'm' and val >= 1 and val <= 1440:
                    return True
        return False

    @staticmethod
    def is_hour_period(period: str)-> bool:
        if period.endswith('h'):
            # Check for 1h-24h tags.
            char_part = period[-1]
            digit_part = period[:-1]
            if digit_part.isdigit():
                val = int(digit_part)
                if char_part == 'h' and val >= 1 and val <= 24:
                    return True
        return False

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
        continuous_periods: Set[str] = set()
        for field in specified_fields:
            cname: Optional[CheetahName] = LoopData.parse_cname(field)
            if cname is not None:
                fields_to_include.add(cname)
                if cname.period is not None and LoopData.is_continuous_period(cname.period):
                    continuous_periods.add(cname.period)

        current_obstypes  : Set[str] = LoopData.compute_period_obstypes(
            fields_to_include, 'current')

        # Fixed Periods
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

        # Contiunous Periods
        continuous_obstypes: Dict[str, Set[str]] = {}
        for per in continuous_periods:
            continuous_obstypes[per] = LoopData.compute_period_obstypes(
                fields_to_include, per)
            current_obstypes.update(continuous_obstypes[per])

        # current_obstypes is special because current observations are
        # needed to feed all the others.  As such, take the union of all.
        # continuous period obstypes were added above.
        current_obstypes = set(itertools.chain(current_obstypes, alltime_obstypes,
            rainyear_obstypes, year_obstypes, month_obstypes, week_obstypes, day_obstypes, hour_obstypes))

        return (fields_to_include, 
                ObsTypes(
                    current         = current_obstypes,
                    alltime         = alltime_obstypes,
                    rainyear        = rainyear_obstypes,
                    year            = year_obstypes,
                    month           = month_obstypes,
                    week            = week_obstypes,
                    day             = day_obstypes,
                    hour            = hour_obstypes,
                    continuous      = continuous_obstypes))

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
        # WeeWX's own skin-dict builder: build_skin_dict since WeeWX 4.6
        # (checking the older _build_skin_dict name too), else assemble the
        # report dict the old fashioned way below.
        build_skin_dict = getattr(weewx.reportengine, 'build_skin_dict',
            getattr(weewx.reportengine, '_build_skin_dict', None))
        if build_skin_dict is not None:
            return build_skin_dict(config_dict, report)
        try:
            skin_dict = weeutil.config.deep_copy(weewx.defaults.defaults)
        except Exception as e:
            reraise_if_terminate(e)
            # Fall back to copy.deepcopy for earlier than weewx 4.1.2 installs.
            skin_dict = copy.deepcopy(weewx.defaults.defaults)
        # Turn off interpolation, exactly as WeeWX's build_skin_dict does: it
        # interferes with the %(hour)d-style delta-time format strings.
        skin_dict.interpolation = False
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
            except Exception as e:
                reraise_if_terminate(e)
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
            t: threading.Thread = threading.Thread(target=lp.process_queue, name='LoopData', daemon=True)
            t.start()
        except Exception as e:
            reraise_if_terminate(e)
            # Print problem to log and give up.
            log.error('Error in LoopData setup.  LoopData is exiting. Exception: %s' % e)
            weeutil.logger.log_traceback(log.error, "    ****  ")

    @staticmethod
    def day_summary_records_generator(dbm, obstype: str, earliest_time: int,
            latest_time: Optional[int] = None
            ) -> Generator[Dict[str, Any], None, None]:
        # Day-summary inclusion follows weewx's DailySummaries convention
        # (weewx.xtypes.DailySummaries): dateTime >= start AND dateTime < stop
        # -- inclusive on the left, EXCLUSIVE on the right.  Note this is the
        # opposite right-edge convention from archive-record queries
        # (start < t <= stop); day-summary rows are keyed by day-start, so the
        # row at exactly 'start' is included and the row at exactly 'stop' is
        # not.  latest_time should be the period span's stop.
        table_name = 'archive_day_%s' % obstype
        cols: List[str] = dbm.connection.columnsOf(table_name)
        if latest_time is None:
            sql = 'SELECT * FROM %s WHERE dateTime >= %d ORDER BY dateTime ASC' % (
                table_name, earliest_time)
        else:
            sql = 'SELECT * FROM %s WHERE dateTime >= %d AND dateTime < %d ORDER BY dateTime ASC' % (
                table_name, earliest_time, latest_time)
        for row in dbm.genSql(sql):
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

            # Create fixed accums
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
                self.cfg.unit_system, self.cfg.archive_interval, self.cfg.obstypes.hour, pkt_time, day_accum, dbm,
                archive_delay=self.cfg.archive_delay)

            # Create continuous accums
            continuous_accums: Dict[str, ContinuousAccum] = {}
            for per, obstypes in self.cfg.obstypes.continuous.items():
                if per == 'trend':
                    timelength = self.cfg.time_delta
                elif LoopData.is_hour_period(per):
                    timelength = int(per[:-1])*3600
                elif LoopData.is_minute_period(per):
                    timelength = int(per[:-1])*60

                cont_accum, obstypes = LoopData.create_continuous_accum(
                    per, self.cfg.unit_system, self.cfg.archive_interval, obstypes, timelength, day_accum, dbm,
                    archive_delay=self.cfg.archive_delay)
                if cont_accum:
                    continuous_accums[per], self.cfg.obstypes.continuous[per]  = cont_accum, obstypes

            self.cfg.queue.put(Accumulators(
                alltime_accum  = alltime_accum,
                rainyear_accum = rainyear_accum,
                year_accum     = year_accum,
                month_accum    = month_accum,
                week_accum     = week_accum,
                day_accum      = day_accum,
                hour_accum     = hour_accum,
                continuous     = continuous_accums))
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
    def create_hour_accum(unit_system: int, archive_interval: int, obstypes: Set[str], pkt_time: int, day_accum: weewx.accum.Accum, dbm,
            archive_delay: int = 15) -> Tuple[Optional[weewx.accum.Accum], Set[str]]:
        log.debug('Creating initial hour_accum')
        span = weeutil.weeutil.archiveHoursAgoSpan(pkt_time)
        return LoopData.create_period_accum('hour', unit_system, archive_interval, obstypes, span, day_accum, dbm, archive_delay=archive_delay)

    @staticmethod
    def create_period_accum(name: str, unit_system: int, archive_interval: int, obstypes: Set[str],
            span: weeutil.weeutil.TimeSpan, day_accum: weewx.accum.Accum, dbm,
            archive_delay: int = 15) -> Tuple[Optional[weewx.accum.Accum], Set[str]]:
        """return period accumulator and (possibly trimmed) obstypes"""

        if len(obstypes) == 0:
            return None, set()

        start = time.time()
        record_count = 0
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
                for record in LoopData.day_summary_records_generator(dbm, obstype, span.start, latest_time=span.stop):
                    record_count += 1
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
                # Reject future-dated records, mirroring weewx's _catchup
                # (engine.StdArchive): accept only ts < now + archive_delay,
                # where archive_delay provides lenience for clock drift.
                if pkt['dateTime'] >= time.time() + archive_delay:
                    log.warning('Ignoring future-dated archive record: %s'
                        % timestamp_to_string(pkt['dateTime']))
                    continue
                pkt['usUnits'] = unit_system
                pruned_pkt = LoopProcessor.prune_period_packet(pkt, obstypes)
                accum.addRecord(pruned_pkt, weight=archive_interval * 60)
                pkt_count += 1
            log.debug('Primed hour_accum with %d archive packets in %f seconds.' % (pkt_count, time.time() - start))

        log.debug('Created %s accum in %f seconds (read %d records).' % (name, time.time() - start, record_count))
        return accum, valid_obstypes

    @staticmethod
    def create_continuous_accum(name: str, unit_system: int, archive_interval: int, obstypes: Set[str],
            timelength, day_accum: weewx.accum.Accum, dbm,
            archive_delay: int = 15) -> Tuple[Optional[ContinuousAccum], Set[str]]:
        """return continuously accumulator and (possibly trimmed) obstypes"""

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
            # Reject future-dated records, mirroring weewx's _catchup
            # (engine.StdArchive): accept only ts < now + archive_delay,
            # where archive_delay provides lenience for clock drift.
            if pkt['dateTime'] >= start + archive_delay:
                log.warning('Ignoring future-dated archive record: %s'
                    % timestamp_to_string(pkt['dateTime']))
                continue
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

        segments: Optional[List[str]] = LoopData.split_field_segments(field)
        if segments is None:
            return None
        segment: List[str] = segments
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
            if LoopData.is_valid_period(segment[next_seg]):
                period = segment[next_seg]
                next_seg += 1
            else:
                return None

        if len(segment) < next_seg:
            # need an obstype, but none there
            return None
        obstype = segment[next_seg]
        next_seg += 1

        agg_type = None
        # all periods, except current and trend, must have an agg_type
        if period is not None and period != 'current' and period != 'trend':
            if len(segment) <= next_seg:
                return None
            # AGG_TYPES is the union of the dispatch tables (SCALAR_AGGS et
            # al.), so an aggregate can only parse if the dispatch implements it.
            if segment[next_seg] not in AGG_TYPES:
                return None
            agg_type = segment[next_seg]
            next_seg += 1

        unit = None
        # Optional unit override (value fields only, never the unit.label prefix
        # form).  Sits between the agg_type and the format_spec, e.g.
        # day.outTemp.avg.degree_C.formatted or current.windSpeed.beaufort.  A
        # segment is a unit only if WeeWX knows it as one; format specs (bare
        # and call-syntax) are a disjoint set, so there is no ambiguity.
        if prefix is None and len(segment) > next_seg:
            if segment[next_seg] not in FORMAT_SPEC_NAMES \
                    and segment[next_seg] not in CALL_FORMAT_SPECS \
                    and LoopData.is_valid_unit(segment[next_seg]):
                unit = segment[next_seg]
                next_seg += 1

        format_spec = None
        format_kwargs = None
        # check for a format spec.  FORMAT_SPEC_NAMES is derived from the
        # FORMAT_SPECS renderer table, so the grammar and the rendering can
        # never drift apart; likewise the call-syntax specs
        # (format/nolabel/string/long_form) parse against CALL_FORMAT_SPECS.
        if prefix is None and len(segment) > next_seg:
            if segment[next_seg] in FORMAT_SPEC_NAMES:
                format_spec = segment[next_seg]
                next_seg += 1
            else:
                parsed_call = LoopData.parse_call_spec(segment[next_seg])
                if parsed_call is not None:
                    format_spec, format_kwargs = parsed_call
                    next_seg += 1

        # windrun_<dir> is not supported for week, month, year, rainyear and alltime
        if obstype.startswith('windrun_') and (
                period == 'week' or period == 'month' or period == 'year' or period == 'rainyear' or period == 'alltime'):
            return None

        if len(segment) > next_seg:
            # There is more.  This is unexpected.
            return None

        return CheetahName(
            field         = field,
            prefix        = prefix,
            prefix2       = prefix2,
            period        = period,
            obstype       = obstype,
            agg_type      = agg_type,
            unit          = unit,
            format_spec   = format_spec,
            format_kwargs = format_kwargs)

    # An almanac field segment: an identifier with an optional call suffix
    # holding kwargs (no nested parens), e.g. sun(use_center=1).
    almanac_segment_re = re.compile(r'^([A-Za-z_][A-Za-z0-9_]*)(?:\(([^()]*)\))?$')

    # Attributes whose value depends only on the local day of the almanac's
    # time (rise/set searches start at local midnight), so one evaluation
    # serves the whole day.
    almanac_day_attrs = { 'sunrise', 'sunset', 'rise', 'set', 'transit',
                          'antitransit', 'visible', 'visible_change' }

    @staticmethod
    def is_almanac_field(field: str) -> bool:
        return field == 'almanac' or field.startswith('almanac.') or field.startswith('almanac(')

    @staticmethod
    def get_almanac_fields(specified_fields: List[str]) -> List[AlmanacField]:
        almanac_fields: List[AlmanacField] = []
        seen: Set[str] = set()
        for field in specified_fields:
            if not LoopData.is_almanac_field(field):
                continue
            almanac_field = LoopData.parse_almanac_field(field)
            if almanac_field is None:
                log.error('Ignoring malformed almanac field: %s' % field)
                continue
            if almanac_field.field not in seen:
                seen.add(almanac_field.field)
                almanac_fields.append(almanac_field)
        return almanac_fields

    @staticmethod
    def split_field_segments(field: str) -> Optional[List[str]]:
        """Split a fields-line entry on '.' at paren depth zero, so call
        suffixes keep their contents (almanac(horizon=-6).sun.rise -> 3
        segments, day.outTemp.maxtime.format("%H:%M") -> 4).  Quoted call
        arguments are opaque: dots, parens and backslash-escaped quotes
        inside them neither split nor count toward the depth.  Returns None
        on unbalanced parens or an unterminated quote."""
        segments: List[str] = []
        current = ''
        depth = 0
        quote: Optional[str] = None
        escaped = False
        for ch in field:
            if quote is not None:
                current += ch
                if escaped:
                    escaped = False
                elif ch == '\\':
                    escaped = True
                elif ch == quote:
                    quote = None
                continue
            if ch == '.' and depth == 0:
                segments.append(current)
                current = ''
                continue
            if ch in ('"', "'"):
                quote = ch
            elif ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
                if depth < 0:
                    return None
            current += ch
        if depth != 0 or quote is not None:
            return None
        segments.append(current)
        return segments

    @staticmethod
    def parse_call_spec(segment: str) -> Optional[Tuple[str, Dict[str, Any]]]:
        """Parse a call-syntax format spec segment -- format("%H:%M"),
        nolabel("%.1f", None_string="--"), string(), long_form() -- into
        (spec_name, kwargs), binding positional arguments to the ValueHelper
        method's parameter names per CALL_FORMAT_SPECS.  A bare name is a
        zero-argument call, as Cheetah's auto-call renders it.  Arguments
        must be Python literals.  Returns None unless the segment is a
        well-formed call of a known spec supplying its required arguments."""
        try:
            node = ast.parse(segment, mode='eval').body
        except (SyntaxError, ValueError):
            return None
        args: List[ast.expr] = []
        keywords: List[ast.keyword] = []
        if isinstance(node, ast.Name):
            name = node.id
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            name = node.func.id
            args = node.args
            keywords = node.keywords
        else:
            return None
        call_spec = CALL_FORMAT_SPECS.get(name)
        if call_spec is None or len(args) > len(call_spec.params):
            return None
        kwargs: Dict[str, Any] = {}
        try:
            for i, arg in enumerate(args):
                kwargs[call_spec.params[i]] = ast.literal_eval(arg)
            for keyword in keywords:
                if keyword.arg is None or keyword.arg not in call_spec.params \
                        or keyword.arg in kwargs:
                    return None
                kwargs[keyword.arg] = ast.literal_eval(keyword.value)
        except (SyntaxError, ValueError, TypeError, MemoryError):
            # Not a literal (e.g. a name or an expression).
            return None
        if any(param not in kwargs
                for param in call_spec.params[:call_spec.required]):
            return None
        return name, kwargs

    @staticmethod
    def parse_almanac_kwargs(kwargs_str: str) -> Optional[Dict[str, float]]:
        """Parse 'horizon=-6, use_center=1' into a dict.  Values must be
        numeric.  Returns None on any malformed part."""
        kwargs: Dict[str, float] = {}
        if kwargs_str.strip() == '':
            return kwargs
        for part in kwargs_str.split(','):
            if '=' not in part:
                return None
            key, value_str = part.split('=', 1)
            key = key.strip()
            value_str = value_str.strip()
            if not key.isidentifier():
                return None
            try:
                kwargs[key] = int(value_str)
            except ValueError:
                try:
                    kwargs[key] = float(value_str)
                except ValueError:
                    return None
        return kwargs

    @staticmethod
    def parse_almanac_field(field: str) -> Optional[AlmanacField]:
        segments = LoopData.split_field_segments(field)
        if segments is None or len(segments) < 2:
            return None

        # The leading segment must be almanac, with an optional call suffix.
        match = LoopData.almanac_segment_re.match(segments[0])
        if match is None or match.group(1) != 'almanac':
            return None
        almanac_kwargs: Dict[str, float] = {}
        if match.group(2) is not None:
            parsed_kwargs = LoopData.parse_almanac_kwargs(match.group(2))
            if parsed_kwargs is None:
                return None
            almanac_kwargs = parsed_kwargs
        days = almanac_kwargs.pop('days', 0)
        if not isinstance(days, int):
            return None

        # A trailing format spec is loopdata's, not the almanac's.  Almanac
        # fields take the renderer specs (FORMAT_SPECS keys) and the
        # call-syntax specs (CALL_FORMAT_SPECS) -- never code/desc, which are
        # trend.barometer classifications -- because to_json_value renders
        # each as the ValueHelper attribute of the same name.
        chain_segments = segments[1:]
        format_spec = None
        format_kwargs = None
        if len(chain_segments) >= 2:
            if chain_segments[-1] in FORMAT_SPECS:
                format_spec = chain_segments[-1]
                chain_segments = chain_segments[:-1]
            else:
                parsed_call = LoopData.parse_call_spec(chain_segments[-1])
                if parsed_call is not None:
                    format_spec, format_kwargs = parsed_call
                    chain_segments = chain_segments[:-1]

        chain: List[AlmanacSegment] = []
        for segment in chain_segments:
            match = LoopData.almanac_segment_re.match(segment)
            if match is None:
                return None
            seg_kwargs: Optional[Dict[str, float]] = None
            if match.group(2) is not None:
                seg_kwargs = LoopData.parse_almanac_kwargs(match.group(2))
                if seg_kwargs is None:
                    return None
            chain.append(AlmanacSegment(name=match.group(1), kwargs=seg_kwargs))
        if len(chain) == 0:
            return None

        if any(seg.name.startswith('next_') or seg.name.startswith('previous_') for seg in chain):
            tier = 'event'
        elif any(seg.name in LoopData.almanac_day_attrs for seg in chain):
            tier = 'day'
        else:
            tier = 'continuous'

        return AlmanacField(
            field          = field,
            almanac_kwargs = almanac_kwargs,
            days           = days,
            chain          = chain,
            format_spec    = format_spec,
            tier           = tier,
            format_kwargs  = format_kwargs)

class AlmanacFieldEvaluator:
    """Evaluates almanac fields against weewx.almanac (whatever AlmanacTypes
    are registered: weewx-skyfield, PyEphem, or the built-in fallback) and
    inserts the results into the loopdata packet.  Runs on the LoopProcessor
    thread.  Caching mirrors weewx-celestial's proven lifetimes: continuous
    attributes (alt/az/ra/dec/phase/distances) are recomputed every packet;
    day-scoped attributes (rise/set/transit/visible) once per local day; event
    attributes (next_*/previous_*) are kept until the local day advances past
    the cached event, so a page can show today's event for the rest of its day.
    The local day is compared for EQUALITY, so backfilled packets get their own
    day, never a newer cache."""

    # Sentinel cached for a field whose evaluation failed, so day/event tiers
    # don't retry every packet.
    SKIP = object()

    def __init__(self, cfg: Configuration) -> None:
        self.fields    = cfg.almanac_fields
        self.latitude  = cfg.latitude
        self.longitude = cfg.longitude
        self.altitude_m = cfg.altitude_m
        self.texts     = cfg.almanac_texts
        self.formatter = cfg.formatter
        self.converter = cfg.converter
        self.values: Dict[str, Any] = {}          # field -> json value or SKIP
        self.event_ts: Dict[str, Optional[float]] = {} # field -> cached event's epoch time
        self.cache_day: Optional[date] = None
        self.warned: Set[str] = set()

    @staticmethod
    def shift_days(time_ts: float, days: int) -> float:
        """The same wall-clock time days local calendar days away (DST-correct,
        unlike time_ts + days*86400)."""
        shifted = datetime.fromtimestamp(time_ts) + timedelta(days=days)
        return shifted.timestamp()

    def build_almanac(self, pkt: Dict[str, Any]) -> weewx.almanac.Almanac:
        """One base Almanac per packet.  Temperature and pressure feed the
        refraction model; like WeeWX's Cheetah generator (which uses the
        archive record closest to report time), take them from the current
        packet when present."""
        temperature_c: Optional[float] = None
        pressure_mbar: Optional[float] = None
        try:
            if pkt.get('outTemp') is not None:
                temperature_c = weewx.units.convert(
                    weewx.units.as_value_tuple(pkt, 'outTemp'), 'degree_C')[0]
            if pkt.get('barometer') is not None:
                pressure_mbar = weewx.units.convert(
                    weewx.units.as_value_tuple(pkt, 'barometer'), 'mbar')[0]
        except (KeyError, weewx.UnitError):
            pass
        return weewx.almanac.Almanac(
            pkt['dateTime'],
            self.latitude,
            self.longitude,
            altitude    = self.altitude_m,
            temperature = temperature_c,
            pressure    = pressure_mbar,
            texts       = self.texts,
            formatter   = self.formatter,
            converter   = self.converter)

    def evaluate(self, almanac_field: AlmanacField, base_almanac: weewx.almanac.Almanac,
            pkt_time: int) -> Any:
        """Walk the attribute chain exactly as Cheetah would walk the report
        tag, including auto-calling a callable result."""
        almanac = base_almanac
        if almanac_field.days != 0:
            almanac = almanac(almanac_time=AlmanacFieldEvaluator.shift_days(
                pkt_time, almanac_field.days))
        if len(almanac_field.almanac_kwargs) > 0:
            almanac = almanac(**almanac_field.almanac_kwargs)
        obj: Any = almanac
        for segment in almanac_field.chain:
            obj = getattr(obj, segment.name)
            if segment.kwargs is not None:
                obj = obj(**segment.kwargs)
        if callable(obj):
            obj = obj()
        return obj

    def to_json_value(self, almanac_field: AlmanacField, obj: Any) -> Any:
        """Apply the format spec (ValueHelpers format exactly as the report
        tag would render) and coerce to a json-serializable value."""
        if isinstance(obj, weewx.units.ValueHelper):
            if almanac_field.format_spec is not None:
                # ValueHelper exposes every format spec as an attribute of
                # the same name; the parser admits nothing else here.  Call
                # specs (format/nolabel/string/long_form) are methods, called
                # with the field's bound kwargs; a bare spec that is callable
                # (ordinal_compass) is called with none, as Cheetah's
                # auto-call renders it.
                value = getattr(obj, almanac_field.format_spec)
                if callable(value):
                    value = value(**(almanac_field.format_kwargs or {}))
            else:
                value = str(obj)
        elif almanac_field.format_spec is not None and almanac_field.format_spec != 'raw':
            # formatted/ordinal_compass need a ValueHelper; .raw is allowed as
            # identity on plain values (almanac.moon_index.raw).
            raise TypeError('%s: %s returned %s, which does not support .%s'
                % (almanac_field.field, '.'.join(seg.name for seg in almanac_field.chain),
                   type(obj).__name__, almanac_field.format_spec))
        else:
            value = obj
        if value is None or isinstance(value, (bool, int, float, str)):
            return value
        return str(value)

    def compute(self, almanac_field: AlmanacField, base_almanac: weewx.almanac.Almanac,
            pkt_time: int) -> None:
        try:
            obj = self.evaluate(almanac_field, base_almanac, pkt_time)
            if almanac_field.tier == 'event':
                raw = obj.raw if isinstance(obj, weewx.units.ValueHelper) else obj
                self.event_ts[almanac_field.field] = raw if isinstance(raw, (int, float)) else None
            self.values[almanac_field.field] = self.to_json_value(almanac_field, obj)
        except Exception as e:
            reraise_if_terminate(e)
            if almanac_field.field not in self.warned:
                self.warned.add(almanac_field.field)
                log.info('Cannot evaluate almanac field %s: %s' % (almanac_field.field, e))
            self.values[almanac_field.field] = AlmanacFieldEvaluator.SKIP
            if almanac_field.tier == 'event':
                self.event_ts[almanac_field.field] = None

    def roll_day(self, day: date) -> None:
        advancing = self.cache_day is not None and day > self.cache_day
        day_start_ts = time.mktime(day.timetuple())
        for almanac_field in self.fields:
            if almanac_field.tier == 'day':
                self.values.pop(almanac_field.field, None)
            elif almanac_field.tier == 'event':
                event_ts = self.event_ts.get(almanac_field.field)
                if not advancing or event_ts is None or event_ts < day_start_ts:
                    self.values.pop(almanac_field.field, None)
                    self.event_ts.pop(almanac_field.field, None)
        self.cache_day = day

    def insert_fields(self, loopdata_pkt: Dict[str, Any], pkt: Dict[str, Any]) -> None:
        if len(self.fields) == 0:
            return
        pkt_time: int = to_int(pkt['dateTime'])
        day = date.fromtimestamp(pkt_time)
        if day != self.cache_day:
            self.roll_day(day)
        base_almanac = self.build_almanac(pkt)
        for almanac_field in self.fields:
            if almanac_field.tier == 'continuous' or almanac_field.field not in self.values:
                self.compute(almanac_field, base_almanac, pkt_time)
            value = self.values.get(almanac_field.field)
            if value is not None and value is not AlmanacFieldEvaluator.SKIP:
                loopdata_pkt[almanac_field.field] = value

class LoopProcessor:
    def __init__(self, cfg: Configuration):
        self.cfg = cfg
        self.archive_start: float = time.time()
        self.almanac_eval: Optional[AlmanacFieldEvaluator] = \
            AlmanacFieldEvaluator(cfg) if len(cfg.almanac_fields) > 0 else None

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
                loopdata_pkt = LoopProcessor.generate_loopdata_dictionary(
                    pkt, self.cfg, self.accumulators, self.almanac_eval)
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
    def generate_loopdata_dictionary(in_pkt: Dict[str, Any], cfg: Configuration, accums: Accumulators,
            almanac_eval: Optional[AlmanacFieldEvaluator] = None) -> Dict[str, Any]:

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

        # Add packets to continuous accumulators.
        for per, accum in accums.continuous.items():
            pruned_pkt = LoopProcessor.prune_period_packet(pkt, cfg.obstypes.continuous[per])
            accums.continuous[per].addRecord(pruned_pkt, weight=cfg.loop_frequency)

        # Create the loopdata dictionary.
        loopdata_pkt = LoopProcessor.create_loopdata_packet(pkt, cfg, accums)

        # Almanac fields are computed from the (unpruned) incoming packet's
        # time, temperature and pressure, not from accumulators.
        if almanac_eval is not None:
            almanac_eval.insert_fields(loopdata_pkt, in_pkt)

        return loopdata_pkt

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
    def render_field(cname: CheetahName, value_t: Tuple[Any, Any, Any],
            loopdata_pkt: Dict[str, Any], formatter: weewx.units.Formatter,
            time_context: str = 'current', is_delta: bool = False) -> None:
        """Render a converted value tuple into loopdata_pkt[cname.field] per
        the field's format_spec, dispatching through FORMAT_SPECS -- or, for
        call-syntax specs (format_kwargs is not None), CALL_FORMAT_SPECS (no
        spec, and specs with no renderer, get the default labeled rendering).

        time_context is the [Units][TimeFormats] context for time values,
        per the field's period.  is_delta marks trend values, which are
        differences rather than observations: a delta of a compass direction
        is not a direction and a delta of a time is not a timestamp, so
        ordinal_compass and .formatted's time-context path fall back to the
        plain numeric renderings (longstanding shipped behavior)."""
        format_spec = cname.format_spec
        if is_delta and format_spec == 'ordinal_compass':
            format_spec = None
        renderer: Optional[Callable[[CheetahName, Tuple[Any, Any, Any],
            Dict[str, Any], weewx.units.Formatter, str, bool], None]] = None
        if format_spec is not None:
            if cname.format_kwargs is not None:
                renderer = _render_call_spec
            else:
                renderer = FORMAT_SPECS.get(format_spec)
        if renderer is None:
            renderer = _render_default
        renderer(cname, value_t, loopdata_pkt, formatter, time_context, is_delta)

    @staticmethod
    def render_missing(cname: CheetahName, loopdata_pkt: Dict[str, Any],
            formatter: weewx.units.Formatter, is_delta: bool = False) -> bool:
        """Missing-data hook: a field whose format spec carries explicit None
        handling (spec_emits_none) is emitted as its None rendering -- what
        the report tag would show -- instead of being omitted; returns True
        if the field was emitted.  The None rendering never reads the unit or
        the time context, so a unitless value tuple suffices."""
        if not spec_emits_none(cname):
            return False
        LoopProcessor.render_field(cname, (None, None, None), loopdata_pkt,
            formatter, is_delta=is_delta)
        return True

    @staticmethod
    def add_current_obstype(cname: CheetahName, pkt: Dict[str, Any],
            loopdata_pkt: Dict[str, Any], converter: weewx.units.Converter,
            formatter: weewx.units.Formatter) -> None:

        if cname.obstype not in pkt:
            if not LoopProcessor.render_missing(cname, loopdata_pkt, formatter):
                log.debug('%s not found in packet, skipping %s' % (cname.obstype, cname.field))
            return

        try:
            value, unit_type, group_type = LoopProcessor.convert_current_obs(
                    converter, cname.obstype, pkt, cname.unit)
        except (KeyError, ValueError) as e:
            # Unit override incompatible with the obstype's group (e.g. a
            # temperature asked for in beaufort).  Skip the field.
            log.debug('%s: cannot convert %s to %s: %s' % (cname.field, cname.obstype, cname.unit, e))
            return

        if value is None:
            if not LoopProcessor.render_missing(cname, loopdata_pkt, formatter):
                log.debug('%s not found in loop packet.' % cname.field)
            return

        LoopProcessor.render_field(cname, (value, unit_type, group_type),
            loopdata_pkt, formatter)

    @staticmethod
    def add_period_obstype(cname: CheetahName, period_accum: Union[weewx.accum.Accum, ContinuousAccum],
            loopdata_pkt: Dict[str, Any], converter: weewx.units.Converter,
            formatter: weewx.units.Formatter) -> None:
        if cname.obstype not in period_accum:
            if not LoopProcessor.render_missing(cname, loopdata_pkt, formatter):
                log.debug('No %s stats for %s, skipping %s' % (cname.period, cname.obstype, cname.field))
            return

        stats = period_accum[cname.obstype]

        # The grammar guarantees an agg_type for every period that reaches this
        # function, but the field is Optional; a None agg matches no dispatch
        # table -- skip, exactly as the old else-branches did.
        agg_type = cname.agg_type
        if agg_type is None:
            return

        if (isinstance(stats, weewx.accum.ScalarStats) or isinstance(stats, ContinuousScalarStats))  and stats.lasttime is not None:
            extractor = SCALAR_AGGS.get(agg_type)
            if extractor is None:
                # Aggregate not defined for scalar stats (e.g. vecdir on a
                # scalar obstype) -- skip, as before.
                return
            src_value = extractor(stats, ScalarStatsTuple(*stats.getStatsTuple()))

        elif (isinstance(stats, weewx.accum.VecStats) or isinstance(stats, ContinuousVecStats)) and stats.count != 0:
            extractor = VEC_AGGS.get(agg_type)
            if extractor is None:
                return
            src_value = extractor(stats, VecStatsTuple(*stats.getStatsTuple()))

        elif isinstance(stats, ContinuousFirstLastAccum) and stats.firsttime is not None:
            # FirstLastAccum may hold values of almost any type (weewx uses it
            # for string obstypes, but the value's native type is preserved).
            # Route through the shared convert/format block below; the default
            # branch handles strings (emit as-is) vs numerics (format).
            extractor = FIRSTLAST_AGGS.get(agg_type)
            if extractor is None:
                return
            src_value = extractor(stats, None)  # firstlast reads only object props

        else:
            # No stats available (e.g. empty accumulator).
            LoopProcessor.render_missing(cname, loopdata_pkt, formatter)
            return

        if src_value is None:
            if not LoopProcessor.render_missing(cname, loopdata_pkt, formatter):
                log.debug('Currently no %s stats for %s.' % (cname.period, cname.field))
            return

        src_type, src_group = weewx.units.getStandardUnitType(period_accum.unit_system, cname.obstype, agg_type=cname.agg_type)

        try:
            if cname.unit is None:
                tgt_value, tgt_type, tgt_group = converter.convert((src_value, src_type, src_group))
            else:
                # Unit override: convert straight to the requested unit rather
                # than the target report's unit for this group.
                tgt_value, tgt_type, tgt_group = weewx.units.convert((src_value, src_type, src_group), cname.unit)
        except (KeyError, ValueError) as e:
            # Unit override incompatible with the obstype's group.  Skip the field.
            log.debug('%s: cannot convert %s to %s: %s' % (cname.field, cname.obstype, cname.unit, e))
            return

        # WeeWX formats times per time context: a report tag like
        # $day.outTemp.maxtime uses the 'day' entry of the target report's
        # [Units][TimeFormats].  Pass the period as the context so loopdata
        # matches.  Continuous periods (Nm/Nh) have no report analog and keep
        # the 'current' context.  'alltime' maps to 'year' because that is the
        # context weewx.tags binds for the $alltime tag (there is no 'alltime'
        # TimeFormats entry).
        time_context = 'current' if cname.period is None \
            or LoopData.is_continuous_period(cname.period) else cname.period
        if time_context == 'alltime':
            time_context = 'year'

        LoopProcessor.render_field(cname, (tgt_value, tgt_type, tgt_group),
            loopdata_pkt, formatter, time_context=time_context)

    @staticmethod
    def add_trend_obstype(cname: CheetahName, accum: ContinuousAccum,
            pkt: Dict[str, Any], loopdata_pkt: Dict[str, Any], time_delta: int,
            loop_frequency: float, baro_trend_descs, converter: weewx.units.Converter,
            formatter: weewx.units.Formatter) -> None:

        if cname.obstype not in accum:
            if not LoopProcessor.render_missing(cname, loopdata_pkt, formatter, is_delta=True):
                log.debug('No %s stats for %s, skipping %s' % (cname.period, cname.obstype, cname.field))
            return

        # A unit override re-targets the numeric trend.  For the barometer
        # code/desc classifications there is no numeric output to re-unit, so any
        # override is ignored there and the trend is computed in report units.
        is_baro_code_desc = cname.obstype == 'barometer' and (cname.format_spec == 'code' or cname.format_spec == 'desc')
        trend_unit = None if is_baro_code_desc else cname.unit

        value, unit_type, group_type = LoopProcessor.get_trend(cname, pkt, accum, converter, time_delta, loop_frequency, trend_unit)
        if value is None:
            if not LoopProcessor.render_missing(cname, loopdata_pkt, formatter, is_delta=True):
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

        LoopProcessor.render_field(cname, (value, unit_type, group_type),
            loopdata_pkt, formatter, is_delta=True)


    @staticmethod
    def convert_current_obs(converter: weewx.units.Converter, obstype: str,
            pkt: Dict[str, Any], target_unit: Optional[str] = None) -> Tuple[Any, Any, Any]:
        """ Returns value, unit_type, group_type.

        When target_unit is None the value is converted to the target report's
        unit for the obstype's group (converter.convert).  When target_unit is
        given (a unit override), the value is converted directly to that unit
        (weewx.units.convert), which raises if the unit is incompatible with the
        obstype's group -- callers guard for that. """

        v_t = weewx.units.as_value_tuple(pkt, obstype)
        if target_unit is None:
            value, unit_type, group_type = converter.convert(v_t)
        else:
            value, unit_type, group_type = weewx.units.convert(v_t, target_unit)

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

            # fixed periods
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

            # continuous periods
            for per, accum in accums.continuous.items():
                if cname.period == per:
                    if per == 'trend':
                        LoopProcessor.add_trend_obstype(cname, accum, pkt,
                            loopdata_pkt, cfg.time_delta, cfg.loop_frequency, cfg.baro_trend_descs, cfg.converter, cfg.formatter)
                    else:
                        LoopProcessor.add_period_obstype(cname,  accum, loopdata_pkt, cfg.converter, cfg.formatter)
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
        # rename it to filename
        os.replace(tmpname, os.path.join(loop_data_dir, filename))
        log.debug('Renamed to %s' % os.path.join(loop_data_dir, filename))

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
        log.info('obstypes.current        : %s' % cfg.obstypes.current)
        log.info('obstypes.alltime        : %s' % cfg.obstypes.alltime)
        log.info('obstypes.rainyear       : %s' % cfg.obstypes.rainyear)
        log.info('obstypes.year           : %s' % cfg.obstypes.year)
        log.info('obstypes.month          : %s' % cfg.obstypes.month)
        log.info('obstypes.week           : %s' % cfg.obstypes.week)
        log.info('obstypes.day            : %s' % cfg.obstypes.day)
        log.info('obstypes.hour           : %s' % cfg.obstypes.hour)
        for per, obstypes in cfg.obstypes.continuous.items():
            log.info('obstypes.%s: %s' % (per, obstypes))
        log.info('baro_trend_descs        : %s' % cfg.baro_trend_descs)
        if len(cfg.almanac_fields) > 0:
            log.info('almanac_fields          : %s' % [ f.field for f in cfg.almanac_fields ])
            log.info('latitude                : %f' % cfg.latitude)
            log.info('longitude               : %f' % cfg.longitude)
            log.info('altitude_m              : %f' % cfg.altitude_m)

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
    def get_trend(cname: CheetahName, pkt: Dict[str, Any], accum: ContinuousAccum,
            converter, time_delta: int, loop_frequency: float,
            target_unit: Optional[str] = None) -> Tuple[Optional[Any], Optional[str], Optional[str]]:
        if not cname.obstype in accum:
            return None, None, None
        first = accum[cname.obstype].first
        firsttime = accum[cname.obstype].firsttime
        last = accum[cname.obstype].last
        lasttime = accum[cname.obstype].lasttime
        if first is None or last is None:
            return None, None, None
        if firsttime == lasttime:
            # Need atleast two readings to get a trend.
            return None, None, None
        try:
            # Convert the endpoints to the trend's output unit (target_unit when a
            # unit override is in play, else the report target unit) BEFORE
            # subtracting.  Doing it in this order is what makes a unit override
            # correct for offset units like temperature: the offset cancels in the
            # difference, so an X degree_F delta yields the right degree_C delta.
            start_value, unit_type, group_type = LoopProcessor.convert_current_obs(
                converter, cname.obstype, { 'dateTime': firsttime, 'usUnits': pkt['usUnits'], cname.obstype: first }, target_unit)
            end_value, unit_type, group_type = LoopProcessor.convert_current_obs(
                converter, cname.obstype, { 'dateTime': lasttime, 'usUnits': pkt['usUnits'], cname.obstype: last }, target_unit)

            log.debug('get_trend: %s: start_value: %s' % (cname.obstype, start_value))
            log.debug('get_trend: %s: end_value: %s' % (cname.obstype, end_value))
            if start_value is not None and end_value is not None:
                trend = end_value - start_value
                # This may not be over the entire range of time_delta (e.g., new station startup)
                # Adjust to spread over entire range.
                actual_time_delta = lasttime - firsttime + loop_frequency
                adj_trend = time_delta / actual_time_delta * trend
                log.debug('get_trend: %s: %s unadjusted(%s)' % (cname.obstype, adj_trend, trend))
                return adj_trend, unit_type, group_type
        except Exception:
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
