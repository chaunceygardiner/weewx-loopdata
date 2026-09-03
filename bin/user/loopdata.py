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
import inspect
import itertools
import json
import logging
import math
import os
import pathlib
import queue
import re
import sys
import tempfile
import threading
import time

from collections import deque, namedtuple
from datetime import date, datetime, timedelta
from heapq import heapify, heappop, heappush
import dataclasses
from dataclasses import dataclass, field as dataclass_field
from typing import Any, Callable, Deque, Dict, FrozenSet, Generator, Generic, List, Optional, Set, Tuple, TypeVar, Union
from enum import Enum

import weewx
import weewx.almanac
import weewx.defaults
import weewx.manager
import weewx.reportengine
import weewx.station
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

LOOP_DATA_VERSION = '7.0.1'

if sys.version_info[0] < 3 or (sys.version_info[0] == 3 and sys.version_info[1] < 7):
    raise weewx.UnsupportedFeature(
        "weewx-loopdata requires Python 3.7 or later, found %s.%s" % (sys.version_info[0], sys.version_info[1]))

def version_tuple(version: Any) -> Tuple[int, ...]:
    """'4.10.2' -> (4, 10, 2), for comparing versions as numbers rather
    than as strings.  install.py imports this so the installer's floor
    check and the service's cannot drift apart."""
    return tuple(int(part) for part in re.findall(r'\d+', str(version))[:3])

# 4.6 is where the module-level skin-dict builder this extension calls
# (weewx.reportengine._build_skin_dict, a method before that) and $gettext
# arrived.
if version_tuple(weewx.__version__) < (4, 6):
    raise weewx.UnsupportedFeature(
        "weewx-loopdata requires WeeWX 4.6 or later, found %s" % weewx.__version__)

windrun_bucket_suffixes: List[str] = [ 'N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE',
                                       'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW' ]

# The 'windrose' obstype: a NOAA-style rose accumulated per period as 16
# compass bins (windrun_bucket_suffixes order, N clockwise) by N speed bands.
WINDROSE_BINS: int = len(windrun_bucket_suffixes)

# Default band edges for windrose, in meter_per_second: the classic
# WRPLOT/NOAA bands.  The first edge doubles as the calm threshold.
# [LoopData] windrose_bands overrides, in the target report's windSpeed unit.
WINDROSE_DEFAULT_BANDS_MPS: List[float] = [0.5, 2.1, 3.6, 5.7, 8.8, 11.1]

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
    round_ndigits: Optional[int] = None # round(n) transform (grammar-ordered between unit and format_spec): round the value to n digits before the format spec renders it.  None means no round segment, or a bare round()/round -- rounder treats ndigits None as identity, so the distinction never matters.
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
    recomputed every packet, 'day' fields once per local day, and 'event'
    fields divide on group: previous_* fields (group None) are kept until the
    local day advances past the cached event, while next_* fields are cached
    as a GROUP that expires the moment the event's own instant passes -- a
    next_* value whose instant is behind us no longer is what the field name
    promises (day/event results holding no data are not cached -- every
    packet retries)."""
    field         : str                  # almanac(horizon=-6).sun(use_center=1).rise.raw
    almanac_kwargs: Dict[str, float]     # kwargs of the leading almanac segment (days removed)
    days          : int                  # local calendar-day shift (almanac(days=±N))
    chain         : List[AlmanacSegment] # attribute chain after the leading almanac segment
    format_spec   : Optional[str]        # formatted, raw, ordinal_compass, a call spec (format/nolabel/string/long_form), or None
    tier          : str                  # continuous, day or event
    format_kwargs : Optional[Dict[str, Any]] = None # call-syntax specs only (see CheetahName.format_kwargs)
    round_ndigits : Optional[int] = None # round(n) transform (see CheetahName.round_ndigits); applied via ValueHelper.round before the format spec
    group         : Optional[str] = None # next_* fields only: key shared by fields whose chains agree up through the first next_* segment (plus the leading kwargs and day shift), so they cache and expire as a unit
    def __hash__(self):
        return hash(self.field)

@dataclass
class StationField:
    """A parsed station entry from the fields line.  The grammar is a WeeWX
    report $station tag with the $ removed (station.uptime.raw,
    station.version, station.altitude.meter.raw): station, then an attribute
    chain walked against weewx.station.Station exactly as Cheetah would walk
    the tag (the first name is the Station attribute, later names are
    ValueHelper attributes such as a unit conversion), then loopdata's
    optional round(n) and format spec.  uptime and os_uptime tick, so they
    are recomputed every packet; every other attribute is constant for the
    life of the weewxd process and is computed once."""
    field        : str                   # station.uptime.raw
    chain        : List[str]             # attribute chain after station
    format_spec  : Optional[str]         # formatted, raw, ordinal_compass, a call spec, or None
    format_kwargs: Optional[Dict[str, Any]] = None # call-syntax specs only (see CheetahName.format_kwargs)
    round_ndigits: Optional[int] = None  # round(n) transform (see CheetahName.round_ndigits)
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
    continuous      : Dict[str, Set[str]] # e.g., continuous['24h'], or ['trend'] as parsed; the union keys trends 'trend@<secs>'

@dataclass
class ReportContext:
    """One report's share of the work: the fields it declared and the
    formatter, converter, [Texts], [Almanac] names and $station it renders
    them with.  Every report that declares fields ([LoopData] [[fields]] in
    its skin.conf, or under its stanza in weewx.conf) is its own context,
    rendered under its report name in the output; the legacy
    [LoopData] [[Include]] fields line is one more context, rendered flat
    at the top level exactly as it always was, through the target_report.

    Accumulation is shared (see Configuration): every context renders off
    the one set of accumulators.  Two report settings reach the
    accumulators rather than the renderers -- the trend window
    ([Units] [Trend] time_delta) and the windrose band edges -- so each
    context resolves its own and selects a shared accumulator BY VALUE:
    trend_key names the ContinuousAccum sized to this report's window,
    windrose_key the WindRose accumulators banded with its edges.  Reports
    that agree share; only a report that differs gets a second one."""
    report_name       : Optional[str] # None for the legacy fields line
    specified_fields  : List[str]
    fields_to_include : Set[CheetahName]
    almanac_fields    : List[AlmanacField]
    station_fields    : List[StationField]
    formatter         : weewx.units.Formatter
    converter         : weewx.units.Converter
    baro_trend_descs  : Any # Dict[BarometerTrend, str], in the report's language
    almanac_texts     : Dict[str, Any] # the report's [Almanac] section (moon_phases, ...)
    station           : Optional[Any] # weewx.station.Station (the report's $station); set when station_fields is non-empty
    time_delta        : int # the trend window, seconds
    windrose_bands    : List[float] # band edges, in the report's windSpeed unit
    obstypes          : ObsTypes # this context's own observation types per period, from its fields
    render_signature  : str # everything that decides how a value renders; equal signatures render identically (see render_signature())
    source_report     : Optional[str] = None # the report whose configuration renders this context (target_report for the legacy line)

    @property
    def label(self) -> str:
        """How log messages name this context."""
        return ReportContext.label_for(self.report_name)

    @staticmethod
    def label_for(report_name: Optional[str]) -> str:
        if report_name is None:
            return '[LoopData] [[Include]] fields'
        return 'report %s' % report_name

    @property
    def trend_key(self) -> str:
        """accums.continuous key of the trend accumulator sized to this
        report's window (see LoopData.trend_key)."""
        return LoopData.trend_key(self.time_delta)

    @property
    def windrose_key(self) -> str:
        """accums.windrose_* key of the accumulators banded with this
        report's edges (see LoopData.windrose_bands_key)."""
        return LoopData.windrose_bands_key(
            self.converter.getTargetUnit('windSpeed')[0], self.windrose_bands)

    @property
    def windrose(self) -> bool:
        """Does this report declare a windrose (a <period>.windrose.<agg>
        field -- unit.label.windrose alone is a label, not a rose)?"""
        return any(cname.obstype == 'windrose' and cname.period is not None
                   for cname in self.fields_to_include)

@dataclass
class Configuration:
    """Everything shared across reports: the accumulators' unit system, the
    union of every context's observation types and periods, the file, the
    rsync.  The per-report half lives in ReportContext: `legacy` for the
    [[Include]] fields line (None when there is none) and `reports` for
    the declaring reports."""
    queue                    : queue.SimpleQueue
    config_dict              : Dict[str, Any]
    unit_system              : int
    archive_interval         : int
    archive_delay            : int
    loop_data_dir            : str
    filename                 : str
    target_report            : Optional[str] # the report the legacy fields line renders through; also anchors a relative loop_data_dir
    loop_frequency           : float
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
    week_start               : int
    rainyear_start           : int
    legacy                   : Optional[ReportContext] = None
    reports                  : List[ReportContext] = dataclass_field(default_factory=list)
    legacy_shared            : Dict[str, str] = dataclass_field(default_factory=dict) # legacy key -> the report whose entry it is copied flat from, instead of rendered again
    # The union over every context, computed by recompute(): observation
    # types per period (continuous keyed by period, trends by trend_key),
    # and the windrose accumulators per band edges.
    obstypes                 : ObsTypes = dataclass_field(default_factory=lambda: ObsTypes(
                                   current=set(), alltime=set(), rainyear=set(), year=set(),
                                   month=set(), week=set(), day=set(), hour=set(), continuous={}))
    windrose_bandings        : Dict[str, Tuple[str, List[float]]] = dataclass_field(default_factory=dict) # windrose_key -> (report windSpeed unit, edges in that unit)
    windrose_span_periods    : Set[Tuple[str, str]] = dataclass_field(default_factory=set) # (windrose_key, period)
    windrose_continuous_periods : Set[Tuple[str, str]] = dataclass_field(default_factory=set) # (windrose_key, period)
    latitude                 : float = 0.0 # station latitude in decimal degrees
    longitude                : float = 0.0 # station longitude in decimal degrees
    altitude_m               : float = 0.0 # station altitude in meters

    @property
    def contexts(self) -> List[ReportContext]:
        """Every context, the legacy one first."""
        return ([self.legacy] if self.legacy is not None else []) + list(self.reports)

    def recompute(self) -> None:
        """Derive the shared unions from the contexts.  Call after the
        contexts change (construction, or a test adding a report)."""
        contexts = self.contexts
        self.obstypes = LoopData.union_obstypes(contexts)
        self.windrose_bandings, self.windrose_span_periods, self.windrose_continuous_periods = \
            LoopData.union_windrose(contexts)

    def almanac_fields_all(self) -> List[AlmanacField]:
        return [f for ctx in self.contexts for f in ctx.almanac_fields]

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

# windrose has its own aggregate set: projections of a WindRoseAccum's cells
# (see LoopProcessor.add_windrose_obstype), not stats-table extractors.
WINDROSE_AGG_TYPES: FrozenSet[str] = frozenset(('sum', 'time', 'banded', 'calm'))

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

# ===============================================================================
#                             WindRose accumulators
# ===============================================================================

@dataclass
class WindRoseBanding:
    """Banding parameters shared by every WindRoseAccum, in the ACCUMULATORS'
    unit system: the band edges (windSpeed units, ascending; edges[0] doubles
    as the calm threshold) and the divisor turning windSpeed x seconds into
    the unit system's distance unit (WXXTypes.calc_windrun parity:
    US mph*s/3600 -> miles, METRIC km/h*s/3600 -> km, METRICWX m/s*s/1000 -> km)."""
    unit_system         : int
    edges               : List[float]
    seconds_per_distance: float

    def classify(self, wind_speed: float, wind_dir: Optional[float]) -> Tuple[int, int]:
        """(bin, band) for a sample; (-1, -1) for calm -- speed below the calm
        threshold, or no wind direction (a vane reading means nothing then)."""
        band = -1
        for edge in self.edges:
            if wind_speed < edge:
                break
            band += 1
        if band < 0 or wind_dir is None:
            return -1, -1
        return LoopProcessor.get_windrun_bucket(wind_dir), band

    def distance(self, wind_speed: float, seconds: float) -> float:
        return wind_speed * seconds / self.seconds_per_distance

class WindRoseAccum:
    """NOAA-style windrose accumulator: 16 compass bins x N speed bands, each
    cell holding seconds and distance (sum of speed*dt), plus a directionless
    calm cell (seconds).  Every windrose field is a projection of these cells:
    .banded (the seconds matrix), .time (per-bin seconds), .sum (per-bin
    distance -- windrun parity), .calm.  Deliberately OUTSIDE the weewx.accum
    machinery: windrose is not a packet obstype, so nothing is registered in
    obs_group_dict and no keys are injected into packets."""

    def __init__(self, banding: WindRoseBanding) -> None:
        self.banding = banding
        self.reset()

    def reset(self) -> None:
        n_bands = len(self.banding.edges)
        self.time_bins = [[0.0] * n_bands for _ in range(WINDROSE_BINS)]
        self.dist_bins = [[0.0] * n_bands for _ in range(WINDROSE_BINS)]
        self.calm_seconds = 0.0

    def _credit(self, bkt: int, band: int, seconds: float, dist: float) -> None:
        """Add (or, negated, subtract) one sample's contribution.  bkt -1 is
        the calm cell: seconds only -- sub-threshold distance is dropped, it
        has no usable direction to attribute it to."""
        if bkt < 0:
            self.calm_seconds += seconds
        else:
            self.time_bins[bkt][band] += seconds
            self.dist_bins[bkt][band] += dist

    def _sample(self, wind_speed: float, wind_dir: Optional[float],
            weight: float) -> Tuple[int, int, float]:
        bkt, band = self.banding.classify(wind_speed, wind_dir)
        dist = 0.0 if bkt < 0 else self.banding.distance(wind_speed, weight)
        return bkt, band, dist

    def add(self, ts: int, wind_speed: float, wind_dir: Optional[float],
            weight: float) -> None:
        raise NotImplementedError

    def bin_times(self) -> List[float]:
        return [sum(bands) for bands in self.time_bins]

    def bin_distances(self) -> List[float]:
        return [sum(bands) for bands in self.dist_bins]

class WindRoseSpanAccum(WindRoseAccum):
    """Windrose over a WeeWX span period (hour/day/week/.../alltime).  Cells
    only grow; a packet outside the span resets to the packet's span (no
    OutOfSpan dance -- there are no merged stats to rebuild).  alltime
    (span_fn None) never resets."""

    def __init__(self, banding: WindRoseBanding,
            span_fn: Optional[Callable[[int], weeutil.weeutil.TimeSpan]],
            ts: int) -> None:
        super().__init__(banding)
        self.span_fn = span_fn
        self.timespan: Optional[weeutil.weeutil.TimeSpan] = \
            span_fn(ts) if span_fn is not None else None

    def add(self, ts: int, wind_speed: float, wind_dir: Optional[float],
            weight: float) -> None:
        if self.timespan is not None and not self.timespan.includesArchiveTime(ts):
            assert self.span_fn is not None
            self.timespan = self.span_fn(ts)
            self.reset()
        bkt, band, dist = self._sample(wind_speed, wind_dir, weight)
        self._credit(bkt, band, weight, dist)

@dataclass
class WindRoseDebit:
    expiration: float
    bkt       : int
    band      : int
    seconds   : float
    dist      : float

class WindRoseContinuousAccum(WindRoseAccum):
    """Windrose over a rolling timelength window: every credit is also queued
    as a future debit (the ContinuousScalarStats pattern) and
    trimExpiredEntries subtracts contributions that age out."""

    def __init__(self, banding: WindRoseBanding, timelength: int) -> None:
        super().__init__(banding)
        self.timelength = timelength
        self.future_debits: Deque[WindRoseDebit] = deque()

    def add(self, ts: int, wind_speed: float, wind_dir: Optional[float],
            weight: float) -> None:
        bkt, band, dist = self._sample(wind_speed, wind_dir, weight)
        self._credit(bkt, band, weight, dist)
        self.future_debits.append(WindRoseDebit(
            expiration = ts + self.timelength,
            bkt        = bkt,
            band       = band,
            seconds    = weight,
            dist       = dist))
        self.trimExpiredEntries(ts)

    def trimExpiredEntries(self, ts: float) -> None:
        while len(self.future_debits) > 0 and self.future_debits[0].expiration <= ts:
            debit = self.future_debits.popleft()
            self._credit(debit.bkt, debit.band, -debit.seconds, -debit.dist)

@dataclass
class Accumulators:
    alltime_accum        : Optional[weewx.accum.Accum]
    rainyear_accum       : Optional[weewx.accum.Accum]
    year_accum           : Optional[weewx.accum.Accum]
    month_accum          : Optional[weewx.accum.Accum]
    week_accum           : Optional[weewx.accum.Accum]
    day_accum            : weewx.accum.Accum
    hour_accum           : Optional[weewx.accum.Accum]
    continuous           : Dict[str, ContinuousAccum] # e.g., continuous['24h'], or ['trend@10800'] (see trend_key())
    windrose_span        : Dict[Tuple[str, str], WindRoseSpanAccum] = dataclass_field(default_factory=dict) # (windrose_key, period)
    windrose_continuous  : Dict[Tuple[str, str], WindRoseContinuousAccum] = dataclass_field(default_factory=dict) # (windrose_key, period)

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

# The English descriptions served for trend.barometer.desc.  Each doubles
# as a gettext-style [Texts] key in the TARGET report: a lang file (or a
# [[[Texts]]] entry on the report's stanza in weewx.conf) translates a
# description by carrying the English string as its key, and a missing key
# falls back to the English one string at a time -- the same machinery as
# every other translated string.
BARO_TREND_DESCS: Dict[BarometerTrend, str] = {
    BarometerTrend.RISING_VERY_RAPIDLY : 'Rising Very Rapidly',
    BarometerTrend.RISING_QUICKLY      : 'Rising Quickly',
    BarometerTrend.RISING              : 'Rising',
    BarometerTrend.RISING_SLOWLY       : 'Rising Slowly',
    BarometerTrend.STEADY              : 'Steady',
    BarometerTrend.FALLING_SLOWLY      : 'Falling Slowly',
    BarometerTrend.FALLING             : 'Falling',
    BarometerTrend.FALLING_QUICKLY     : 'Falling Quickly',
    BarometerTrend.FALLING_VERY_RAPIDLY: 'Falling Very Rapidly',
}

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

        station_dict             = config_dict.get('Station', {})
        std_archive_dict         = config_dict.get('StdArchive', {})
        loop_config_dict         = config_dict.get('LoopData', {})
        file_spec_dict           = loop_config_dict.get('FileSpec', {})
        formatting_spec_dict     = loop_config_dict.get('Formatting', {})
        loop_frequency_spec_dict = loop_config_dict.get('LoopFrequency', {})
        rsync_spec_dict          = loop_config_dict.get('RsyncSpec', {})
        include_spec_dict        = loop_config_dict.get('Include', {})

        # Get the unit_system as specified by StdConvert->target_unit.
        # Note: this value will be overwritten if the day accumulator has a a unit_system.
        default_binding = config_dict.get('StdReport')['data_binding']
        with weewx.manager.DBBinder(config_dict) as db_binder:
            unit_system = db_binder.get_manager(default_binding).std_unit_system
        if unit_system is None:
            unit_system = weewx.units.unit_constants[self.config_dict['StdConvert'].get('target_unit', 'US').upper()]

        # The report skin dicts, one build per report name (the legacy
        # target_report usually is a declaring report too).
        report_dicts: Dict[str, Dict[str, Any]] = {}
        def report_dict(report: str) -> Dict[str, Any]:
            if report not in report_dicts:
                report_dicts[report] = LoopData.get_target_report_dict(config_dict, report)
            return report_dicts[report]

        # The legacy context: the [[Include]] fields line, rendered flat
        # through the target_report.  Deprecated as a unit with target_report
        # and the flat output: reports declare their own fields now.
        target_report: str = formatting_spec_dict.get('target_report', 'LoopDataReport')
        # target_report's dict serves the legacy line, the deprecated
        # [LoopData] windrose_bands fallback, and a relative loop_data_dir.
        target_dict: Optional[Dict[str, Any]] = None
        target_error: Optional[BaseException] = None
        try:
            target_dict = report_dict(target_report)
        except Exception as e:
            reraise_if_terminate(e)
            target_error = e
        legacy: Optional[ReportContext] = None
        legacy_fields: List[str] = LoopData.normalize_fields(include_spec_dict.get('fields'))
        windrose_shared: Dict[str, Any] = {}   # shared windrose_bands values, validated once
        if len(legacy_fields) == 0 and 'target_report' in formatting_spec_dict \
                and target_report != 'LoopDataReport':
            log.warning('[[Formatting]] target_report = %s is deprecated with the [[Include]] '
                'fields line, and with no fields line it does one thing only: a relative '
                'loop_data_dir is relative to its directory.  Before a later release removes '
                'it, set loop_data_dir to an absolute path.' % target_report)
        if loop_config_dict.get('windrose_bands') is not None:
            log.warning('[LoopData] windrose_bands is deprecated: it bands the rose of '
                'target_report (%s) and no other, which is what it did before 7.0.  '
                'windrose_bands is a report option now -- put it on that report\'s stanza in '
                'weewx.conf, in its own windSpeed unit, which is where finishing the migration '
                'moves it.' % target_report)
        if len(legacy_fields) > 0:
            log.warning('The [LoopData] [[Include]] fields line and [[Formatting]] target_report '
                'are deprecated: a report declares the fields it needs in its own '
                'skin.conf ([LoopData] [[fields]]), and an extension declares its own when '
                'installed.  Once every extension whose pages read the loop-data file has '
                'been upgraded, FINISH THE MIGRATION: run user.loopdata as a command (see '
                'https://chaunceygardiner.github.io/weewx-loopdata/declaring-fields.html'
                '#finishing-the-migration), which reports what the line still holds and, '
                'when every entry is accounted for, removes it with --apply.  Until then '
                'the line is honored as it always was.')
            if target_dict is None:
                if target_report not in config_dict.get('StdReport', {}):
                    log.error('Could not find target_report: %s.  The [LoopData] [[Include]] '
                        'fields line cannot be rendered and is ignored.' % target_report)
                else:
                    log.error('Could not build target_report %s.  The [LoopData] [[Include]] '
                        'fields line cannot be rendered and is ignored.  Exception: %s' % (
                        target_report, target_error))

        # The declaring reports: every enabled report whose merged skin dict
        # carries [LoopData] [[fields]].
        reports: List[ReportContext] = []
        for report in LoopData.enabled_reports(config_dict):
            try:
                skin_dict = report_dict(report)
            except Exception as e:
                reraise_if_terminate(e)
                log.error('Could not build report %s, skipping it.  Exception: %s' % (report, e))
                continue
            declared = LoopData.declared_fields_from_skin_dict(skin_dict, report)
            if len(declared) == 0:
                continue
            try:
                ctx = LoopData.build_report_context(report, declared, skin_dict,
                    LoopData.report_windrose_bands(config_dict, report, skin_dict,
                        loop_config_dict, target_dict, windrose_shared, target_report),
                    engine.stn_info)
            except Exception as e:
                reraise_if_terminate(e)
                log.error('Could not set up report %s, skipping it.  Exception: %s' % (report, e))
                continue
            reports.append(ctx)
            if report in legacy_fields:
                # Contrived, but the flat key and the report key would collide.
                log.error('Report %s is named like a field on the [[Include]] fields '
                    'line; the report overwrites the field in the output.' % report)

        # An upgraded station's [[Include]] line usually lists what its
        # target_report's skin now declares (the sample panel's 56, or all
        # of LiveSeasons').  Both render through the same report dict, so
        # the values would be identical: render the shared fields once, in
        # the declaring report's context, and copy them flat.  The legacy
        # context keeps only what nothing else renders.
        # The legacy context, built once, of what is left after the reports
        # that declare an entry -- and render it identically -- have taken
        # it over.  Parsing it before that and again afterwards would report
        # a skin author's typo twice.
        legacy_shared: Dict[str, str] = {}
        if len(legacy_fields) > 0 and target_dict is not None:
            legacy_bands = LoopData.legacy_windrose_bands(config_dict, target_report,
                target_dict, loop_config_dict, windrose_shared)
            legacy_signature = LoopData.render_signature(target_dict, legacy_bands)
            legacy_shared = LoopData.share_legacy_fields_by_name(
                legacy_fields, legacy_signature, reports, target_report)
            residual = [f for f in legacy_fields if f not in legacy_shared]
            if len(legacy_shared) > 0:
                by_report: Dict[str, int] = {}
                for field, report in legacy_shared.items():
                    if field != 'windrose.bands':
                        by_report[report] = by_report.get(report, 0) + 1
                log.info('%d of the %d [[Include]] fields are declared by reports that render '
                    'them identically (%s) and are rendered once, for both.' % (
                    sum(by_report.values()), len(legacy_fields),
                    ', '.join('%s: %d' % (r, n) for r, n in sorted(by_report.items()))))
            try:
                legacy = LoopData.build_report_context(None, residual, target_dict,
                    legacy_bands, engine.stn_info, source_report=target_report)
            except Exception as e:
                reraise_if_terminate(e)
                legacy_shared = {}
                log.error('Could not set up target_report %s for the [LoopData] [[Include]] '
                    'fields line, which is ignored.  Exception: %s' % (target_report, e))

        if legacy is None and len(reports) == 0:
            if len(legacy_fields) > 0:
                log.error('No fields to write: no enabled report declares [LoopData] [[fields]] '
                    'and the [LoopData] [[Include]] fields line could not be set up (see above).  '
                    'LoopData is exiting.')
            else:
                log.error('No fields to write: no enabled report declares [LoopData] [[fields]] '
                    'and there is no [LoopData] [[Include]] fields line.  LoopData is exiting.')
            return

        # A relative loop_data_dir is relative to the target_report's
        # directory -- LoopDataReport when none is named -- so the default
        # writes the file beside the sample page.  The installer puts the
        # [[LoopDataReport]] section back if it is missing, and enable =
        # false is enough to turn the page off, so the section is there on
        # any installed station; should it be gone anyway, [StdReport]
        # HTML_ROOT, said out loud, because a page polling beside itself
        # will not find the file there.
        anchor_dict: Dict[str, Any]
        if target_dict is not None:
            anchor_dict = target_dict
        else:
            anchor_dict = {'HTML_ROOT': config_dict['StdReport'].get('HTML_ROOT', 'public_html')}
            log.warning('No report %s; a relative loop_data_dir is relative to [StdReport] '
                'HTML_ROOT (%s).  A page expecting the file beside itself will not find it '
                'there: set loop_data_dir, or the page\'s loop_data_file, accordingly.' % (
                target_report, anchor_dict['HTML_ROOT']))
        loop_data_dir = LoopData.compose_loop_data_dir(config_dict, anchor_dict, file_spec_dict)
        os.makedirs(loop_data_dir, exist_ok=True)

        # Reserve a unique name to write each packet to before renaming it
        # onto the loop-data file.  Only the NAME is wanted: mkstemp creates
        # the file to guarantee the name is nobody else's, and it is removed
        # again straight away, which is the state it is in between writes
        # anyway (write_packet_to_file renames it away every packet).
        # Leaving it behind littered loop_data_dir -- which is inside the
        # web-served report tree by default -- with a zero-byte file for
        # every process that builds this service without ever reaching a
        # packet, `weectl report run` above all, since report_services
        # carries LoopData and its processor thread never starts there.
        tmp = tempfile.NamedTemporaryFile(prefix='LoopData', dir=loop_data_dir, delete=False)
        tmp.close()
        os.unlink(tmp.name)

        # Get the loop frequency seconds to be passed as the weight to accumulators.
        loop_frequency = to_float(loop_frequency_spec_dict.get('seconds', '2.0'))

        altitude_m = weewx.units.convert(engine.stn_info.altitude_vt, 'meter')[0]

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

        # The rsync timeout also drives the ssh-side time bounds folded into
        # ssh_options (see compose_ssh_options), so resolve it first.
        rsync_timeout: int = to_int(rsync_spec_dict.get('timeout', 1))

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
            tmpname                  = tmp.name,
            # The rsync switches default to False rather than being read
            # bare: to_bool(None) raises, so a station that deletes one of
            # these keys -- or the whole [[RsyncSpec]] section -- used to
            # stop LoopData at startup.  False is what the installer has
            # always written, and it now writes compress and log_success
            # commented out, leaving these fallbacks to answer.
            enable                   = to_bool(rsync_spec_dict.get('enable', False)),
            remote_server            = rsync_spec_dict.get('remote_server'),
            remote_port              = to_int(rsync_spec_dict.get('remote_port')) if rsync_spec_dict.get(
                                      'remote_port') is not None else None,
            remote_user              = rsync_spec_dict.get('remote_user'),
            remote_dir               = rsync_spec_dict.get('remote_dir'),
            compress                 = to_bool(rsync_spec_dict.get('compress', False)),
            log_success              = to_bool(rsync_spec_dict.get('log_success', False)),
            ssh_options              = LoopData.compose_ssh_options(
                                       rsync_spec_dict.get('ssh_options', ''), rsync_timeout),
            timeout                  = rsync_timeout,
            skip_if_older_than       = to_int(rsync_spec_dict.get('skip_if_older_than', 3)),
            week_start               = week_start,
            rainyear_start           = rainyear_start,
            legacy                   = legacy,
            reports                  = reports,
            legacy_shared            = legacy_shared,
            latitude                 = engine.stn_info.latitude_f,
            longitude                = engine.stn_info.longitude_f,
            altitude_m               = altitude_m if altitude_m is not None else 0.0)

        self.cfg.recompute()

        log.info('LoopData file is: %s' % os.path.join(self.cfg.loop_data_dir, self.cfg.filename))
        for ctx in self.cfg.contexts:
            log.info('%s: %d fields (%d almanac, %d station), trend window %ds, windrose bands %s' % (
                ctx.label, len(ctx.specified_fields), len(ctx.almanac_fields),
                len(ctx.station_fields), ctx.time_delta, ctx.windrose_bands))

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
    def compose_ssh_options(user_options: str, timeout: int) -> str:
        """The ssh options for the rsync transport.  rsync's --timeout bounds
        only rsync protocol I/O; the phases ssh owns can each hang the
        LoopProcessor thread for minutes against a dead remote, so each gets
        its own bound: ConnectTimeout for connect and the initial handshake,
        ServerAliveInterval/CountMax for a session that dies mid-transfer, and
        BatchMode for an interactive auth prompt (unanswerable under weewxd,
        so it must fail rather than wait).  A default is appended only when
        user_options doesn't already set that keyword, so anything the user
        wrote wins; timeout <= 0 (rsync's "no timeout") omits the time bounds
        but still sets BatchMode."""
        options = user_options.strip()
        defaults = ['BatchMode=yes']
        if timeout > 0:
            defaults = ['ConnectTimeout=%d' % timeout,
                        'ServerAliveInterval=%d' % timeout,
                        'ServerAliveCountMax=2',
                        'BatchMode=yes']
        for default in defaults:
            keyword = default.split('=')[0]
            if keyword.lower() not in options.lower():
                options = ('%s -o %s' % (options, default)).strip()
        return options

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
    def construct_baro_trend_descs(texts_dict: Dict[str, str]) -> Dict[BarometerTrend, str]:
        """The descriptions for trend.barometer.desc, in the target report's
        language: each English description in BARO_TREND_DESCS is a
        gettext-style key into the target report's [Texts], falling back to
        the English itself one string at a time."""
        return {trend: str(texts_dict.get(english, english))
                for trend, english in BARO_TREND_DESCS.items()}

    @staticmethod
    def normalize_fields(value: Any) -> List[str]:
        """A fields value as a list of field strings.  ConfigObj hands back a
        list for a comma-separated value but a bare str for a single value
        -- which iterated by character before 7.0.  The str is ONE field,
        never split: a quoted entry with a comma inside (a format() call
        with two arguments, an almanac tag with two keywords) is exactly
        the single value ConfigObj returns as a str, and splitting it is
        the mangling the quoting was for.  None (no such option) is no
        fields.  Whitespace is stripped and empties dropped."""
        if value is None:
            return []
        if isinstance(value, str):
            value = [value]
        fields: List[str] = []
        for entry in value:
            field = str(entry).strip()
            if field != '':
                fields.append(field)
        return fields

    @staticmethod
    def declared_fields_from_skin_dict(skin_dict: Dict[str, Any], report: str) -> List[str]:
        """The fields a report declares: [LoopData] [[fields]] in its merged
        skin dict (skin.conf, [StdReport] [[Defaults]] and the report's own
        stanza in weewx.conf, in that order), a section of named groups
        whose values are unioned in order.  A field in two groups counts
        once.  [] when the report declares nothing.  The groups are the
        author's own: they exist because ConfigObj has no line continuation
        for lists, so one fields = line would be one unreadable line."""
        loopdata_section = skin_dict.get('LoopData')
        if not isinstance(loopdata_section, dict):
            return []
        groups = loopdata_section.get('fields')
        if groups is None:
            return []
        if not isinstance(groups, dict):
            log.warning('Ignoring [LoopData] fields in report %s: declare fields as named '
                'groups in a [[fields]] section, not as a single fields = line.' % report)
            return []
        fields: List[str] = []
        seen: Set[str] = set()
        for group, value in groups.items():
            if isinstance(value, dict):
                log.warning('Ignoring [LoopData] [[fields]] [[[%s]]] in report %s: a group '
                    'is a line of fields, not a section.' % (group, report))
                continue
            for field in LoopData.normalize_fields(value):
                if field not in seen:
                    seen.add(field)
                    fields.append(field)
        return fields

    @staticmethod
    def enabled_reports(config_dict: Dict[str, Any]) -> List[str]:
        """The enabled reports, in [StdReport] order, exactly as
        StdReportEngine.run picks them: every sub-section but Defaults whose
        enable is not false.  Only sub-sections count -- SKIN_ROOT,
        HTML_ROOT and data_binding are scalars, not reports."""
        std_report = config_dict.get('StdReport')
        if std_report is None:
            return []
        sections = getattr(std_report, 'sections', None)
        if sections is None:
            sections = [key for key, value in std_report.items() if isinstance(value, dict)]
        reports: List[str] = []
        for report in sections:
            if report == 'Defaults':
                continue
            try:
                enabled = to_bool(std_report[report].get('enable', True))
            except ValueError as e:
                # Another report's malformed enable is not loopdata's to
                # fail weewxd over; WeeWX's own report thread will complain.
                log.warning('Report %s has an unreadable enable (%s); treating it as enabled.' % (report, e))
                enabled = True
            if not enabled:
                continue
            reports.append(report)
        return reports

    @staticmethod
    def trend_key(time_delta: int) -> str:
        """The accums.continuous key of the trend accumulator sized to
        time_delta seconds: reports with the same window share one."""
        return 'trend@%d' % time_delta

    @staticmethod
    def is_trend_key(period: str) -> bool:
        return period.startswith('trend@')

    @staticmethod
    def trend_key_seconds(key: str) -> int:
        assert LoopData.is_trend_key(key), key
        return int(key[len('trend@'):])

    @staticmethod
    def windrose_bands_key(unit: str, edges: List[float]) -> str:
        """The accums.windrose_* key of the accumulators banded with these
        edges (in this report windSpeed unit): reports with the same edges
        in the same unit share one set."""
        return '%s:%s' % (unit, ','.join(repr(float(edge)) for edge in edges))

    @staticmethod
    def build_report_context(report_name: Optional[str], specified_fields: List[str],
            skin_dict: Dict[str, Any], windrose_bands: Optional[List[float]], stn_info: Any,
            source_report: Optional[str] = None) -> ReportContext:
        """Parse a context's fields with the three parsers and resolve
        everything it renders with from its skin dict.  windrose_bands are
        the band edges already resolved into the report's windSpeed unit
        (see report_windrose_bands); None means the WRPLOT defaults.  A
        field none of the parsers accepts is reported, attributed to the
        context -- a skin author's typo must not vanish without trace."""
        (fields_to_include, obstypes) = LoopData.get_fields_to_include(set(specified_fields))
        # Almanac fields (almanac.sunrise, almanac(horizon=-6).sun(use_center=1).rise, ...)
        # are evaluated against weewx.almanac rather than the loop packet.
        almanac_fields = LoopData.get_almanac_fields(specified_fields)
        # Station fields (station.uptime.raw, station.version, ...) are
        # evaluated against weewx.station.Station -- the exact object behind
        # the report's $station tag -- rather than the loop packet.
        station_fields = LoopData.get_station_fields(specified_fields)
        label = ReportContext.label_for(report_name)
        accepted: Set[str] = {cname.field for cname in fields_to_include}
        accepted |= {f.field for f in almanac_fields}
        accepted |= {f.field for f in station_fields}
        for field in specified_fields:
            if field in accepted:
                continue
            if LoopData.is_almanac_field(field) or LoopData.is_station_field(field):
                continue    # already logged, with the reason, by its parser
            log.warning('Ignoring unrecognized field %s (%s)' % (field, label))

        formatter = weewx.units.Formatter.fromSkinDict(skin_dict)
        converter = weewx.units.Converter.fromSkinDict(skin_dict)

        # [possibly localized] strings for trend.barometer.desc: the English
        # descriptions are gettext-style keys into the report's [Texts] (its
        # lang file already merged in).
        baro_trend_descs = LoopData.construct_baro_trend_descs(
            dict(skin_dict.get('Texts', {})))

        # The trend window: the report's own [Units] [Trend] time_delta.
        try:
            time_delta: int = to_int(skin_dict['Units']['Trend']['time_delta'])
            if time_delta > 259200:
                log.info('time_delta of %d specified, LoopData will use max value of 259200.' % time_delta)
                time_delta = 259200
        except KeyError:
            time_delta = 10800

        if windrose_bands is None:
            windrose_bands = LoopData.parse_windrose_bands(None, converter)

        return ReportContext(
            report_name       = report_name,
            specified_fields  = list(specified_fields),
            fields_to_include = fields_to_include,
            almanac_fields    = almanac_fields,
            station_fields    = station_fields,
            formatter         = formatter,
            converter         = converter,
            baro_trend_descs  = baro_trend_descs,
            almanac_texts     = dict(skin_dict.get('Almanac', {})),
            station           = weewx.station.Station(stn_info, formatter, converter, skin_dict)
                                if len(station_fields) > 0 and stn_info is not None else None,
            time_delta        = time_delta,
            windrose_bands    = windrose_bands,
            obstypes          = obstypes,
            render_signature  = LoopData.render_signature(skin_dict, windrose_bands),
            source_report     = source_report if source_report is not None else report_name)

    @staticmethod
    def render_signature(skin_dict: Dict[str, Any], windrose_bands: List[float]) -> str:
        """Everything about a report that decides how a value comes out:
        its unit groups, string formats, labels, time formats, ordinates
        and trend window ([Units]), its hemispheres and observation labels
        ([Labels]), its translations ([Texts]), its almanac names
        ([Almanac]), and its windrose band edges.  Two contexts with the
        same signature render every field identically, which is what lets
        one stand in for the other -- so a field the deprecated fields
        line shares with ANY report that renders it the same way is
        computed once rather than twice per packet."""
        def normalize(value: Any) -> Any:
            if isinstance(value, dict):
                return {str(k): normalize(v) for k, v in value.items()}
            if isinstance(value, (list, tuple)):
                return [str(v) for v in value]
            return str(value)
        sections = {section: normalize(skin_dict.get(section, {}))
                    for section in ('Units', 'Labels', 'Texts', 'Almanac')}
        return json.dumps({'sections': sections,
                           'windrose_bands': [float(edge) for edge in windrose_bands]},
                          sort_keys=True)

    @staticmethod
    def share_legacy_fields_by_name(legacy_fields: List[str], legacy_signature: str,
            reports: List[ReportContext], target_report: str) -> Dict[str, str]:
        """field -> the report that renders it for the legacy fields line.

        The line is rendered through target_report, but any report whose
        render_signature matches produces byte-identical values, so a
        field declared by such a report is computed once, in that report's
        entry, and copied flat.  Without this, a station whose fields line
        carries another extension's entries -- what every celestial or
        weatherboard installer wrote for years -- evaluates each of them
        twice on every packet, which for a page's worth of satellite
        almanac fields is most of a loop interval on a Pi.

        target_report's own declaration is preferred, so the flat value
        comes from the report the line names; then the rest, in
        [StdReport] order."""
        wanted = set(legacy_fields)
        twins = [ctx for ctx in reports if ctx.render_signature == legacy_signature]
        twins.sort(key=lambda ctx: ctx.report_name != target_report)
        shared: Dict[str, str] = {}
        for ctx in twins:
            if ctx.report_name is None:
                continue
            for field in ctx.specified_fields:
                if field in wanted and field not in shared:
                    shared[field] = ctx.report_name
            # The legend key rides along with the rose it describes.
            if ctx.windrose and 'windrose.bands' not in shared and any(
                    shared.get(f) == ctx.report_name and '.windrose.' in f for f in shared):
                shared['windrose.bands'] = ctx.report_name
        return shared

    @staticmethod
    def union_obstypes(contexts: List[ReportContext]) -> ObsTypes:
        """The observation types the shared accumulators must track: the
        union over every context, per period.  A context's trend obstypes
        land under its trend_key rather than under 'trend', so two reports
        with different windows feed two accumulators and two with the same
        window feed one."""
        union = ObsTypes(current=set(), alltime=set(), rainyear=set(), year=set(),
            month=set(), week=set(), day=set(), hour=set(), continuous={})
        for ctx in contexts:
            for field in dataclasses.fields(ObsTypes):
                if field.name != 'continuous':
                    getattr(union, field.name).update(getattr(ctx.obstypes, field.name))
            for per, per_obstypes in ctx.obstypes.continuous.items():
                key = ctx.trend_key if per == 'trend' else per
                union.continuous.setdefault(key, set()).update(per_obstypes)
        return union

    @staticmethod
    def union_windrose(contexts: List[ReportContext]
            ) -> Tuple[Dict[str, Tuple[str, List[float]]], Set[Tuple[str, str]], Set[Tuple[str, str]]]:
        """The windrose accumulators the contexts need: the distinct band
        edges (windrose_key -> (report windSpeed unit, edges)), and the
        (windrose_key, period) pairs for span and continuous periods."""
        bandings: Dict[str, Tuple[str, List[float]]] = {}
        span_periods: Set[Tuple[str, str]] = set()
        continuous_periods: Set[Tuple[str, str]] = set()
        for ctx in contexts:
            if not ctx.windrose:
                continue
            bandings[ctx.windrose_key] = (
                ctx.converter.getTargetUnit('windSpeed')[0], ctx.windrose_bands)
            ctx_span, ctx_continuous = LoopData.get_windrose_periods(ctx.fields_to_include)
            span_periods |= {(ctx.windrose_key, per) for per in ctx_span}
            continuous_periods |= {(ctx.windrose_key, per) for per in ctx_continuous}
        return bandings, span_periods, continuous_periods

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
                if cname.obstype == 'windrose':
                    # windrose rides its own accumulators (WindRoseAccum), not
                    # the period accums; only the observations it consumes
                    # must survive pruning into the packet.
                    period_obstypes.add('windSpeed')
                    period_obstypes.add('windDir')
                    continue
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
        """A report's merged configuration, built by WeeWX's own skin-dict
        builder: WeeWX's defaults, the skin's skin.conf and lang file,
        [StdReport] [[Defaults]], the [StdReport] scalars, then the report's
        own stanza, unit_system applied where WeeWX applies it.  The
        builder is module-level since 4.6 (this extension's floor), named
        _build_skin_dict through 4.x and build_skin_dict since 5.0."""
        build_skin_dict = getattr(weewx.reportengine, 'build_skin_dict',
            getattr(weewx.reportengine, '_build_skin_dict', None))
        if build_skin_dict is None:
            raise weewx.UnsupportedFeature('weewx.reportengine has no skin-dict builder; '
                'weewx-loopdata requires WeeWX 4.6 or later')
        return build_skin_dict(config_dict, report)

    def pre_loop(self, event):
        if self.loop_processor_started:
            return
        # Start the loop processor thread.
        self.loop_processor_started = True

        try:
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
            binding = self.config_dict.get('StdReport')['data_binding']
            with weewx.manager.DBBinder(self.config_dict) as binder:
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
                    if LoopData.is_trend_key(per):
                        timelength = LoopData.trend_key_seconds(per)
                    elif LoopData.is_hour_period(per):
                        timelength = int(per[:-1])*3600
                    elif LoopData.is_minute_period(per):
                        timelength = int(per[:-1])*60
                    else:
                        # Unreachable: is_continuous_period admits only the three
                        # forms above, and union_obstypes re-keys 'trend'.  Skip
                        # rather than carry the previous iteration's window.
                        log.debug('No window for continuous period %s, skipping it.' % per)
                        continue

                    cont_accum, obstypes = LoopData.create_continuous_accum(
                        per, self.cfg.unit_system, self.cfg.archive_interval, obstypes, timelength, day_accum, dbm,
                        archive_delay=self.cfg.archive_delay)
                    if cont_accum:
                        continuous_accums[per], self.cfg.obstypes.continuous[per]  = cont_accum, obstypes

                # Create windrose accums (span periods seeded by one SQL GROUP BY
                # each, continuous periods by archive replay).
                windrose_span_accums, windrose_continuous_accums = \
                    LoopData.create_windrose_accums(self.cfg, dbm, pkt_time)

                # Inside the with: the payload goes on the queue before the
                # binder closes, since a close that raised would otherwise
                # leave it unsent with accumulator_payload_sent already set,
                # and no later packet rebuilds the accumulators.
                self.cfg.queue.put(Accumulators(
                    alltime_accum       = alltime_accum,
                    rainyear_accum      = rainyear_accum,
                    year_accum          = year_accum,
                    month_accum         = month_accum,
                    week_accum          = week_accum,
                    day_accum           = day_accum,
                    hour_accum          = hour_accum,
                    continuous          = continuous_accums,
                    windrose_span       = windrose_span_accums,
                    windrose_continuous = windrose_continuous_accums))
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
    def parse_windrose_bands(spec: Any,
            converter: weewx.units.Converter) -> List[float]:
        """Band edges for windrose, in the target report's windSpeed unit.
        spec is the [LoopData] windrose_bands value (a list, or a string for a
        single edge) or None; an invalid spec logs and falls back to the
        default: the classic WRPLOT/NOAA bands (WINDROSE_DEFAULT_BANDS_MPS)
        converted to the report unit and rounded to one decimal -- banding
        applies exactly the edges the page's legend will show."""
        edges = LoopData.windrose_edges(spec, 'windrose_bands')
        if edges is not None:
            return edges
        tgt_unit, _ = converter.getTargetUnit('windSpeed')
        return [round(weewx.units.convert(
                (edge, 'meter_per_second', 'group_speed'), tgt_unit)[0], 1)
            for edge in WINDROSE_DEFAULT_BANDS_MPS]

    @staticmethod
    def windrose_edges(spec: Any, where: str) -> Optional[List[float]]:
        """A windrose_bands value validated: ascending, non-negative floats;
        None when absent, or invalid (logged, naming where it was written).
        Validation is separate from the default so a shared invalid value
        is reported once and every report then takes the defaults in its
        OWN unit -- never the defaults in the source unit converted and
        rounded a second time."""
        if spec is None:
            return None
        try:
            edges = [float(s) for s in ([spec] if isinstance(spec, str) else spec)]
        except (TypeError, ValueError):
            log.error('Ignoring non-numeric windrose_bands: %s (%s)' % (spec, where))
            return None
        if len(edges) > 0 and edges[0] >= 0.0 and all(a < b for a, b in zip(edges, edges[1:])):
            return edges
        log.error('Ignoring windrose_bands (need ascending, non-negative edges): %s (%s)' % (spec, where))
        return None

    @staticmethod
    def defaults_converter(config_dict: Dict[str, Any]) -> weewx.units.Converter:
        """The units a [StdReport]-level setting is written in.

        WeeWX lets [[Defaults]] say what those are in three ways, and all
        three count: an explicit [[[Units]]] [[[[Groups]]]], a
        unit_system, or a lang whose language file carries a unit_system
        (WeeWX's own de.conf says metricwx).  build_skin_dict resolves the
        first two for the pseudo-report 'Defaults' but not the third --
        language files live in a skin's lang directory and [[Defaults]]
        has no skin -- so the lang is resolved here, against the skin of a
        report that inherits it, exactly as that report would: the
        language's unit system FIRST, then [[Defaults]]' own explicit
        groups over it, which is the order build_skin_dict uses.
        """
        std_report = config_dict.get('StdReport', {})
        if 'Defaults' not in std_report:
            return weewx.units.Converter.fromSkinDict(weewx.defaults.defaults)
        defaults = std_report['Defaults']
        lang = defaults.get('lang')
        unit_system: Optional[str] = None
        if lang is not None and 'unit_system' not in defaults:
            unit_system = LoopData.lang_unit_system(config_dict, str(lang))
        skin_dict = LoopData.get_target_report_dict(config_dict, 'Defaults')
        if unit_system is not None:
            groups = dict(weewx.units.std_groups[weewx.units.unit_constants[unit_system.upper()]])
            # The language's unit system underneath, [[Defaults]]' own
            # explicit groups on top -- WeeWX's order, not the reverse.
            explicit = defaults.get('Units', {})
            explicit = explicit.get('Groups', {}) if isinstance(explicit, dict) else {}
            groups.update({str(k): str(v) for k, v in explicit.items()})
            skin_dict['Units']['Groups'].update(groups)
        return weewx.units.Converter.fromSkinDict(skin_dict)

    @staticmethod
    def lang_unit_system(config_dict: Dict[str, Any], lang: str) -> Optional[str]:
        """The unit system a language file declares, read from the skin of
        a report that inherits the language.  Every skin ships its own
        language files and they agree about the unit system, so the first
        one that answers is the answer; None when no skin has that
        language or none of them names a unit system.  A report that sets
        its own lang counts too: the file asked for is this language's,
        whichever report happens to point at the skin holding it.

        get_lang_dict's second parameter is the language DIRECTORY only
        from WeeWX 5.3; from 4.6 to 5.2 it is the whole config_dict, and
        the function works the directory out itself.  Ask the signature
        rather than the version -- the question is precisely which
        parameters exist, the same reasoning as the almanac's texts=."""
        get_lang_dict = getattr(weewx.reportengine, 'get_lang_dict', None)
        if get_lang_dict is None:
            return None
        try:
            takes_dir = 'lang_spec_dir' in inspect.signature(get_lang_dict).parameters
        except (TypeError, ValueError):
            return None
        std_report = config_dict.get('StdReport', {})
        for report in LoopData.enabled_reports(config_dict):
            stanza = std_report.get(report, {})
            if not isinstance(stanza, dict) or 'skin' not in stanza:
                continue        # nothing to look in
            try:
                if takes_dir:
                    # 5.3+: it calls .is_dir(), so this must be a Path.
                    lang_dict = get_lang_dict(lang, pathlib.Path(
                        str(config_dict.get('WEEWX_ROOT', '')),
                        str(std_report.get('SKIN_ROOT', 'skins')),
                        str(stanza['skin']), 'lang'), report)
                else:
                    lang_dict = get_lang_dict(lang, config_dict, report)
            except Exception as e:
                reraise_if_terminate(e)
                log.debug('Could not read the %s language file for report %s: %s'
                    % (lang, report, e))
                continue
            unit_system = lang_dict.get('unit_system')
            if unit_system is not None and str(unit_system).upper() in weewx.units.unit_constants:
                return str(unit_system)
        return None

    @staticmethod
    def convert_windrose_bands(edges: List[float], src_converter: weewx.units.Converter,
            dst_converter: weewx.units.Converter) -> List[float]:
        """Band edges written in one report's windSpeed unit, in another's:
        converted and rounded to one decimal, like the defaults, so the
        legend the report publishes is exactly what banded its samples.
        Same unit: the edges as written."""
        src_unit = src_converter.getTargetUnit('windSpeed')[0]
        dst_unit = dst_converter.getTargetUnit('windSpeed')[0]
        if src_unit == dst_unit:
            return list(edges)
        converted = [round(weewx.units.convert((edge, src_unit, 'group_speed'), dst_unit)[0], 1)
                     for edge in edges]
        if not all(a < b for a, b in zip(converted, converted[1:])):
            # Edges closer than a tenth of the target unit collapsed; keep
            # the exact conversion rather than a non-ascending legend.
            converted = [weewx.units.convert((edge, src_unit, 'group_speed'), dst_unit)[0]
                         for edge in edges]
        return converted

    @staticmethod
    def stdreport_windrose_bands(config_dict: Dict[str, Any]) -> Any:
        """The [StdReport]-level windrose_bands, if any: [[Defaults]]
        windrose_bands, or the same option as a bare scalar under
        [StdReport] (WeeWX's older spelling of a default, which its skin-dict
        build applies after [[Defaults]] -- so it wins here too)."""
        std_report = config_dict.get('StdReport', {})
        spec = std_report.get('windrose_bands')
        if spec is None or isinstance(spec, dict):
            defaults = std_report.get('Defaults')
            spec = defaults.get('windrose_bands') if isinstance(defaults, dict) else None
        return spec

    @staticmethod
    def legacy_windrose_bands(config_dict: Dict[str, Any], target_report: str,
            target_report_dict: Dict[str, Any], loop_config_dict: Dict[str, Any],
            shared: Optional[Dict[str, Any]] = None) -> List[float]:
        """The band edges of the legacy [[Include]] fields line.  The line
        is rendered THROUGH target_report, so it bands the way that report
        bands -- resolved by report_windrose_bands, whose last fallback is
        the deprecated [LoopData] windrose_bands.  A station that has not
        moved that value therefore gets exactly what it got before 7.0,
        and one that has moved it to the report's stanza or to
        [StdReport] [[Defaults]] (which the manual tells it to do while
        still on the fields line) gets the value it moved -- rather than
        silently reverting to the WRPLOT defaults and, because the two no
        longer agree, losing the shared rendering as well."""
        return LoopData.report_windrose_bands(config_dict, target_report,
            target_report_dict, loop_config_dict, target_report_dict, shared, target_report)

    @staticmethod
    def shared_windrose_edges(shared: Optional[Dict[str, Any]], key: str, spec: Any,
            where: str, converter_fn: Callable[[], weewx.units.Converter]
            ) -> Tuple[Optional[List[float]], weewx.units.Converter]:
        """(validated edges or None, the converter of the unit they are in)
        for a value shared by every report, validated once per startup
        through the memo.  The converter arrives as a thunk because
        building it can mean a whole skin dict ([[Defaults]]'), which a
        memo hit must not pay for."""
        if shared is None:
            shared = {}
        if key not in shared:
            shared[key] = (LoopData.windrose_edges(spec, where), converter_fn())
        return shared[key]

    @staticmethod
    def report_windrose_bands(config_dict: Dict[str, Any], report: str,
            skin_dict: Dict[str, Any], loop_config_dict: Dict[str, Any],
            target_report_dict: Optional[Dict[str, Any]],
            shared: Optional[Dict[str, Any]] = None,
            target_report: Optional[str] = None) -> List[float]:
        """A declaring report's band edges, in its own windSpeed unit.
        windrose_bands is an ordinary report option, and where it is written
        says what unit it is in: on the report's own stanza in weewx.conf,
        or at the top of its skin.conf, it is in that report's unit; under
        [StdReport] [[Defaults]] (or as a bare [StdReport] scalar) it
        applies to every report and is in the Defaults' unit, converted to
        each report's.  Precedence is WeeWX's own for a report option: the
        stanza, then [StdReport], then skin.conf.  A report with none of
        those, and being target_report itself, falls back to the deprecated
        [LoopData] windrose_bands (already in its unit), so an upgraded
        station's rose does not change bands; then the WRPLOT defaults.  An invalid value
        at any level is reported (once, through the shared memo for the
        shared levels) and the report takes the defaults in its own unit."""
        converter = weewx.units.Converter.fromSkinDict(skin_dict)
        defaults = lambda: LoopData.parse_windrose_bands(None, converter)
        stanza = config_dict.get('StdReport', {}).get(report, {})
        if isinstance(stanza, dict) and stanza.get('windrose_bands') is not None:
            edges = LoopData.windrose_edges(stanza['windrose_bands'], 'report %s' % report)
            return edges if edges is not None else defaults()
        std_spec = LoopData.stdreport_windrose_bands(config_dict)
        if std_spec is not None:
            shared_edges, defaults_converter = LoopData.shared_windrose_edges(shared, 'std',
                std_spec, '[StdReport] [[Defaults]]',
                lambda: LoopData.defaults_converter(config_dict))
            if shared_edges is None:
                return defaults()
            return LoopData.convert_windrose_bands(shared_edges, defaults_converter, converter)
        # The skin's own: at the top of skin.conf, or -- the natural guess,
        # the deprecated key being [LoopData] windrose_bands -- inside its
        # [LoopData] section beside [[fields]].
        skin_spec = skin_dict.get('windrose_bands')
        if skin_spec is None:
            loopdata_section = skin_dict.get('LoopData')
            if isinstance(loopdata_section, dict):
                skin_spec = loopdata_section.get('windrose_bands')
        if skin_spec is not None:
            edges = LoopData.windrose_edges(skin_spec, 'skin of report %s' % report)
            return edges if edges is not None else defaults()
        # The deprecated [LoopData] windrose_bands, for target_report only.
        # Before 7.0 it banded exactly one rose -- the one in the flat file,
        # rendered through target_report -- so it belongs to that report and
        # to no other; every other report's rose is new in 7.0 and takes the
        # defaults until someone chooses otherwise.
        if report == target_report and loop_config_dict.get('windrose_bands') is not None \
                and target_report_dict is not None:
            legacy_edges, target_converter = LoopData.shared_windrose_edges(shared, 'legacy',
                loop_config_dict.get('windrose_bands'), '[LoopData]',
                lambda: weewx.units.Converter.fromSkinDict(target_report_dict))
            if legacy_edges is None:
                return defaults()
            return LoopData.convert_windrose_bands(legacy_edges, target_converter, converter)
        return defaults()

    @staticmethod
    def get_windrose_periods(fields_to_include: Set[CheetahName]
            ) -> Tuple[Set[str], Set[str]]:
        """The periods needing a windrose accumulator: (span, continuous)."""
        span_periods: Set[str] = set()
        continuous_periods: Set[str] = set()
        for cname in fields_to_include:
            if cname.obstype == 'windrose' and cname.period is not None:
                if LoopData.is_continuous_period(cname.period):
                    continuous_periods.add(cname.period)
                else:
                    span_periods.add(cname.period)
        return span_periods, continuous_periods

    @staticmethod
    def windrose_span_fn(period: str, week_start: int, rainyear_start: int
            ) -> Optional[Callable[[int], weeutil.weeutil.TimeSpan]]:
        """ts -> the period's span, for WindRoseSpanAccum's self-reset; None
        for alltime (never resets)."""
        if period == 'alltime':
            return None
        if period == 'hour':
            return weeutil.weeutil.archiveHoursAgoSpan
        if period == 'day':
            return weeutil.weeutil.archiveDaySpan
        if period == 'week':
            return lambda ts: weeutil.weeutil.archiveWeekSpan(ts, week_start)
        if period == 'month':
            return weeutil.weeutil.archiveMonthSpan
        if period == 'year':
            return weeutil.weeutil.archiveYearSpan
        assert period == 'rainyear'
        return lambda ts: weeutil.weeutil.archiveRainYearSpan(ts, rainyear_start)

    @staticmethod
    def create_windrose_banding(unit_system: int, tgt_unit: str,
            windrose_bands: List[float]) -> WindRoseBanding:
        """windrose_bands is in the report's windSpeed unit (tgt_unit);
        banding runs in the accumulators' unit system."""
        accum_speed_unit = weewx.units.getStandardUnitType(unit_system, 'windSpeed')[0]
        return WindRoseBanding(
            unit_system          = unit_system,
            edges                = [weewx.units.convert(
                (edge, tgt_unit, 'group_speed'), accum_speed_unit)[0]
                for edge in windrose_bands],
            seconds_per_distance = 1000.0 if unit_system == weewx.METRICWX else 3600.0)

    @staticmethod
    def create_windrose_accums(cfg: 'Configuration', dbm, pkt_time: int
            ) -> Tuple[Dict[Tuple[str, str], WindRoseSpanAccum], Dict[Tuple[str, str], WindRoseContinuousAccum]]:
        """Create and seed the windrose accumulators for the configured
        periods, one set per distinct band edges (cfg.windrose_bandings).
        Called from new_loop, after cfg.unit_system is final (the day
        accumulator may have overridden it)."""
        if len(cfg.windrose_span_periods) == 0 and len(cfg.windrose_continuous_periods) == 0:
            return {}, {}

        now = time.time()
        span_accums: Dict[Tuple[str, str], WindRoseSpanAccum] = {}
        continuous_accums: Dict[Tuple[str, str], WindRoseContinuousAccum] = {}
        for windrose_key, (tgt_unit, windrose_bands) in cfg.windrose_bandings.items():
            banding = LoopData.create_windrose_banding(cfg.unit_system, tgt_unit, windrose_bands)
            for key, period in sorted(cfg.windrose_span_periods):
                if key != windrose_key:
                    continue
                accum = WindRoseSpanAccum(banding,
                    LoopData.windrose_span_fn(period, cfg.week_start, cfg.rainyear_start),
                    pkt_time)
                earliest = accum.timespan.start if accum.timespan is not None else 0
                # Reject future-dated records, as the other accum seeding does.
                latest = int(now) + cfg.archive_delay
                if accum.timespan is not None:
                    latest = min(latest, accum.timespan.stop)
                start = time.time()
                LoopData.seed_windrose_span_accum(accum, dbm, earliest, latest)
                log.debug('Seeded windrose(%s, %s) accum in %f seconds.' % (period, windrose_key, time.time() - start))
                span_accums[(windrose_key, period)] = accum

            banded_continuous: Dict[str, WindRoseContinuousAccum] = {}
            for key, period in sorted(cfg.windrose_continuous_periods):
                if key != windrose_key:
                    continue
                timelength = int(period[:-1]) * (3600 if period.endswith('h') else 60)
                banded_continuous[period] = WindRoseContinuousAccum(banding, timelength)
            LoopData.seed_windrose_continuous_accums(
                banded_continuous, dbm, cfg.unit_system, now, cfg.archive_delay)
            for period, continuous_accum in banded_continuous.items():
                continuous_accums[(windrose_key, period)] = continuous_accum

        return span_accums, continuous_accums

    @staticmethod
    def seed_windrose_span_accum(accum: WindRoseSpanAccum, dbm,
            earliest: int, latest: int) -> None:
        """Fill a span windrose from the archive with one GROUP BY: rows
        bucket by compass bin (calm rows -- windSpeed below the calm threshold
        in db units, or null windDir -- to -1) and by speed band, summing
        seconds and windSpeed*seconds.  The per-cell sums convert to the
        accumulator's unit system afterward, so even alltime stays a single
        SQL aggregate scan.  CASE ladders, not CAST/FLOOR: portable across
        sqlite and MySQL."""
        db_unit_system = dbm.std_unit_system
        if db_unit_system is None:
            # Brand new database: nothing to seed.
            return
        banding = accum.banding
        db_speed_unit = weewx.units.getStandardUnitType(db_unit_system, 'windSpeed')[0]
        accum_speed_unit = weewx.units.getStandardUnitType(banding.unit_system, 'windSpeed')[0]
        db_edges = [weewx.units.convert(
            (edge, accum_speed_unit, 'group_speed'), db_speed_unit)[0]
            for edge in banding.edges]
        db_dist_unit = weewx.units.getStandardUnitType(db_unit_system, 'windrun')[0]
        accum_dist_unit = weewx.units.getStandardUnitType(banding.unit_system, 'windrun')[0]
        db_divisor = 1000.0 if db_unit_system == weewx.METRICWX else 3600.0

        # Compass bins per get_windrun_bucket: bin i covers
        # [i*22.5 - 11.25, i*22.5 + 11.25), with >= 348.75 wrapping to N.
        bkt_expr = 'CASE WHEN windDir IS NULL OR windSpeed < %.9f THEN -1' % db_edges[0]
        for i in range(WINDROSE_BINS):
            bkt_expr += ' WHEN windDir < %.9f THEN %d' % (11.25 + i * 22.5, i)
        bkt_expr += ' ELSE 0 END'
        band_expr = 'CASE'
        for i in range(len(db_edges) - 1, 0, -1):
            band_expr += ' WHEN windSpeed >= %.9f THEN %d' % (db_edges[i], i)
        band_expr += ' ELSE 0 END'
        sql = ('SELECT %s AS bkt, %s AS band,'
               ' SUM(`interval` * 60.0), SUM(windSpeed * `interval` * 60.0)'
               ' FROM archive'
               ' WHERE dateTime > %d AND dateTime <= %d AND windSpeed IS NOT NULL'
               ' GROUP BY bkt, band' % (bkt_expr, band_expr, earliest, latest))
        for bkt, band, seconds, speed_seconds in dbm.genSql(sql):
            if seconds is None:
                continue
            dist = weewx.units.convert(
                (speed_seconds / db_divisor, db_dist_unit, 'group_distance'),
                accum_dist_unit)[0]
            accum._credit(bkt, band, seconds, dist)

    @staticmethod
    def seed_windrose_continuous_accums(
            accums: Dict[str, WindRoseContinuousAccum], dbm, unit_system: int,
            now: float, archive_delay: int) -> None:
        """Prime the continuous windroses by replaying archive records (a
        rolling window must expire its seed later, so aggregated seeding
        won't do -- every contribution needs its timestamp)."""
        if len(accums) == 0:
            return
        earliest = int(now) - max(a.timelength for a in accums.values())
        archive_columns: List[str] = dbm.connection.columnsOf('archive')
        for pkt in LoopData.get_archive_packets(dbm, archive_columns, earliest):
            if pkt['dateTime'] >= now + archive_delay:
                log.warning('Ignoring future-dated archive record: %s'
                    % timestamp_to_string(pkt['dateTime']))
                continue
            cvt_pkt = weewx.units.StdUnitConverters[unit_system].convertDict(pkt)
            wind_speed = cvt_pkt.get('windSpeed')
            if wind_speed is None:
                continue
            weight = to_float(pkt['interval']) * 60.0
            for accum in accums.values():
                if pkt['dateTime'] > now - accum.timelength:
                    accum.add(pkt['dateTime'], wind_speed,
                        cvt_pkt.get('windDir'), weight)

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
            # al.), so an aggregate can only parse if the dispatch implements
            # it.  windrose has its own aggregate set (projections of a
            # WindRoseAccum, dispatched in add_windrose_obstype).
            valid_aggs: FrozenSet[str] = \
                WINDROSE_AGG_TYPES if obstype == 'windrose' else AGG_TYPES
            if segment[next_seg] not in valid_aggs:
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

        round_ndigits = None
        # Optional round(n) transform, ordered between the unit override and
        # the format spec, matching report tags
        # ($day.outTemp.max.degree_C.round(1).raw): the value is rounded,
        # then the format spec renders the rounded value.
        if prefix is None and len(segment) > next_seg:
            parsed_round = LoopData.parse_round_spec(segment[next_seg])
            if parsed_round is not None:
                round_ndigits = parsed_round[0]
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

        # windrose: not defined for current (a single sample is not a rose) or
        # trend (a delta of a histogram is meaningless).  Unit override and
        # .formatted apply only to .sum -- the other projections are seconds;
        # no call-syntax specs and no compass/baro specs (arrays, not scalars).
        if obstype == 'windrose' and prefix is None:
            if period == 'current' or period == 'trend':
                return None
            if format_kwargs is not None:
                return None
            if format_spec is not None and format_spec not in ('raw', 'formatted'):
                return None
            if agg_type != 'sum' and (unit is not None or format_spec == 'formatted'):
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
            format_kwargs = format_kwargs,
            round_ndigits = round_ndigits)

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
    def parse_call(segment: str) -> Optional[Tuple[str, List[ast.expr], List[ast.keyword]]]:
        """Parse a segment as a bare name or a name(args) call, returning
        (name, positional arg nodes, keyword arg nodes) -- a bare name is a
        call with no arguments, as Cheetah's auto-call renders it.  Returns
        None for anything else."""
        try:
            node = ast.parse(segment, mode='eval').body
        except (SyntaxError, ValueError):
            return None
        if isinstance(node, ast.Name):
            return node.id, [], []
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            return node.func.id, node.args, node.keywords
        return None

    @staticmethod
    def bind_call_args(args: List[ast.expr], keywords: List[ast.keyword],
            params: Tuple[str, ...], required: int) -> Optional[Dict[str, Any]]:
        """Bind a call's arguments to params (the method's parameter names in
        positional order), exactly as Python would.  Arguments must be
        literals.  Returns the bound kwargs, or None on too many positionals,
        an unknown or duplicate keyword, a non-literal argument, or a missing
        required (leading) parameter."""
        if len(args) > len(params):
            return None
        kwargs: Dict[str, Any] = {}
        try:
            for i, arg in enumerate(args):
                kwargs[params[i]] = ast.literal_eval(arg)
            for keyword in keywords:
                if keyword.arg is None or keyword.arg not in params \
                        or keyword.arg in kwargs:
                    return None
                kwargs[keyword.arg] = ast.literal_eval(keyword.value)
        except (SyntaxError, ValueError, TypeError, MemoryError):
            # Not a literal (e.g. a name or an expression).
            return None
        if any(param not in kwargs for param in params[:required]):
            return None
        return kwargs

    @staticmethod
    def parse_call_spec(segment: str) -> Optional[Tuple[str, Dict[str, Any]]]:
        """Parse a call-syntax format spec segment -- format("%H:%M"),
        nolabel("%.1f", None_string="--"), string(), long_form() -- into
        (spec_name, kwargs), binding positional arguments to the ValueHelper
        method's parameter names per CALL_FORMAT_SPECS.  Returns None unless
        the segment is a well-formed call of a known spec supplying its
        required arguments."""
        parsed = LoopData.parse_call(segment)
        if parsed is None:
            return None
        name, args, keywords = parsed
        call_spec = CALL_FORMAT_SPECS.get(name)
        if call_spec is None:
            return None
        kwargs = LoopData.bind_call_args(args, keywords,
            call_spec.params, call_spec.required)
        if kwargs is None:
            return None
        return name, kwargs

    @staticmethod
    def parse_round_spec(segment: str) -> Optional[Tuple[Optional[int]]]:
        """Parse a round / round(n) / round(ndigits=n) segment into the
        1-tuple (ndigits,) -- a tuple so that 'not a round segment' (None)
        stays distinct from a bare round() ((None,), the identity, exactly
        as ValueHelper.round with no argument).  ndigits must be an int.
        round is not a format spec: it rounds the VALUE, and any format spec
        that follows renders the rounded value (ValueHelper.round returns a
        new ValueHelper)."""
        parsed = LoopData.parse_call(segment)
        if parsed is None:
            return None
        name, args, keywords = parsed
        if name != 'round':
            return None
        kwargs = LoopData.bind_call_args(args, keywords, ('ndigits',), 0)
        if kwargs is None:
            return None
        ndigits = kwargs.get('ndigits')
        # bool is an int subclass; round(True) is nonsense, reject it.
        if ndigits is not None and (not isinstance(ndigits, int)
                or isinstance(ndigits, bool)):
            return None
        if not hasattr(weeutil.weeutil, 'rounder'):
            log.error('round requires a WeeWX with weeutil.weeutil.rounder '
                '(4.6 or later); ignoring %s' % segment)
            return None
        return (ndigits,)

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
        # An optional round(n) transform sits before the format spec
        # (almanac.moon.az.round(1).raw), so peel it after the spec.
        round_ndigits = None
        if len(chain_segments) >= 2:
            parsed_round = LoopData.parse_round_spec(chain_segments[-1])
            if parsed_round is not None:
                round_ndigits = parsed_round[0]
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

        group: Optional[str] = None
        if any(seg.name.startswith('next_') or seg.name.startswith('previous_') for seg in chain):
            tier = 'event'
            # next_* fields cache and expire as a group: the key is everything
            # that determines WHICH event -- the leading almanac kwargs, the
            # day shift, and the chain up through the first next_* segment --
            # so almanac.iss.next_pass.rise.raw and almanac.iss.next_pass.set.raw
            # always describe the same pass.  previous_* fields (group None)
            # keep the per-field day-rolled cache: their instant is always in
            # the past, so expire-at-event would recompute every packet.
            for i, seg in enumerate(chain):
                if seg.name.startswith('next_'):
                    group = repr((sorted(almanac_kwargs.items()), days,
                        [(s.name, None if s.kwargs is None else sorted(s.kwargs.items()))
                         for s in chain[:i + 1]]))
                    break
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
            format_kwargs  = format_kwargs,
            round_ndigits  = round_ndigits,
            group          = group)

    @staticmethod
    def is_station_field(field: str) -> bool:
        return field == 'station' or field.startswith('station.')

    @staticmethod
    def get_station_fields(specified_fields: List[str]) -> List[StationField]:
        station_fields: List[StationField] = []
        seen: Set[str] = set()
        for field in specified_fields:
            if not LoopData.is_station_field(field):
                continue
            station_field = LoopData.parse_station_field(field)
            if station_field is None:
                log.error('Ignoring malformed station field: %s' % field)
                continue
            if station_field.field not in seen:
                seen.add(station_field.field)
                station_fields.append(station_field)
        return station_fields

    @staticmethod
    def parse_station_field(field: str) -> Optional[StationField]:
        segments = LoopData.split_field_segments(field)
        if segments is None or len(segments) < 2:
            return None

        # The leading segment must be exactly station -- unlike almanac,
        # Station takes no call suffix.
        if segments[0] != 'station':
            return None

        # As with almanac fields, a trailing format spec is loopdata's:
        # the renderer specs (FORMAT_SPECS keys) and the call-syntax specs
        # (CALL_FORMAT_SPECS) -- never code/desc, which are trend.barometer
        # classifications -- because the evaluator renders each as the
        # ValueHelper attribute of the same name.
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
        # An optional round(n) transform sits before the format spec
        # (station.altitude.meter.round(0).raw), so peel it after the spec.
        round_ndigits = None
        if len(chain_segments) >= 2:
            parsed_round = LoopData.parse_round_spec(chain_segments[-1])
            if parsed_round is not None:
                round_ndigits = parsed_round[0]
                chain_segments = chain_segments[:-1]

        if len(chain_segments) == 0 or not all(
                segment.isidentifier() for segment in chain_segments):
            return None

        return StationField(
            field         = field,
            chain         = chain_segments,
            format_spec   = format_spec,
            format_kwargs = format_kwargs,
            round_ndigits = round_ndigits)

def cheetah_autocall(obj: Any) -> Any:
    """Auto-call a dotted-segment result exactly as Cheetah's NameMapper
    does: a method, function or builtin is called with no arguments; a class,
    or an instance -- even a callable one, such as an AlmanacBinder or
    weewx-skyfield's CallableRadians -- is left alone (NameMapper's
    _isInstanceOrClass test).  Cheetah applies this at EVERY dotted segment
    of a tag, so the almanac and station evaluators do too."""
    if callable(obj) and not isinstance(obj, type) and (
            hasattr(obj, '__func__') or hasattr(obj, '__code__')
            or hasattr(obj, '__self__')):
        return obj()
    return obj

def render_endpoint_value(field: str, chain_desc: str, format_spec: Optional[str],
        format_kwargs: Optional[Dict[str, Any]], round_ndigits: Optional[int],
        obj: Any) -> Any:
    """Apply a field's format spec to an evaluated endpoint (ValueHelpers
    format exactly as the report tag would render) and coerce to a
    json-serializable value.  Shared by the almanac and station field
    evaluators; chain_desc names the attribute chain in the error raised
    when a spec is applied to a value that cannot take it."""
    if isinstance(obj, weewx.units.ValueHelper):
        if round_ndigits is not None:
            # round(n) first: ValueHelper.round returns a new ValueHelper
            # with the value rounded, then the spec (or str()) renders it,
            # exactly as the report tag chain does.
            obj = obj.round(round_ndigits)
        if format_spec is not None:
            # ValueHelper exposes every format spec as an attribute of
            # the same name; the parser admits nothing else here.  Call
            # specs (format/nolabel/string/long_form) are methods, called
            # with the field's bound kwargs; a bare spec that is callable
            # (ordinal_compass) is called with none, as Cheetah's
            # auto-call renders it.
            value = getattr(obj, format_spec)
            if callable(value):
                value = value(**(format_kwargs or {}))
        else:
            value = str(obj)
    elif round_ndigits is not None or (
            format_spec is not None and format_spec != 'raw'):
        # round and formatted/ordinal_compass/the calls need a
        # ValueHelper; .raw is allowed as identity on plain values
        # (almanac.moon_index.raw).
        raise TypeError('%s: %s returned %s, which does not support .%s'
            % (field, chain_desc, type(obj).__name__, format_spec or 'round'))
    else:
        value = obj
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (tuple, list)) and all(
            v is None or isinstance(v, (bool, int, float, str)) for v in value):
        # Emit a tuple of scalars as a json array, preserving the parts the
        # report tag exposes -- $station.latitude is ('37', '24.00', 'N'),
        # indexed by templates exactly as page javascript would index this.
        return list(value)
    return str(value)

class AlmanacFieldEvaluator:
    """Evaluates almanac fields against weewx.almanac (whatever AlmanacTypes
    are registered: weewx-skyfield, PyEphem, or the built-in fallback) and
    inserts the results into the loopdata packet.  Runs on the LoopProcessor
    thread.  Caching mirrors weewx-celestial's proven lifetimes: continuous
    attributes (alt/az/ra/dec/phase/distances) are recomputed every packet;
    day-scoped attributes (rise/set/transit/visible) once per local day;
    previous_* attributes are kept until the local day advances past the
    cached event.  next_* attributes expire at the event's OWN instant: a
    next_* value whose instant has passed no longer is what the field name
    promises (a page that wants today's sunrise all day spells it
    almanac.sunrise), and the recompute returns the FOLLOWING occurrence,
    in the future, re-arming the cache -- once per lunar month for
    next_full_moon, once per pass for a satellite.  Fields naming the same
    next_* event are cached and expired as a group (see compute_group).
    The local day is compared for EQUALITY, so backfilled packets get their
    own day, never a newer cache.  A day/event evaluation that yields no
    data is not cached at all -- every packet retries until data exists
    (see compute())."""

    # Sentinel cached for a field whose evaluation failed, so day/event tiers
    # don't retry every packet.
    SKIP = object()

    def __init__(self, ctx: ReportContext, cfg: Configuration) -> None:
        # One evaluator per report, never shared: the cache below holds
        # RENDERED values, produced with this report's formatter, converter
        # and [Almanac] texts.
        self.fields    = ctx.almanac_fields
        self.latitude  = cfg.latitude
        self.longitude = cfg.longitude
        self.altitude_m = cfg.altitude_m
        self.texts     = ctx.almanac_texts
        self.formatter = ctx.formatter
        self.converter = ctx.converter
        self.values: Dict[str, Any] = {}          # field -> json value or SKIP
        self.event_ts: Dict[str, Optional[float]] = {} # previous_* field -> cached event's epoch time
        self.groups: Dict[str, List[AlmanacField]] = {} # group key -> its next_* fields
        for almanac_field in self.fields:
            if almanac_field.group is not None:
                self.groups.setdefault(almanac_field.group, []).append(almanac_field)
        self.group_expiry: Dict[str, float] = {}  # group key -> cached event's governing instant (epoch)
        self.cache_day: Optional[date] = None
        self.warned: Set[str] = set()
        # How this WeeWX's Almanac takes the target report's [Almanac]
        # section.  Settled once here: the section does not change while
        # weewxd runs, so the packet path just splats the result.
        self.texts_kwargs = AlmanacFieldEvaluator.build_texts_kwargs(
            weewx.almanac.Almanac, self.texts)

    @staticmethod
    def build_texts_kwargs(almanac_class: Any, texts: Dict[str, Any]) -> Dict[str, Any]:
        """The Almanac constructor's language argument, spelled for the
        running WeeWX.  WeeWX 5.3 added texts=, which takes the whole
        [Almanac] section (moon_phases, body names, constellation names);
        before that the constructor took only moon_phases=, the eight phase
        names alone, and passing texts= to it is a TypeError.  Ask the
        signature rather than weewx.__version__: the question is precisely
        which parameters exist.

        On a pre-5.3 WeeWX the phase names are all that can be passed, and
        only when the target report overrides them -- passing nothing leaves
        WeeWX's own default (weeutil.Moon.moon_phases) in place, which is
        what a report on that version does too."""
        parameters = inspect.signature(almanac_class.__init__).parameters
        if 'texts' in parameters:
            return {'texts': texts}
        if 'moon_phases' in parameters and 'moon_phases' in texts:
            return {'moon_phases': texts['moon_phases']}
        return {}

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
            formatter   = self.formatter,
            converter   = self.converter,
            **self.texts_kwargs)

    def evaluate(self, almanac_field: AlmanacField, base_almanac: weewx.almanac.Almanac,
            pkt_time: int) -> Any:
        """Walk the attribute chain exactly as Cheetah would walk the report
        tag: a segment with call syntax is called with its arguments, and
        every other segment is auto-called per NameMapper's rule (methods
        and functions, not callable instances) -- see cheetah_autocall."""
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
            else:
                obj = cheetah_autocall(obj)
        return obj

    def to_json_value(self, almanac_field: AlmanacField, obj: Any) -> Any:
        """Apply the format spec (ValueHelpers format exactly as the report
        tag would render) and coerce to a json-serializable value."""
        return render_endpoint_value(almanac_field.field,
            '.'.join(seg.name for seg in almanac_field.chain),
            almanac_field.format_spec, almanac_field.format_kwargs,
            almanac_field.round_ndigits, obj)

    def compute(self, almanac_field: AlmanacField, base_almanac: weewx.almanac.Almanac,
            pkt_time: int) -> Any:
        """Evaluate the field and return this packet's json value (SKIP on
        failure).  The result is cached in self.values -- UNLESS it is a
        day/event field whose evaluation yielded no data (raw None: e.g. a
        satellite whose elements have not been fetched yet, or a body with
        no rise this day).  Such a value is served for this packet only, so
        the next packet retries and picks the real value up the moment the
        data exists -- report tags self-heal on the next report cycle, and
        without this a startup N/A would stick until midnight.  The retry
        is cheap: a no-data evaluation is a few dict lookups, and a
        legitimately-empty one is served from the almanac's own cache.  The
        no-data test is on the evaluated object, never on the rendered
        string ("N/A" is formatter/language dependent)."""
        try:
            obj = self.evaluate(almanac_field, base_almanac, pkt_time)
            raw = obj.raw if isinstance(obj, weewx.units.ValueHelper) else obj
            value = self.to_json_value(almanac_field, obj)
            if almanac_field.tier != 'continuous' and raw is None:
                return value
            if almanac_field.tier == 'event':
                self.event_ts[almanac_field.field] = raw if isinstance(raw, (int, float)) else None
            self.values[almanac_field.field] = value
            return value
        except Exception as e:
            reraise_if_terminate(e)
            if almanac_field.field not in self.warned:
                self.warned.add(almanac_field.field)
                log.info('Cannot evaluate almanac field %s: %s' % (almanac_field.field, e))
            self.values[almanac_field.field] = AlmanacFieldEvaluator.SKIP
            if almanac_field.tier == 'event':
                self.event_ts[almanac_field.field] = None
            return AlmanacFieldEvaluator.SKIP

    def compute_group(self, group_key: str, group_fields: List[AlmanacField],
            base_almanac: weewx.almanac.Almanac, pkt_time: int) -> Dict[str, Any]:
        """Evaluate every leaf of a next_* group against the same base
        almanac and cache them as a unit, so a pass's rise, set and azimuth
        can never mix two passes.  The group's expiry is the LATEST instant
        among its time-typed leaves -- detected by the ValueHelper's unit
        group (group_time) and converted to unix_epoch, so a pinned unit
        segment (or a report's group_time override) cannot skew it, and
        duration never qualifies (group_deltatime, a span not an instant).
        For a satellite pass that lands on .set with no pass-specific
        knowledge here: an in-progress pass keeps serving until it ends,
        then expires.  A group with no time-typed leaf has no expiry signal
        and falls back to the day roll (include a pass time field to get
        event-time expiry).  A leaf with no data (raw None) keeps the WHOLE
        group uncached -- every packet retries, as for any day/event field,
        and atomically, so a healed group can never mix a fresh leaf with a
        stale one."""
        results: Dict[str, Any] = {}
        expiry: Optional[float] = None
        no_data = False
        for almanac_field in group_fields:
            try:
                obj = self.evaluate(almanac_field, base_almanac, pkt_time)
                raw = obj.raw if isinstance(obj, weewx.units.ValueHelper) else obj
                results[almanac_field.field] = self.to_json_value(almanac_field, obj)
                if raw is None:
                    no_data = True
                elif (isinstance(obj, weewx.units.ValueHelper)
                        and getattr(obj.value_t, 'group', None) == 'group_time'):
                    event_ts = weewx.units.convert(obj.value_t, 'unix_epoch')[0]
                    if event_ts is not None:
                        expiry = event_ts if expiry is None else max(expiry, event_ts)
            except Exception as e:
                reraise_if_terminate(e)
                if almanac_field.field not in self.warned:
                    self.warned.add(almanac_field.field)
                    log.info('Cannot evaluate almanac field %s: %s' % (almanac_field.field, e))
                results[almanac_field.field] = AlmanacFieldEvaluator.SKIP
        if not no_data:
            self.values.update(results)
            if expiry is not None:
                self.group_expiry[group_key] = expiry
        return results

    def roll_day(self, day: date) -> None:
        advancing = self.cache_day is not None and day > self.cache_day
        day_start_ts = time.mktime(day.timetuple())
        for almanac_field in self.fields:
            if almanac_field.tier == 'day':
                self.values.pop(almanac_field.field, None)
            elif almanac_field.group is not None:
                # A next_* group's life is governed by its expiry instant, so
                # an advancing day keeps a still-armed group (a full moon
                # three weeks out is not recomputed daily; a stale instant is
                # caught by the per-packet expiry check in insert_fields).  A
                # group with no expiry (no time-typed leaf, or a failed
                # compute) day-rolls here, and any non-advancing change
                # (backfill) recomputes -- equality semantics, as for the
                # day tier.
                if not advancing or almanac_field.group not in self.group_expiry:
                    self.values.pop(almanac_field.field, None)
                    self.group_expiry.pop(almanac_field.group, None)
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
        # A next_* group expires the moment its governing instant is no
        # longer ahead (>=, matching the engines' strictly-after rule: asked
        # at the instant itself, wxskyfield and PyEphem already serve the
        # following occurrence).  Mid-pass packets keep the cached pass --
        # its expiry is the set time.
        for group_key in [k for k, expiry in self.group_expiry.items() if pkt_time >= expiry]:
            del self.group_expiry[group_key]
            for almanac_field in self.groups[group_key]:
                self.values.pop(almanac_field.field, None)
        base_almanac = self.build_almanac(pkt)
        group_results: Dict[str, Dict[str, Any]] = {}
        for group_key, group_fields in self.groups.items():
            if any(f.field not in self.values for f in group_fields):
                group_results[group_key] = self.compute_group(
                    group_key, group_fields, base_almanac, pkt_time)
        for almanac_field in self.fields:
            if almanac_field.group is not None:
                if almanac_field.group in group_results:
                    value = group_results[almanac_field.group][almanac_field.field]
                else:
                    value = self.values[almanac_field.field]
            elif almanac_field.tier == 'continuous' or almanac_field.field not in self.values:
                value = self.compute(almanac_field, base_almanac, pkt_time)
            else:
                value = self.values[almanac_field.field]
            if value is not None and value is not AlmanacFieldEvaluator.SKIP:
                loopdata_pkt[almanac_field.field] = value

class StationFieldEvaluator:
    """Evaluates station fields against weewx.station.Station -- the exact
    object behind the report's $station tag -- and inserts the results into
    the loopdata packet.  Runs on the LoopProcessor thread.  uptime and
    os_uptime tick, so they are recomputed every packet; every other
    attribute is constant for the life of the weewxd process and is computed
    once, on the first packet that needs it."""

    # Station attributes whose value changes packet to packet.
    DYNAMIC_ATTRS = frozenset(('uptime', 'os_uptime'))

    # Sentinel cached for a field whose evaluation failed, so it is not
    # retried (and re-logged) every packet.
    SKIP = object()

    def __init__(self, fields: List[StationField], station: Any) -> None:
        self.fields = fields
        self.station = station
        self.static_values: Dict[str, Any] = {}   # field -> json value or SKIP
        self.warned: Set[str] = set()

    def compute(self, station_field: StationField) -> Any:
        """Walk the attribute chain exactly as Cheetah would walk the report
        tag, auto-calling every segment per NameMapper's rule (methods and
        functions, not callable instances -- see cheetah_autocall), and
        render per the field's format spec.  Returns SKIP (logged once) on
        any failure."""
        try:
            obj: Any = self.station
            for name in station_field.chain:
                obj = cheetah_autocall(getattr(obj, name))
            return render_endpoint_value(station_field.field,
                '.'.join(station_field.chain), station_field.format_spec,
                station_field.format_kwargs, station_field.round_ndigits, obj)
        except Exception as e:
            reraise_if_terminate(e)
            if station_field.field not in self.warned:
                self.warned.add(station_field.field)
                log.info('Cannot evaluate station field %s: %s'
                    % (station_field.field, e))
            return StationFieldEvaluator.SKIP

    def insert_fields(self, loopdata_pkt: Dict[str, Any]) -> None:
        for station_field in self.fields:
            if station_field.chain[0] in StationFieldEvaluator.DYNAMIC_ATTRS:
                value = self.compute(station_field)
            else:
                if station_field.field not in self.static_values:
                    self.static_values[station_field.field] = \
                        self.compute(station_field)
                value = self.static_values[station_field.field]
            if value is not None and value is not StationFieldEvaluator.SKIP:
                loopdata_pkt[station_field.field] = value

@dataclass
class ReportRenderer:
    """A context with its evaluators.  The evaluators are per report, not
    merely per process: both cache rendered values (the almanac evaluator's
    by field string, the station evaluator's static tier), produced with
    the report's own formatter, converter and texts -- one shared evaluator
    would hand report B report A's rendering."""
    ctx          : ReportContext
    almanac_eval : Optional[AlmanacFieldEvaluator]
    station_eval : Optional[StationFieldEvaluator]

    @staticmethod
    def for_context(ctx: ReportContext, cfg: Configuration) -> 'ReportRenderer':
        return ReportRenderer(
            ctx          = ctx,
            almanac_eval = AlmanacFieldEvaluator(ctx, cfg) if len(ctx.almanac_fields) > 0 else None,
            station_eval = StationFieldEvaluator(ctx.station_fields, ctx.station)
                           if len(ctx.station_fields) > 0 and ctx.station is not None else None)

class LoopProcessor:
    def __init__(self, cfg: Configuration):
        self.cfg = cfg
        # Contexts whose rendering has failed, so the error is logged once
        # rather than on every packet.  Per instance: class state would
        # leak between services in one interpreter, silently swallowing
        # the report of a later failure.
        self.render_failures: Set[str] = set()
        self.archive_start: float = time.time()
        self.renderers: List[ReportRenderer] = [
            ReportRenderer.for_context(ctx, cfg) for ctx in cfg.contexts]

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
                loopdata_pkt = LoopProcessor.generate_output(
                    pkt, self.cfg, self.accumulators, self.renderers, self.render_failures)
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
            # Normally already renamed onto the loop-data file by the last
            # write, and absent before the first one.
            try:
                os.unlink(self.cfg.tmpname)
            except FileNotFoundError:
                pass

    @staticmethod
    def generate_output(in_pkt: Dict[str, Any], cfg: Configuration, accums: Accumulators,
            renderers: List[ReportRenderer],
            render_failures: Optional[Set[str]] = None) -> Dict[str, Any]:
        """The file's contents for one packet: accumulate the packet exactly
        once, then render every context off the shared accumulators -- the
        legacy fields line flat at the top level, each declaring report
        under its report name (the [StdReport] section name, never the skin
        name: one skin can be listed under two reports)."""
        if render_failures is None:
            render_failures = set()
        pkt = LoopProcessor.accumulate_packet(in_pkt, cfg, accums)
        output: Dict[str, Any] = {}
        for renderer in renderers:
            try:
                rendered = LoopProcessor.render_report(pkt, in_pkt, renderer.ctx, cfg, accums,
                    renderer.almanac_eval, renderer.station_eval)
            except Exception as e:
                # Every enabled report's declaration is rendered here,
                # including skins this station's operator never wrote.  One
                # of them failing must cost that report its entry, not stop
                # the file for all of them: process_queue re-raises, which
                # ends the LoopProcessor thread for good and freezes
                # loop-data.txt while weewxd carries on none the wiser.
                reraise_if_terminate(e)
                if renderer.ctx.label not in render_failures:
                    render_failures.add(renderer.ctx.label)
                    log.error('Could not render %s; its values are omitted.  Exception: %s'
                        % (renderer.ctx.label, e))
                    weeutil.logger.log_traceback(log.error, "    ****  ")
                continue
            if renderer.ctx.report_name is None:
                output.update(rendered)
            else:
                output[renderer.ctx.report_name] = rendered
        # Legacy fields that a report renders identically were computed
        # once, in that report's entry; copy them flat (see __init__).  A
        # report whose rendering failed above simply has no entry, and its
        # flat keys are absent for this packet like any other missing one.
        for key, report in cfg.legacy_shared.items():
            entry = output.get(report)
            if entry is not None and key in entry:
                output[key] = entry[key]
        return output

    @staticmethod
    def generate_loopdata_dictionary(in_pkt: Dict[str, Any], cfg: Configuration, accums: Accumulators,
            almanac_eval: Optional[AlmanacFieldEvaluator] = None,
            station_eval: Optional[StationFieldEvaluator] = None,
            ctx: Optional[ReportContext] = None) -> Dict[str, Any]:
        """Accumulate one packet and render ONE context flat: ctx, else the
        legacy context.  The shape every test asserts on; process_queue
        renders every context through generate_output."""
        if ctx is None:
            ctx = cfg.legacy
        assert ctx is not None, 'no context to render'
        if almanac_eval is None or station_eval is None:
            # Each independently: a caller supplying one must still get the
            # other, or its fields vanish from the result with no signal.
            renderer = ReportRenderer.for_context(ctx, cfg)
            if almanac_eval is None:
                almanac_eval = renderer.almanac_eval
            if station_eval is None:
                station_eval = renderer.station_eval
        pkt = LoopProcessor.accumulate_packet(in_pkt, cfg, accums)
        return LoopProcessor.render_report(pkt, in_pkt, ctx, cfg, accums, almanac_eval, station_eval)

    @staticmethod
    def render_report(pkt: Dict[str, Any], in_pkt: Dict[str, Any], ctx: ReportContext,
            cfg: Configuration, accums: Accumulators,
            almanac_eval: Optional[AlmanacFieldEvaluator] = None,
            station_eval: Optional[StationFieldEvaluator] = None) -> Dict[str, Any]:
        """One context's fields, rendered off the shared accumulators with
        its own formatter, converter and texts.  pkt is the accumulated
        (converted, pruned) packet; in_pkt the packet as it arrived."""
        loopdata_pkt = LoopProcessor.create_loopdata_packet(pkt, ctx, accums, cfg.loop_frequency)

        # Almanac fields are computed from the (unpruned) incoming packet's
        # time, temperature and pressure, not from accumulators.
        if almanac_eval is not None:
            almanac_eval.insert_fields(loopdata_pkt, in_pkt)

        # Station fields depend on nothing in the packet.
        if station_eval is not None:
            station_eval.insert_fields(loopdata_pkt)

        return loopdata_pkt

    @staticmethod
    def accumulate_packet(in_pkt: Dict[str, Any], cfg: Configuration, accums: Accumulators
            ) -> Dict[str, Any]:
        """Add one packet to every accumulator -- once per packet, whatever
        the number of reports -- and return it converted to the
        accumulators' unit system and pruned to the observations in use."""

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

        # Feed the windrose accumulators straight from the converted packet --
        # windrose is not a packet obstype and never rides the weewx.accum
        # machinery (compute_period_obstypes keeps windSpeed/windDir alive
        # in the packet for it).
        if len(accums.windrose_span) > 0 or len(accums.windrose_continuous) > 0:
            wr_speed = pkt.get('windSpeed')
            if wr_speed is not None:
                wr_dir = pkt.get('windDir')
                for wr_accum in itertools.chain(
                        accums.windrose_span.values(),
                        accums.windrose_continuous.values()):
                    wr_accum.add(pkt['dateTime'], wr_speed, wr_dir, cfg.loop_frequency)

        return pkt

    @staticmethod
    def add_unit_obstype(cname: CheetahName, loopdata_pkt: Dict[str, Any],
            converter: weewx.units.Converter,
            formatter: weewx.units.Formatter) -> None:

        if cname.prefix2 == 'label':
            # agg_type not allowed
            # tgt_type, tgt_group = converter.getTargetUnit(cname.obstype, agg_type=cname.agg_type)
            # windrose is loopdata's own composite; its .sum values are
            # distances (WeeWX's windrun group), so the label is windrun's.
            obstype = 'windrun' if cname.obstype == 'windrose' else cname.obstype
            tgt_type, tgt_group = converter.getTargetUnit(obstype)
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
        if cname.round_ndigits is not None and not isinstance(value_t[0], str):
            # ValueHelper.round parity: round the value, then render.  String
            # values (firstlast obstypes) have nothing to round and pass
            # through, as in the default renderer's string bypass; rounder
            # passes None through, so the render_missing path composes.
            value_t = (weeutil.weeutil.rounder(value_t[0], cname.round_ndigits),
                       value_t[1], value_t[2])
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
    def add_windrose_obstype(cname: CheetahName, accums: Accumulators,
            loopdata_pkt: Dict[str, Any], converter: weewx.units.Converter,
            formatter: weewx.units.Formatter, windrose_key: str) -> None:
        """Render a windrose field: a projection of the period's
        WindRoseAccum.  .calm is a scalar (seconds); .time a 16-array of
        seconds; .banded the 16xN seconds matrix; .sum a 16-array of distances
        converted to the target report's (or the override's) distance unit.
        Arrays emit raw numbers -- charting js wants numbers -- with round(n)
        applied per element; .formatted (sum only) emits the report-formatted
        strings.  Bin order is windrun_bucket_suffixes (N clockwise)."""
        # The grammar guarantees a period (the unit.label form dispatches to
        # add_unit_obstype before this is reached).
        assert cname.period is not None
        accum: Optional[WindRoseAccum] = \
            accums.windrose_span.get((windrose_key, cname.period)) \
            or accums.windrose_continuous.get((windrose_key, cname.period))
        if accum is None:
            log.debug('No windrose accumulator for %s (%s), skipping %s' % (cname.period, windrose_key, cname.field))
            return

        def rnd(value: float) -> Any:
            if cname.round_ndigits is None:
                return value
            return weeutil.weeutil.rounder(value, cname.round_ndigits)

        if cname.agg_type == 'calm':
            loopdata_pkt[cname.field] = rnd(accum.calm_seconds)
            return
        if cname.agg_type == 'time':
            loopdata_pkt[cname.field] = [rnd(v) for v in accum.bin_times()]
            return
        if cname.agg_type == 'banded':
            loopdata_pkt[cname.field] = \
                [[rnd(v) for v in bands] for bands in accum.time_bins]
            return

        # sum: per-bin distance, unit-converted like any group_distance value.
        assert cname.agg_type == 'sum'
        src_unit = weewx.units.getStandardUnitType(
            accum.banding.unit_system, 'windrun')[0]
        values: List[float] = []
        tgt_unit: Optional[str] = None
        try:
            for v in accum.bin_distances():
                if cname.unit is None:
                    tgt_value, tgt_unit, _ = converter.convert((v, src_unit, 'group_distance'))
                else:
                    tgt_value, tgt_unit, _ = weewx.units.convert((v, src_unit, 'group_distance'), cname.unit)
                values.append(tgt_value)
        except (KeyError, ValueError) as e:
            # Unit override incompatible with group_distance.  Skip the field.
            log.debug('%s: cannot convert windrose to %s: %s' % (cname.field, cname.unit, e))
            return
        if cname.format_spec == 'formatted':
            fmt_str = formatter.get_format_string(tgt_unit)
            loopdata_pkt[cname.field] = [fmt_str % rnd(v) for v in values]
        else:
            loopdata_pkt[cname.field] = [rnd(v) for v in values]

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
    def create_loopdata_packet(pkt: Dict[str, Any], ctx: ReportContext, accums: Accumulators,
            loop_frequency: float) -> Dict[str, Any]:
        """One context's fields, rendered off the shared accumulators with
        its own formatter, converter, trend window and band edges."""

        loopdata_pkt: Dict[str, Any] = {}

        # Iterate through fields.
        for cname in ctx.fields_to_include:
            if cname is None:
                continue
            if cname.prefix == 'unit':
                LoopProcessor.add_unit_obstype(cname, loopdata_pkt, ctx.converter, ctx.formatter)
                continue

            if cname.obstype == 'windrose':
                LoopProcessor.add_windrose_obstype(cname, accums, loopdata_pkt,
                    ctx.converter, ctx.formatter, ctx.windrose_key)
                continue

            if cname.period == 'current':
                LoopProcessor.add_current_obstype(cname, pkt, loopdata_pkt, ctx.converter, ctx.formatter)
                continue

            # fixed periods
            if cname.period == 'alltime' and accums.alltime_accum is not None:
                LoopProcessor.add_period_obstype(cname, accums.alltime_accum, loopdata_pkt, ctx.converter, ctx.formatter)
                continue
            if cname.period == 'rainyear' and accums.rainyear_accum is not None:
                LoopProcessor.add_period_obstype(cname, accums.rainyear_accum, loopdata_pkt, ctx.converter, ctx.formatter)
                continue
            if cname.period == 'year' and accums.year_accum is not None:
                LoopProcessor.add_period_obstype(cname, accums.year_accum, loopdata_pkt, ctx.converter, ctx.formatter)
                continue
            if cname.period == 'month' and accums.month_accum is not None:
                LoopProcessor.add_period_obstype(cname, accums.month_accum, loopdata_pkt, ctx.converter, ctx.formatter)
                continue
            if cname.period == 'week' and accums.week_accum is not None:
                LoopProcessor.add_period_obstype(cname, accums.week_accum, loopdata_pkt, ctx.converter, ctx.formatter)
                continue
            if cname.period == 'day':
                LoopProcessor.add_period_obstype(cname, accums.day_accum, loopdata_pkt, ctx.converter, ctx.formatter)
                continue
            if cname.period == 'hour' and accums.hour_accum is not None:
                LoopProcessor.add_period_obstype(cname, accums.hour_accum, loopdata_pkt, ctx.converter, ctx.formatter)
                continue

            # continuous periods; the trend is the accumulator sized to THIS
            # report's window.
            if cname.period == 'trend':
                trend_accum = accums.continuous.get(ctx.trend_key)
                if trend_accum is not None:
                    LoopProcessor.add_trend_obstype(cname, trend_accum, pkt,
                        loopdata_pkt, ctx.time_delta, loop_frequency, ctx.baro_trend_descs, ctx.converter, ctx.formatter)
                continue
            continuous_accum = accums.continuous.get(cname.period) if cname.period is not None else None
            if continuous_accum is not None:
                LoopProcessor.add_period_obstype(cname, continuous_accum, loopdata_pkt, ctx.converter, ctx.formatter)

        if ctx.windrose:
            # Legend helper: the band edges, in this report's windSpeed
            # unit -- exactly the values banding applies, so a page never
            # hardcodes them.
            loopdata_pkt['windrose.bands'] = ctx.windrose_bands

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
        # The accumulators the two report-scoped settings resolved to: one
        # trend accumulator per distinct window, one windrose set per
        # distinct band edges.  A second one here is a report that differs.
        log.info('trend accumulators      : %s' % sorted(
            per for per in cfg.obstypes.continuous if LoopData.is_trend_key(per)))
        for windrose_key, (unit, bands) in cfg.windrose_bandings.items():
            log.info('windrose bands %s: %s %s, periods %s' % (windrose_key, bands, unit, sorted(
                per for key, per in cfg.windrose_span_periods | cfg.windrose_continuous_periods
                if key == windrose_key)))
        if len(cfg.almanac_fields_all()) > 0:
            log.info('latitude                : %f' % cfg.latitude)
            log.info('longitude               : %f' % cfg.longitude)
            log.info('altitude_m              : %f' % cfg.altitude_m)
        for ctx in cfg.contexts:
            log.info('--- %s' % ctx.label)
            log.info('specified_fields        : %s' % ctx.specified_fields)
            log.info('time_delta              : %d' % ctx.time_delta)
            log.info('windrose_bands          : %s' % ctx.windrose_bands)
            log.info('baro_trend_descs        : %s' % ctx.baro_trend_descs)
            if len(ctx.almanac_fields) > 0:
                log.info('almanac_fields          : %s' % [ f.field for f in ctx.almanac_fields ])
            if len(ctx.station_fields) > 0:
                log.info('station_fields          : %s' % [ f.field for f in ctx.station_fields ])

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


# ==============================================================================
#     Finishing the migration: python3 -m user.loopdata [--apply]
# ==============================================================================
#
# The [LoopData] [[Include]] fields line is deprecated, and the reports that
# used to depend on it now declare their own fields.  What is left on it,
# once every extension has been upgraded, is a mixture: entries a report now
# declares (safe to lose from the flat namespace) and entries nothing
# declares at all -- stale ones from pages that changed years ago, and the
# occasional live one that some script reads.  Nothing can tell those two
# apart automatically, which is why this reports first and changes nothing
# unless it can account for every entry.

def declaring_reports(config_dict: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """report -> {'fields': [...], 'signature': str} for every enabled
    report that declares fields, in [StdReport] order."""
    loop_config_dict = config_dict.get('LoopData', {})
    target_report = loop_config_dict.get('Formatting', {}).get('target_report', 'LoopDataReport')
    try:
        target_dict: Optional[Dict[str, Any]] = LoopData.get_target_report_dict(config_dict, target_report)
    except Exception:
        target_dict = None
    shared: Dict[str, Any] = {}
    found: Dict[str, Dict[str, Any]] = {}
    for report in LoopData.enabled_reports(config_dict):
        try:
            skin_dict = LoopData.get_target_report_dict(config_dict, report)
        except Exception:
            continue
        declared = LoopData.declared_fields_from_skin_dict(skin_dict, report)
        if len(declared) == 0:
            continue
        bands = LoopData.report_windrose_bands(config_dict, report, skin_dict,
            loop_config_dict, target_dict, shared, target_report)
        found[report] = {'fields': declared,
                         'signature': LoopData.render_signature(skin_dict, bands)}
    return found

def migration_report(config_dict: Dict[str, Any]) -> Dict[str, Any]:
    """What the fields line still holds, and who claims each entry."""
    loop_config_dict = config_dict.get('LoopData', {})
    line = LoopData.normalize_fields(loop_config_dict.get('Include', {}).get('fields'))
    target_report = loop_config_dict.get('Formatting', {}).get('target_report', 'LoopDataReport')
    reports = declaring_reports(config_dict)
    try:
        target_dict = LoopData.get_target_report_dict(config_dict, target_report)
        legacy_signature: Optional[str] = LoopData.render_signature(target_dict,
            LoopData.legacy_windrose_bands(config_dict, target_report, target_dict, loop_config_dict))
    except Exception:
        legacy_signature = None
    # The line is rendered through target_report, so credit it first: it
    # renders identically by definition, and anything credited elsewhere is
    # then genuinely a different report's.
    order = ([target_report] if target_report in reports else []) + \
            [r for r in reports if r != target_report]
    owner: Dict[str, str] = {}
    differs: Set[str] = set()
    for field in line:
        for report in order:
            info = reports[report]
            if field in info['fields']:
                owner[field] = report
                if legacy_signature is not None and info['signature'] != legacy_signature:
                    differs.add(field)
                break
    return {'line': line, 'reports': reports, 'owner': owner, 'differs': differs,
            'unclaimed': [f for f in line if f not in owner],
            'target_report': target_report,
            'windrose_bands': loop_config_dict.get('windrose_bands')}

def print_migration_report(config_path: str, report: Dict[str, Any]) -> None:
    print('weewx-loopdata migration report for %s\n' % config_path)
    if len(report['reports']) == 0:
        print('No enabled report declares [LoopData] [[fields]].  Upgrade the extensions')
        print('whose pages read the loop-data file before finishing the migration.')
    else:
        print('Reports declaring their own fields:')
        for name, info in report['reports'].items():
            print('    %-24s %4d fields' % (name, len(info['fields'])))
    print()
    if len(report['line']) == 0:
        print('There is no [LoopData] [[Include]] fields line.')
        return
    counts: Dict[str, int] = {}
    for field, name in report['owner'].items():
        counts[name] = counts.get(name, 0) + 1
    print('[LoopData] [[Include]] fields: %d entries' % len(report['line']))
    for name, count in sorted(counts.items(), key=lambda kv: -kv[1]):
        note = ''
        if any(report['owner'].get(f) == name and f in report['differs'] for f in report['line']):
            note = ('   (renders these differently from %s -- check the units and formats on\n'
                    '%s a page still reading the flat keys)' % (report['target_report'], ' ' * 37))
        print('    declared by %-24s %4d%s' % (name, count, note))
    print('    claimed by nobody             %4d' % len(report['unclaimed']))

def print_unclaimed_advice(report: Dict[str, Any]) -> None:
    print()
    print('Nothing has been changed: %d entries are claimed by nobody.' % len(report['unclaimed']))
    for field in report['unclaimed']:
        print('    %s' % field)
    print()
    print('Almost always these are simply cruft -- entries a page asked for years ago and')
    print('no longer reads, or that an extension appended for a version since changed.  If')
    print('nothing outside WeeWX reads your loop-data file, that is the whole story: delete')
    print('them from the fields line and run this again.')
    print()
    print('Only two other things can be true of an entry, and both are unusual:')
    print('  * a page of your own reads it -- declare it under that page\'s report, and')
    print('    have the page read that report\'s own entry rather than the flat key;')
    print('  * a script, an SNMP check or something else outside WeeWX reads it -- declare')
    print('    it under the ScriptData report (enable it first) and have that reader take')
    print('    d["ScriptData"].')
    print()
    print('https://chaunceygardiner.github.io/weewx-loopdata/declaring-fields.html')

def anchored_loop_data_dir(config_dict: Dict[str, Any], target_report: str) -> Optional[str]:
    """Where a relative loop_data_dir resolves to right now, or None when
    it is already absolute.  A relative one is measured from
    target_report's directory, so removing target_report can move the
    file out from under every page that polls it."""
    file_spec = config_dict.get('LoopData', {}).get('FileSpec', {})
    if os.path.isabs(str(file_spec.get('loop_data_dir', '.'))):
        return None
    try:
        anchor = LoopData.get_target_report_dict(config_dict, target_report)
    except Exception:
        anchor = {'HTML_ROOT': config_dict.get('StdReport', {}).get('HTML_ROOT', 'public_html')}
    return os.path.normpath(LoopData.compose_loop_data_dir(config_dict, anchor, file_spec))

def windrose_bands_source(config_dict: Dict[str, Any], report: str) -> Optional[str]:
    """Where a report's windrose bands come from, if from anywhere nearer
    than the deprecated [LoopData] value: its own stanza, a station-wide
    [StdReport] setting, or its skin.  None when the [LoopData] value is
    what it is actually using."""
    std_report = config_dict.get('StdReport', {})
    stanza = std_report.get(report, {})
    if isinstance(stanza, dict) and stanza.get('windrose_bands') is not None:
        return '[StdReport] [[%s]] windrose_bands' % report
    if LoopData.stdreport_windrose_bands(config_dict) is not None:
        return 'a station-wide [StdReport] windrose_bands'
    try:
        skin_dict = LoopData.get_target_report_dict(config_dict, report)
    except Exception:
        return None
    if skin_dict.get('windrose_bands') is not None:
        return "%s's skin" % report
    loopdata_section = skin_dict.get('LoopData')
    if isinstance(loopdata_section, dict) and loopdata_section.get('windrose_bands') is not None:
        return "%s's skin" % report
    return None

def apply_migration(config_dict: Dict[str, Any], report: Dict[str, Any]) -> List[str]:
    """Delete the deprecated trio, moving windrose_bands somewhere it still
    applies and pinning loop_data_dir if removing target_report would move
    the file.  Returns a description of every change made."""
    changes: List[str] = []
    loopdata = config_dict['LoopData']
    target_report = report['target_report']

    # A relative loop_data_dir is measured from target_report's directory.
    # Once target_report is gone that becomes LoopDataReport's, so pin the
    # path first: a page polling the old URL must not start 404ing because
    # the file quietly moved.
    if 'Formatting' in loopdata:
        here = anchored_loop_data_dir(config_dict, target_report)
        there = anchored_loop_data_dir(config_dict, 'LoopDataReport')
        if here is not None and here != there:
            loopdata.setdefault('FileSpec', {})['loop_data_dir'] = here
            changes.append('pinned [LoopData] [[FileSpec]] loop_data_dir = %s (it was relative to '
                '%s, which is going)' % (here, target_report))

    # windrose_bands belongs to the rose it bands.  Before 7.0 that was the
    # one rose in the flat file, rendered through target_report, and the
    # value is already in that report's windSpeed unit -- so it moves to
    # that report's stanza, as it is, and nothing station-wide is invented.
    bands = report['windrose_bands']
    if bands is not None:
        stanza = config_dict.get('StdReport', {}).get(target_report)
        printable = ', '.join(str(b) for b in (bands if isinstance(bands, list) else [bands]))
        covered = windrose_bands_source(config_dict, target_report)
        if not isinstance(stanza, dict):
            changes.append('LEFT [LoopData] windrose_bands alone: there is no [StdReport] [[%s]] '
                'to move it to -- put it on the report whose rose it bands' % target_report)
        elif covered is not None:
            # It is already shadowed: something nearer the report answers
            # first, so the value has not banded anything for a while.
            del loopdata['windrose_bands']
            changes.append('removed [LoopData] windrose_bands (%s): %s already sets the bands '
                'for %s, so it was doing nothing' % (printable, covered, target_report))
        else:
            stanza['windrose_bands'] = bands
            del loopdata['windrose_bands']
            changes.append('moved [LoopData] windrose_bands to [StdReport] [[%s]]: %s (its own '
                'unit, unchanged)' % (target_report, printable))

    if 'Include' in loopdata:
        del loopdata['Include']
        changes.append('removed [LoopData] [[Include]] fields (%d entries)' % len(report['line']))
    if 'Formatting' in loopdata:
        del loopdata['Formatting']
        changes.append('removed [LoopData] [[Formatting]] target_report = %s' % report['target_report'])
    return changes

def main() -> int:
    import argparse
    parser = argparse.ArgumentParser(
        description='Finish the weewx-loopdata 7.0 migration: report on what the deprecated '
                    '[LoopData] [[Include]] fields line still holds and, once every entry is '
                    'accounted for, remove it along with [[Formatting]] target_report.')
    parser.add_argument('--config', metavar='PATH', help='path to weewx.conf')
    parser.add_argument('--apply', action='store_true',
        help='make the change, rather than only reporting it')
    args = parser.parse_args()

    import weecfg
    try:
        config_path, config_dict = weecfg.read_config(args.config, [])
    except Exception as e:
        print('Could not read weewx.conf: %s' % e, file=sys.stderr)
        return 1
    if 'LoopData' not in config_dict:
        print('No [LoopData] section in %s.' % config_path, file=sys.stderr)
        return 1

    report = migration_report(config_dict)
    print_migration_report(config_path, report)
    if len(report['line']) > 0 and len(report['unclaimed']) > 0:
        print_unclaimed_advice(report)
        return 1
    loopdata = config_dict['LoopData']
    leftovers = [name for name, present in (
        ('the [[Include]] fields line', len(report['line']) > 0),
        ('[[Formatting]] target_report', 'Formatting' in loopdata),
        ('[LoopData] windrose_bands', loopdata.get('windrose_bands') is not None)) if present]
    print()
    if len(leftovers) == 0:
        print('Nothing deprecated is left in [LoopData]: this station is migrated.')
        return 0
    if len(report['line']) > 0:
        print('Every entry is declared by a report, so the fields line and target_report')
        print('can go.')
    else:
        print('Still to remove: %s.' % ', '.join(leftovers))
    if not args.apply:
        print('Nothing has been changed.  Run again with --apply to make the change.')
        return 0
    backup = '%s.%s' % (config_path, time.strftime('%Y%m%d%H%M%S'))
    with open(config_path, 'rb') as src, open(backup, 'wb') as dst:
        dst.write(src.read())
    changes = apply_migration(config_dict, report)
    config_dict.write()
    print()
    for change in changes:
        print('    %s' % change)
    print('    backup written to %s' % backup)
    print()
    print('Restart weewxd for this to take effect.')
    return 0

if __name__ == '__main__':
    sys.exit(main())
