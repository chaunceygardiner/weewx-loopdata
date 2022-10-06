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
import json
import logging
import os
import queue
import shutil
import sys
import tempfile
import threading
import time

from dataclasses import dataclass
from typing import Any, Dict, Generator, List, Optional, Tuple
from enum import Enum

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

LOOP_DATA_VERSION = '2.12'

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
    period     : Optional[str] # 2m, 10m, 24h, hour, day, week, month, year, rainyear, current, trend
    obstype    : str           # e.g,. outTemp
    agg_type   : Optional[str] # avg, sum, etc. (required if period, other than current, is specified, else None)
    format_spec: Optional[str] # formatted (formatted value sans label), raw or ordinal_compass (could be on direction), or None

@dataclass
class Configuration:
    queue              : queue.SimpleQueue
    config_dict        : Dict[str, Any]
    unit_system        : int
    archive_interval   : int
    loop_data_dir      : str
    filename           : str
    target_report      : str
    loop_frequency     : float
    specified_fields   : List[str]
    fields_to_include  : List[CheetahName]
    formatter          : weewx.units.Formatter
    converter          : weewx.units.Converter
    tmpname            : str
    enable             : bool
    remote_server      : str
    remote_port        : int
    remote_user        : str
    remote_dir         : str
    compress           : bool
    log_success        : bool
    ssh_options        : str
    skip_if_older_than : int
    timeout            : int
    time_delta         : int
    week_start         : int
    rainyear_start     : int
    current_obstypes   : List[str]
    trend_obstypes     : List[str]
    rainyear_obstypes  : List[str]
    year_obstypes      : List[str]
    month_obstypes     : List[str]
    week_obstypes      : List[str]
    day_obstypes       : List[str]
    hour_obstypes      : List[str]
    ten_min_obstypes   : List[str]
    two_min_obstypes   : List[str]
    day_24h_obstypes   : List[str]
    baro_trend_descs   : Any # Dict[BarometerTrend, str]

@dataclass
class AccumulatorPayload:
    rainyear_accum: Optional[weewx.accum.Accum]
    year_accum : Optional[weewx.accum.Accum]
    month_accum: Optional[weewx.accum.Accum]
    week_accum : Optional[weewx.accum.Accum]
    day_accum  : Optional[weewx.accum.Accum]
    hour_accum : Optional[weewx.accum.Accum]

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
    timestamp: int
    value    : Any

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

        self.loop_proccessor_started = False
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
        (fields_to_include, current_obstypes, trend_obstypes, rainyear_obstypes,
            year_obstypes, month_obstypes, week_obstypes, day_obstypes, hour_obstypes,
            ten_min_obstypes, two_min_obstypes, day_24h_obstypes) = LoopData.get_fields_to_include(specified_fields)

        # Get the time span (number of seconds) to use for trend.
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
            queue               = queue.SimpleQueue(),
            config_dict         = config_dict,
            unit_system         = unit_system,
            archive_interval    = to_int(std_archive_dict.get('archive_interval')),
            loop_data_dir       = loop_data_dir,
            filename            = file_spec_dict.get('filename', 'loop-data.txt'),
            target_report       = target_report,
            loop_frequency      = loop_frequency,
            specified_fields    = specified_fields,
            fields_to_include   = fields_to_include,
            formatter           = weewx.units.Formatter.fromSkinDict(target_report_dict),
            converter           = weewx.units.Converter.fromSkinDict(target_report_dict),
            tmpname             = tmp.name,
            enable              = to_bool(rsync_spec_dict.get('enable')),
            remote_server       = rsync_spec_dict.get('remote_server'),
            remote_port         = to_int(rsync_spec_dict.get('remote_port')) if rsync_spec_dict.get(
                                      'remote_port') is not None else None,
            remote_user         = rsync_spec_dict.get('remote_user'),
            remote_dir          = rsync_spec_dict.get('remote_dir'),
            compress            = to_bool(rsync_spec_dict.get('compress')),
            log_success         = to_bool(rsync_spec_dict.get('log_success')),
            ssh_options         = rsync_spec_dict.get('ssh_options', '-o ConnectTimeout=1'),
            timeout             = to_int(rsync_spec_dict.get('timeout', 1)),
            skip_if_older_than  = to_int(rsync_spec_dict.get('skip_if_older_than', 3)),
            time_delta          = time_delta,
            week_start          = week_start,
            rainyear_start      = rainyear_start,
            current_obstypes    = current_obstypes,
            trend_obstypes      = trend_obstypes,
            rainyear_obstypes   = rainyear_obstypes,
            year_obstypes       = year_obstypes,
            month_obstypes      = month_obstypes,
            week_obstypes       = week_obstypes,
            day_obstypes        = day_obstypes,
            hour_obstypes       = hour_obstypes,
            ten_min_obstypes    = ten_min_obstypes,
            two_min_obstypes    = two_min_obstypes,
            day_24h_obstypes    = day_24h_obstypes,
            baro_trend_descs    = baro_trend_descs)

        if not os.path.exists(self.cfg.loop_data_dir):
            os.makedirs(self.cfg.loop_data_dir)

        log.info('LoopData file is: %s' % os.path.join(self.cfg.loop_data_dir, self.cfg.filename))

        self.bind(weewx.PRE_LOOP, self.pre_loop)
        self.bind(weewx.NEW_LOOP_PACKET, self.new_loop)

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
    def get_fields_to_include(specified_fields: List[str]
            ) -> Tuple[List[CheetahName], List[str], List[str], List[str], List[str],
            List[str], List[str], List[str], List[str], List[str], List[str], List[str]]:
        """
        Return fields_to_include, current_obstypes, trend_obstypes,
               rainyear_obstypes, year_obstypes, month_obstypes, week_obstypes,
               day_obstypes, hour_obstypes, ten_min_obstypes, two_min_obstypes, day_24h_obstypes
        """
        specified_fields = list(dict.fromkeys(specified_fields))
        fields_to_include: List[CheetahName] = []
        for field in specified_fields:
            cname: Optional[CheetahName] = LoopData.parse_cname(field)
            if cname is not None:
                fields_to_include.append(cname)
        current_obstypes  : List[str] = LoopData.compute_period_obstypes(
            fields_to_include, 'current')
        trend_obstypes  : List[str] = LoopData.compute_period_obstypes(
            fields_to_include, 'trend')
        rainyear_obstypes    : List[str] = LoopData.compute_period_obstypes(
            fields_to_include, 'rainyear')
        year_obstypes    : List[str] = LoopData.compute_period_obstypes(
            fields_to_include, 'year')
        month_obstypes    : List[str] = LoopData.compute_period_obstypes(
            fields_to_include, 'month')
        week_obstypes    : List[str] = LoopData.compute_period_obstypes(
            fields_to_include, 'week')
        day_obstypes    : List[str] = LoopData.compute_period_obstypes(
            fields_to_include, 'day')
        hour_obstypes    : List[str] = LoopData.compute_period_obstypes(
            fields_to_include, 'hour')
        ten_min_obstypes: List[str] = LoopData.compute_period_obstypes(
            fields_to_include, '10m')
        two_min_obstypes: List[str] = LoopData.compute_period_obstypes(
            fields_to_include, '2m')
        day_24h_obstypes: List[str] = LoopData.compute_period_obstypes(
            fields_to_include, '24h')

        # current_obstypes is special because current observations are
        # needed to feed all the others.  As such, take the union of all.
        current_obstypes = current_obstypes + trend_obstypes + \
            rainyear_obstypes + year_obstypes + month_obstypes + \
            week_obstypes + day_obstypes + hour_obstypes + ten_min_obstypes + two_min_obstypes + \
            day_24h_obstypes
        current_obstypes = list(dict.fromkeys(current_obstypes))

        return (fields_to_include, current_obstypes, trend_obstypes,
            rainyear_obstypes, year_obstypes, month_obstypes, week_obstypes,
            day_obstypes, hour_obstypes, ten_min_obstypes, two_min_obstypes, day_24h_obstypes)

    @staticmethod
    def compute_period_obstypes(fields_to_include: List[CheetahName], period: str) -> List[str]:
        period_obstypes: List[str] = []
        for cname in fields_to_include:
            if cname.period == period:
                period_obstypes.append(cname.obstype)
                if cname.obstype == 'wind':
                    period_obstypes.append('windSpeed')
                    period_obstypes.append('windDir')
                    period_obstypes.append('windGust')
                    period_obstypes.append('windGustDir')
                if cname.obstype == 'appTemp':
                    period_obstypes.append('outTemp')
                    period_obstypes.append('outHumidity')
                    period_obstypes.append('windSpeed')
                if cname.obstype.startswith('windrun'):
                    period_obstypes.append('windSpeed')
                    period_obstypes.append('windDir')
                if cname.obstype == 'beaufort':
                    period_obstypes.append('windSpeed')
        return list(dict.fromkeys(period_obstypes))

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
        if self.loop_proccessor_started:
            return
        # Start the loop processor thread.
        self.loop_proccessor_started = True

        try:
            binder = weewx.manager.DBBinder(self.config_dict)
            binding = self.config_dict.get('StdReport')['data_binding']
            dbm = binder.get_manager(binding)

            # Get archive packets to prime accumulators.  First find earliest
            # record we need to fetch.

            # Fetch them just once with the greatest time period.
            now = time.time()

            # two_min fixed at now - 120
            earliest_two_min = now - 120 if len(self.cfg.two_min_obstypes) > 0 else now
            log.debug('Earliest time for 2m is %s' % timestamp_to_string(earliest_two_min))

            # 24h fixed at now - 86400
            earliest_24h = now - 86400 if len(self.cfg.day_24h_obstypes) > 0 else now
            log.debug('Earliest time for 24h is %s' % timestamp_to_string(earliest_24h))

            # ten_min fixed at now - 600
            earliest_ten_min = now - 600 if len(self.cfg.ten_min_obstypes) > 0 else now
            log.debug('Earliest time for 10m is %s' % timestamp_to_string(earliest_ten_min))

            # trend fixed at now - time_delta
            earliest_trend = now - self.cfg.time_delta if len(self.cfg.trend_obstypes) > 0 else now
            log.debug('Earliest time for trend is %s' % timestamp_to_string(earliest_trend))

            # day fixed at the start of current day
            earliest_day = weeutil.weeutil.startOfDay(now)

            # We want the earliest time needed.
            earliest_time: int = to_int(min(earliest_ten_min, earliest_trend, earliest_day))
            log.debug('Earliest time selected is %s' % timestamp_to_string(earliest_time))

            # Fetch the records.
            start = time.time()
            archive_pkts: List[Dict[str, Any]] = LoopData.get_archive_packets(
                dbm, self.archive_columns, earliest_time)

            # Save packets as appropriate.
            trend_packets: List[PeriodPacket] = []
            two_min_packets: List[PeriodPacket] = []
            ten_min_packets: List[PeriodPacket] = []
            day_24h_packets: List[PeriodPacket] = []
            pkt_count: int = 0
            for pkt in archive_pkts:
                pkt_time = pkt['dateTime']
                if 'windrun' in pkt and 'windDir' in pkt and pkt['windDir'] is not None:
                    bkt = LoopProcessor.get_windrun_bucket(pkt['windDir'])
                    pkt['windrun_%s' % windrun_bucket_suffixes[bkt]] = pkt['windrun']
                if len(self.cfg.trend_obstypes) > 0 and pkt_time >= earliest_trend:
                    LoopProcessor.save_period_packet(pkt['dateTime'], pkt, trend_packets, self.cfg.time_delta, self.cfg.trend_obstypes)
                if len(self.cfg.ten_min_obstypes) > 0 and pkt_time >= earliest_ten_min:
                    LoopProcessor.save_period_packet(pkt['dateTime'], pkt, ten_min_packets, 600, self.cfg.ten_min_obstypes)
                if len(self.cfg.two_min_obstypes) > 0 and pkt_time >= earliest_two_min:
                    LoopProcessor.save_period_packet(pkt['dateTime'], pkt, two_min_packets, 120, self.cfg.two_min_obstypes)
                if len(self.cfg.day_24h_obstypes) > 0 and pkt_time >= earliest_24h:
                    LoopProcessor.save_period_packet(pkt['dateTime'], pkt, day_24h_packets, 86400, self.cfg.day_24h_obstypes)
                if len(self.cfg.day_obstypes) > 0 and pkt_time >= earliest_day:
                    self.day_packets.append(pkt)
                pkt_count += 1
            log.debug('Collected %d archive packets in %f seconds.' % (pkt_count, time.time() - start))

            # accumulator_payload_sent is used to only create accumulators on first new_loop packet
            self.accumulator_payload_sent = False
            lp: LoopProcessor = LoopProcessor(self.cfg, trend_packets, ten_min_packets, two_min_packets, day_24h_packets)
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

            rainyear_accum, self.cfg.rainyear_obstypes = LoopData.create_rainyear_accum(
                self.cfg.unit_system, self.cfg.archive_interval, self.cfg.rainyear_obstypes, pkt_time, self.cfg.rainyear_start, day_accum, dbm)
            year_accum, self.cfg.year_obstypes = LoopData.create_year_accum(
                self.cfg.unit_system, self.cfg.archive_interval, self.cfg.year_obstypes, pkt_time, day_accum, dbm)
            month_accum, self.cfg.month_obstypes = LoopData.create_month_accum(
                self.cfg.unit_system, self.cfg.archive_interval, self.cfg.month_obstypes, pkt_time, day_accum, dbm)
            week_accum, self.cfg.week_obstypes = LoopData.create_week_accum(
                self.cfg.unit_system, self.cfg.archive_interval, self.cfg.week_obstypes, pkt_time, self.cfg.week_start, day_accum, dbm)
            hour_accum, self.cfg.hour_obstypes = LoopData.create_hour_accum(
                self.cfg.unit_system, self.cfg.archive_interval, self.cfg.hour_obstypes, pkt_time, day_accum, dbm)
            self.cfg.queue.put(AccumulatorPayload(
                rainyear_accum = rainyear_accum,
                year_accum     = year_accum,
                month_accum    = month_accum,
                week_accum     = week_accum,
                day_accum      = day_accum,
                hour_accum     = hour_accum))
        self.cfg.queue.put(event)

    @staticmethod
    def create_rainyear_accum(unit_system: int, archive_interval: int, obstypes: List[str], pkt_time: int,
            rainyear_start: int, day_accum: weewx.accum.Accum, dbm) -> Tuple[Optional[weewx.accum.Accum], List[str]]:
        log.debug('Creating initial rainyear_accum')
        span = weeutil.weeutil.archiveRainYearSpan(pkt_time, rainyear_start)
        return LoopData.create_period_accum('rainyear', unit_system, archive_interval, obstypes, span, day_accum, dbm)

    @staticmethod
    def create_year_accum(unit_system: int, archive_interval: int, obstypes: List[str], pkt_time: int, day_accum: weewx.accum.Accum, dbm
            ) -> Tuple[Optional[weewx.accum.Accum], List[str]]:
        log.debug('Creating initial year_accum')
        span = weeutil.weeutil.archiveYearSpan(pkt_time)
        return LoopData.create_period_accum('year', unit_system, archive_interval, obstypes, span, day_accum, dbm)

    @staticmethod
    def create_month_accum(unit_system: int, archive_interval: int, obstypes: List[str], pkt_time: int, day_accum: weewx.accum.Accum, dbm
            ) -> Tuple[Optional[weewx.accum.Accum], List[str]]:
        log.debug('Creating initial month_accum')
        span = weeutil.weeutil.archiveMonthSpan(pkt_time)
        return LoopData.create_period_accum('month', unit_system, archive_interval, obstypes, span, day_accum, dbm)

    @staticmethod
    def create_week_accum(unit_system: int, archive_interval: int, obstypes: List[str], pkt_time: int,
            week_start: int, day_accum: weewx.accum.Accum, dbm) -> Tuple[Optional[weewx.accum.Accum], List[str]]:
        log.debug('Creating initial week_accum')
        span = weeutil.weeutil.archiveWeekSpan(pkt_time, week_start)
        return LoopData.create_period_accum('week', unit_system, archive_interval, obstypes, span, day_accum, dbm)

    @staticmethod
    def create_hour_accum(unit_system: int, archive_interval: int, obstypes: List[str], pkt_time: int, day_accum: weewx.accum.Accum, dbm
            ) -> Tuple[Optional[weewx.accum.Accum], List[str]]:
        log.debug('Creating initial hour_accum')
        span = weeutil.weeutil.archiveHoursAgoSpan(pkt_time)
        return LoopData.create_period_accum('hour', unit_system, archive_interval, obstypes, span, day_accum, dbm)

    @staticmethod
    def create_period_accum(name: str, unit_system: int, archive_interval: int, obstypes: List[str],
            span: weeutil.weeutil.TimeSpan, day_accum: weewx.accum.Accum, dbm) -> Tuple[Optional[weewx.accum.Accum], List[str]]:
        """return period accumulator and (possibly trimmed) obstypes"""

        if len(obstypes) == 0:
            return None, []

        start = time.time()
        accum = weewx.accum.Accum(span, unit_system)

        # valid observation types will be returned
        valid_obstypes: List[str] = []

        # for each obstype, create the appropriate stats.
        for obstype in obstypes:
            stats: Optional[Any] = None
            if obstype not in day_accum:
                # Obstypes implemented with xtypes will fall out here.
                # As well as typos or any obstype that is not in day_accum.
                log.info('Ignoring %s for %s time period as this observation has no day accumulator.'
                    % (obstype, name))
                continue
            valid_obstypes.append(obstype)
            if type(day_accum[obstype]) == weewx.accum.ScalarStats:
                stats = weewx.accum.ScalarStats()
            elif type(day_accum[obstype]) == weewx.accum.VecStats:
                stats = weewx.accum.VecStats()
            elif type(day_accum[obstype]) == weewx.accum.FirstLastAccum:
                stats = weewx.accum.FirstLastAccum()
            else:
                return None, []
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
                            return None, []
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
                pkt_time = pkt['dateTime']
                pkt['usUnits'] = unit_system
                pruned_pkt = LoopProcessor.prune_period_packet(pkt_time, pkt, obstypes)
                accum.addRecord(pruned_pkt, weight=archive_interval * 60)
                pkt_count += 1
            log.debug('Primed hour_accum with %d archive packets in %f seconds.' % (pkt_count, time.time() - start))

        log.debug('Created %s accum in %f seconds (read %d records).' % (name, time.time() - start, record_count))
        return accum, valid_obstypes

    @staticmethod
    def parse_cname(field: str) -> Optional[CheetahName]:
        valid_prefixes    : List[str] = [ 'unit' ]
        valid_prefixes2   : List[str] = [ 'label' ]
        valid_periods     : List[str] = [ 'rainyear', 'year', 'month', 'week',
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
        # 2m/10m/24h/hour/day/week/month/year/rainyear must have an agg_type
        if period in [ '2m', '10m', '24h', 'hour', 'day', 'week','month', 'year', 'rainyear' ]:
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

        # windrun_<dir> is not supported for week, month, year and rainyear
        if obstype.startswith('windrun_') and (
                period == 'week' or period == 'month' or period == 'year' or period == 'rainyear'):
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
    def __init__(self, cfg: Configuration, trend_packets: List[PeriodPacket],
                 ten_min_packets: List[PeriodPacket], two_min_packets: List[PeriodPacket], day_24h_packets: List[PeriodPacket]):
        self.cfg = cfg
        self.archive_start: float = time.time()
        self.trend_packets: List[PeriodPacket] = trend_packets
        self.ten_min_packets: List[PeriodPacket] = ten_min_packets
        self.two_min_packets: List[PeriodPacket] = two_min_packets
        self.day_24h_packets: List[PeriodPacket] = day_24h_packets

    def process_queue(self) -> None:
        try:
            while True:
                event               = self.cfg.queue.get()

                if type(event) == AccumulatorPayload:
                    LoopProcessor.log_configuration(self.cfg)
                    self.rainyear_accum = event.rainyear_accum
                    self.year_accum = event.year_accum
                    self.month_accum = event.month_accum
                    self.week_accum = event.week_accum
                    self.day_accum = event.day_accum
                    self.hour_accum = event.hour_accum
                    continue

                pkt: Dict[str, Any] = event.packet
                pkt_time: int       = to_int(pkt['dateTime'])
                pkt['interval']     = self.cfg.loop_frequency / 60.0

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

                # This is a loop packet.
                assert event.event_type == weewx.NEW_LOOP_PACKET
                log.debug('Dequeued loop event(%s): %s' % (event, timestamp_to_string(pkt_time)))
                log.debug(pkt)

                # Process new packet.
                (loopdata_pkt, self.rainyear_accum, self.year_accum,
                    self.month_accum, self.week_accum, self.day_accum,
                    self.hour_accum) = LoopProcessor.generate_loopdata_dictionary(
                    pkt, pkt_time, self.cfg.unit_system,
                    self.cfg.loop_frequency, self.cfg.converter, self.cfg.formatter,
                    self.cfg.fields_to_include, self.cfg.current_obstypes,
                    self.rainyear_accum, self.cfg.rainyear_start, self.cfg.rainyear_obstypes,
                    self.year_accum, self.cfg.year_obstypes,
                    self.month_accum, self.cfg.month_obstypes,
                    self.week_accum, self.cfg.week_start, self.cfg.week_obstypes,
                    self.day_accum, self.cfg.day_obstypes,
                    self.hour_accum, self.cfg.hour_obstypes,
                    self.trend_packets, self.cfg.time_delta, self.cfg.trend_obstypes,
                    self.cfg.baro_trend_descs,
                    self.ten_min_packets, self.cfg.ten_min_obstypes,
                    self.two_min_packets, self.cfg.two_min_obstypes,
                    self.day_24h_packets, self.cfg.day_24h_obstypes)

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
    def generate_loopdata_dictionary(
            in_pkt: Dict[str, Any], pkt_time: int, unit_system: int,
            loop_frequency: float,
            converter: weewx.units.Converter, formatter: weewx.units.Formatter,
            fields_to_include: List[CheetahName], current_obstypes: List[str],
            rainyear_accum: Optional[weewx.accum.Accum], rainyear_start: int, rainyear_obstypes: List[str],
            year_accum: Optional[weewx.accum.Accum], year_obstypes: List[str],
            month_accum: Optional[weewx.accum.Accum], month_obstypes: List[str],
            week_accum: Optional[weewx.accum.Accum], week_start: int, week_obstypes: List[str],
            day_accum: weewx.accum.Accum, day_obstypes: List[str],
            hour_accum: weewx.accum.Accum, hour_obstypes: List[str],
            trend_packets: List[PeriodPacket], time_delta: int, trend_obstypes: List[str],
            baro_trend_descs: Dict[BarometerTrend, str],
            ten_min_packets: List[PeriodPacket], ten_min_obstypes: List[str],
            two_min_packets: List[PeriodPacket], two_min_obstypes: List[str],
            day_24h_packets: List[PeriodPacket], day_24h_obstypes: List[str]
            ) -> Tuple[Dict[str, Any], Optional[weewx.accum.Accum], Optional[weewx.accum.Accum],
            Optional[weewx.accum.Accum], Optional[weewx.accum.Accum], Optional[weewx.accum.Accum],
            Optional[weewx.accum.Accum]]:

        # pkt needs to be in the units that the accumulators are expecting.
        pruned_pkt = LoopProcessor.prune_period_packet(pkt_time, in_pkt, current_obstypes)
        pkt = weewx.units.StdUnitConverters[unit_system].convertDict(pruned_pkt)
        pkt['usUnits'] = unit_system

        # Save needed data for trend.
        LoopProcessor.save_period_packet(pkt_time, pkt, trend_packets, time_delta, trend_obstypes)

        # Save needed data for 2m.
        LoopProcessor.save_period_packet(pkt_time, pkt, two_min_packets, 120, two_min_obstypes)

        # Save needed data for 24h.
        LoopProcessor.save_period_packet(pkt_time, pkt, day_24h_packets, 86400, day_24h_obstypes)

        # Save needed data for 10m.
        LoopProcessor.save_period_packet(pkt_time, pkt, ten_min_packets, 600, ten_min_obstypes)

        # Add packet to rainyear accumulator.
        try:
          if len(rainyear_obstypes) > 0 and rainyear_accum is not None:
              pruned_pkt = LoopProcessor.prune_period_packet(pkt_time, pkt, rainyear_obstypes)
              rainyear_accum.addRecord(pruned_pkt, weight=loop_frequency)
        except weewx.accum.OutOfSpan:
            timespan = weeutil.weeutil.archiveRainYearSpan(pkt['dateTime'], rainyear_start)
            rainyear_accum = weewx.accum.Accum(timespan, unit_system=unit_system)
            # Try again:
            rainyear_accum.addRecord(pkt, weight=loop_frequency)

        # Add packet to year accumulator.
        try:
          if len(year_obstypes) > 0 and year_accum is not None:
              pruned_pkt = LoopProcessor.prune_period_packet(pkt_time, pkt, year_obstypes)
              year_accum.addRecord(pruned_pkt, weight=loop_frequency)
        except weewx.accum.OutOfSpan:
            timespan = weeutil.weeutil.archiveYearSpan(pkt['dateTime'])
            year_accum = weewx.accum.Accum(timespan, unit_system=unit_system)
            # Try again:
            year_accum.addRecord(pkt, weight=loop_frequency)

        # Add packet to month accumulator.
        try:
          if len(month_obstypes) > 0 and month_accum is not None:
              pruned_pkt = LoopProcessor.prune_period_packet(pkt_time, pkt, month_obstypes)
              month_accum.addRecord(pruned_pkt, weight=loop_frequency)
        except weewx.accum.OutOfSpan:
            timespan = weeutil.weeutil.archiveMonthSpan(pkt['dateTime'])
            month_accum = weewx.accum.Accum(timespan, unit_system=unit_system)
            # Try again:
            month_accum.addRecord(pkt, weight=loop_frequency)

        # Add packet to week accumulator.
        try:
          if len(week_obstypes) > 0 and week_accum is not None:
              pruned_pkt = LoopProcessor.prune_period_packet(pkt_time, pkt, week_obstypes)
              week_accum.addRecord(pruned_pkt, weight=loop_frequency)
        except weewx.accum.OutOfSpan:
            timespan = weeutil.weeutil.archiveWeekSpan(pkt['dateTime'], week_start)
            week_accum = weewx.accum.Accum(timespan, unit_system=unit_system)
            # Try again:
            week_accum.addRecord(pkt, weight=loop_frequency)

        # Add packet to day accumulator.
        try:
          if len(day_obstypes) > 0:
              pruned_pkt = LoopProcessor.prune_period_packet(pkt_time, pkt, day_obstypes)
              day_accum.addRecord(pruned_pkt, weight=loop_frequency)
        except weewx.accum.OutOfSpan:
            timespan = weeutil.weeutil.archiveDaySpan(pkt['dateTime'])
            day_accum = weewx.accum.Accum(timespan, unit_system=unit_system)
            # Try again:
            day_accum.addRecord(pkt, weight=loop_frequency)

        # Add packet to hour accumulator.
        try:
          if len(hour_obstypes) > 0:
              pruned_pkt = LoopProcessor.prune_period_packet(pkt_time, pkt, hour_obstypes)
              hour_accum.addRecord(pruned_pkt, weight=loop_frequency)
        except weewx.accum.OutOfSpan:
            timespan = weeutil.weeutil.archiveHoursAgoSpan(pkt['dateTime'])
            hour_accum = weewx.accum.Accum(timespan, unit_system=unit_system)
            # Try again:
            hour_accum.addRecord(pkt, weight=loop_frequency)

        # Create a 2m accumulator.
        two_min_accum = LoopProcessor.create_two_min_accum(
            two_min_packets, two_min_obstypes, unit_system, loop_frequency)

        # Create a 24h accumulator.
        day_24h_accum = LoopProcessor.create_day_24h_accum(
            day_24h_packets, day_24h_obstypes, unit_system, loop_frequency)

        # Create a 10m accumulator.
        ten_min_accum = LoopProcessor.create_ten_min_accum(
            ten_min_packets, ten_min_obstypes, unit_system, loop_frequency)

        # Create the loopdata dictionary.
        return (LoopProcessor.create_loopdata_packet(pkt,
            fields_to_include, trend_packets, rainyear_accum,
            year_accum, month_accum, week_accum, day_accum, hour_accum,
            ten_min_accum, two_min_accum, day_24h_accum, time_delta, baro_trend_descs, converter, formatter),
            rainyear_accum, year_accum, month_accum, week_accum, day_accum, hour_accum)

    @staticmethod
    def create_two_min_accum(two_min_packets: List[PeriodPacket],
            two_min_obstypes: List[str], unit_system: int, loop_frequency: float,
            ) -> Optional[weewx.accum.Accum]:

        if len(two_min_obstypes) != 0 and len(two_min_packets) > 0:
            # Construct a 2m accumulator
            two_min_accum: Optional[weewx.accum.Accum] = weewx.accum.Accum(
                weeutil.weeutil.TimeSpan(
                    two_min_packets[0].timestamp - 1,
                    two_min_packets[-1].timestamp),
                unit_system)
            for two_min_packet in two_min_packets:
                if two_min_accum is not None:
                    if 'interval' in two_min_packet.packet:
                        weight = two_min_packet.packet['interval'] * 60
                    else:
                        weight = loop_frequency
                    two_min_accum.addRecord(two_min_packet.packet, weight=weight)
        else:
            two_min_accum = None

        return two_min_accum

    @staticmethod
    def create_day_24h_accum(day_24h_packets: List[PeriodPacket],
            day_24h_obstypes: List[str], unit_system: int, loop_frequency: float,
            ) -> Optional[weewx.accum.Accum]:

        if len(day_24h_obstypes) != 0 and len(day_24h_packets) > 0:
            # Construct a 24h accumulator
            day_24h_accum: Optional[weewx.accum.Accum] = weewx.accum.Accum(
                weeutil.weeutil.TimeSpan(
                    day_24h_packets[0].timestamp - 1,
                    day_24h_packets[-1].timestamp),
                unit_system)
            for day_24h_packet in day_24h_packets:
                if day_24h_accum is not None:
                    if 'interval' in day_24h_packet.packet:
                        weight = day_24h_packet.packet['interval'] * 60
                    else:
                        weight = loop_frequency
                    day_24h_accum.addRecord(day_24h_packet.packet, weight=weight)
        else:
            day_24h_accum = None

        return day_24h_accum

    @staticmethod
    def create_ten_min_accum(ten_min_packets: List[PeriodPacket],
            ten_min_obstypes: List[str], unit_system: int, loop_frequency: float,
            ) -> Optional[weewx.accum.Accum]:

        if len(ten_min_obstypes) != 0 and len(ten_min_packets) > 0:
            # Construct a 10m accumulator
            ten_min_accum: Optional[weewx.accum.Accum] = weewx.accum.Accum(
                weeutil.weeutil.TimeSpan(
                    ten_min_packets[0].timestamp - 1,
                    ten_min_packets[-1].timestamp),
                unit_system)
            for ten_min_packet in ten_min_packets:
                if ten_min_accum is not None:
                    if 'interval' in ten_min_packet.packet:
                        weight = ten_min_packet.packet['interval'] * 60
                    else:
                        weight = loop_frequency
                    ten_min_accum.addRecord(ten_min_packet.packet, weight=weight)
        else:
            ten_min_accum = None

        return ten_min_accum

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
    def add_period_obstype(cname: CheetahName, period_accum: weewx.accum.Accum,
            loopdata_pkt: Dict[str, Any], converter: weewx.units.Converter,
            formatter: weewx.units.Formatter) -> None:
        if cname.obstype not in period_accum:
            log.debug('No %s stats for %s, skipping %s' % (cname.period, cname.obstype, cname.field))
            return

        stats = period_accum[cname.obstype]

        if isinstance(stats, weewx.accum.ScalarStats) and stats.lasttime is not None:
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

        elif isinstance(stats, weewx.accum.VecStats) and stats.count != 0:
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
    def add_trend_obstype(cname: CheetahName, trend_packets: List[PeriodPacket],
            pkt: Dict[str, Any], loopdata_pkt: Dict[str, Any], time_delta: int,
            baro_trend_descs, converter: weewx.units.Converter,
            formatter: weewx.units.Formatter) -> None:

        if len(trend_packets) == 0:
            log.debug('No trend_packets with which to compute trend: %s.' % cname.field)
            return

        value, unit_type, group_type = LoopProcessor.get_trend(cname, pkt, trend_packets, converter)
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
    def create_loopdata_packet(pkt: Dict[str, Any],
            fields_to_include: List[CheetahName],
            trend_packets: List[PeriodPacket],
            rainyear_accum: Optional[weewx.accum.Accum], year_accum: Optional[weewx.accum.Accum],
            month_accum: Optional[weewx.accum.Accum], week_accum: Optional[weewx.accum.Accum],
            day_accum: weewx.accum.Accum,
            hour_accum: weewx.accum.Accum,
            ten_min_accum: Optional[weewx.accum.Accum],
            two_min_accum: Optional[weewx.accum.Accum],
            day_24h_accum: Optional[weewx.accum.Accum],
            time_delta: int, baro_trend_descs: Dict[BarometerTrend, str],
            converter: weewx.units.Converter, formatter: weewx.units.Formatter) -> Dict[str, Any]:

        loopdata_pkt: Dict[str, Any] = {}

        # Iterate through fields.
        for cname in fields_to_include:
            if cname is None:
                continue
            if cname.prefix == 'unit':
                LoopProcessor.add_unit_obstype(cname, loopdata_pkt, converter, formatter)
                continue
            if cname.period == 'current':
                LoopProcessor.add_current_obstype(cname, pkt, loopdata_pkt, converter, formatter)
                continue
            if cname.period == 'trend':
                LoopProcessor.add_trend_obstype(cname, trend_packets, pkt,
                    loopdata_pkt, time_delta, baro_trend_descs, converter, formatter)
                continue
            if cname.period == 'rainyear' and rainyear_accum is not None:
                LoopProcessor.add_period_obstype(cname, rainyear_accum, loopdata_pkt, converter, formatter)
                continue
            if cname.period == 'year' and year_accum is not None:
                LoopProcessor.add_period_obstype(cname, year_accum, loopdata_pkt, converter, formatter)
                continue
            if cname.period == 'month' and month_accum is not None:
                LoopProcessor.add_period_obstype(cname, month_accum, loopdata_pkt, converter, formatter)
                continue
            if cname.period == 'week' and week_accum is not None:
                LoopProcessor.add_period_obstype(cname, week_accum, loopdata_pkt, converter, formatter)
                continue
            if cname.period == 'day':
                LoopProcessor.add_period_obstype(cname, day_accum, loopdata_pkt, converter, formatter)
                continue
            if cname.period == 'hour':
                LoopProcessor.add_period_obstype(cname, hour_accum, loopdata_pkt, converter, formatter)
                continue
            if cname.period == '10m' and ten_min_accum is not None:
                LoopProcessor.add_period_obstype(cname, ten_min_accum, loopdata_pkt, converter, formatter)
                continue
            if cname.period == '2m' and two_min_accum is not None:
                LoopProcessor.add_period_obstype(cname, two_min_accum, loopdata_pkt, converter, formatter)
                continue
            if cname.period == '24h' and day_24h_accum is not None:
                LoopProcessor.add_period_obstype(cname, day_24h_accum, loopdata_pkt, converter, formatter)
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
    def log_configuration(cfg: Configuration):
        # queue
        # config_dict
        log.info('unit_system        : %d' % cfg.unit_system)
        log.info('archive_interval   : %d' % cfg.archive_interval)
        log.info('loop_data_dir      : %s' % cfg.loop_data_dir)
        log.info('filename           : %s' % cfg.filename)
        log.info('target_report      : %s' % cfg.target_report)
        log.info('loop_frequency     : %s' % cfg.loop_frequency)
        log.info('specified_fields   : %s' % cfg.specified_fields)
        # fields_to_include
        # formatter
        # converter
        log.info('tmpname            : %s' % cfg.tmpname)
        log.info('enable             : %d' % cfg.enable)
        log.info('remote_server      : %s' % cfg.remote_server)
        log.info('remote_port        : %r' % cfg.remote_port)
        log.info('remote_user        : %s' % cfg.remote_user)
        log.info('remote_dir         : %s' % cfg.remote_dir)
        log.info('compress           : %d' % cfg.compress)
        log.info('log_success        : %d' % cfg.log_success)
        log.info('ssh_options        : %s' % cfg.ssh_options)
        log.info('timeout            : %d' % cfg.timeout)
        log.info('skip_if_older_than : %d' % cfg.skip_if_older_than)
        log.info('time_delta         : %d' % cfg.time_delta)
        log.info('week_start         : %d' % cfg.week_start)
        log.info('rainyear_start     : %d' % cfg.rainyear_start)
        log.info('trend_obstypes     : %s' % cfg.trend_obstypes)
        log.info('rainyear_obstypes  : %s' % cfg.rainyear_obstypes)
        log.info('year_obstypes      : %s' % cfg.year_obstypes)
        log.info('month_obstypes     : %s' % cfg.month_obstypes)
        log.info('week_obstypes      : %s' % cfg.week_obstypes)
        log.info('day_obstypes       : %s' % cfg.day_obstypes)
        log.info('hour_obstypes      : %s' % cfg.hour_obstypes)
        log.info('ten_min_obstypes   : %s' % cfg.ten_min_obstypes)
        log.info('two_min_obstypes   : %s' % cfg.two_min_obstypes)
        log.info('day_24h_obstypes   : %s' % cfg.day_24h_obstypes)
        log.info('baro_trend_descs   : %s' % cfg.baro_trend_descs)

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
    def get_first_packet_with_obstype(cname: CheetahName, trend_packets: List[PeriodPacket]) -> Optional[Dict[str, Any]]:
        for trend_pkt in trend_packets:
            if cname.obstype in trend_pkt.packet:
                return trend_pkt.packet
        return None

    @staticmethod
    def get_last_packet_with_obstype(cname: CheetahName, trend_packets: List[PeriodPacket]) -> Optional[Dict[str, Any]]:
        for trend_pkt in reversed(trend_packets):
            if cname.obstype in trend_pkt.packet:
                return trend_pkt.packet
        return None

    @staticmethod
    def get_trend(cname: CheetahName, pkt: Dict[str, Any], trend_packets: List[PeriodPacket],
            converter) -> Tuple[Optional[Any], Optional[str], Optional[str]]:
        if len(trend_packets) != 0:
            start_packet = LoopProcessor.get_first_packet_with_obstype(cname, trend_packets)
            end_packet: Optional[Dict[str, Any]] = None
            if start_packet is None:
                return None, None, None
            if cname.obstype in pkt:
                end_packet = pkt
            else:
                end_packet = LoopProcessor.get_last_packet_with_obstype(cname, trend_packets)
            if end_packet is None:
                return None, None, None
            if start_packet['dateTime'] == end_packet['dateTime']:
                return None, None, None
            log.debug('get_trend: starting packet(%s): %s: %s' % (timestamp_to_string(start_packet['dateTime']), cname.field, start_packet[cname.obstype]))
            log.debug('get_trend: end      packet(%s): %s: %s' % (timestamp_to_string(end_packet['dateTime']), cname.field, end_packet[cname.obstype]))
            # Trend needs to be in report target units.
            start_value, unit_type, group_type = LoopProcessor.convert_current_obs(
                converter, cname.obstype, start_packet)
            log.debug('get_trend: %s: start_value: %s' % (cname.field, start_value))
            end_value, _, _ = LoopProcessor.convert_current_obs(
                converter, cname.obstype, end_packet)
            log.debug('get_trend: %s: end_value: %s' % (cname.field, end_value))
            try:
                if start_value is not None and end_value is not None:
                    log.debug('get_trend: %s: %s' % (cname.field, end_value - start_value))
                    return end_value - start_value, unit_type, group_type
            except:
                # Perhaps not a scalar value
                log.debug('Could not compute trend for %s' % cname.field)
        return None, None, None

    @staticmethod
    def save_period_packet(pkt_time: int, pkt: Dict[str, Any],
            period_packets: List[PeriodPacket], period_length: int,
            in_use_obstypes: List[str]) -> None:

        # Don't save the entire packet as only in_use_obstypes are needed.
        new_pkt = LoopProcessor.prune_period_packet(pkt_time, pkt, in_use_obstypes)

        period_packet: PeriodPacket = PeriodPacket(
            timestamp = pkt_time,
            packet = new_pkt)
        period_packets.append(period_packet)
        LoopProcessor.trim_old_period_packets(period_packets, period_length, pkt_time)

    @staticmethod
    def prune_period_packet(pkt_time: int, pkt: Dict[str, Any], in_use_obstypes: List[str]
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
    def trim_old_period_packets(period_packets: List[PeriodPacket],
            period_length: int, current_pkt_time) -> None:
        # Trim readings older than time_delta
        earliest: float = current_pkt_time - period_length
        del_count: int = 0
        for pkt in period_packets:
            if pkt.timestamp <= earliest:
                del_count += 1
        for i in range(del_count):
            log.debug('trim_old_periodpackets: Deleting expired packet(%s)' % timestamp_to_string(
                period_packets[0].timestamp))
            del period_packets[0]

    @staticmethod
    def get_windrun_bucket(wind_dir: float) -> int:
        bucket_count = len(windrun_bucket_suffixes)
        slice_size: float = 360.0 / bucket_count
        bucket: int = to_int((wind_dir + slice_size / 2.0) / slice_size)
        if bucket >= bucket_count:
            bucket = 0
        log.debug('get_windrun_bucket: wind_dir: %d, bucket: %d' % (wind_dir, bucket))
        return bucket
