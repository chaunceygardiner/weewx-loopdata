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
from typing import Any, Dict, List, Optional, Tuple
from enum import Enum

import weewx
import weewx.defaults
import weewx.manager
import weewx.units
import weeutil.config
import weeutil.logger
import weeutil.rsyncupload
import weeutil.weeutil


from weeutil.weeutil import timestamp_to_string
from weeutil.weeutil import to_bool
from weeutil.weeutil import to_int
from weewx.engine import StdService

# get a logger object
log = logging.getLogger(__name__)

LOOP_DATA_VERSION = '2.0.b14'

if sys.version_info[0] < 3 or (sys.version_info[0] == 3 and sys.version_info[1] < 7):
    raise weewx.UnsupportedFeature(
        "weewx-loopdata requires Python 3.7 or later, found %s.%s" % (sys.version_info[0], sys.version_info[1]))

if weewx.__version__ < "4":
    raise weewx.UnsupportedFeature(
        "WeeWX 4 is required, found %s" % weewx.__version__)

@dataclass
class CheetahName:
    field      : str           # $day.outTemp.avg.formatted
    prefix     : Optional[str] # unit or None
    prefix2    : Optional[str] # label or None
    period     : Optional[str] # obs, 10m, day, current, trend
    obstype    : str           # outTemp
    agg_type   : Optional[str] # avg, sum, etc. (required for day, else None)
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
    trend_obstypes     : List[str]
    ten_min_obstypes   : List[str]
    baro_trend_descs   : Any # Dict[BarometerTrend, str]

class BarometerTrend(Enum):
    RISING_VERY_RAPIDLY  = 1
    RISING_QUICKLY       = 2
    RISING               = 3
    RISING_SLOWLY        = 4
    STEADY               = 5
    FALLING_SLOWLY       = 6
    FALLING              = 7
    FALLING_QUICKLY      = 8
    FALLING_VERY_RAPIDLY = 9

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

        std_archive_dict      = config_dict.get('StdArchive', {})
        std_report_dict       = config_dict.get('StdReport', {})

        loop_config_dict      = config_dict.get('LoopData', {})
        file_spec_dict        = loop_config_dict.get('FileSpec', {})
        formatting_spec_dict  = loop_config_dict.get('Formatting', {})
        rsync_spec_dict       = loop_config_dict.get('RsyncSpec', {})
        include_spec_dict     = loop_config_dict.get('Include', {})
        baro_trend_trans_dict = loop_config_dict.get('BarometerTrendDescriptions', {})

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

        # Compose the directory in which to write the file.
        weewx_root: str = config_dict.get('WEEWX_ROOT')
        html_root: str = std_report_dict.get('HTML_ROOT')
        loop_dir: str = file_spec_dict.get('loop_data_dir', '.')
        loop_data_dir = os.path.join(weewx_root, html_root, loop_dir)

        # Get a temporay file in which to write data before renaming.
        tmp = tempfile.NamedTemporaryFile(prefix='LoopData', delete=False)
        tmp.close()

        # Get a target report dictionary we can use for converting units and formatting.
        target_report = formatting_spec_dict.get('target_report', 'SeasonsReport')
        try:
            target_report_dict = LoopData.get_target_report_dict(
                config_dict, target_report)
        except Exception as e:
            log.error('Could not find target_report: %s.  LoopData is exiting. Exception: %s' % (target_report, e))
            return

        # Get [possibly localized] strings for trend.barometer.desc
        baro_trend_descs = LoopData.construct_baro_trend_descs(baro_trend_trans_dict)

        # Process fields line of LoopData section.
        specified_fields = include_spec_dict.get('fields', [])
        fields_to_include, trend_obstypes, ten_min_obstypes = LoopData.get_fields_to_include(specified_fields)

        # Get the time span (number of seconds) to use for trend.
        try:
            time_delta: int = to_int(target_report_dict.get('Units').get('Trend').get('time_delta'))
        except:
            time_delta = 10800

        self.cfg: Configuration = Configuration(
            queue               = queue.SimpleQueue(),
            config_dict         = config_dict,
            unit_system         = unit_system,
            archive_interval    = to_int(std_archive_dict.get('archive_interval')),
            loop_data_dir       = loop_data_dir,
            filename            = file_spec_dict.get('filename', 'loop-data.txt'),
            target_report       = target_report,
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
            trend_obstypes      = trend_obstypes,
            ten_min_obstypes    = ten_min_obstypes,
            baro_trend_descs    = baro_trend_descs)

        if not os.path.exists(self.cfg.loop_data_dir):
            os.makedirs(self.cfg.loop_data_dir)

        log.info('LoopData file is: %s' % os.path.join(self.cfg.loop_data_dir, self.cfg.filename))

        self.bind(weewx.PRE_LOOP, self.pre_loop)
        self.bind(weewx.NEW_LOOP_PACKET, self.new_loop)

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
            ) -> Tuple[List[CheetahName], List[str], List[str]]:
        """
        Return fields_to_include, trend_obstypes, ten_min_obstypes
        """
        specified_fields = list(dict.fromkeys(specified_fields))
        fields_to_include: List[CheetahName] = []
        for field in specified_fields:
            cname: Optional[CheetahName] = LoopData.parse_cname(field)
            if cname is not None:
                fields_to_include.append(cname)
        trend_obstypes  : List[str] = LoopData.compute_period_obstypes(fields_to_include, 'trend')
        ten_min_obstypes: List[str] = LoopData.compute_period_obstypes(fields_to_include, '10m')
        return fields_to_include, trend_obstypes, ten_min_obstypes

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
        return list(dict.fromkeys(period_obstypes))

    @staticmethod
    def get_target_report_dict(config_dict, report) -> Dict[str, Any]:
        # This code is from WeeWX's ReportEngine. Copyright Tom Keffer
        # TODO: See if Tom will take a PR to make this available to extensions.
        # In the meaintime, it's probably safe to copy as the cofiguration files are public API.
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

            # Get archive packets to prime trend and 10m packets.
            # Fetch them just once with the greater time period.
            delta: int = max(600, self.cfg.time_delta)
            earliest_time: int = to_int(time.time() - delta)
            archive_pkts: List[Dict[str, Any]] = LoopData.get_archive_packets(
                dbm, self.archive_columns, earliest_time)
            trend_packets: List[PeriodPacket] = []
            ten_min_packets: List[PeriodPacket] = []
            for pkt in archive_pkts:
                # It's ok to add more than needed, they will fall off the end.
                LoopProcessor.save_period_packet(pkt['dateTime'], pkt, trend_packets, self.cfg.time_delta, self.cfg.trend_obstypes)
                LoopProcessor.save_period_packet(pkt['dateTime'], pkt, ten_min_packets, 600, self.cfg.ten_min_obstypes)

            # Init day accumulator from day_summary
            day_summary = dbm._get_day_summary(time.time())
            # Init an accumulator
            timespan = weeutil.weeutil.archiveDaySpan(time.time())
            unit_system = day_summary.unit_system
            if unit_system is not None:
                # Database has a unit_system already (true unless the db just got intialized.)
                self.cfg.unit_system = unit_system
            day_accum = weewx.accum.Accum(timespan, unit_system=self.cfg.unit_system)
            for k in day_summary:
                day_accum.set_stats(k, day_summary[k].getStatsTuple())

            lp: LoopProcessor = LoopProcessor(self.cfg, day_accum,
                trend_packets, ten_min_packets)
            t: threading.Thread = threading.Thread(target=lp.process_queue)
            t.setName('LoopData')
            t.setDaemon(True)
            t.start()
        except Exception as e:
            # Print problem to log and give up.
            log.error('Error in LoopData setup.  LoopData is exiting. Exception: %s' % e)
            weeutil.logger.log_traceback(log.error, "    ****  ")

    @staticmethod
    def get_archive_packets(dbm, archive_columns: List[str],
            earliest_time: int) -> List[Dict[str, Any]]:
        packets = []
        for cols in dbm.genSql('SELECT * FROM archive' \
                ' WHERE dateTime >= %d ORDER BY dateTime ASC' % earliest_time):
            pkt: Dict[str, Any] = {}
            for i in range(len(cols)):
                pkt[archive_columns[i]] = cols[i]
            packets.append(pkt)
            log.debug('get_archive_packets: pkt(%s): %s' % (
                timestamp_to_string(pkt['dateTime']), pkt))
        return packets

    def new_loop(self, event):
        log.debug('new_loop: event: %s' % event)
        self.cfg.queue.put(event)

    @staticmethod
    def parse_cname(field: str) -> Optional[CheetahName]:
        valid_prefixes    : List[str] = [ 'unit' ]
        valid_prefixes2   : List[str] = [ 'label' ]
        valid_periods     : List[str] = [ 'current', '10m', 'day', 'trend' ]
        valid_agg_types   : List[str] = [ 'max', 'min', 'maxtime', 'mintime', 'gustdir', 'avg', 'sum', 'vecavg', 'vecdir', 'rms' ]
        valid_format_specs: List[str] = [ 'formatted', 'raw', 'ordinal_compass', 'desc' ]

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
        # $10m/$day must have an agg_type
        if period == '10m' or period == 'day':
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
    def __init__(self, cfg: Configuration, day_accum,
                 trend_packets: List[PeriodPacket],
                 ten_min_packets: List[PeriodPacket]):
        self.cfg = cfg
        self.archive_start: float = time.time()
        self.day_accum = day_accum
        self.trend_packets: List[PeriodPacket] = trend_packets
        self.ten_min_packets: List[PeriodPacket] = ten_min_packets
        LoopProcessor.log_configuration(cfg)

    def process_queue(self) -> None:
        try:
            while True:
                event               = self.cfg.queue.get()
                pkt: Dict[str, Any] = event.packet
                pkt_time: int       = to_int(pkt['dateTime'])

                # This is a loop packet.
                assert event.event_type == weewx.NEW_LOOP_PACKET
                log.debug('Dequeued loop event(%s): %s' % (event, timestamp_to_string(pkt_time)))
                log.debug(pkt)

                # Process new packet.
                loopdata_pkt: Dict[str, Any] = LoopProcessor.generate_loopdata_dictionary(
                    pkt, pkt_time,
                    self.cfg.unit_system, self.cfg.converter, self.cfg.formatter,
                    self.cfg.fields_to_include,
                    self.day_accum, 
                    self.trend_packets, self.cfg.time_delta, self.cfg.trend_obstypes,
                    self.cfg.baro_trend_descs,
                    self.ten_min_packets, self.cfg.ten_min_obstypes)

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
            pkt: Dict[str, Any], pkt_time: int,
            unit_system: int, converter: weewx.units.Converter, formatter: weewx.units.Formatter,
            fields_to_include: List[CheetahName],
            day_accum: weewx.accum.Accum,
            trend_packets: List[PeriodPacket], time_delta: int, trend_obstypes: List[str],
            baro_trend_descs: Dict[BarometerTrend, str],
            ten_min_packets: List[PeriodPacket], ten_min_obstypes: List[str]) -> Dict[str, Any]:

        # Save needed data for trend.
        LoopProcessor.save_period_packet(pkt_time, pkt, trend_packets, time_delta, trend_obstypes)

        # Save needed data for 10m.
        LoopProcessor.save_period_packet(pkt_time, pkt, ten_min_packets, 600, ten_min_obstypes)

        # Add packet to day accumulator.
        try:
          day_accum.addRecord(pkt)
        except weewx.accum.OutOfSpan:
            timespan = weeutil.weeutil.archiveDaySpan(pkt['dateTime'])
            day_accum = weewx.accum.Accum(timespan, unit_system=unit_system)
            # Try again:
            day_accum.addRecord(pkt)

        # Create a 10m accumulator.
        ten_min_accum = LoopProcessor.create_ten_min_accum(
            ten_min_packets, ten_min_obstypes, unit_system)

        # Create the loopdata dictionary.
        return LoopProcessor.create_loopdata_packet(pkt,
            fields_to_include, trend_packets,
            day_accum, ten_min_accum, time_delta,
            baro_trend_descs, converter, formatter)

    @staticmethod
    def create_ten_min_accum(ten_min_packets: List[PeriodPacket],
            ten_min_obstypes: List[str], unit_system: int
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
                    ten_min_accum.addRecord(ten_min_packet.packet)
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
            loopdata_pkt: Dict[str, Any], time_delta: int, converter: weewx.units.Converter,
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

        if cname.obstype == 'barometer' and cname.format_spec == 'desc':
            baroTrend: BarometerTrend = LoopProcessor.get_barometer_trend(value, unit_type, group_type, time_delta)
            loopdata_pkt[cname.field] = baro_trend_descs[baroTrend]
            return
        elif cname.format_spec == 'desc':
            # desc is only supported for trend.barometer
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
    def create_loopdata_packet(pkt: Dict[str, Any], fields_to_include: List[CheetahName],
            trend_packets: List[PeriodPacket], day_accum: weewx.accum.Accum,
            ten_min_accum: Optional[weewx.accum.Accum], time_delta: int,
            baro_trend_descs: Dict[BarometerTrend, str],
            converter: weewx.units.Converter,
            formatter: weewx.units.Formatter) -> Dict[str, Any]:

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
            if cname.period == 'day':
                LoopProcessor.add_period_obstype(cname, day_accum, loopdata_pkt, time_delta, converter, formatter)
                continue
            if cname.period == '10m' and ten_min_accum is not None:
                LoopProcessor.add_period_obstype(cname, ten_min_accum, loopdata_pkt, time_delta, converter, formatter)
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
        log.info('archive_interval   : %d' % cfg.archive_interval)
        log.info('unit_system        : %d' % cfg.unit_system)
        log.info('loop_data_dir      : %s' % cfg.loop_data_dir)
        log.info('filename           : %s' % cfg.filename)
        log.info('target_report      : %s' % cfg.target_report)
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
        log.info('trend_obstypes     : %s' % cfg.trend_obstypes)
        log.info('ten_min_obstypes   : %s' % cfg.ten_min_obstypes)
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
        new_pkt: Dict[str, Any] = {}
        new_pkt['dateTime'] = pkt['dateTime']
        new_pkt['usUnits'] = pkt['usUnits']
        for obstype in in_use_obstypes:
            if obstype in pkt:
                new_pkt[obstype] = pkt[obstype]
        period_packet: PeriodPacket = PeriodPacket(
            timestamp = pkt_time,
            packet = new_pkt)
        period_packets.append(period_packet)
        log.debug('save_period_packet: PeriodPacket(%s): %s' % (
            timestamp_to_string(pkt_time), new_pkt))
        LoopProcessor.trim_old_period_packets(period_packets, period_length, pkt_time)

    @staticmethod
    def trim_old_period_packets(period_packets: List[PeriodPacket],
            period_length: int, current_pkt_time) -> None:
        # Trim readings older than time_delta
        earliest: float = current_pkt_time - period_length
        del_count: int = 0
        for pkt in period_packets:
            if pkt.timestamp < earliest:
                del_count += 1
        for i in range(del_count):
            log.debug('trim_old_periodpackets: Deleting expired packet(%s)' % timestamp_to_string(
                period_packets[0].timestamp))
            del period_packets[0]
