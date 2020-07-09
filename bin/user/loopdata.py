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
from typing import Any, Dict, List, Optional

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
from weeutil.weeutil import to_float
from weeutil.weeutil import to_int
from weewx.engine import StdService

# get a logger object
log = logging.getLogger(__name__)

LOOP_DATA_VERSION = '1.3.19'

if sys.version_info[0] < 3 or (sys.version_info[0] == 3 and sys.version_info[1] < 7):
    raise weewx.UnsupportedFeature(
        "weewx-loopdata requires Python 3.7 or later, found %s.%s" % (sys.version_info[0], sys.version_info[1]))

if weewx.__version__ < "4":
    raise weewx.UnsupportedFeature(
        "WeeWX 4 is required, found %s" % weewx.__version__)

# Note: These two observations are also included below.
COMPASS_OBSERVATIONS: List[str] = ['windDir', 'windGustDir']

@dataclass
class Configuration:
    queue              : queue.SimpleQueue
    config_dict        : Dict[str, Any]
    unit_system        : int
    archive_interval   : int
    loop_data_dir      : str
    filename           : str
    target_report      : str
    fields_to_include  : List[str]
    fields_to_rename   : Dict[str, str]
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
    barometer_rate_secs: int
    wind_rose_secs     : int
    wind_rose_points   : int

@dataclass
class WindroseReading:
    timestamp: int
    bucket   : int
    distance : float

@dataclass
class Reading:
    timestamp: int
    value    : Any

class LoopData(StdService):
    def __init__(self, engine, config_dict):
        super(LoopData, self).__init__(engine, config_dict)
        log.info("Service version is %s." % LOOP_DATA_VERSION)

        if sys.version_info[0] < 3:
            raise Exception("Python 3 is required for the loopdata plugin.")

        self.loop_proccessor_started = False

        std_archive_dict     = config_dict.get('StdArchive', {})
        std_report_dict      = config_dict.get('StdReport', {})

        loop_config_dict     = config_dict.get('LoopData', {})
        file_spec_dict       = loop_config_dict.get('FileSpec', {})
        formatting_spec_dict = loop_config_dict.get('Formatting', {})
        rsync_spec_dict      = loop_config_dict.get('RsyncSpec', {})
        include_spec_dict    = loop_config_dict.get('Include', {})
        fields_to_rename     = loop_config_dict.get('Rename', {})

        # Get the unit_system as specified by StdConvert->target_unit.
        # Note: this value will be overwritten if the day accumulator has a a unit_system.
        db_binder = weewx.manager.DBBinder(config_dict)
        default_binding = config_dict.get('StdReport')['data_binding']
        dbm = db_binder.get_manager(default_binding)
        unit_system = dbm.std_unit_system
        if unit_system is None:
            unit_system = weewx.units.unit_constants[self.config_dict['StdConvert'].get('target_unit', 'US').upper()]

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
        fields_to_include = include_spec_dict.get('fields', [])


        # Get converter from target report (if specified),
        # else Defaults (if specified),
        # else USUnits converter.
        try:
            group_unit_dict = target_report_dict['Units']['Groups']
        except KeyError:
            group_unit_dict = weewx.units.USUnits
        converter = weewx.units.Converter(group_unit_dict)

        self.cfg: Configuration = Configuration(
            queue               = queue.SimpleQueue(),
            config_dict         = config_dict,
            unit_system         = unit_system,
            archive_interval    = to_int(std_archive_dict.get('archive_interval')),
            loop_data_dir       = loop_data_dir,
            filename            = file_spec_dict.get('filename', 'loop-data.txt'),
            target_report       = target_report,
            fields_to_include   = fields_to_include,
            fields_to_rename    = fields_to_rename,
            formatter           = weewx.units.Formatter.fromSkinDict(target_report_dict),
            converter           = converter,
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
            barometer_rate_secs = 10800,
            wind_rose_secs      = 86400,
            wind_rose_points    = 16)

        if not os.path.exists(self.cfg.loop_data_dir):
            os.makedirs(self.cfg.loop_data_dir)

        log.info('LoopData file is: %s' % os.path.join(self.cfg.loop_data_dir, self.cfg.filename))

        self.bind(weewx.PRE_LOOP, self.pre_loop)
        self.bind(weewx.END_ARCHIVE_PERIOD, self.end_archive_period)
        self.bind(weewx.NEW_LOOP_PACKET, self.new_loop)

    @staticmethod
    def get_target_report_dict(config_dict, report) -> Dict[str, Any]:
        # This code is from WeeWX's ReportEngine. Copyright Tom Keffer
        # TODO: See if Tome will take a PR to make this available as a staticmethod.
        # In the meaintime, it's probably safe to copy as the cofiguration files are public API.
        try:
            skin_dict = weeutil.config.deep_copy(weewx.defaults.defaults)
        except Exception:
            # Fall back to copy.deepcopy for earlier weewx 4 installs.
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

            # Init barometer, windgust, windrose and day_accumulator from the database
            barometer_readings = self.fill_in_barometer_readings_at_startup(dbm)
            wind_gust_readings = self.fill_in_10m_wind_gust_readings_at_startup(dbm)
            wind_rose_readings = self.fill_in_wind_rose_readings_at_startup(dbm)

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

            lp: LoopProcessor = LoopProcessor(self.cfg, day_accum, barometer_readings,
                wind_gust_readings, wind_rose_readings)
            t: threading.Thread = threading.Thread(target=lp.process_queue)
            t.setName('LoopData')
            t.setDaemon(True)
            t.start()
        except Exception as e:
            # Print problem to log and give up.
            log.error('Error in LoopData setup.  LoopData is exiting. Exception: %s' % e)
            weeutil.logger.log_traceback(log.error, "    ****  ")

    def fill_in_barometer_readings_at_startup(self, dbm) -> List[Reading]:
        barometer_readings = []
        earliest: int = to_int(time.time() - self.cfg.barometer_rate_secs)
        for cols in dbm.genSql('SELECT dateTime, barometer FROM archive' \
                ' WHERE dateTime >= %d ORDER BY dateTime ASC' % earliest):
            if cols[1] is not None:
                reading: Reading = Reading(timestamp = cols[0], value = cols[1])
                barometer_readings.append(reading)
                log.debug('fill_in_barometer_readings_at_startup: Reading(%s): %f' % (
                    timestamp_to_string(reading.timestamp), reading.value))
        return barometer_readings

    def fill_in_10m_wind_gust_readings_at_startup(self, dbm) -> List[Reading]:
        wind_gust_readings = []
        earliest: int = to_int(time.time() - 600)
        for cols in dbm.genSql('SELECT dateTime, windGust FROM ' \
                'archive WHERE dateTime >= %d ORDER BY dateTime ASC' % earliest):
            reading: Reading = Reading(timestamp = cols[0], value = cols[1])
            if reading.value is not None:
                wind_gust_readings.append(reading)
                log.debug('fill_in_10m_wind_gust_readings_at_startup: Reading(%s): %f' % (
                          timestamp_to_string(reading.timestamp), reading.value))
        return wind_gust_readings

    def fill_in_wind_rose_readings_at_startup(self, dbm) -> List[WindroseReading]:
        wind_rose_readings = []
        earliest: int = to_int(time.time() - self.cfg.wind_rose_secs)
        for cols in dbm.genSql('SELECT dateTime, windSpeed, windDir FROM' \
                ' archive WHERE dateTime >= %d ORDER BY dateTime ASC' % earliest):
            dateTime  = cols[0]
            windSpeed = cols[1]
            windDir   = cols[2]
            if windSpeed is not None and windSpeed != 0 and windDir is not None:
                reading = WindroseReading(
                    timestamp = dateTime,
                    bucket    = LoopProcessor.get_wind_rose_bucket(self.cfg.wind_rose_points, windDir),
                    distance  = windSpeed / (3600.0 / self.cfg.archive_interval))
                wind_rose_readings.append(reading)
                log.debug('fill_in_wind_rose_readings_at_startup: Reading(%s): ' \
                         'bucket[%d]: distance: %f' % (
                         timestamp_to_string(reading.timestamp), reading.bucket, reading.distance))
        return wind_rose_readings

    def new_loop(self, event):
        log.debug('new_loop: event: %s' % event)
        self.cfg.queue.put(event)

    def end_archive_period(self, event):
        log.debug('end_archive_period: event: %s' % event)
        self.cfg.queue.put(event)

class LoopProcessor:
    def __init__(self, cfg: Configuration, day_accum, barometer_readings: List[Reading],
                 wind_gust_readings: List[Reading], wind_rose_readings: List[WindroseReading]):
        self.cfg = cfg
        self.archive_start: float = time.time()
        self.day_accum = day_accum
        self.arc_per_accum: Optional[weewx.accum.Accum] = None
        self.barometer_readings: List[Reading] = barometer_readings
        self.wind_gust_readings: List[Reading] = wind_gust_readings
        self.wind_rose_readings: List[WindroseReading] = wind_rose_readings
        LoopProcessor.log_configuration(cfg)

    def process_queue(self) -> None:
        try:
            while True:
                event               = self.cfg.queue.get()
                pkt: Dict[str, Any] = event.packet
                pkt_time: int       = to_int(pkt['dateTime'])

                if event.event_type == weewx.END_ARCHIVE_PERIOD:
                    # Archive records come through just to save off pressure as we need
                    # a 3 hour trend; and WindRose data for a 24 hour trend.  This
                    # is done with archive records rather than loop records.
                    self.save_barometer_reading(pkt_time, pkt)
                    self.save_wind_rose_data(pkt_time, pkt)
                    continue

                # This is a loop packet.
                assert event.event_type == weewx.NEW_LOOP_PACKET
                log.debug('Dequeued loop event(%s): %s' % (event, timestamp_to_string(pkt_time)))

                  # Process new packet.
                log.debug(pkt)

                try:
                  self.day_accum.addRecord(pkt)
                except weewx.accum.OutOfSpan:
                    timespan = weeutil.weeutil.archiveDaySpan(pkt['dateTime'])
                    self.day_accum = weewx.accum.Accum(timespan, unit_system=self.cfg.unit_system)
                    # Try again:
                    self.day_accum.addRecord(pkt)

                if self.arc_per_accum is None:
                    self.arc_per_accum = LoopProcessor.create_arc_per_accum(pkt_time,
                        self.cfg.archive_interval, self.cfg.unit_system)

                try:
                  self.arc_per_accum.addRecord(pkt)
                except weewx.accum.OutOfSpan:
                    log.debug('Creating new arc_per_accum')
                    self.arc_per_accum = LoopProcessor.create_arc_per_accum(pkt_time,
                        self.cfg.archive_interval, self.cfg.unit_system)
                    # Try again:
                    self.arc_per_accum.addRecord(pkt)

                # Keep 10 minutes of wind gust readings.
                # If windGust unavailable, use windSpeed.
                try:
                    self.save_wind_gust_reading(pkt_time, to_float(pkt['windGust']))
                except KeyError:
                    try:
                        self.save_wind_gust_reading(pkt_time, to_float(pkt['windSpeed']))
                    except KeyError:
                        log.debug("No windGust nor windSpeed.  Can't save a windGust reading.")

                loopdata_pkt = LoopProcessor.create_loopdata_packet(pkt,
                    self.day_accum, self.cfg.converter, self.cfg.formatter)

                # Add barometerRate
                LoopProcessor.insert_barometer_rate(self.barometer_readings,
                    loopdata_pkt)
                LoopProcessor.convert_units(self.cfg.converter,
                    self.cfg.formatter, loopdata_pkt, "barometerRate",
                    do_hi_lo_etc=False)

                # Add 10mMaxGust
                LoopProcessor.insert_10m_max_windgust(self.wind_gust_readings,
                    loopdata_pkt)
                LoopProcessor.convert_10m_max_windgust(loopdata_pkt,
                    self.cfg.converter, self.cfg.formatter)

                # Add windRose
                LoopProcessor.insert_wind_rose(self.wind_rose_readings,
                    self.cfg.wind_rose_points, loopdata_pkt)
                LoopProcessor.convert_wind_rose(loopdata_pkt,
                    self.cfg.converter, self.cfg.formatter)

                selective_pkt: Dict[str, Any] = LoopProcessor.compose_selective_packet(
                    loopdata_pkt, self.cfg.fields_to_include, self.cfg.fields_to_rename)
                LoopProcessor.write_packet_to_file(selective_pkt,
                    self.cfg.tmpname, self.cfg.loop_data_dir, self.cfg.filename)
                if self.cfg.enable:
                    LoopProcessor.rsync_data(selective_pkt['dateTime'],
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
    def create_loopdata_packet(pkt: Dict[str, Any], day_accum: weewx.accum.Accum,
            converter: weewx.units.Converter, formatter: weewx.units.Formatter) -> Dict[str, Any]:

        # Iterate through all scalar stats and add mins, maxes, sums, averages,
        # and weighted averages to the record.

        loopdata_pkt = copy.deepcopy(pkt)

        loopdata_pkt['FMT_dateTime'] = formatter.toString(
            (loopdata_pkt['dateTime'], 'unix_epoch', 'group_time'))

        for obstype in day_accum:
            accum = day_accum[obstype]
            if isinstance(accum, weewx.accum.ScalarStats) and accum.lasttime is not None:
                min, mintime, max, maxtime, sum, count, wsum, sumtime = accum.getStatsTuple()
                loopdata_pkt['T_LO_%s' % obstype] = mintime
                loopdata_pkt['LO_%s' % obstype] = min
                loopdata_pkt['T_HI_%s' % obstype] = maxtime
                loopdata_pkt['HI_%s' % obstype] = max
                loopdata_pkt['SUM_%s' % obstype] = sum
                if count != 0:
                    loopdata_pkt['AVG_%s' % obstype] = sum / count
                if sumtime != 0:
                    loopdata_pkt['WAVG_%s' % obstype] = wsum / sumtime
                LoopProcessor.convert_units(converter, formatter, loopdata_pkt, obstype)
            elif isinstance(accum, weewx.accum.VecStats) and accum.count != 0:
                min, mintime, max, maxtime, _, _, _, _, max_dir, _, _, _, _, _ = accum.getStatsTuple()
                loopdata_pkt['HI_%s' % obstype] = accum.max
                loopdata_pkt['HI_DIR_%s' % obstype] = accum.max_dir
                loopdata_pkt['T_HI_%s' % obstype] = accum.maxtime
                loopdata_pkt['LO_%s' % obstype] = accum.min
                loopdata_pkt['T_LO_%s' % obstype] = accum.mintime
                loopdata_pkt['AVG_%s' % obstype] = accum.avg
                loopdata_pkt['RMS_%s' % obstype] = accum.rms
                loopdata_pkt['VEC_AVG_%s' % obstype] = accum.vec_avg
                loopdata_pkt['VEC_DIR_%s' % obstype] = accum.vec_dir
                LoopProcessor.convert_vector_units(converter, formatter, loopdata_pkt, obstype)
            elif isinstance(accum, weewx.accum.FirstLastAccum) and accum.first is not None:
                first, firsttime, last, lasttime = accum.getStatsTuple()
                loopdata_pkt['T_FIRST_%s' % obstype] = firsttime
                loopdata_pkt['FIRST_%s' % obstype] = first
                loopdata_pkt['T_LAST_%s' % obstype] = lasttime
                loopdata_pkt['LAST_%s' % obstype] = last
                # No formatting and conversions for FirstLastAccum

        return loopdata_pkt

    @staticmethod
    def compose_selective_packet(loopdata_pkt: Dict[str, Any],
            fields_to_include: List[str],
            fields_to_rename: Dict[str, str]) -> Dict[str, Any]:

        selective_pkt: Dict[str, Any] = {}

        if len(fields_to_include) == 0 and len(fields_to_rename) == 0:
            selective_pkt = copy.copy(loopdata_pkt)
        else:
            if len(fields_to_include) != 0:
                for obstype in fields_to_include:
                    if obstype in loopdata_pkt:
                        selective_pkt[obstype] = loopdata_pkt[obstype]
            if len(fields_to_rename) != 0:
                for obstype in fields_to_rename:
                    if obstype in loopdata_pkt:
                        selective_pkt[fields_to_rename[obstype]] = loopdata_pkt[obstype]
        return selective_pkt

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
    def create_arc_per_accum(ts: int, arcint: int, unit_system: int) -> weewx.accum.Accum:
        start_ts = weeutil.weeutil.startOfInterval(ts, arcint)
        end_ts = start_ts + arcint
        return weewx.accum.Accum(weeutil.weeutil.TimeSpan(start_ts, end_ts), unit_system)

    @staticmethod
    def log_configuration(cfg: Configuration):
        # queue
        # config_dict
        log.info('archive_interval   : %d' % cfg.archive_interval)
        log.info('unit_system        : %d' % cfg.unit_system)
        log.info('loop_data_dir      : %s' % cfg.loop_data_dir)
        log.info('filename           : %s' % cfg.filename)
        log.info('target_report      : %s' % cfg.target_report)
        log.info('fields_to_include  : %s' % cfg.fields_to_include)
        log.info('fields_to_rename   : %s' % cfg.fields_to_rename)
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
        log.info('barometer_rate_secs: %d' % cfg.barometer_rate_secs)
        log.info('wind_rose_secs     : %d' % cfg.wind_rose_secs)
        log.info('wind_rose_points   : %d' % cfg.wind_rose_points)

    @staticmethod
    def get_wind_rose_bucket(wind_rose_points: int, wind_dir: float) -> int:
        slice_size: float = 360.0 / wind_rose_points
        bucket: int = to_int((wind_dir + slice_size / 2.0) / slice_size)

        bkt =  bucket if bucket < wind_rose_points else 0
        log.debug('get_wind_rose_bucket: wind_dir: %d, bucket: %d' % (wind_dir, bkt))

        return bucket if bucket < wind_rose_points else 0

    def save_wind_rose_data(self, pkt_time: int, pkt: Dict[str, Any]) -> None:
        # Example: 3.1 mph, 202 degrees
        # archive_interval:  300 seconds
        # intervals in an hour: 3600 / 300 (12)
        # distance = 3.1 / 12 = 0.258333 miles
        if 'windSpeed' in pkt and 'windDir' in pkt and pkt['windSpeed'] is not None and pkt['windDir'] is not None:
            wind_speed = to_float(pkt['windSpeed'])
            wind_dir   = to_float(pkt['windDir'])
        else:
            log.debug('save_wind_rose_data: windSpeed and/or windDir not in archive packet, nothing to save for wind rose.')
            return

        if wind_speed is not None and wind_speed != 0 and wind_dir is not None:
            log.debug('pkt_time: %d, bucket: %d, distance: %f' % (pkt_time,
                LoopProcessor.get_wind_rose_bucket(self.cfg.wind_rose_points, wind_dir), wind_speed / (
                3600.0 / self.cfg.archive_interval)))
            self.wind_rose_readings.append(WindroseReading(
                timestamp = pkt_time,
                bucket    = LoopProcessor.get_wind_rose_bucket(self.cfg.wind_rose_points, wind_dir),
                distance  = wind_speed / (3600.0 / self.cfg.archive_interval)))

        self.delete_old_windrose_data()

    def delete_old_windrose_data(self) -> None:
        # Delete old WindRose data
        cutoff_age: float = time.time() - self.cfg.wind_rose_secs
        del_count = 0
        for r in self.wind_rose_readings:
            if r.timestamp < cutoff_age:
                del_count += 1
        for _ in range(del_count):
            del self.wind_rose_readings[0]

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
    def convert_hi_lo_etc_units(converter: weewx.units.Converter, formatter: weewx.units.Formatter,
            pkt: Dict[str, Any], obstype, original_unit_type, original_unit_group, target_unit_type,
            target_unit_group) -> None:
        # convert high and low
        if 'HI_%s' % obstype not in pkt:
            log.info('pkt[HI_%s] is missing.' % obstype)
        else:
            hi, _, _ = converter.convert(
                (pkt['HI_%s' % obstype], original_unit_type, original_unit_group))
            pkt['HI_%s' % obstype] = formatter.get_format_string(target_unit_type) % hi
            pkt['FMT_HI_%s' % obstype] = formatter.toString(
                (hi, target_unit_type, target_unit_group))
            if 'T_HI_%s' % obstype in pkt:
                pkt['FMT_T_HI_%s' % obstype] = formatter.toString(
                    (pkt['T_HI_%s' % obstype], 'unix_epoch', 'group_time'))
        if 'LO_%s' % obstype not in pkt:
            log.info('pkt[LO_%s] is missing.' % obstype)
        else:
            lo, _, _ = converter.convert(
                (pkt['LO_%s' % obstype], original_unit_type, original_unit_group))
            pkt['LO_%s' % obstype] = formatter.get_format_string(target_unit_type) % lo
            pkt['FMT_LO_%s' % obstype] = formatter.toString(
                (lo, target_unit_type, target_unit_group))
            if 'T_LO_%s' % obstype in pkt:
                pkt['FMT_T_LO_%s' % obstype] = formatter.toString(
                    (pkt['T_LO_%s' % obstype], 'unix_epoch', 'group_time'))
        if 'SUM_%s' % obstype not in pkt:
            log.info('pkt[SUM_%s] is missing.' % obstype)
        else:
            sum, _, _ = converter.convert(
                (pkt['SUM_%s' % obstype], original_unit_type, original_unit_group))
            pkt['SUM_%s' % obstype] = formatter.get_format_string(target_unit_type) % sum
            if target_unit_type != 'unix_epoch':
                try:
                    pkt['FMT_SUM_%s' % obstype] = formatter.toString(
                        (sum, target_unit_type, target_unit_group))
                except Exception as e:
                    log.error('Could not format sum for obstype: %s, target_unit_type: %s, target_unit_group: %s, exception: %s' % (
                        obstype, target_unit_type, target_unit_group, e))
        if 'AVG_%s' % obstype not in pkt:
            log.info('pkt[AVG_%s] is missing.' % obstype)
        else:
            avg, _, _ = converter.convert(
                (pkt['AVG_%s' % obstype], original_unit_type, original_unit_group))
            pkt['AVG_%s' % obstype] = formatter.get_format_string(target_unit_type) % avg
            pkt['FMT_AVG_%s' % obstype] = formatter.toString(
                (avg, target_unit_type, target_unit_group))
        if 'WAVG_%s' % obstype not in pkt:
            log.info('pkt[WAVG_%s] is missing.' % obstype)
        else:
            wavg, _, _ = converter.convert(
                (pkt['WAVG_%s' % obstype], original_unit_type, original_unit_group))
            pkt['WAVG_%s' % obstype] = formatter.get_format_string(target_unit_type) % wavg
            pkt['FMT_WAVG_%s' % obstype] = formatter.toString(
                (wavg, target_unit_type, target_unit_group))

    @staticmethod
    def convert_10m_max_windgust(loopdata_pkt: Dict[str, Any],
            converter: weewx.units.Converter,
            formatter: weewx.units.Formatter) -> None:
        obstype = '10mMaxGust'
        # get windGust value tuple as it will give us the unit type and unit group
        v_t = weewx.units.as_value_tuple(loopdata_pkt, 'windGust')
        wind_gust_v_t = weewx.units.ValueTuple(loopdata_pkt[obstype], v_t.unit, v_t.group)
        value, unit_type, unit_group = converter.convert(wind_gust_v_t)
        loopdata_pkt[obstype] = formatter.get_format_string(unit_type) % value
        loopdata_pkt['UNITS_%s' % obstype] = unit_type
        loopdata_pkt['FMT_%s' % obstype] = formatter.toString((value, unit_type, unit_group))
        loopdata_pkt['LABEL_%s' % obstype] = formatter.get_label_string(unit_type)

    @staticmethod
    def convert_wind_rose(loopdata_pkt: Dict[str, Any],
            converter: weewx.units.Converter,
            formatter: weewx.units.Formatter) -> None:
        obstype = 'windRose'
        # The windrun observation type will yield the units we need.
        std_unit_system = loopdata_pkt['usUnits']
        (unit_type, unit_group) = weewx.units.StdUnitConverters[
            std_unit_system].getTargetUnit('windrun')
        distances: List[float] = loopdata_pkt[obstype]
        converted_distances: List[float] = []
        for i in range(len(distances)):
            distance_v_t = weewx.units.ValueTuple(
                distances[i], unit_type, unit_group)
            value, unit_type, unit_group = converter.convert(distance_v_t)
            fmt_value = formatter.get_format_string(unit_type) % value
            converted_distances.append(fmt_value)
        log.debug('distances: %s' % distances)
        loopdata_pkt[obstype] = converted_distances
        loopdata_pkt['UNITS_%s' % obstype] = unit_type
        loopdata_pkt['LABEL_%s' % obstype] = formatter.get_label_string(unit_type)

    @staticmethod
    def convert_units(converter: weewx.units.Converter, formatter: weewx.units.Formatter,
            pkt: Dict[str, Any], obstype: str, do_hi_lo_etc = True) -> None:
        v_t = weewx.units.as_value_tuple(pkt, obstype)
        _, original_unit_type, original_unit_group = v_t
        value, unit_type, unit_group = converter.convert(v_t)
        # windDir and gustDir could be None.
        if value is not None:
            pkt[obstype] = formatter.get_format_string(unit_type) % value
            pkt['FMT_%s' % obstype] = formatter.toString((
                value, unit_type, unit_group))
        else:
            pkt[obstype] = None
            pkt['FMT_%s' % obstype] = None
        pkt['UNITS_%s' % obstype] = unit_type
        pkt['LABEL_%s' % obstype] = formatter.get_label_string(unit_type)
        if obstype in COMPASS_OBSERVATIONS:
            pkt['COMPASS_%s' % obstype] = formatter.to_ordinal_compass(
                (value, unit_type, unit_group))
        if do_hi_lo_etc:
            LoopProcessor.convert_hi_lo_etc_units(converter, formatter, pkt, obstype,
                original_unit_type, original_unit_group, unit_type, unit_group)

    @staticmethod
    def convert_vector_units(converter: weewx.units.Converter, formatter: weewx.units.Formatter,
            pkt: Dict[str, Any], obstype: str) -> None:
        """ Special conversion and formatting for vector stats (aka, wind). """
        kv = {
            'T_HI_%s'    % obstype: 'maxtime',
            'HI_%s'      % obstype: None,
            'HI_DIR_%s'  % obstype: '%sDir' % obstype,
            'T_LO_%s'    % obstype: 'mintime',
            'LO_%s'      % obstype: None,
            'AVG_%s'     % obstype: 'avg',
            'RMS_%s'     % obstype: 'rms',
            'VEC_AVG_%s' % obstype: 'vecavg',
            'VEC_DIR_%s' % obstype: 'vecdir'}

        for key in kv:
            try:
                value = pkt[key]
            except KeyError:
                log.info('pkt[%s] does not exist' % key)
                continue
            std_unit_system = pkt['usUnits']
            if key.startswith('HI_DIR_'):
                obstype_for_converter = '%sDir' % obstype
            else:
                obstype_for_converter = obstype
            (orig_unit_type, orig_unit_group) = weewx.units.StdUnitConverters[
                std_unit_system].getTargetUnit(obstype_for_converter, agg_type=kv[key])

            value, unit_type, unit_group = converter.convert((value, orig_unit_type, orig_unit_group))

            if value is not None:
                pkt[key] = formatter.get_format_string(unit_type) % value
                pkt['FMT_%s' % key] = formatter.toString(
                    ((value, unit_type, unit_group)))
            else:
                pkt[key] = None
                pkt['FMT_%s' % key] = None

            pkt['UNITS_%s' % key] = unit_type
            pkt['LABEL_%s' % key] = formatter.get_label_string(unit_type)

    @staticmethod
    def insert_barometer_rate_desc(loopdata_pkt: Dict[str, Any]) -> None:
        # Shipping forecast descriptions for the 3 hour change in baromter readings.
        # Falling (or rising) slowly: 0.1 - 1.5mb in 3 hours
        # Falling (or rising): 1.6 - 3.5mb in 3 hours
        # Falling (or rising) quickly: 3.6 - 6.0mb in 3 hours
        # Falling (or rising) very rapidly: More than 6.0mb in 3 hours

        # Get delta in mbars as that is the standard we have for descriptions.
        v_t = weewx.units.as_value_tuple(loopdata_pkt, 'barometerRate')
        converter = weewx.units.Converter(weewx.units.MetricUnits)
        delta_mbar, _, _ = converter.convert(v_t)
        log.debug('Converted to mbar/h: %f' % delta_mbar)
        desc: str = ""
        # Table above is for 3 hours.  The rate is over a 3 hour period, but expressed per hour;
        # thus, multiply by 3.0.
        delta_mbar = delta_mbar * 3.0

        if delta_mbar > 6.0:
            desc = 'Rising Very Rapidly'
        elif delta_mbar > 3.5:
            desc = 'Rising Quickly'
        elif delta_mbar > 1.5:
            desc = 'Rising'
        elif delta_mbar > 0.1:
            desc = 'Rising Slowly'
        elif delta_mbar >= -0.1:
            desc = 'Steady'
        elif delta_mbar >= -1.5:
            desc = 'Falling Slowly'
        elif delta_mbar >= -3.5:
            desc = 'Falling'
        elif delta_mbar >= -6.0:
            desc = 'Falling Quickly'
        else:
            desc = 'Falling Very Rapidly'

        loopdata_pkt['DESC_barometerRate'] = desc

    @staticmethod
    def insert_barometer_rate(barometer_readings: List[Reading],
            loopdata_pkt: Dict[str, Any]) -> None:
        if len(barometer_readings) != 0:
            # The saved readings are used to get the starting point,
            # but the current loopdata_pkt is used for the last barometer reading.
            delta3H = to_float(loopdata_pkt['barometer']) - barometer_readings[0].value
            # Report rate per hour
            delta = delta3H / 3.0
            loopdata_pkt['barometerRate'] = delta
            log.debug('insert_barometer_rate: %s' % delta)
            LoopProcessor.insert_barometer_rate_desc(loopdata_pkt)

    def save_barometer_reading(self, pkt_time: int, pkt: Dict[str, Any]) -> None:
        value = None
        if 'barometer' in pkt and pkt['barometer'] is not None:
            value = to_float(pkt['barometer'])
        else:
            # No barometer in archive record, use the archival period accumulator instead
            if self.arc_per_accum is not None:
                log.debug('barometer not in archive pkt, using arc_per_accum')
                _, _, _, _, sum, count, _, _ = self.arc_per_accum['barometer'].getStatsTuple()
                if count > 0:
                    value = sum / count

        if value is not None:
            reading: Reading = Reading(
                timestamp = pkt_time,
                value = value)
            self.barometer_readings.append(reading)
            log.debug('save_barometer_reading: Reading(%s): %f' % (
                timestamp_to_string(reading.timestamp), reading.value))
        else:
            log.debug('save_barometer_reading: Reading(%s): None' % timestamp_to_string(pkt_time))

        self.trim_old_barometer_readings()

    def trim_old_barometer_readings(self) -> None:
        # Trim readings older than barometer_rate_secs
        earliest: float = time.time() - self.cfg.barometer_rate_secs
        del_count: int = 0
        for reading in self.barometer_readings:
            if reading.timestamp < earliest:
                del_count += 1
        for i in range(del_count):
            log.debug('save_barometer_reading: Deleting old reading(%s)' % timestamp_to_string(
                self.barometer_readings[0].timestamp))
            del self.barometer_readings[0]

    @staticmethod
    def insert_10m_max_windgust(wind_gust_readings: List[Reading],
            loopdata_pkt: Dict[str, Any]) -> None:
        max_gust     : float = 0.0
        max_gust_time: int = 0
        for reading in wind_gust_readings:
            if reading.value > max_gust:
                max_gust = reading.value
                max_gust_time = reading.timestamp
        loopdata_pkt['10mMaxGust'] = max_gust
        loopdata_pkt['T_10mMaxGust'] = max_gust_time
        log.debug('insert_10m_max_windgust: ts: %d, gust: %f' % (
                  loopdata_pkt['T_10mMaxGust'], loopdata_pkt['10mMaxGust']))

    @staticmethod
    def insert_wind_rose(wind_rose_readings: List[WindroseReading],
            wind_rose_points: int, loopdata_pkt: Dict[str, Any]) -> None:
        buckets: List[float] = [0.0]*wind_rose_points
        for r in wind_rose_readings:
            buckets[r.bucket] += r.distance
        loopdata_pkt['windRose'] = buckets

    def save_wind_gust_reading(self, pkt_time: int, value: float) -> None:
        if value is not None:
            self.wind_gust_readings.append(Reading(timestamp = pkt_time, value = value))
        # Trim anything older than 10 minutes.
        earliest: float = time.time() - 600
        del_count: int = 0
        for reading in self.wind_gust_readings:
            if reading.timestamp < earliest:
                del_count += 1
        for i in range(del_count):
            log.debug('save_wind_gust_reading: Deleting old reading(%s)' % timestamp_to_string(
                self.wind_gust_readings[0].timestamp))
            del self.wind_gust_readings[0]
