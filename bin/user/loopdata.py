"""
loopdata.py

A WeeWX service to generate a json file (typically, loop-data.txt)
containing observations from loop packets as they are generated in
weewx.

Copyright (C)2020 by John A Kline (john@johnkline.com)

Inspired by https://github.com/gjr80/weewx-realtime_gauge-data.  This does not attempt to duplicate
Gary's realtime gauge data plugin for the SteelSeries gauges.  To power Steel Series gauges from
WeeWX, you definitely want to use weewx-realtime_gauge_data.

Installation Instructions
1. Install bin/user/loopdata.py in /home/weewx/bin/user.
2. Add user.loopdata.LoopData to report_servcices in weewx.con.

[LoopData]
    [[FileSpec]]
        loop_data_dir = /home/weewx/gauge-data
        filename = loop-data.txt
    [[Formatting]]
        target_report = LiveSeasonsReport
    [[RsyncSpec]]
        #remote_server = foo.bar.com
        #remote_user = root
        #remote_dir = /home/weewx/gauge-data
        #compress = False
        #log_success = False
        #ssh_options = "-o ConnectTimeout=1"
        #timeout = 1
        #skip_if_older_than = 3
    [[Include]]
        fields = dateTime, windSpeed, COMPASS_windDir, DESC_barometerRate, FMT_barometer, FMT_day_rain_total, FMT_dewpoint, FMT_heatindex, FMT_outHumidity, FMT_outTemp, FMT_rain, FMT_rainRate, FMT_windchill, FMT_windSpeed, FMT_HI_windGust, FMT_10mMaxGust
    [[Rename]]
        windRose = WindRose

 Fill out the following fields:
   loop_data_dir     : The directory into which the loop data file should be written.
   filename          : The name of the loop data file to write.
   target_report     : The WeeWX report to target.  LoopData will use this report to determine the
                       units to use and the formatting to apply.
   include           : Used to specify which fields to include in the file.  If include is missing
                       and rename (see below) is also missing, all fields are included.
   rename            : Used to specify which fields to include and which names should be used as
                       keys (i.e., what these fields should be renamed.  If rename is missing and
                       include (see above).
                       is also missing, all fields are included.
   remote_server     : The server to which gauge-data.txt will be copied.
                       To use rsync to sync loop-data.txt to a remote computer, passwordless ssh
                       using public/private key must be configured for authentication from the user
                       account that weewx runs under on this computer to the user account on the
                       remote machine with write access to the destination directory (remote_dir).
   remote_user       : The userid on remote_server with write permission to remote_server_dir.
   remote_directory  : The directory on remote_server where filename will be copied.
   compress          : True to compress the file before sending.  Default is False.
   log_success       : True to write success with timing messages to the log (for debugging).
                       Default is False.
   ssh_options       : ssh options Default is '-o ConnectTimeout=1' (When connecting, time out in
                       1 second.)
   timeout           : I/O timeout. Default is 1.  (When sending, timeout in 1 second.)
   skip_if_older_than: Don't bother to rsync if greater than this number of seconds.  Default is 4.
                       (Skip this and move on to the next if this data is older than 4 seconds.

   List of all fields: dateTime          : The time of this loop packet (seconds since the epoch).
                       usUnits           : The units system all obeservations are expressed in.
                                           This will be the unit system of the report specified by
                                           target_report in weewx.conf.
                       All observations available in WeeWX's day_summary.
                       Currently:          outTemp
                                           inTemp
                                           outHumidity
                                           pressure
                                           windSpeed
                                           windDir
                                           windGust
                                           windGustDir
                                           day_rain_total
                                           rain
                                           altimeter
                                           appTemp
                                           barometer
                                           beaufort
                                           cloudbase
                                           dewpoint
                                           heatindex
                                           humidex
                                           maxSolarRad
                                           rainRate
                                           windchill
                       FMT_<obs>         : The observation expressed as a formatted value, including
                                           the units (e.g., '4.8 mph').
                       LABEL_<obs>       : The label for the units associted with the observation (e.g., 'mph').
                                           This label also applies to the high and low fields for this observation.
                       UNITS_<obs>       : The units that the observation is expressed in.  Also the units
                                           for the corresponding HI and LO entries.  Example: 'mile_per_hour'.
                       LO_<obs>          : The minimum value of the observation today.
                       FMT_LO_<obs>      : The low observation expressed as a formatted value, including
                                           the units (e.g., '4.8 mph').
                       T_LO<obs>         : The time of the daily minimum observation.
                       HI<obs>           : The maximum value of the observation today.
                       FMT_HI_<obs>      : The high observation expressed as a formatted value, including
                                           the units (e.g., '4.8 mph').
                       T_HI<obs>         : The time of the daily maximum observation.
                       COMPASS_<obs>     : For windDir and windGustDir, text expression for the direction
                                           (.e., 'NNE').
                       10mMaxGust        : The maximum wind gust in the last 10m.
                       T_10mMaxGust      : The time of the max gust (seconds since the epoch).
                       FMT_10mMaxGust    : 10mMaxGust expressed as a formatted value ('8.6 mph').
                       LABEL_10mMaxGust  : The label of the units for 10mMaxGust (e.g., 'mph').
                       UNITS_10mMaxGust  : The units that 10mMaxGust is expressed in (e.g., 'mile_per_hour').
                       barometerRate     : The difference in barometer in the last 3 hours
                                           (i.e., barometer_3_hours_ago - barometer_now)
                       DESC_barometerRate: Shipping forecast descriptions for the 3 hour change in
                                           barometer readings (e.g., "Falling Slowly').
                       FMT_barometerRate:  Formatted baromter rate (e.g., '0.2 inHg/h').
                       UNITS_barometerRate:The units used in baromter rate (e.g., 'inHg_per_hour').
                                           barometer readings (e.g., "Falling Slowly').
                       LABEL_barometerRate:The label used for baromter rate units (e.g., 'inHg/hr').
                       windRose          : An array of 16 directions (N,NNE,NE,ENE,E,ESE,SE,SSE,S,SSW,SW,
                                           WSW,W,WNW,NW,NNW) containing the distance traveled in each 
                                           direction.)
                       LABEL_windRose    : The label of the units for windRose (e.g., 'm')
                       UNITS_windRose    : The units that windrose values are expressed in (e.g., 'mile').
"""

import copy
import datetime
import json
import logging
import os
import queue
import shutil
import sys
import tempfile
import threading
import time

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import weewx
import weewx.manager
import weewx.units
import weeutil.logger
import weeutil.rsyncupload
import weeutil.weeutil


from weeutil.weeutil import timestamp_to_string
from weeutil.weeutil import to_float
from weeutil.weeutil import to_int
from weewx.engine import StdService
from weewx.units import ValueTuple

# get a logger object
log = logging.getLogger(__name__)

REALTIME_DATA_VERSION = '1.0'

# Note: These two observations are also included below.
COMPASS_OBSERVATIONS: List[str] = ['windDir', 'windGustDir']

@dataclass
class Configuration:
    queue              : queue.SimpleQueue
    config_dict        : Dict[str, Any]
    archive_interval   : int
    loop_data_dir      : str
    filename           : str
    target_report      : str
    formatter          : weewx.units.Formatter
    converter          : weewx.units.Converter
    tmpname            : str
    skip_if_older_than : int
    remote_server      : str
    remote_port        : int
    remote_user        : str
    remote_dir         : str
    compress           : bool
    log_success        : bool
    ssh_options        : str
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
        log.info("Service version is %s." % REALTIME_DATA_VERSION)

        if sys.version_info[0] < 3:
            raise Exception("Python 3 is required for the loopdata plugin.")

        loop_config_dict     = config_dict.get('LoopData', {})
        file_spec_dict       = loop_config_dict.get('FileSpec', {})
        formatting_spec_dict = loop_config_dict.get('Formatting', {})
        rsync_spec_dict      = loop_config_dict.get('RsyncSpec', {})
        include_spec_dict    = loop_config_dict.get('Include', {})
        rename_spec_dict     = loop_config_dict.get('Rename', {})

        tmp = tempfile.NamedTemporaryFile(prefix='LoopData', delete=False)
        tmp.close()

        target_report = formatting_spec_dict.get('target_report', 'LiveSeasonsReport')
        target_report_dict = config_dict.get('StdReport').get(target_report)

        self.cfg: Configuration = Configuration(
            queue               = queue.SimpleQueue(),
            config_dict         = config_dict,
            archive_interval    = to_int(config_dict.get('StdArchive').get('archive_interval')),
            loop_data_dir       = LoopData.compute_loop_data_dir(config_dict),
            filename            = file_spec_dict.get('filename', 'loop-data.txt'),
            target_report       = target_report,
            formatter           = weewx.units.Formatter.fromSkinDict( target_report_dict),
            converter           = weewx.units.Converter.fromSkinDict( target_report_dict),
            tmpname             = tmp.name,
            remote_server       = rsync_spec_dict.get('remote_server'),
            remote_port         = to_int(rsync_spec_dict.get('remote_port')) if rsync_spec_dict.get(
                                      'remote_port') is not None else None,
            remote_user         = rsync_spec_dict.get('remote_user'),
            remote_dir          = rsync_spec_dict.get('remote_dir'),
            compress            = True if rsync_spec_dict.get('compress') == 'True' else False,
            log_success         = True if rsync_spec_dict.get('log_success') == 'True' else False,
            ssh_options         = rsync_spec_dict.get('ssh_options', '-o ConnectTimeout=1'),
            timeout             = rsync_spec_dict.get('timeout', 1),
            skip_if_older_than  = to_int(rsync_spec_dict.get('skip_if_older_than', 3)),
            barometer_rate_secs = 10800,
            wind_rose_secs      = 86400,
            wind_rose_points    = 16)

        if not os.path.exists(self.cfg.loop_data_dir):
            os.makedirs(self.cfg.loop_data_dir)

        log.info('LoopData file is: %s' % os.path.join(self.cfg.loop_data_dir, self.cfg.filename))

        lp: LoopProcessor = LoopProcessor(self.cfg)
        t: threading.Thread = threading.Thread(target=lp.process_queue)
        t.setName('LoopData')
        t.setDaemon(True)
        t.start()

        self.bind(weewx.STARTUP, self._catchup)
        self.bind(weewx.END_ARCHIVE_PERIOD, self.end_archive_period)
        self.bind(weewx.CHECK_LOOP, self.check_loop)

    def check_loop(self, event):
        log.debug('check_loop: event: %s' % event)
        self.cfg.queue.put(event)

    def _catchup(self, generator):
        log.debug('_catchup: generator: %r' % generator)

    def end_archive_period(self, event):
        log.debug('end_archive_period: event: %s' % event)
        self.cfg.queue.put(event)

    @staticmethod
    def compute_loop_data_dir(config_dict):
        weewx_root: str = config_dict.get('WEEWX_ROOT')
        html_root: str = config_dict.get('StdReport').get('HTML_ROOT')
        loop_data_dict = config_dict.get('LoopData', {})
        file_spec_dict = loop_data_dict.get('FileSpec', {})
        loop_dir: str = file_spec_dict.get('loop_data_dir')
        log.debug('compute_loop_data_dir: %s' %  os.path.join(weewx_root, html_root, loop_dir))
        return os.path.join(weewx_root, html_root, loop_dir)

class LoopProcessor:
    def __init__(self, cfg: Configuration):
        self.cfg = cfg
        self.next_day: int = 0
        self.archive_start: float = time.time()
        self.barometer_readings: List[Reading] = []
        self.wind_gust_readings: List[Reading] = []
        self.wind_rose_readings: List[WindroseReading] = []

    def process_queue(self) -> None:
        try:
            while True:
                event = self.cfg.queue.get()
                pkt: Dict[str, Any] = event.packet
                pkt_time = to_int(pkt['dateTime'])

                # Archive records come through just to save off pressure as we need
                # a 3 hour trend; and WindRose data for a 24 hour trend.  This
                # is done with archive records rather than loop records.
                # END_ARCHIVE_PERIOD is used to save barometer and wind.
                if event.event_type == weewx.END_ARCHIVE_PERIOD:
                    self.save_barometer_reading(pkt_time, to_float(pkt['barometer']))
                    self.save_wind_rose_data(pkt_time, to_float(pkt['windSpeed']), pkt['windDir'])
                    continue

                log.debug('Dequeued loop event(%s): %s' % (event, timestamp_to_string(pkt_time)))
                assert event.event_type == weewx.CHECK_LOOP

                if self.next_day == 0:
                    # Startup work
                    start: float = time.time()
                    self.next_day = LoopProcessor.local_midnight_timestamp()
                    log.debug('Next day is: %s' % timestamp_to_string(self.next_day))

                    # This is the first loop, initalize the highs, lows,
                    # barometer rate and 10m high gust.  Note: Not done in init()
                    # as the daily summaries might not be up to date at init time.
                    db_manager = LoopProcessor.get_db_manager(self.cfg.config_dict)
                    self.day_summary = db_manager._get_day_summary(time.time())
                    log.debug('startup: day_summary: %s' % self.day_summary)

                    self.fill_in_wind_rose_readings_at_startup(db_manager)
                    self.fill_in_barometer_readings_at_startup(db_manager)
                    self.fill_in_10m_wind_gust_readings_at_startup(db_manager)
                    LoopProcessor.fixup_rain_on_startup(pkt)
                    log.info('LoopData process queue took %f seconds to set up.' % (
                             time.time() - start))
                elif pkt_time > self.next_day:
                    self.next_day = LoopProcessor.local_midnight_timestamp()
                    log.debug('Next day is: %s' % timestamp_to_string(self.next_day))
                    # NOTE: Add 10s to be sure we get back today's (empty) summary.
                    self.day_summary = db_manager._get_day_summary(time.time()+10)
                    log.debug('midnight_reset: day_summary: %s' % self.day_summary)
                    for obstype in self.day_summary:
                        log.debug('midnight_reset: accum[%s].lasttime: %r' % (
                                  obstype, self.day_summary[obstype].lasttime))

                # Process new packet.  This handles all accumulators.
                self.day_summary.addRecord(pkt)

                # Keep 10 minutes of wind gust readings.
                # Barometer rate is dealt with via archive records.
                self.save_wind_gust_reading(pkt_time, to_float(pkt['windGust']))

                pkt = copy.deepcopy(pkt)

                # Iterate through all scalar stats and add mins and maxes to record.
                for obstype in self.day_summary:
                    accum = self.day_summary[obstype]
                    if isinstance(accum, weewx.accum.ScalarStats):
                        if accum.lasttime is not None:
                            min, mintime, max, maxtime, _, _, _, _ = accum.getStatsTuple()
                            pkt['T_LO_%s' % obstype] = mintime
                            pkt['LO_%s' % obstype] = min
                            pkt['T_HI_%s' % obstype] = maxtime
                            pkt['HI_%s' % obstype] = max
                            self.convert_units(pkt, obstype)

                # Add barometerRate
                self.insert_barometer_rate(pkt)
                self.convert_barometer_rate_units(pkt)

                # Add 10mMaxGust
                self.insert_10m_max_windgust(pkt)
                self.convert_10m_max_windgust(pkt)

                # Add windRose
                self.insert_wind_rose(pkt)
                self.convert_wind_rose(pkt)

                self.compose_and_write_packet(pkt)
        except Exception as e:
            weeutil.logger.log_traceback(log.critical, "    ****  ")
            raise
        finally:
            os.unlink(self.cfg.tmpname)

    def compose_and_write_packet(self, pkt: Dict[str, Any]) -> None:
        loop_data_section = self.cfg.config_dict.get('LoopData', {})
        include_section = loop_data_section.get('Include', {})
        fields_to_include = include_section.get('fields', None)
        rename_section = loop_data_section.get('Rename', None)
        if fields_to_include == None and rename_section == None:
            self.write_packet(pkt)
        else:
            selective_pkt: Dict[str, Any] = {}
            if fields_to_include != None:
                for obstype in fields_to_include:
                    if obstype in pkt:
                        selective_pkt[obstype] = pkt[obstype]
            if rename_section != None:
                for obstype in rename_section:
                    if obstype in pkt:
                        selective_pkt[rename_section[obstype]] = pkt[obstype]
            self.write_packet(selective_pkt)

    def write_packet(self, pkt: Dict[str, Any]) -> None:
        log.debug('Writing packet to %s' % self.cfg.tmpname)
        with open(self.cfg.tmpname, "w") as f:
            f.write(json.dumps(pkt))
            f.flush()
            os.fsync(f.fileno())
        log.debug('Wrote to %s' % self.cfg.tmpname)
        # move it to filename
        shutil.move(self.cfg.tmpname, os.path.join(self.cfg.loop_data_dir, self.cfg.filename))
        log.debug('Moved to %s' % os.path.join(self.cfg.loop_data_dir, self.cfg.filename))
        if self.cfg.remote_server is not None:
            # rsync the data
            self.rsync_data(pkt['dateTime'])

    @staticmethod
    def fixup_rain_on_startup(pkt):
        # Special case 'rain' because weewx.weewxformulas.calculate_rain
        # returns None on startup.
        # Not all driver's use weewx.weewxformulas.calculate_rain, but
        # for those that do, we translate rain of None to rain of 0.0.
        if 'rain' not in pkt:
            log.error('Expected rain in pkt: %s' % pkt)
            pkt['rain'] = 0.0
        elif pkt['rain'] is None:
            pkt['rain'] = 0.0
        else:
            log.info('Expected None for pkt[rain], but found %f' % pkt['rain'])

    def get_wind_rose_bucket(self, wind_dir: float) -> int:
        slice_size: float = 360.0 / self.cfg.wind_rose_points
        bucket: int = to_int((wind_dir + slice_size / 2.0) / slice_size)

        bkt =  bucket if bucket < self.cfg.wind_rose_points else 0
        log.debug('get_wind_rose_bucket: wind_dir: %d, bucket: %d' % (wind_dir, bkt))

        return bucket if bucket < self.cfg.wind_rose_points else 0

    def save_wind_rose_data(self, pkt_time: int, wind_speed: float, wind_dir: Optional[float]):
        # Example: 3.1 mph, 202 degrees
        # archive_interval:  300 seconds
        # intervals in an hour: 3600 / 300 (12)
        # distance = 3.1 / 12 = 0.258333 miles

        if wind_speed != 0:
            log.debug('pkt_time: %d, bucket: %d, distance: %f' % (pkt_time,
                self.get_wind_rose_bucket(wind_dir), wind_speed / (
                3600.0 / self.cfg.archive_interval)))
            self.wind_rose_readings.append(WindroseReading(
                timestamp = pkt_time,
                bucket    = self.get_wind_rose_bucket(wind_dir),
                distance  = wind_speed / (3600.0 / self.cfg.archive_interval)))

            # Delete old WindRose data
            cutoff_age: float = time.time() - self.cfg.wind_rose_secs
            del_count = 0
            for r in self.wind_rose_readings:
                if r.timestamp < cutoff_age:
                    del_count += 1
            for _ in range(del_count):
                del self.wind_rose_readings[0]

    def rsync_data(self, pktTime: int):
        log.debug('rsync_data(%d) start' % pktTime)
        # Don't upload if more than skip_if_older_than seconds behind.
        if self.cfg.skip_if_older_than != 0:
            age = time.time() - pktTime
            if age > self.cfg.skip_if_older_than:
                log.info('skipping packet (%s) with age: %f' % (timestamp_to_string(pktTime), age))
                return
        rsync_upload = weeutil.rsyncupload.RsyncUpload(
            local_root= os.path.join(self.cfg.loop_data_dir, self.cfg.filename),
            remote_root = os.path.join(self.cfg.remote_dir, self.cfg.filename),
            server=self.cfg.remote_server,
            user=self.cfg.remote_user,
            port=self.cfg.remote_port,
            ssh_options=self.cfg.ssh_options,
            compress=self.cfg.compress,
            delete=False,
            log_success=self.cfg.log_success,
            timeout=self.cfg.timeout)
        try:
            rsync_upload.run()
        except IOError as e:
            (cl, unused_ob, unused_tr) = sys.exc_info()
            log.error("rsync_data: Caught exception %s: %s" % (cl, e))

    @staticmethod
    def local_midnight_timestamp() -> int:
        tmrw = datetime.datetime.now() + datetime.timedelta(days=1)
        return to_int(datetime.datetime(tmrw.year, tmrw.month, tmrw.day).timestamp())

    def convert_hi_lo_units(self, pkt: Dict[str, Any], obstype, unit_type, unit_group) -> None:
        # convert high and low
        if 'HI_%s' % obstype not in pkt:
            log.info('pkt[HI_%s] is missing.' % obstype)
        else:
            hi, _, _ = weewx.units.convert(
                       (pkt['HI_%s' % obstype], unit_type, unit_group), unit_type)
            pkt['HI_%s' % obstype] = self.cfg.formatter.get_format_string(unit_type) % hi
            pkt['FMT_HI_%s' % obstype] = self.cfg.formatter.toString((hi, unit_type, unit_group))
        if 'LO_%s' % obstype not in pkt:
            log.info('pkt[LO_%s] is missing.' % obstype)
        else:
            lo, _, _ = weewx.units.convert((
                pkt['LO_%s' % obstype], unit_type, unit_group), unit_type)
            pkt['LO_%s' % obstype] = self.cfg.formatter.get_format_string(unit_type) % lo
            pkt['FMT_LO_%s' % obstype] = self.cfg.formatter.toString((lo, unit_type, unit_group))

    def convert_barometer_rate_units(self, pkt: Dict[str, Any]) -> None:
        self.convert_units(pkt, "barometerRate", do_hi_lo=False)

    def convert_10m_max_windgust(self, pkt: Dict[str, Any]):
        obstype = '10mMaxGust'
        # get windGust value tuple as it will give us the unit type and unit group
        v_t = weewx.units.as_value_tuple(pkt, 'windGust')
        wind_gust_v_t = weewx.units.ValueTuple(pkt[obstype], v_t.unit, v_t.group)
        value, unit_type, unit_group = self.cfg.converter.convert(wind_gust_v_t)
        pkt[obstype] = self.cfg.formatter.get_format_string(unit_type) % value
        pkt['UNITS_%s' % obstype] = unit_type
        pkt['FMT_%s' % obstype] = self.cfg.formatter.toString((value, unit_type, unit_group))
        pkt['LABEL_%s' % obstype] = self.cfg.formatter.get_label_string(unit_type)

    def convert_wind_rose(self, pkt: Dict[str, Any]):
        obstype = 'windRose'
        # The windrun observation type will yield the units we need.
        std_unit_system = pkt['usUnits']
        (unit_type, unit_group) = weewx.units.StdUnitConverters[
            std_unit_system].getTargetUnit('windrun')
        distances: List[float] = pkt[obstype]
        converted_distances: List[float] = []
        for i in range(len(distances)):
            distance_v_t = weewx.units.ValueTuple(
                distances[i], unit_type, unit_group)
            value, unit_type, unit_group = self.cfg.converter.convert(distance_v_t)
            fmt_value = self.cfg.formatter.get_format_string(unit_type) % value
            converted_distances.append(fmt_value)
        log.debug('distances: %s' % distances)
        pkt[obstype] = converted_distances
        pkt['UNITS_%s' % obstype] = unit_type
        pkt['LABEL_%s' % obstype] = self.cfg.formatter.get_label_string(unit_type)

    def convert_units(self, pkt: Dict[str, Any],
            obstype: str, do_hi_lo = True) -> None:
        # Pull a switcharoo for day_rain_total else weewx doesn't know the units.
        v_t = weewx.units.as_value_tuple(
            pkt, 'rain' if obstype == 'day_rain_total' else obstype)
        if obstype == 'day_rain_total':
            v_t = weewx.units.ValueTuple(pkt['day_rain_total'], v_t.unit, v_t.group)
        value, unit_type, unit_group = self.cfg.converter.convert(v_t)
        # windDir and gustDir could be None.
        if value is not None:
            pkt[obstype] = self.cfg.formatter.get_format_string(unit_type) % value
            pkt['FMT_%s' % obstype] = self.cfg.formatter.toString((
                value, unit_type, unit_group))
        else:
            # TODO: Is it better to put None or leave out the observation?
            pkt[obstype] = None
            pkt['FMT_%s' % obstype] = None
        pkt['UNITS_%s' % obstype] = unit_type
        pkt['LABEL_%s' % obstype] = self.cfg.formatter.get_label_string(unit_type)
        if obstype in COMPASS_OBSERVATIONS:
            pkt['COMPASS_%s' % obstype] = self.cfg.formatter.to_ordinal_compass(
                (value, unit_type, unit_group))
        if do_hi_lo and obstype != 'day_rain_total':
            self.convert_hi_lo_units(pkt, obstype, unit_type, unit_group)

    def fill_in_wind_rose_readings_at_startup(self, db_manager) -> None:
        earliest: int = to_int(time.time() - self.cfg.wind_rose_secs)
        for cols in db_manager.genSql('SELECT dateTime, windSpeed, windDir FROM' \
                ' archive WHERE dateTime >= %d ORDER BY dateTime ASC' % earliest):
            dateTime  = cols[0]
            windSpeed = cols[1]
            windDir   = cols[2]
            if windSpeed != 0:
                reading = WindroseReading(
                    timestamp = dateTime,
                    bucket    = self.get_wind_rose_bucket(windDir),
                    distance  = windSpeed / (3600.0 / self.cfg.archive_interval))
                self.wind_rose_readings.append(reading)
                log.debug('fill_in_wind_rose_readings_at_startup: Reading(%s): ' \
                         'bucket[%d]: distance: %f' % (
                         timestamp_to_string(reading.timestamp), reading.bucket, reading.distance))

    def fill_in_barometer_readings_at_startup(self, db_manager) -> None:
        earliest: int = to_int(time.time() - self.cfg.barometer_rate_secs)
        for cols in db_manager.genSql('SELECT dateTime, barometer FROM archive' \
                ' WHERE dateTime >= %d ORDER BY dateTime ASC' % earliest):
            reading: Reading = Reading(timestamp = cols[0], value = cols[1])
            self.barometer_readings.append(reading)
            log.debug('fill_in_barometer_readings_at_startup: Reading(%s): %f' % (
                timestamp_to_string(reading.timestamp), reading.value))

    def insert_barometer_rate_desc(self, pkt: Dict[str, Any]) -> None:
        # Shipping forecast descriptions for the 3 hour change in baromter readings.
        # Falling (or rising) slowly: 0.1 - 1.5mb in 3 hours
        # Falling (or rising): 1.6 - 3.5mb in 3 hours
        # Falling (or rising) quickly: 3.6 - 6.0mb in 3 hours
        # Falling (or rising) very rapidly: More than 6.0mb in 3 hours

        v_t = weewx.units.as_value_tuple(pkt, 'barometerRate')
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

        pkt['DESC_barometerRate'] = desc

    def insert_barometer_rate(self, pkt: Dict[str, Any]) -> None:
        if len(self.barometer_readings) != 0:
            # The saved readings are used to get the starting point,
            # but the current pkt is used for the last barometer reading.
            delta3H = to_float(pkt['barometer']) - self.barometer_readings[0].value
            # Report rate per hour
            delta = delta3H / 3.0
            pkt['barometerRate'] = delta
            log.debug('insert_barometer_rate: %s' % delta)
            self.insert_barometer_rate_desc(pkt)

    def save_barometer_reading(self, pkt_time: int, value: float) -> None:
        reading = Reading(timestamp = pkt_time, value = value)
        self.barometer_readings.append(reading)
        log.debug('save_barometer_reading: Reading(%s): %f' % (
            timestamp_to_string(reading.timestamp), reading.value))
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

    def fill_in_10m_wind_gust_readings_at_startup(self, db_manager) -> None:
        earliest: int = to_int(time.time() - 600)
        for cols in db_manager.genSql('SELECT dateTime, windGust FROM archive WHERE dateTime >= %d' \
                                      ' ORDER BY dateTime ASC' % earliest):
            reading: Reading = Reading(timestamp = cols[0], value = cols[1])
            self.wind_gust_readings.append(reading)
            log.debug('fill_in_10m_wind_gust_readings_at_startup: Reading(%s): %f' % (
                      timestamp_to_string(reading.timestamp), reading.value))

    def insert_10m_max_windgust(self, pkt: Dict[str, Any]) -> None:
        max_gust     : float = 0.0
        max_gust_time: int = 0
        for reading in self.wind_gust_readings:
            if reading.value > max_gust:
                max_gust = reading.value
                max_gust_time = reading.timestamp
        pkt['10mMaxGust'] = max_gust
        pkt['T_10mMaxGust'] = max_gust_time
        log.debug('insert_10m_max_windgust: ts: %d, gust: %f' % (
                  pkt['T_10mMaxGust'], pkt['10mMaxGust']))

    def insert_wind_rose(self, pkt: Dict[str, Any]) -> None:
        buckets: List[float] = [0.0]*self.cfg.wind_rose_points
        for r in self.wind_rose_readings:
            buckets[r.bucket] += r.distance
            log.debug('Added %f to wind_rose_readings[%d]' % (r.distance, r.bucket))
        pkt['windRose'] = buckets

    def save_wind_gust_reading(self, pkt_time: int, value: float) -> None:
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

    @staticmethod
    def get_db_manager(config_dict):
        db_binder = weewx.manager.DBBinder(config_dict)
        default_binding = config_dict.get('StdReport')['data_binding']
        default_archive = db_binder.get_manager(default_binding)
        return default_archive
