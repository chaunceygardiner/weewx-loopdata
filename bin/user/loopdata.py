"""
loopdata.py

Copyright (C)2020 by John A Kline (john@johnkline.com)
Distributed under the terms of the GNU Public License (GPLv3)

LoopData is a WeeWX service to generate a json file (loop-data.txt) containing
observations from loop packets as they are typically frequently generated in
WeeWX (e.g, every 2s).

LoopData is easy to set up.  Specify the report to target and a json file will
be generated on every loop with observations formatted and converted for that
report.  That is, the units and format for observations areappropriate for the
specified the report.

From there, just write javascript to load the json file on a regular basis
(typically, at the same rate as loop packets are generated in WeeWX) and update
your report's html page.

For an example skin that is wired to use loopdata, see:
https://github.com/chaunceygardiner/weewx-weatherboard

Inspired by https://github.com/gjr80/weewx-realtime_gauge-data.  This skin does
not attempt to duplicate Gary's realtime gauge data plugin for the SteelSeries
gauges.  If you are looking to for realtime Steel Series gauges, you definitely
want to use weewx-realtime_gauge_data.

Installation Instructions

1. cd to the directory where you have cloned this extension, e.g.,

   cd ~/software/weewx-loopdata

2. Run the following command.

   sudo /home/weewx/bin/wee_extension --install .

   Note: this command assumes weewx is installed in /home/weewx.  If it's installed
   elsewhere, adjust the path of wee_extension accordingly.

3. The install creates a LoopData section in weewx.conf as shown below.  Adjust
   the values accordingly.  In particular, specify the `target_report` for the
   report you wish to use for formatting and units.

[LoopData]
    [[FileSpec]]
        loop_data_dir = /home/weewx/gauge-data
        filename = loop-data.txt
    [[Formatting]]
        target_report = SeasonsReport
    [[RsyncSpec]]
        enable = False
        remote_server = foo.bar.com
        remote_user = root
        remote_dir = /home/weewx/gauge-data
        compress = False
        log_success = False
        ssh_options = -o ConnectTimeout=1
        timeout = 1
        skip_if_older_than = 3
    [[Include]]
        fields = dateTime, windSpeed, COMPASS_windDir, DESC_barometerRate, \
                 FMT_barometer, FMT_day_rain_total, FMT_dewpoint, FMT_heatindex, \
                 FMT_outHumidity, FMT_outTemp, FMT_rain, FMT_rainRate, \
                 FMT_windchill, FMT_windSpeed, FMT_HI_windGust, FMT_10mMaxGust
    [[Rename]]
        windRose = WindRose

 A description of the fields follows:
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
   enable            : Enable rsyncing the loop-data.txt file to remote_server.
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
import types

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import weewx
import weewx.manager
import weewx.units
import weeutil.logger
import weeutil.rsyncupload
import weeutil.weeutil


from weeutil.weeutil import timestamp_to_string
from weeutil.weeutil import to_bool
from weeutil.weeutil import to_float
from weeutil.weeutil import to_int
from weewx.engine import StdService
from weewx.units import ValueTuple

# get a logger object
log = logging.getLogger(__name__)

LOOP_DATA_VERSION = '1.1'

if sys.version_info[0] < 3:
    raise weewx.UnsupportedFeature(
        "weewx-loopdata requires Python 3, found %s" % sys.version_info[0])

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

        std_archive_dict     = config_dict.get('StdArchive', {})
        std_report_dict      = config_dict.get('StdReport', {})

        loop_config_dict     = config_dict.get('LoopData', {})
        file_spec_dict       = loop_config_dict.get('FileSpec', {})
        formatting_spec_dict = loop_config_dict.get('Formatting', {})
        rsync_spec_dict      = loop_config_dict.get('RsyncSpec', {})
        include_spec_dict    = loop_config_dict.get('Include', {})
        fields_to_rename     = loop_config_dict.get('Rename', {})

        # Get the unit_system
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
        target_report_dict = std_report_dict.get(target_report)

        fields_to_include = include_spec_dict.get('fields', [])


        # Get converter from target report (if specified),
        # else Defaults (if specified),
        # else USUnits converter.
        try:
            group_unit_dict = target_report_dict['Units']['Groups']
        except KeyError:
            try:
                group_unit_dict = std_report_dict['Defaults']['Units']['Groups']
            except KeyError:
                group_unit_dict = USUnits
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

    def pre_loop(self, event):
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
        day_accum = weewx.accum.Accum(timespan, unit_system=self.cfg.unit_system)
        for k in day_summary:
            day_accum[k] = day_summary[k]

        lp: LoopProcessor = LoopProcessor(self.cfg, day_accum, barometer_readings, wind_gust_readings, wind_rose_readings)
        t: threading.Thread = threading.Thread(target=lp.process_queue)
        t.setName('LoopData')
        t.setDaemon(True)
        t.start()

    def fill_in_barometer_readings_at_startup(self, dbm) -> List[Reading]:
        barometer_readings = []
        earliest: int = to_int(time.time() - self.cfg.barometer_rate_secs)
        for cols in dbm.genSql('SELECT dateTime, barometer FROM archive' \
                ' WHERE dateTime >= %d ORDER BY dateTime ASC' % earliest):
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
            if windSpeed != 0:
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
        self.barometer_readings: List[Reading] = barometer_readings
        self.wind_gust_readings: List[Reading] = wind_gust_readings
        self.wind_rose_readings: List[WindroseReading] = wind_rose_readings
        LoopProcessor.log_configuration(cfg)

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
                    if 'barometer' in pkt:
                        self.save_barometer_reading(pkt_time, to_float(pkt['barometer']))
                    else:
                        log.info('process_queue: barometer not in archive pkt, nothing to save for trend.')
                    if 'windSpeed' in pkt and 'windDir' in pkt:
                        self.save_wind_rose_data(pkt_time, to_float(pkt['windSpeed']), pkt['windDir'])
                    else:
                        log.info('process_queue: windSpeed and/or windDir not in archive packet, nothing to save for wind rose.')
                    continue

                log.debug('Dequeued loop event(%s): %s' % (event, timestamp_to_string(pkt_time)))
                assert event.event_type == weewx.NEW_LOOP_PACKET

                try:
                  # Process new packet.
                  log.debug(pkt)
                  self.day_accum.addRecord(pkt)
                except weewx.accum.OutOfSpan:
                    timespan = weeutil.weeutil.archiveDaySpan(time.time())
                    self.day_accum = weewx.accum.Accum(timespan, unit_system=self.cfg.unit_system)
                    # Try again:
                    self.day_accum.addRecord(pkt)


                # Keep 10 minutes of wind gust readings.
                # Barometer rate is dealt with via archive records.
                self.save_wind_gust_reading(pkt_time, to_float(pkt['windGust']))

                pkt = copy.deepcopy(pkt)

                # Iterate through all scalar stats and add mins and maxes to record.
                for obstype in self.day_accum:
                    accum = self.day_accum[obstype]
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
        if len(self.cfg.fields_to_include) == 0 and len(self.cfg.fields_to_rename) == 0:
            self.write_packet(pkt)
        else:
            selective_pkt: Dict[str, Any] = {}
            if len(self.cfg.fields_to_include) != 0:
                for obstype in self.cfg.fields_to_include:
                    if obstype in pkt:
                        selective_pkt[obstype] = pkt[obstype]
            if len(self.cfg.fields_to_rename) != 0:
                for obstype in self.cfg.fields_to_rename:
                    if obstype in pkt:
                        selective_pkt[self.cfg.fields_to_rename[obstype]] = pkt[obstype]
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
        if self.cfg.enable:
            # rsync the data
            self.rsync_data(pkt['dateTime'])

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

    def save_wind_rose_data(self, pkt_time: int, wind_speed: float, wind_dir: float):
        # Example: 3.1 mph, 202 degrees
        # archive_interval:  300 seconds
        # intervals in an hour: 3600 / 300 (12)
        # distance = 3.1 / 12 = 0.258333 miles

        if wind_speed is not None and wind_speed != 0:
            log.debug('pkt_time: %d, bucket: %d, distance: %f' % (pkt_time,
                LoopProcessor.get_wind_rose_bucket(self.cfg.wind_rose_points, wind_dir), wind_speed / (
                3600.0 / self.cfg.archive_interval)))
            self.wind_rose_readings.append(WindroseReading(
                timestamp = pkt_time,
                bucket    = LoopProcessor.get_wind_rose_bucket(self.cfg.wind_rose_points, wind_dir),
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
        if value is not None:
            reading = Reading(timestamp = pkt_time, value = value)
            self.barometer_readings.append(reading)
            log.debug('save_barometer_reading: Reading(%s): %f' % (
                timestamp_to_string(reading.timestamp), reading.value))
        else:
            log.debug('save_barometer_reading: Reading(%s): None' % timestamp_to_string(reading.timestamp))
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
        pkt['windRose'] = buckets

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
