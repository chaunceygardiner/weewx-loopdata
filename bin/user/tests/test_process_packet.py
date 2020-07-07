#    Copyright (c) 2020 John A Kline <john@johnkline.com>
#
#    See the file LICENSE.txt for your full rights.
#
"""Test processing packets."""

import configobj
import logging
import unittest

import weewx
import weewx.accum
import weewx.units

import weeutil.logger

import user.loopdata
import cc3000_packets
import ip100_packets
import simulator_packets

weewx.debug = 1

log = logging.getLogger(__name__)

# Set up logging using the defaults.
weeutil.logger.setup('test_config', {})

class ProcessPacketTests(unittest.TestCase):

    def test_ip100_packet_processing(self):

        config_dict = ProcessPacketTests._get_config_dict('us')

        first_pkt_time, pkts = ip100_packets.IP100Packets._get_packets()
        day_accum = ProcessPacketTests._get_day_accum(config_dict, first_pkt_time)

        converter, formatter = ProcessPacketTests._get_converter_and_formatter(config_dict)
        self.assertEqual(type(converter), weewx.units.Converter)
        self.assertEqual(type(formatter), weewx.units.Formatter)

        for pkt in pkts:
            day_accum.addRecord(pkt)

        loopdata_pkt = user.loopdata.LoopProcessor.create_loopdata_packet(pkt,
            day_accum, converter, formatter)
        self.maxDiff = None

        self.assertEqual(loopdata_pkt['FMT_outTemp'], '72.0°F')
        self.assertEqual(loopdata_pkt['FMT_barometer'], '30.055 inHg')
        self.assertEqual(loopdata_pkt['FMT_windSpeed'], '6 mph')
        self.assertEqual(loopdata_pkt['FMT_windDir'], '45°')

        self.assertEqual(loopdata_pkt['FMT_AVG_outTemp'], '72.0°F')
        self.assertEqual(loopdata_pkt['FMT_AVG_barometer'], '30.055 inHg')
        self.assertEqual(loopdata_pkt['FMT_AVG_windSpeed'], '3 mph')
        self.assertEqual(loopdata_pkt['FMT_AVG_windDir'], '87°')

        self.assertEqual(loopdata_pkt['FMT_WAVG_outTemp'], '72.0°F')
        self.assertEqual(loopdata_pkt['FMT_WAVG_barometer'], '30.055 inHg')
        self.assertEqual(loopdata_pkt['FMT_WAVG_windSpeed'], '3 mph')
        self.assertEqual(loopdata_pkt['FMT_WAVG_windDir'], '87°')

        self.assertEqual(loopdata_pkt['FMT_HI_outTemp'], '72.0°F')
        self.assertEqual(loopdata_pkt['FMT_HI_barometer'], '30.055 inHg')
        self.assertEqual(loopdata_pkt['FMT_HI_windSpeed'], '6 mph')
        self.assertEqual(loopdata_pkt['FMT_HI_windDir'], '360°')

        self.assertEqual(loopdata_pkt['FMT_LO_outTemp'], '72.0°F')
        self.assertEqual(loopdata_pkt['FMT_LO_barometer'], '30.055 inHg')
        self.assertEqual(loopdata_pkt['FMT_LO_windSpeed'], '1 mph')
        self.assertEqual(loopdata_pkt['FMT_LO_windDir'], '22°')

        self.assertEqual(loopdata_pkt['LABEL_outTemp'], '°F')
        self.assertEqual(loopdata_pkt['LABEL_barometer'], ' inHg')
        self.assertEqual(loopdata_pkt['LABEL_windSpeed'], ' mph')
        self.assertEqual(loopdata_pkt['LABEL_windDir'], '°')

        self.assertEqual(loopdata_pkt['FMT_AVG_wind'], '3 mph')
        self.assertEqual(loopdata_pkt['UNITS_AVG_wind'], 'mile_per_hour')
        self.assertEqual(loopdata_pkt['LABEL_AVG_wind'], ' mph')

        self.assertEqual(loopdata_pkt['FMT_RMS_wind'], '4 mph')
        self.assertEqual(loopdata_pkt['UNITS_RMS_wind'], 'mile_per_hour')
        self.assertEqual(loopdata_pkt['LABEL_RMS_wind'], ' mph')

        self.assertEqual(loopdata_pkt['FMT_VECAVG_wind'], '3 mph')
        self.assertEqual(loopdata_pkt['UNITS_VECAVG_wind'], 'mile_per_hour')
        self.assertEqual(loopdata_pkt['LABEL_VECAVG_wind'], ' mph')

        self.assertEqual(loopdata_pkt['FMT_VECDIR_wind'], '28°')
        self.assertEqual(loopdata_pkt['UNITS_VECDIR_wind'], 'degree_compass')
        self.assertEqual(loopdata_pkt['LABEL_VECDIR_wind'], '°')

    def test_cc3000_packet_processing(self):

        config_dict = ProcessPacketTests._get_config_dict('us')

        first_pkt_time, pkts = cc3000_packets.CC3000Packets._get_packets()
        day_accum = ProcessPacketTests._get_day_accum(config_dict, first_pkt_time)

        converter, formatter = ProcessPacketTests._get_converter_and_formatter(config_dict)
        self.assertEqual(type(converter), weewx.units.Converter)
        self.assertEqual(type(formatter), weewx.units.Formatter)

        for pkt in pkts:
            day_accum.addRecord(pkt)

        loopdata_pkt = user.loopdata.LoopProcessor.create_loopdata_packet(pkt,
            day_accum, converter, formatter)
        self.maxDiff = None

        self.assertEqual(loopdata_pkt['FMT_outTemp'], '75.4°F')
        self.assertEqual(loopdata_pkt['FMT_barometer'], '30.005 inHg')
        self.assertEqual(loopdata_pkt['FMT_windSpeed'], '2 mph')
        self.assertEqual(loopdata_pkt['FMT_windDir'], '45°')

        self.assertEqual(loopdata_pkt['FMT_AVG_outTemp'], '75.4°F')
        self.assertEqual(loopdata_pkt['FMT_AVG_barometer'], '30.005 inHg')
        self.assertEqual(loopdata_pkt['FMT_AVG_windSpeed'], '4 mph')
        self.assertEqual(loopdata_pkt['FMT_AVG_windDir'], '166°')

        self.assertEqual(loopdata_pkt['FMT_WAVG_outTemp'], '75.4°F')
        self.assertEqual(loopdata_pkt['FMT_WAVG_barometer'], '30.005 inHg')
        self.assertEqual(loopdata_pkt['FMT_WAVG_windSpeed'], '4 mph')
        self.assertEqual(loopdata_pkt['FMT_WAVG_windDir'], '166°')

        self.assertEqual(loopdata_pkt['FMT_HI_outTemp'], '75.4°F')
        self.assertEqual(loopdata_pkt['FMT_HI_barometer'], '30.005 inHg')
        self.assertEqual(loopdata_pkt['FMT_HI_windSpeed'], '7 mph')
        self.assertEqual(loopdata_pkt['FMT_HI_windDir'], '360°')

        self.assertEqual(loopdata_pkt['FMT_LO_outTemp'], '75.3°F')
        self.assertEqual(loopdata_pkt['FMT_LO_barometer'], '30.005 inHg')
        self.assertEqual(loopdata_pkt['FMT_LO_windSpeed'], '0 mph')
        self.assertEqual(loopdata_pkt['FMT_LO_windDir'], '22°')

        self.assertEqual(loopdata_pkt['LABEL_outTemp'], '°F')
        self.assertEqual(loopdata_pkt['LABEL_barometer'], ' inHg')
        self.assertEqual(loopdata_pkt['LABEL_windSpeed'], ' mph')
        self.assertEqual(loopdata_pkt['LABEL_windDir'], '°')

        self.assertEqual(loopdata_pkt['FMT_AVG_wind'], '4 mph')
        self.assertEqual(loopdata_pkt['UNITS_AVG_wind'], 'mile_per_hour')
        self.assertEqual(loopdata_pkt['LABEL_AVG_wind'], ' mph')

        self.assertEqual(loopdata_pkt['FMT_RMS_wind'], '4 mph')
        self.assertEqual(loopdata_pkt['UNITS_RMS_wind'], 'mile_per_hour')
        self.assertEqual(loopdata_pkt['LABEL_RMS_wind'], ' mph')

        self.assertEqual(loopdata_pkt['FMT_VECAVG_wind'], '3 mph')
        self.assertEqual(loopdata_pkt['UNITS_VECAVG_wind'], 'mile_per_hour')
        self.assertEqual(loopdata_pkt['LABEL_VECAVG_wind'], ' mph')

        self.assertEqual(loopdata_pkt['FMT_VECDIR_wind'], '22°')
        self.assertEqual(loopdata_pkt['UNITS_VECDIR_wind'], 'degree_compass')
        self.assertEqual(loopdata_pkt['LABEL_VECDIR_wind'], '°')

    def test_simulator_packet_processing(self):

        config_dict = ProcessPacketTests._get_config_dict('metric')

        first_pkt_time, pkts = simulator_packets.SimulatorPackets._get_packets()
        day_accum = ProcessPacketTests._get_day_accum(config_dict, first_pkt_time)

        converter, formatter = ProcessPacketTests._get_converter_and_formatter(config_dict)
        self.assertEqual(type(converter), weewx.units.Converter)
        self.assertEqual(type(formatter), weewx.units.Formatter)

        for pkt in pkts:
            day_accum.addRecord(pkt)

        loopdata_pkt = user.loopdata.LoopProcessor.create_loopdata_packet(pkt,
            day_accum, converter, formatter)
        self.maxDiff = None

        self.assertEqual(loopdata_pkt['FMT_outTemp'], '0.0°C')
        self.assertEqual(loopdata_pkt['FMT_barometer'], '1053.1 mbar')
        self.assertEqual(loopdata_pkt['FMT_windSpeed'], '0 km/h')
        self.assertEqual(loopdata_pkt['FMT_windDir'], '360°')

        self.assertEqual(loopdata_pkt['FMT_AVG_outTemp'], '0.2°C')
        self.assertEqual(loopdata_pkt['FMT_AVG_barometer'], '1053.2 mbar')
        self.assertEqual(loopdata_pkt['FMT_AVG_windSpeed'], '0 km/h')
        self.assertEqual(loopdata_pkt['FMT_AVG_windDir'], '360°')

        self.assertEqual(loopdata_pkt['FMT_WAVG_outTemp'], '0.2°C')
        self.assertEqual(loopdata_pkt['FMT_WAVG_barometer'], '1053.2 mbar')
        self.assertEqual(loopdata_pkt['FMT_WAVG_windSpeed'], '0 km/h')
        self.assertEqual(loopdata_pkt['FMT_WAVG_windDir'], '360°')

        self.assertEqual(loopdata_pkt['FMT_HI_outTemp'], '0.4°C')
        self.assertEqual(loopdata_pkt['FMT_HI_barometer'], '1053.2 mbar')
        self.assertEqual(loopdata_pkt['FMT_HI_windSpeed'], '0 km/h')
        self.assertEqual(loopdata_pkt['FMT_HI_windDir'], '360°')

        self.assertEqual(loopdata_pkt['FMT_LO_outTemp'], '0.0°C')
        self.assertEqual(loopdata_pkt['FMT_LO_barometer'], '1053.1 mbar')
        self.assertEqual(loopdata_pkt['FMT_LO_windSpeed'], '0 km/h')
        self.assertEqual(loopdata_pkt['FMT_LO_windDir'], '360°')

        self.assertEqual(loopdata_pkt['LABEL_outTemp'], '°C')
        self.assertEqual(loopdata_pkt['LABEL_barometer'], ' mbar')
        self.assertEqual(loopdata_pkt['LABEL_windSpeed'], ' km/h')
        self.assertEqual(loopdata_pkt['LABEL_windDir'], '°')

        self.assertEqual(loopdata_pkt['FMT_AVG_wind'], '0 km/h')
        self.assertEqual(loopdata_pkt['UNITS_AVG_wind'], 'km_per_hour')
        self.assertEqual(loopdata_pkt['LABEL_AVG_wind'], ' km/h')

        self.assertEqual(loopdata_pkt['FMT_RMS_wind'], '0 km/h')
        self.assertEqual(loopdata_pkt['UNITS_RMS_wind'], 'km_per_hour')
        self.assertEqual(loopdata_pkt['LABEL_RMS_wind'], ' km/h')

        self.assertEqual(loopdata_pkt['FMT_VECAVG_wind'], '0 km/h')
        self.assertEqual(loopdata_pkt['UNITS_VECAVG_wind'], 'km_per_hour')
        self.assertEqual(loopdata_pkt['LABEL_VECAVG_wind'], ' km/h')

        self.assertEqual(loopdata_pkt['FMT_VECDIR_wind'], '360°')
        self.assertEqual(loopdata_pkt['UNITS_VECDIR_wind'], 'degree_compass')
        self.assertEqual(loopdata_pkt['LABEL_VECDIR_wind'], '°')

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

        try:
            group_unit_dict = target_report_dict['Units']['Groups']
        except KeyError:
            group_unit_dict = weewx.units.USUnits
        converter = weewx.units.Converter(group_unit_dict)

        formatter = weewx.units.Formatter.fromSkinDict(target_report_dict)

        return converter, formatter


if __name__ == '__main__':
    unittest.main()
