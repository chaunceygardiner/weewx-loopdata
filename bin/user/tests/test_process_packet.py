#    Copyright (c) 2020 John A Kline <john@johnkline.com>
#
#    See the file LICENSE.txt for your full rights.
#
"""Test processing packets."""

import configobj
import logging
import time
import unittest

import weewx
import weewx.accum
import weewx.units

import weeutil.config
#import weeutil.logger

import user.loopdata

weewx.debug = 1

log = logging.getLogger(__name__)

# Set up logging using the defaults.
#weeutil.logger.setup('test_config', {})

class ProcessPacketTests(unittest.TestCase):

    def test_packet_processing(self):

        config_dict = ProcessPacketTests._get_config_dict('us')

        day_accum = ProcessPacketTests._get_day_accum(config_dict, 1593883326)

        converter, formatter = ProcessPacketTests._get_converter_and_formatter(config_dict)
        self.assertEqual(type(converter), weewx.units.Converter)
        self.assertEqual(type(formatter), weewx.units.Formatter)

        pkt = {'dateTime': 1593883326, 'usUnits': 1, 'outTemp': 72.0, 'inTemp': 74.0, 'outHumidity': 65.0, 'pressure': 30.03, 'windSpeed': 3.5, 'windDir': 45.0, 'windGust': 4.2, 'windGustDir': 22.0, 'day_rain_total': 0.0, 'rain': 0.0, 'pm1_0': 11.621667, 'pm2_5': 21.748333000000002, 'pm10_0': 25.384999999999998, 'pm2_5_aqi': 74.0, 'pm2_5_aqic': 16774144, 'altimeter': 30.05874612262195, 'appTemp': 73.14727414149363, 'barometer': 30.055425865734495, 'beaufort': 1, 'cloudbase': 2847.2963742754514, 'dewpoint': 59.57749595318801, 'heatindex': 72.0, 'humidex': 79.50646352704942, 'maxSolarRad': 766.8834257673739, 'rainRate': 0.0, 'windchill': 72.0}

        day_accum.addRecord(pkt)
        loopdata_pkt = user.loopdata.LoopProcessor.create_loopdata_packet(pkt,
            day_accum, converter, formatter)
        self.maxDiff = None

        self.assertEqual(loopdata_pkt['FMT_outTemp'], '72.0°F')
        self.assertEqual(loopdata_pkt['FMT_barometer'], '30.055 inHg')
        self.assertEqual(loopdata_pkt['FMT_windSpeed'], '4 mph')
        self.assertEqual(loopdata_pkt['FMT_windDir'], '45°')

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
        std_report_dict      = config_dict.get('StdReport', {})
        target_report_dict = std_report_dict.get('SeasonsReport')

        try:
            group_unit_dict = target_report_dict['Units']['Groups']
        except KeyError:
            try:
                group_unit_dict = std_report_dict['Defaults']['Units']['Groups']
            except KeyError:
                group_unit_dict = USUnits
        converter = weewx.units.Converter(group_unit_dict)

        formatter = weewx.units.Formatter.fromSkinDict(target_report_dict)

        return converter, formatter


if __name__ == '__main__':
    unittest.main()
