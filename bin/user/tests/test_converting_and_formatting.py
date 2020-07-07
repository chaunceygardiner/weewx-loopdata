
#
#    See the file LICENSE.txt for your full rights.
#
"""Test loopdata's conversions and formattings of loop packets."""

import configobj
import logging
import unittest

import weewx
import weewx.units

import weeutil.logger

import user.loopdata

weewx.debug = 1

log = logging.getLogger(__name__)

# Set up logging using the defaults.
weeutil.logger.setup('test_config', {})

class ConversionAndFormattingTests(unittest.TestCase):
    def test_METRIC_to_METRIC(self):
        config_dict = ConversionAndFormattingTests._get_config_dict('metric')
        converter, formatter = ConversionAndFormattingTests._get_converter_and_formatter(config_dict)
        pkt = {
            'dateTime'     : 12345689,
            'usUnits'      : weewx.METRIC,

            'rain'         : 0.10, # cm
            'LO_rain'      : 0.00,
            'HI_rain'      : 0.20,
            'SUM_rain'     : 1.31,
            'AVG_rain'     : 0.02,
            'WAVG_rain'    : 0.03,

            'rainRate'     : 1.21, # cm/h
            'LO_rainRate'  : 0.01,
            'HI_rainRate'  : 1.86,
            'SUM_rainRate' : 9.54,
            'AVG_rainRate' : 0.08,
            'WAVG_rainRate': 0.07,

            'AVG_wind'     : 3.125, # km/h
            'RMS_wind'     : 4.071, # km/h
            'VECAVG_wind'  : 3.186, # km/h
            'VECDIR_wind'  : 28.32, # degrees
        }
        user.loopdata.LoopProcessor.convert_units(converter, formatter, pkt, 'rain')
        self.assertEqual(pkt['rain'], '1.0')
        self.assertEqual(pkt['FMT_SUM_rain'], '13.1 mm')
        self.assertEqual(pkt['LABEL_rain'], ' mm')
        self.assertEqual(pkt['FMT_rain'], '1.0 mm')
        self.assertEqual(pkt['UNITS_rain'], 'mm')

        user.loopdata.LoopProcessor.convert_units(converter, formatter, pkt, 'rainRate')
        self.assertEqual(pkt['FMT_rainRate'], '12.1 mm/h')
        self.assertEqual(pkt['FMT_LO_rainRate'], '0.1 mm/h')
        self.assertEqual(pkt['FMT_AVG_rainRate'], '0.8 mm/h')
        self.assertEqual(pkt['FMT_WAVG_rainRate'], '0.7 mm/h')
        self.assertEqual(pkt['UNITS_rainRate'], 'mm_per_hour')

        user.loopdata.LoopProcessor.convert_vector_units(converter, formatter, pkt, 'wind')
        self.assertEqual(pkt['FMT_AVG_wind'], '3 km/h')
        self.assertEqual(pkt['FMT_RMS_wind'], '4 km/h')
        self.assertEqual(pkt['FMT_VECAVG_wind'], '3 km/h')
        self.assertEqual(pkt['FMT_VECDIR_wind'], '28°')

    def test_METRIC_to_MODIFIED_METRIC(self):
        config_dict = ConversionAndFormattingTests._get_config_dict('metric')
        # Add StdReport > Defaults > Units > Labels
        config_dict['StdReport']['Defaults']['Units']['Labels']['km_per_hour'] = ' kph'

        self.maxDiff = None
        converter, formatter = ConversionAndFormattingTests._get_converter_and_formatter(config_dict)
        pkt = {
            'dateTime'     : 12345689,
            'usUnits'      : weewx.METRIC,

            'rain'         : 0.10, # cm
            'LO_rain'      : 0.00,
            'HI_rain'      : 0.20,
            'SUM_rain'     : 1.31,
            'AVG_rain'     : 0.02,
            'WAVG_rain'    : 0.03,

            'rainRate'     : 1.21, # cm/h
            'LO_rainRate'  : 0.01,
            'HI_rainRate'  : 1.86,
            'SUM_rainRate' : 9.54,
            'AVG_rainRate' : 0.08,
            'WAVG_rainRate': 0.07,

            'AVG_wind'     : 3.125, # km/h
            'RMS_wind'     : 4.071, # km/h
            'VECAVG_wind'  : 3.186, # km/h
            'VECDIR_wind'  : 28.32, # degrees
        }
        user.loopdata.LoopProcessor.convert_units(converter, formatter, pkt, 'rain')
        self.assertEqual(pkt['rain'], '1.0')
        self.assertEqual(pkt['FMT_SUM_rain'], '13.1 mm')
        self.assertEqual(pkt['LABEL_rain'], ' mm')
        self.assertEqual(pkt['FMT_rain'], '1.0 mm')
        self.assertEqual(pkt['UNITS_rain'], 'mm')

        user.loopdata.LoopProcessor.convert_units(converter, formatter, pkt, 'rainRate')
        self.assertEqual(pkt['FMT_rainRate'], '12.1 mm/h')
        self.assertEqual(pkt['FMT_LO_rainRate'], '0.1 mm/h')
        self.assertEqual(pkt['FMT_AVG_rainRate'], '0.8 mm/h')
        self.assertEqual(pkt['FMT_WAVG_rainRate'], '0.7 mm/h')
        self.assertEqual(pkt['UNITS_rainRate'], 'mm_per_hour')

        user.loopdata.LoopProcessor.convert_vector_units(converter, formatter, pkt, 'wind')
        self.assertEqual(pkt['FMT_AVG_wind'], '3 kph')
        self.assertEqual(pkt['FMT_RMS_wind'], '4 kph')
        self.assertEqual(pkt['FMT_VECAVG_wind'], '3 kph')
        self.assertEqual(pkt['FMT_VECDIR_wind'], '28°')

    def test_METRIC_to_US(self):
        config_dict = ConversionAndFormattingTests._get_config_dict('us')
        converter, formatter = ConversionAndFormattingTests._get_converter_and_formatter(config_dict)
        pkt = {
            'dateTime'     : 12345689,
            'usUnits'      : weewx.METRIC,

            'rain'         : 0.10, # cm
            'LO_rain'      : 0.00,
            'HI_rain'      : 0.20,
            'SUM_rain'     : 1.31,
            'AVG_rain'     : 0.02,
            'WAVG_rain'    : 0.03,

            'rainRate'     : 1.21, # cm/h
            'LO_rainRate'  : 0.01,
            'HI_rainRate'  : 1.86,
            'SUM_rainRate' : 9.54,
            'AVG_rainRate' : 0.08,
            'WAVG_rainRate': 0.07,

            'AVG_wind'     : 3.125, # km/h
            'RMS_wind'     : 4.071, # km/h
            'VECAVG_wind'  : 3.186, # km/h
            'VECDIR_wind'  : 28.32, # degrees
        }
        user.loopdata.LoopProcessor.convert_units(converter, formatter, pkt, 'rain')
        self.assertEqual(pkt['rain'], '0.04')
        self.assertEqual(pkt['FMT_SUM_rain'], '0.52 in')
        self.assertEqual(pkt['LABEL_rain'], ' in')
        self.assertEqual(pkt['UNITS_rain'], 'inch')

        user.loopdata.LoopProcessor.convert_units(converter, formatter, pkt, 'rainRate')
        self.assertEqual(pkt['FMT_rainRate'], '0.48 in/h')
        self.assertEqual(pkt['FMT_LO_rainRate'], '0.00 in/h')
        self.assertEqual(pkt['FMT_AVG_rainRate'], '0.03 in/h')
        self.assertEqual(pkt['FMT_WAVG_rainRate'], '0.03 in/h')
        self.assertEqual(pkt['UNITS_rainRate'], 'inch_per_hour')

        user.loopdata.LoopProcessor.convert_vector_units(converter, formatter, pkt, 'wind')
        unit_type, unit_group = converter.getTargetUnit('wind', 'avg')
        self.assertEqual(unit_type, 'mile_per_hour')
        self.assertEqual(pkt['FMT_AVG_wind'], '2 mph')
        self.assertEqual(pkt['FMT_RMS_wind'], '3 mph')
        self.assertEqual(pkt['FMT_VECAVG_wind'], '2 mph')
        self.assertEqual(pkt['FMT_VECDIR_wind'], '28°')

    def test_US_to_US(self):
        config_dict = ConversionAndFormattingTests._get_config_dict('us')
        converter, formatter = ConversionAndFormattingTests._get_converter_and_formatter(config_dict)
        pkt = {
            'dateTime'     : 12345689,
            'usUnits'      : weewx.US,

            'rain'         : 0.10, # in
            'LO_rain'      : 0.00,
            'HI_rain'      : 0.20,
            'SUM_rain'     : 1.31,
            'AVG_rain'     : 0.02,
            'WAVG_rain'    : 0.03,

            'rainRate'     : 1.21, # in/h
            'LO_rainRate'  : 0.01,
            'HI_rainRate'  : 1.86,
            'SUM_rainRate' : 9.54,
            'AVG_rainRate' : 0.08,
            'WAVG_rainRate': 0.07,

            'AVG_wind'     : 3.125, # mph
            'RMS_wind'     : 4.071, # mph
            'VECAVG_wind'  : 3.186, # mph
            'VECDIR_wind'  : 28.32, # degrees
        }
        user.loopdata.LoopProcessor.convert_units(converter, formatter, pkt, 'rain')
        self.assertEqual(pkt['rain'], '0.10')
        self.assertEqual(pkt['FMT_SUM_rain'], '1.31 in')
        self.assertEqual(pkt['LABEL_rain'], ' in')

        user.loopdata.LoopProcessor.convert_units(converter, formatter, pkt, 'rainRate')
        self.assertEqual(pkt['FMT_rainRate'], '1.21 in/h')
        self.assertEqual(pkt['FMT_LO_rainRate'], '0.01 in/h')
        self.assertEqual(pkt['FMT_AVG_rainRate'], '0.08 in/h')
        self.assertEqual(pkt['FMT_WAVG_rainRate'], '0.07 in/h')

        user.loopdata.LoopProcessor.convert_vector_units(converter, formatter, pkt, 'wind')
        self.assertEqual(pkt['FMT_AVG_wind'], '3 mph')
        self.assertEqual(pkt['FMT_RMS_wind'], '4 mph')
        self.assertEqual(pkt['FMT_VECAVG_wind'], '3 mph')
        self.assertEqual(pkt['FMT_VECDIR_wind'], '28°')

    def test_US_to_METRIC(self):
        config_dict = ConversionAndFormattingTests._get_config_dict('metric')
        converter, formatter = ConversionAndFormattingTests._get_converter_and_formatter(config_dict)
        pkt = {
            'dateTime'      : 12345689,
            'usUnits'       : weewx.US,

            'rain'          : 0.10, # in
            'LO_rain'       : 0.00,
            'HI_rain'       : 0.20,
            'SUM_rain'      : 1.31,
            'AVG_rain'      : 0.02,
            'WAVG_rain'     : 0.03,

            'rainRate'      : 1.21, # in/h
            'LO_rainRate'   : 0.01,
            'HI_rainRate'   : 1.86,
            'SUM_rainRate'  : 9.54,
            'AVG_rainRate'  : 0.08,
            'WAVG_rainRate' : 0.07,

            'AVG_wind'      : 3.125, # mph
            'RMS_wind'      : 4.071, # mph
            'VECAVG_wind'   : 3.186, # mph
            'VECDIR_wind'   : 28.32, # degrees
        }
        user.loopdata.LoopProcessor.convert_units(converter, formatter, pkt, 'rain')
        self.assertEqual(pkt['rain'], '2.5')
        self.assertEqual(pkt['FMT_SUM_rain'], '33.3 mm')
        self.assertEqual(pkt['LABEL_rain'], ' mm')
        self.assertEqual(pkt['LABEL_rain'], ' mm')

        user.loopdata.LoopProcessor.convert_units(converter, formatter, pkt, 'rainRate')
        self.assertEqual(pkt['FMT_rainRate'], '30.7 mm/h')
        self.assertEqual(pkt['FMT_LO_rainRate'], '0.3 mm/h')
        self.assertEqual(pkt['FMT_AVG_rainRate'], '2.0 mm/h')
        self.assertEqual(pkt['FMT_WAVG_rainRate'], '1.8 mm/h')
        self.assertEqual(pkt['LABEL_rainRate'], ' mm/h')

        user.loopdata.LoopProcessor.convert_vector_units(converter, formatter, pkt, 'wind')
        self.assertEqual(pkt['FMT_AVG_wind'], '5 km/h')
        self.assertEqual(pkt['FMT_RMS_wind'], '7 km/h')
        self.assertEqual(pkt['FMT_VECAVG_wind'], '5 km/h')
        self.assertEqual(pkt['FMT_VECDIR_wind'], '28°')

    def test_METRICWX_to_METRIC(self):
        config_dict = ConversionAndFormattingTests._get_config_dict('metric')
        converter, formatter = ConversionAndFormattingTests._get_converter_and_formatter(config_dict)
        pkt = {
            'dateTime'     : 12345689,
            'usUnits'      : weewx.METRICWX,

            'rain'         : 0.10, # mm
            'LO_rain'      : 0.00,
            'HI_rain'      : 0.20,
            'SUM_rain'     : 1.31,
            'AVG_rain'     : 0.02,
            'WAVG_rain'    : 0.03,

            'rainRate'     : 1.21, # mm/h
            'LO_rainRate'  : 0.01,
            'HI_rainRate'  : 1.86,
            'SUM_rainRate' : 9.54,
            'AVG_rainRate' : 0.08,
            'WAVG_rainRate': 0.07,

            'AVG_wind'     : 3.125, # m/s
            'RMS_wind'     : 4.071, # m/s
            'VECAVG_wind'  : 3.186, # m/s
            'VECDIR_wind'  : 28.32, # degrees
        }
        user.loopdata.LoopProcessor.convert_units(converter, formatter, pkt, 'rain')
        self.assertEqual(pkt['rain'], '0.1')
        self.assertEqual(pkt['FMT_SUM_rain'], '1.3 mm')
        self.assertEqual(pkt['LABEL_rain'], ' mm')
        self.assertEqual(pkt['LABEL_rain'], ' mm')

        user.loopdata.LoopProcessor.convert_units(converter, formatter, pkt, 'rainRate')
        self.assertEqual(pkt['FMT_rainRate'], '1.2 mm/h')
        self.assertEqual(pkt['FMT_LO_rainRate'], '0.0 mm/h')
        self.assertEqual(pkt['FMT_AVG_rainRate'], '0.1 mm/h')
        self.assertEqual(pkt['FMT_WAVG_rainRate'], '0.1 mm/h')
        self.assertEqual(pkt['LABEL_rainRate'], ' mm/h')

        user.loopdata.LoopProcessor.convert_vector_units(converter, formatter, pkt, 'wind')
        self.assertEqual(pkt['FMT_AVG_wind'], '11 km/h')
        self.assertEqual(pkt['FMT_RMS_wind'], '15 km/h')
        self.assertEqual(pkt['FMT_VECAVG_wind'], '11 km/h')
        self.assertEqual(pkt['FMT_VECDIR_wind'], '28°')

    def test_METRICWX_to_US(self):
        config_dict = ConversionAndFormattingTests._get_config_dict('us')
        converter, formatter = ConversionAndFormattingTests._get_converter_and_formatter(config_dict)
        pkt = {
            'dateTime'     : 12345689,
            'usUnits'      : weewx.METRICWX,

            'rain'         : 0.10, # mm
            'LO_rain'      : 0.00,
            'HI_rain'      : 0.20,
            'SUM_rain'     : 1.31,
            'AVG_rain'     : 0.02,
            'WAVG_rain'    : 0.03,

            'rainRate'     : 1.21, # mm/h
            'LO_rainRate'  : 0.01,
            'HI_rainRate'  : 1.86,
            'SUM_rainRate' : 9.54,
            'AVG_rainRate' : 0.08,
            'WAVG_rainRate': 0.07,

            'AVG_wind'     : 3.125, # m/s
            'RMS_wind'     : 4.071, # m/s
            'VECAVG_wind'  : 3.186, # m/s
            'VECDIR_wind'  : 28.32, # degrees
        }
        user.loopdata.LoopProcessor.convert_units(converter, formatter, pkt, 'rain')
        self.assertEqual(pkt['rain'], '0.00')
        self.assertEqual(pkt['FMT_SUM_rain'], '0.05 in')
        self.assertEqual(pkt['LABEL_rain'], ' in')
        self.assertEqual(pkt['LABEL_rain'], ' in')

        user.loopdata.LoopProcessor.convert_units(converter, formatter, pkt, 'rainRate')
        self.assertEqual(pkt['FMT_rainRate'], '0.05 in/h')
        self.assertEqual(pkt['FMT_LO_rainRate'], '0.00 in/h')
        self.assertEqual(pkt['FMT_AVG_rainRate'], '0.00 in/h')
        self.assertEqual(pkt['FMT_WAVG_rainRate'], '0.00 in/h')
        self.assertEqual(pkt['LABEL_rainRate'], ' in/h')

        user.loopdata.LoopProcessor.convert_vector_units(converter, formatter, pkt, 'wind')
        self.assertEqual(pkt['FMT_AVG_wind'], '7 mph')
        self.assertEqual(pkt['FMT_RMS_wind'], '9 mph')
        self.assertEqual(pkt['FMT_VECAVG_wind'], '7 mph')
        self.assertEqual(pkt['FMT_VECDIR_wind'], '28°')

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
