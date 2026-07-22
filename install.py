# Copyright 20222-2026 by John A Kline <john@johnkline.com>
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

import sys
import weewx
from setup import ExtensionInstaller

def loader():
    if sys.version_info[0] < 3 or (sys.version_info[0] == 3 and sys.version_info[1] < 7):
        sys.exit("weewx-loopdata requires Python 3.7 or later, found %s.%s" % (
            sys.version_info[0], sys.version_info[1]))

    if weewx.__version__ < "4":
        sys.exit("weewx-loopdata requires WeeWX 4, found %s" % weewx.__version__)

    return LoopDataInstaller()

class LoopDataInstaller(ExtensionInstaller):
    def __init__(self):
        super(LoopDataInstaller, self).__init__(
            version = "6.0",
            name = 'loopdata',
            description = 'Loop statistics for real time reporting.',
            author = "John A Kline",
            author_email = "john@johnkline.com",
            report_services = 'user.loopdata.LoopData',
            config = {
                'LoopData': {
                    'FileSpec': {
                        'loop_data_dir': '.',
                        'filename':  'loop-data.txt'},
                    'Formatting': {
                        'target_report': 'LoopDataReport'},
                    'LoopFrequency': {
                        'seconds': '2.0'},
                    'RsyncSpec': {
                        'enable':'false',
                        'remote_server': 'www.foobar.com',
                        'remote_user': 'root',
                        'remote_dir': '/home/weewx/loop-data',
                        'compress': 'false',
                        'log_success': 'false',
                        'ssh_options': '-o ConnectTimeout=1',
                        'timeout': '1',
                        'skip_if_older_than': '3'},
                    'Include': {
                        # Exactly the fields the sample report's instrument
                        # panel reads: .raw for needle/petal geometry,
                        # report-formatted for every readout, unit.label to
                        # pick the dial scales.
                        'fields': [
                            'current.dateTime.raw',

                            'current.outTemp',
                            'current.outTemp.raw',
                            'day.outTemp.min.raw',
                            'day.outTemp.max.raw',
                            'day.outTemp.min.formatted',
                            'day.outTemp.max.formatted',

                            'current.outHumidity',
                            'current.outHumidity.raw',
                            'day.outHumidity.min.raw',
                            'day.outHumidity.max.raw',

                            'current.windSpeed',
                            'current.windSpeed.raw',
                            'current.windDir.raw',
                            'current.windDir.ordinal_compass',
                            '10m.windGust.max',
                            '10m.wind.gustdir.raw',
                            '10m.wind.gustdir.ordinal_compass',

                            'current.barometer',
                            'current.barometer.raw',
                            'trend.barometer.raw',
                            'trend.barometer.desc',

                            'current.rainRate',
                            'current.rainRate.raw',
                            'day.rain.sum',
                            'day.rain.sum.raw',
                            'day.rainRate.max',
                            'day.rainRate.max.raw',

                            'current.dewpoint',
                            'current.dewpoint.raw',
                            'day.dewpoint.min.raw',
                            'day.dewpoint.max.raw',
                            'day.dewpoint.min.formatted',
                            'day.dewpoint.max.formatted',

                            # The gauges below hide themselves on stations
                            # that do not report the observation.
                            'current.appTemp',
                            'current.appTemp.raw',
                            'day.appTemp.min.raw',
                            'day.appTemp.max.raw',
                            'day.appTemp.min.formatted',
                            'day.appTemp.max.formatted',

                            'current.UV',
                            'current.UV.raw',
                            'day.UV.max',

                            'current.radiation',
                            'current.radiation.raw',
                            'day.radiation.max',

                            'current.pm2_5',
                            'current.pm2_5_aqi.raw',
                            'current.pm2_5_aqi.formatted',

                            'day.windrose.banded',
                            'day.windrose.calm',

                            'unit.label.outTemp',
                            'unit.label.barometer',
                            'unit.label.rain',
                            'unit.label.rainRate',
                            'unit.label.windSpeed',
                            ]},

                    'BarometerTrendDescriptions': {
                        'RISING_VERY_RAPIDLY' : 'Rising Very Rapidly',
                        'RISING_QUICKLY'      : 'Rising Quickly',
                        'RISING'              : 'Rising',
                        'RISING_SLOWLY'       : 'Rising Slowly',
                        'STEADY'              : 'Steady',
                        'FALLING_SLOWLY'      : 'Falling Slowly',
                        'FALLING'             : 'Falling',
                        'FALLING_QUICKLY'     : 'Falling Quickly',
                        'FALLING_VERY_RAPIDLY': 'Falling Very Rapidly',
                    },
                },
                'StdReport': {
                    'LoopDataReport': {
                        'HTML_ROOT':'loopdata',
                        'enable': 'true',
                        'skin':'LoopData',
                        'Extras': {
                            'loop_data_file'   : 'loop-data.txt',
                            'expiration_time'  : 4,
                            'page_update_pwd'  : 'foobar',
                            'googleAnalyticsId': '',
                            'analytics_host'   : '',
                        },
                        'Units' : {
                            'StringFormats': {
                                'mile_per_hour': '%.0f',
                                'degree_C': '%.1f',
                                'km_per_hour': '%.0f',
                                'degree_F': '%.1f',
                            },
                        },
                    },
                },
            },
            files = [
                ('bin/user', [
                    'bin/user/loopdata.py',
                    ]),
                ('skins/LoopData', [
                    'skins/LoopData/analytics.inc',
                    'skins/LoopData/favicon.ico',
                    'skins/LoopData/index.html.tmpl',
                    'skins/LoopData/realtime_updater.inc',
                    'skins/LoopData/skin.conf',
                    ]),
            ])
