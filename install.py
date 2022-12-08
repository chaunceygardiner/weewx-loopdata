# Copyright 2020 by John A Kline <john@johnkline.com>
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
            version = "3.3.1",
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
                        'fields': [
                            'trend.outTemp',
                            'trend.barometer.code',
                            'trend.barometer.desc',

                            'current.barometer',
                            'current.barometer.formatted',
                            'current.dateTime.raw',
                            'current.dewpoint',
                            'current.dewpoint.formatted',
                            'current.heatindex',
                            'current.outHumidity',
                            'current.outTemp',
                            'current.outTemp.formatted',
                            'current.rain',
                            'current.rainRate',
                            'current.rainRate.formatted',
                            'current.UV.formatted',
                            'current.windchill',
                            'current.windDir.ordinal_compass',
                            'current.windSpeed',
                            'current.windSpeed.formatted',
                            'current.windSpeed.raw',

                            '2m.outTemp.max.formatted',
                            '2m.outTemp.min.formatted',
                            '2m.rain.sum.formatted',
                            '2m.wind.rms.formatted',
                            '2m.windGust.max',
                            '2m.windGust.max.formatted',

                            '10m.outTemp.max.formatted',
                            '10m.outTemp.min.formatted',
                            '10m.rain.sum.formatted',
                            '10m.wind.rms.formatted',
                            '10m.windGust.max',
                            '10m.windGust.max.formatted',

                            '24h.outTemp.max.formatted',
                            '24h.outTemp.min.formatted',
                            '24h.rain.sum.formatted',
                            '24h.wind.rms.formatted',
                            '24h.windGust.max',
                            '24h.windGust.max.formatted',

                            'hour.outTemp.max.formatted',
                            'hour.outTemp.min.formatted',
                            'hour.rain.sum.formatted',
                            'hour.wind.rms.formatted',
                            'hour.windGust.max',
                            'hour.windGust.max.formatted',

                            'day.outTemp.max.formatted',
                            'day.outTemp.min.formatted',
                            'day.rain.sum',
                            'day.rain.sum.formatted',
                            'day.wind.rms.formatted',
                            'day.windGust.max',
                            'day.windGust.max.formatted',

                            'week.outTemp.max.formatted',
                            'week.outTemp.min.formatted',
                            'week.rain.sum.formatted',
                            'week.wind.rms.formatted',
                            'week.windGust.max.formatted',

                            'month.outTemp.max.formatted',
                            'month.outTemp.min.formatted',
                            'month.rain.sum.formatted',
                            'month.wind.rms.formatted',
                            'month.windGust.max.formatted',

                            'year.outTemp.max.formatted',
                            'year.outTemp.min.formatted',
                            'year.rain.sum.formatted',
                            'year.wind.rms.formatted',
                            'year.windGust.max.formatted',

                            'rainyear.outTemp.max.formatted',
                            'rainyear.outTemp.min.formatted',
                            'rainyear.rain.sum.formatted',
                            'rainyear.wind.rms.formatted',
                            'rainyear.windGust.max.formatted',

                            'alltime.outTemp.max.formatted',
                            'alltime.outTemp.min.formatted',
                            'alltime.rain.sum.formatted',
                            'alltime.wind.rms.formatted',
                            'alltime.windGust.max.formatted',
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
