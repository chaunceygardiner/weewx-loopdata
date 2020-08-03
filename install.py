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
            version = "2.5",
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
                        'target_report': 'WeatherBoardReport'},
                    'LoopFrequency': {
                        'seconds': '2.5'},
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
                        'fields': ['current.dateTime.raw','current.windSpeed.raw','current.windDir.ordinal_compass',
                            'trend.barometerRate.desc','current.barometer',
                            'day.rain.sum','current.dewpoint','current.heatindex',
                            'current.outHumidity','current.outTemp','current.rain',
                            'current.rainRate','current.windchill','current.windSpeed',
                            'day.windGust.max','10m.windGust.max']},
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
                }
            },
            files = [('bin/user', ['bin/user/loopdata.py'])])
