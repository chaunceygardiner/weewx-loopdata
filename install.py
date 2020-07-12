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

from setup import ExtensionInstaller

def loader():
    return LoopDataInstaller()

class LoopDataInstaller(ExtensionInstaller):
    def __init__(self):
        super(LoopDataInstaller, self).__init__(
            version = "2.0.b3",
            name = 'loopdata',
            description = 'Loop statistics for real time reporting.',
            author = "John A Kline",
            author_email = "john@johnkline.com",
            report_services = 'user.loopdata.LoopData',
            config = {
                'LoopData': {
                    'FileSpec': {
                        'loop_data_dir': '/home/weewx/public_html',
                        'filename':  'loop-data.txt'},
                    'Formatting': {
                        'target_report': 'SeasonsReport'},
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
                        'fields': ['current.dateTime.raw','current.windSpeed.formatted','current.windDir.ordinal_compass',
                            'trend.barometerRate.desc','current.barometer',
                            'day.rain.sum','current.dewpoint','current.heatindex',
                            'current.outHumidity','current.outTemp','current.rain',
                            'current.rainRate','current.windchill','current.windSpeed',
                            'day.windGust.max','10m.windGust.max']},
                    'Rename': {
                    }
                }
            },
            files = [('bin/user', ['bin/user/loopdata.py'])])
