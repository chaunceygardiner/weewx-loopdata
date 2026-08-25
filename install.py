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

# The stanza weectl writes into a fresh weewx.conf.  It is written in
# weewx.conf syntax rather than as a Python dict so that the comments come
# with it: weecfg merges it with weeutil.config.conditional_merge, which
# carries a key's comments across whenever the source has them -- and a
# plain dict has none.
#
# Only absent keys are merged, so nothing here rewrites a weewx.conf that
# already has the option.
#
# The one option missing below is [[Include]] fields, which is assigned
# after this string is parsed: ConfigObj has no line continuation for
# lists, so a fields line here would have to be a single 1200-character
# line.  See FIELDS.
#
# HTML_ROOT below must be a BARE subdirectory name.  weecfg prepends the
# installation's own StdReport HTML_ROOT at install time
# (ExtensionEngine.prepend_path), so writing 'public_html/loopdata' here
# would land the report in public_html/public_html/loopdata.  Note this
# means the value a user reads in their weewx.conf is the prefixed one --
# a comment here is a comment on THEIR line, so keep it true of the
# installed text, not of this source.
CONFIG = """
[LoopData]
    # Where to write the loop-data file.
    [[FileSpec]]
        # The directory to write it into.  A relative path is relative to
        # the target_report's directory, so the default writes the file
        # beside the sample report's page, which is where that page looks
        # for it.
        loop_data_dir = .

        # The name of the file.  It is written atomically (a temp file in
        # the same directory, then a rename), so a reader can never see a
        # partial write.
        filename = loop-data.txt

    # Which report's units, formats and language the values follow.
    [[Formatting]]
        # Conversions are decided by the report, not by the units stored in
        # the database, so each value lands in the file exactly as that
        # report would have rendered it -- page javascript can drop it
        # straight into HTML.  This is the sample report that ships with the
        # extension; point it at your own report once you write your own
        # live page.
        target_report = LoopDataReport

    # Your station's loop cadence.  Worth getting right -- see below.
    [[LoopFrequency]]
        # How often your station emits loop packets, in seconds.  LoopData
        # weights its accumulator entries with it, so a wrong value skews
        # averages while a period holds both database-seeded and live data
        # -- and windrun and every windrose distance and band time are wind
        # speed multiplied by this interval, so a wrong value scales those
        # and no period roll-over ever cleans them.  2.0 is what a Davis
        # Vantage emits.  Other drivers vary, and many have a polling
        # interval you chose yourself: use that number.
        seconds = 2.0

    # Copying the loop-data file to a remote webserver, at loop cadence.
    # Only needed if the machine serving your live page is not the machine
    # weewx runs on.  Passwordless ssh (public/private key) must be
    # configured from the account weewx runs under to remote_user on
    # remote_server.  See
    # https://chaunceygardiner.github.io/weewx-loopdata/rsync.html
    [[RsyncSpec]]
        # true to rsync the loop data file to remote_server.  While this is
        # false the rest of this section is ignored.
        enable = false

        # PLACEHOLDER -- replace with the server to copy the file to.
        remote_server = www.foobar.com

        # PLACEHOLDER -- replace with the userid on remote_server that has
        # write permission to remote_dir.
        remote_user = root

        # PLACEHOLDER -- replace with the directory on remote_server that
        # filename will be copied into.
        remote_dir = /home/weewx/loop-data

        # true to compress the file before sending it.
        compress = false

        # true to log every successful send, with timings (for debugging).
        log_success = false

        # I/O timeout in seconds.  Also bounds the ssh connect and
        # keepalive, so a dead or hanging remote cannot stall the loop
        # processing thread.  0 disables all time bounds.
        timeout = 1

        # Don't bother to rsync if the data is already older than this many
        # seconds: skip the packet and move on rather than shipping stale
        # data late.
        skip_if_older_than = 3

    # What to put in the file.  Nothing not listed here is written.
    [[Include]]

[StdReport]
    # The sample report that ships with the extension: a live instrument
    # panel that polls the loop-data file.  It is a working example to crib
    # from -- see
    # https://chaunceygardiner.github.io/weewx-loopdata/sample-skin.html
    [[LoopDataReport]]
        # Where this report's page is written.  The installer prefixed your
        # installation's own StdReport HTML_ROOT to the subdirectory the
        # extension asks for.
        HTML_ROOT = loopdata

        # false here turns the sample page off without uninstalling
        # anything.  It does not stop LoopData from writing the file.
        enable = true

        # The skin the extension installs.
        skin = LoopData

        # Options read by the sample page itself.
        [[[Extras]]]
            # The URL the page polls for the json file, relative to
            # HTML_ROOT above.  The default loop_data_dir writes the file
            # right beside the page, so a bare filename finds it.
            loop_data_file = loop-data.txt

            # Hours the page keeps polling before it gives up, so an
            # abandoned browser tab does not poll forever.  A mouse click
            # starts it again.
            expiration_time = 4

            # PLACEHOLDER -- choose your own password.  Loading the page as
            # ?pageUpdate=<this password> exempts it from expiring, which is
            # what a kiosk display wants.  Note the URL parameter is
            # pageUpdate, not page_update_pwd.  The password is visible to
            # anyone reading the page source.
            page_update_pwd = foobar

            # Fill in a google analytics id to have the page report to
            # google analytics.  Empty means no analytics at all.
            googleAnalyticsId = ""

            # Report to google analytics only when the page is served from
            # this hostname.  Empty means report from wherever it is served.
            analytics_host = ""

        # Formatting overrides for this report -- which, since it is the
        # target_report above, is also how LoopData formats the values it
        # writes into the file.
        [[[Units]]]
            # How a number of each unit is rendered.
            [[[[StringFormats]]]]
                # Whole-number wind speeds: the dial has no room for a
                # decimal, and gusts are not measured that finely.
                mile_per_hour = %.0f
                # One decimal place of temperature, as the readout under
                # each dial shows it.
                degree_C = %.1f
                # As mile_per_hour above.
                km_per_hour = %.0f
                # As degree_C above.
                degree_F = %.1f
"""

# Exactly the fields the sample report's instrument panel reads: .raw for
# needle/petal geometry, report-formatted for every readout, unit.label to
# pick the dial scales.
#
# This stays a Python list rather than a line in CONFIG above because
# ConfigObj has no line continuation for lists -- one line there would put
# every field in a single unreadable run, losing the gauge-by-gauge
# grouping that keeps this list in step with the manual's "What each gauge
# reads".  ConfigObj writes it out comma-separated either way, so the
# weewx.conf a user gets is the same.
FIELDS = [
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

    # The gauges below hide themselves on stations that do not report the
    # observation.
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
    ]

# The comment fields carries into weewx.conf.  It is attached here rather
# than written in CONFIG because the value is assigned here; comments live
# on the section, keyed by the option name, and conditional_merge copies
# them across with the value.
FIELDS_COMMENT = [
    '# The fields to write into the json file -- a bare comma-separated',
    '# list.  Each entry is a WeeWX report tag with the $ removed, and',
    '# becomes a key in the file.  The full grammar (rolling periods,',
    '# trends, spans, windrose, almanac and station tags, format specs)',
    '# is at',
    '# https://chaunceygardiner.github.io/weewx-loopdata/field-reference.html',
    '#',
    '# These are exactly the fields the sample report reads, and this is',
    '# the only fields line there is -- one [LoopData] section, one',
    '# loop-data file.  A live page of your own adds whatever it needs to',
    '# this same list; a field not named here is not written, and a page',
    '# polling for it gets nothing.',
    '#',
    '# An extension may add to the line for you: installing weewx-celestial',
    '# appends the entries its own page reads and leaves everything already',
    '# here untouched.  Restart weewxd afterwards so LoopData reloads the',
    '# line.',
    '#',
    '# A field entry containing a comma -- a formatting call with two',
    '# arguments, or an almanac tag with two keywords -- must be quoted, or',
    '# ConfigObj will split it at the comma into two bogus fields.',
]

def build_config():
    """CONFIG plus the fields line, as a ConfigObj.

    weeutil.config is imported HERE rather than at module scope so that
    loader()'s version guards below can still be reached.  A module-level
    import runs before loader() does, and on WeeWX 3 it does not survive:
    weeutil/config.py is missing entirely before 3.9, and 3.9's copy has no
    config_from_str (that arrived in 4.0.0b9).  Either way the user would
    get a raw traceback in place of "weewx-loopdata requires WeeWX 4".
    """
    import weeutil.config

    config = weeutil.config.config_from_str(CONFIG)
    config['LoopData']['Include']['fields'] = FIELDS
    config['LoopData']['Include'].comments['fields'] = FIELDS_COMMENT

    # A comment ahead of a top-level section header has to be attached to
    # the key: ConfigObj files everything above the first section header
    # under initial_comment, and conditional_merge does not carry that
    # across.
    config.comments['LoopData'] = [
        '',
        '#   This section is for the weewx-loopdata extension, which writes a',
        '#   json file of live values on every loop packet, for a web page to',
        '#   poll.  Every option is documented at',
        '#   https://chaunceygardiner.github.io/weewx-loopdata/',
        '',
    ]
    return config

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
            version = "6.11.3",
            name = 'loopdata',
            description = 'Loop statistics for real time reporting.',
            author = "John A Kline",
            author_email = "john@johnkline.com",
            report_services = 'user.loopdata.LoopData',
            config = build_config(),
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
                ('skins/LoopData/lang', [
                    'skins/LoopData/lang/en.conf',
                    'skins/LoopData/lang/de.conf',
                    'skins/LoopData/lang/fr.conf',
                    'skins/LoopData/lang/nl.conf',
                    'skins/LoopData/lang/es.conf',
                    'skins/LoopData/lang/da.conf',
                    'skins/LoopData/lang/it.conf',
                    'skins/LoopData/lang/no.conf',
                    'skins/LoopData/lang/sv.conf',
                    ]),
            ])
