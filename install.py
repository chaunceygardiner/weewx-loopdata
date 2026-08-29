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

import re
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
# already has the option.  That is why an option shown COMMENTED OUT stays
# commented out: written live it would freeze this station on today's
# default for ever, while left commented the extension's own fallback
# answers -- including a better one a later release brings.
#
# ORDER MATTERS.  ConfigObj attaches a comment block to the NEXT key and
# writes it at THAT key's indent, so every section here ends with a LIVE
# key.  Last in its section a comment block lands outside the block it
# documents -- or, when the key it attaches to already exists in the
# target weewx.conf ([StdReport] always does), is dropped without a
# trace.
#
# No fields line and no target_report: since 7.0 a report declares the
# fields it needs in its own skin.conf ([LoopData] [[fields]] -- see
# skins/LoopData/skin.conf), and every declaring report is its own target.
# The old [[Include]] fields / [[Formatting]] target_report pair is still
# honored on an upgraded station (rendered flat, as before) but no longer
# written; a later release removes it once every extension that used it
# declares its fields.
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
        # the sample report's directory (LoopDataReport below), so the
        # default writes the file beside the sample report's page, which is
        # where that page looks for it.
        loop_data_dir = .

        # The name of the file.  It is written atomically (a temp file in
        # the same directory, then a rename), so a reader can never see a
        # partial write.
        filename = loop-data.txt

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

        # true to compress the file before sending it.
        #compress = false

        # true to log every successful send, with timings (for debugging).
        #log_success = false

        # I/O timeout in seconds.  Also bounds the ssh connect and
        # keepalive, so a dead or hanging remote cannot stall the loop
        # processing thread.  0 disables all time bounds.
        #timeout = 1

        # Don't bother to rsync if the data is already older than this many
        # seconds: skip the packet and move on rather than shipping stale
        # data late.
        #skip_if_older_than = 3

        # PLACEHOLDER -- replace with the server to copy the file to.
        remote_server = www.foobar.com

        # PLACEHOLDER -- replace with the userid on remote_server that has
        # write permission to remote_dir.
        remote_user = root

        # PLACEHOLDER -- replace with the directory on remote_server that
        # filename will be copied into.
        remote_dir = /home/weewx/loop-data

[StdReport]
    # A report that generates nothing, for fields read by something that is
    # not a report at all -- a shell script, an SNMP extension, a monitoring
    # check.  Enable it and declare those fields here, and they arrive in
    # the loop-data file under "ScriptData", rather than being parked on a
    # page's report where an upgrade would overwrite them.  See
    # https://chaunceygardiner.github.io/weewx-loopdata/declaring-fields.html
    [[ScriptData]]
        # false because most stations have no such script.  Nothing is
        # generated for this report either way -- its skin defines no
        # generators -- but LoopData reads a report's fields only while it
        # is enabled.
        enable = false

        # Your fields go here, in named groups, exactly as a skin declares
        # them.  For example:
        #     [[[LoopData]]]
        #         [[[[fields]]]]
        #             my_script = current.extraTemp2.raw
        #
        # The skin below exists only because WeeWX requires every report to
        # name one; it contains no templates.
        skin = ScriptData

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
        # anything.
        enable = true

        # The skin the extension installs.
        skin = LoopData

        # Options read by the sample page itself.
        [[[Extras]]]
            # The URL the page polls for the json file, relative to
            # HTML_ROOT above.  The default loop_data_dir writes the file
            # right beside the page, so a bare filename finds it.
            #loop_data_file = loop-data.txt

            # Hours the page keeps polling before it gives up, so an
            # abandoned browser tab does not poll forever.  A mouse click
            # starts it again.
            #expiration_time = 4

            # EXAMPLE, not a default: fill in your own google analytics
            # measurement id and uncomment to have the page report to it.
            # Left out, as it is here, the page loads nothing from google
            # and reports nothing.
            #googleAnalyticsId = G-XXXXXXXXXX

            # EXAMPLE, not a default: uncomment with your own hostname to
            # report only when the page is served from that host, which
            # keeps a copy you are testing locally out of your figures.
            # Left out, the page reports from wherever it is served.  It
            # does nothing unless googleAnalyticsId is set.
            #analytics_host = www.example.com

            # PLACEHOLDER -- choose your own password.  Loading the page as
            # ?pageUpdate=<this password> exempts it from expiring, which is
            # what a kiosk display wants.  Note the URL parameter is
            # pageUpdate, not page_update_pwd.  The password is visible to
            # anyone reading the page source.
            page_update_pwd = foobar

        # Formatting overrides for this report -- which is also how
        # LoopData formats the values it writes into the file for this
        # report's page.
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

def build_config():
    """CONFIG as a ConfigObj.

    weeutil.config is imported HERE rather than at module scope so that
    loader()'s version guards below can still be reached.  A module-level
    import runs before loader() does, and on WeeWX 3 it does not survive:
    weeutil/config.py is missing entirely before 3.9, and 3.9's copy has no
    config_from_str (that arrived in 4.0.0b9).  Either way the user would
    get a raw traceback in place of "weewx-loopdata requires WeeWX 4".
    """
    import weeutil.config

    config = weeutil.config.config_from_str(CONFIG)

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

def version_tuple(version):
    """'4.10.2' -> (4, 10, 2); compares as numbers, not as strings.

    The service's copy (user.loopdata.version_tuple) is the original and
    is preferred when importable.  On an upgrade that will usually be the
    PREVIOUSLY installed loopdata rather than this one -- and before 7.0
    it had no version_tuple at all -- so the local fallback below is the
    normal path at install time.  The two are the same rule, written
    twice only because the installer runs before its own files are in
    place.
    """
    try:
        import user.loopdata
        return user.loopdata.version_tuple(version)
    except Exception:
        return tuple(int(part) for part in re.findall(r'\d+', str(version))[:3])

def loader():
    if sys.version_info[0] < 3 or (sys.version_info[0] == 3 and sys.version_info[1] < 7):
        sys.exit("weewx-loopdata requires Python 3.7 or later, found %s.%s" % (
            sys.version_info[0], sys.version_info[1]))

    if version_tuple(weewx.__version__) < (4, 6):
        sys.exit("weewx-loopdata requires WeeWX 4.6 or later, found %s" % weewx.__version__)

    return LoopDataInstaller()

class LoopDataInstaller(ExtensionInstaller):
    def __init__(self):
        super(LoopDataInstaller, self).__init__(
            version = "7.0",
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
                ('skins/ScriptData', [
                    'skins/ScriptData/skin.conf',
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
