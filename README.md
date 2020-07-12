# weewx-loopdata
*Open source plugin for WeeWX software.

Copyright (C)2020 by John A Kline (john@johnkline.com)

**This extension requires Python 3.7 or later and WeeWX 4.**

## Description

LoopData is a WeeWX service that generates a json file (loop-data.txt)
containing values for the observations in the loop packet.

For scalar observations (i.e., everything but wind), today's high, low,
sum, average and weighted averages are also availble.

For vector observations (i.e. wind), today's high speed/direction and
today's low speed are available.  Additionally available are average,
RMS average, vector average and vector direction.

Use LoopData, and some JavaScript to make your report pages update on
every loop cycle.

A WeeWX report is specified in the LoopData configuration (e.g.,
`SeasonsReport`).  With this information, LoopData automatically converts
all values to the units called for in the report and also formats all
readings according to the report specified.  Thus, it is simple to replace
the reports observations in JavaScript as they will already be in the
correct units and in the correct format.

The keys in the json file use WeeWX cheetah syntax.

For example, the current outside temperature can be included as:
* `current.outTemp.formatted`: 79.2
* `current.outTemp`          : 79.2°F
* `current.outTemp.raw`      : 79.175

The day average of outside tempeture can be included as:
* `day.outTemp.avg.formatted`: 64.7
* `day.outTemp.avg`          : 64.7°
* `day.outTemp.avg.raw`      : 64.711

In addition to $current and $day, a few special cases are supported:

* trend.barometerRate
* trend.barometerRate.desc (e.g., rising slowly -- currently not localized)
* 10m.windGust.max
* 10m.windGust.maxtime

Note: week, month year and rainYear are under consideration.

Typically, loop packets are generated frequently in WeeWX
(e.g, every 2s).  With the constantly updating loop-data.txt file, one
can write javascript to update a report's html page in near real time.

LoopData should be able to show any obervation in the loop packet and
any observation for which there is a day accumulator.

If and current.<observation> is included in fields, but it is missing from
a packet, it will not be present in loop-data.txt.

Similarly, if a day.<observation> does has not stats for the day, it
will not be present in loop-data.txt.

LoopData is easy to set up.  Specify the report to target and the fields
to be included and a json file will be generated on every loop.
All will be formatted and converted for that report.  That is,
the units and format for observations areappropriate for the targeted
report.

From there, just write JavaScript to load the json file on a regular basis
(typically, at the same rate as loop packets are generated in WeeWX) and update
your report's html page.

### Why?  What does it do?

LoopData writes a json file for every loop packet generated by WeeWX.  Just add
some javascript to a skin to take advantage of the json file to update the
report in near real time.

See weewx-loopdata in action with a WeatherBoard&trade; skin at
[www.paloaltoweather.com/weatherboard/](https://www.paloaltoweather.com/weatherboard/)
and in a "LiveSeasons" skin at
[www.paloaltoweather.com/](https://www.paloaltoweather.com/).

A WeatherBoard&trade; screenshot is below.

![Weatherbaord&trade; Report](WeatherBoard.png)

This extension was inspired by [weewx-realtime_gauge_data](https://github.com/gjr80/weewx-realtime_gauge-data).
This does not attempt to duplicate Gary's fantastic realtime gauge data plugin
for SteelSeries gauges.  In fact, I use that great extension [here](https://www.paloaltoweather.com/LiveSeasonsGauges/).
If you want to power Steel Series gauges from WeeWX, you definitely want to use Gary's extension.

# Installation Instructions

1. cd to the directory where you have cloned this extension, e.g.,

   `cd ~/software/weewx-loopdata`

1. Run the following command.

   `sudo /home/weewx/bin/wee_extension --install .`

   Note: this command assumes weewx is installed in /home/weewx.  If it's installed
   elsewhere, adjust the path of wee_extension accordingly.

1. The install creates a LoopData section in weewx.conf as shown below.  Adjust
   the values accordingly.  In particular, specify the `target_report` for the
   report you wish to use for formatting and units.

```
[LoopData]
    [[FileSpec]]
        loop_data_dir = /home/weewx/loop-data
        filename = loop-data.txt
    [[Formatting]]
        target_report = SeasonsReport
    [[RsyncSpec]]
        enable = false
        remote_server = foo.bar.com
        remote_user = root
        remote_dir = /home/weewx/loop-data
        compress = False
        log_success = False
        ssh_options = "-o ConnectTimeout=1"
        timeout = 1
        skip_if_older_than = 3
    [[Include]]
        fields = current.dateTime.raw, current.windDir.ordinal_compass, day.rain.sum, current.dewpoint, current.outTemp, current.rainRate, current.windSpeed, day.windGust.max, 10m.windGust.max, current.windSpeed
```

## Entries in `LoopData` sections of `weewx.conf`:
 * `loop_data_dir`     : The directory into which the loop data file should be written.
 * `filename`          : The name of the loop data file to write.
 * `target_report`     : The WeeWX report to target.  LoopData will use this report to determine the
                         units to use and the formatting to apply.
 * `enable`            : Set to true to rsync the loop data file to `remote_server`.
 * `remote_server`     : The server to which gauge-data.txt will be copied.
                         To use rsync to sync loop-data.txt to a remote computer, passwordless ssh
                         using public/private key must be configured for authentication from the user
                         account that weewx runs under on this computer to the user account on the
                         remote machine with write access to the destination directory (remote_dir).
 * `remote_user`       : The userid on remote_server with write permission to remote_server_dir.
 * `remote_directory`  : The directory on remote_server where filename will be copied.
 * `compress`          : True to compress the file before sending.  Default is False.
 * `log_success`       : True to write success with timing messages to the log (for debugging).
                         Default is False.
 * `ssh_options`       : ssh options Default is '-o ConnectTimeout=1' (When connecting, time out in
                       1 second.)
 * `timeout`           : I/O timeout. Default is 1.  (When sending, timeout in 1 second.)
 * `skip_if_older_than`: Don't bother to rsync if greater than this number of seconds.  Default is 4.
                         (Skip this and move on to the next if this data is older than 4 seconds.
 * `fields`            : Used to specify which fields to include in the file.  If fields is missing
                         and Rename (see below) is also missing, all fields are included.
 * `Rename`            : Used to specify which fields to include and which names should be used as
                         keys (i.e., what these fields should be renamed.  If neither Rename nor fields
                         is specified, all fields are included.

## What fields are available.

Generally, if you can specify a field in a Cheetah template, and that field begins with $current
or $day, you can specify it here.

unit.label.<obs> is also supported.

trend.barometer is supported, but it is always a 3 hour trend.

10m.windGust.max
10m.windGust.maxtime

## Rsync isn't Working for Me, Help!
LoopData's uses WeeWX's `weeutil.rsyncupload.RsyncUpload` utility.  If you have rsync working
for WeeWX to push your web pages to a remote server, loopdata's rsync is likely to work too.
First get WeeWX working with rsync before you try to get loopdata working with rsync.

By the way, it's best to put loop-data.txt outside of WeeWX's html tree so that WeeWX's rsync
and loopdata's rsync don't both write the loop-data.txt file.  If you're up for configuring
your websever to move it elsewhere (e.g., /home/weewx/loopdata/loop-data.txt), you should
do so.  If not, it's probably OK.  There just *might* be the rare complaint in the log
because the WeeWX main thread and the LoopData thread both tried to sync the same file at
the same time.

## Do I have to use rsync to sync loop-data.txt to a remote server?
You don't have to sync to a remote server; but if you do want to sync to a remote server,
rsync is the only mechanism provided.  That's not going to change as the author believes
rsync/ssh is the secure way to accomplish this task.

## About those Rsync Errors in the Log
If one is using rsync, especially if the loop interval is short (e.g., 2s), it is expected that
there will be log entries for connection timeouts, transmit timeouts, write errors and skipped
packets.  By default only one second is allowed to connect or transmit the data.  Also, by
default, if the loop data is older than 3s, it is skipped.  With these settings, the remote
server may miss receiving some loop-data packets, but it won't get caught behind trying to send
a backlog of old loop data.

Following are examples of a connection timeout, transmission timeout, writer error and a skipped
packet.  These errors are fine in moderation.  If too many packets are timing out, one might try
changing the connection timeout or timeout values.
```
Jul  1 04:12:03 charlemagne weewx[1126] ERROR weeutil.rsyncupload: [['rsync', '--archive', '--stats', '--timeout=1', '-e ssh -o ConnectTimeout=1', '/home/weewx/gauge-data/loop-data.txt', 'root@www.paloaltoweather.com:/home/weewx/gauge-data/loop-data.txt']] reported errors: ssh: connect to host www.paloaltoweather.com port 22: Connection timed out. rsync: connection unexpectedly closed (0 bytes received so far) [sender]. rsync error: unexplained error (code 255) at io.c(235) [sender=3.1.3]
Jun 30 20:51:48 charlemagne weewx[1126] ERROR weeutil.rsyncupload: [['rsync', '--archive', '--stats', '--timeout=1', '-e ssh -o ConnectTimeout=1', '/home/weewx/gauge-data/loop-data.txt', 'root@www.paloaltoweather.com:/home/weewx/gauge-data/loop-data.txt']] reported errors: [sender] io timeout after 1 seconds -- exiting. rsync error: timeout in data send/receive (code 30) at io.c(204) [sender=3.1.3]
Jun 27 10:18:37 charlemagne weewx[17982] ERROR weeutil.rsyncupload: [['rsync', '--archive', '--stats', '--timeout=1', '-e ssh -o ConnectTimeout=1', '/home/weewx/gauge-data/loop-data.txt', 'root@www.paloaltoweather.com:/home/weewx/gauge-data/loop-data.txt']] reported errors: rsync: [sender] write error: Broken pipe (32). rsync error: error in socket IO (code 10) at io.c(829) [sender=3.1.3]
Jun 27 23:15:53 charlemagne weewx[10156] INFO user.loopdata: skipping packet (2020-06-27 23:15:50 PDT (1593324950)) with age: 3.348237
```

## Why require Python 3.7 or later?

LoopData is a new extension.  The author believes software written after Python 2 end of life
should not target Python 2.  The code includes type annotation which do not work with Python 2,
nor earlier versions of Python 3.

## Licensing

weewx-loopdata is licensed under the GNU Public License v3.
