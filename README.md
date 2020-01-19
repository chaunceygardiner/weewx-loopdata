# weewx-loopdata
*Open source plugin for WeeWX software.

## Description

A WeeWX service to generate a json file (typically, loop-data.txt)
containing observations from loop packets as they are generated in
weewx.

Copyright (C)2020 by John A Kline (john@johnkline.com)

### Why?  What does it do?

LoopData writes a json file for every loop packet received.  With some
javascript, one can have reports like this [Weatherbaord(TM) Report]
(https://www.paloaltoweather.com/weatherboard.html) and this
[LiveSeasons Report](https://www.paloaltoweather.com/index.html).
Note: Skins coming soon!

**THIS PLUGIN REQUIRES PYTHON 3 AND WEEWX 4**

Inspired by https://github.com/gjr80/weewx-realtime_gauge-data.  This does not attempt to duplicate
Gary's realtime gauge data plugin for the SteelSeries gauges.  To power Steel Series gauges from
WeeWX, you definitely want to use weewx-realtime_gauge_data.

# Installation Instructions
1. Run the following command.

`sudo /home/weewx/bin/wee_extension --install ~/software/weewx-loopdata`

Note: The above command assumes a WeeWX installation of `/home/weewx` and
      that this extension was downloaded to `~/software/weewx-loopdata`.
      Adjust the command as necessary.

The above command will insert a section in weewx.conf similar to the following.
This section is explained below.

```
[LoopData]
    [[FileSpec]]
        loop_data_dir = /home/weewx/loop-data
        filename = loop-data.txt
    [[Formatting]]
        target_report = LiveSeasonsReport
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
        fields = dateTime, windSpeed, COMPASS_windDir, DESC_barometerRate, FMT_barometer
    [[Rename]]
        windRose = WindRose
```

## Fields in `LoopData` sections of `weewx.conf`:
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

## List of all fields available:
 * `dateTime`          : The time of this loop packet (seconds since the epoch).
 * `usUnits`           : The units system all obeservations are expressed in.
                       This will be the unit system of the report specified by
                       target_report in weewx.conf.
 * `outTemp`           : Outside temperature.
 * `inTemp`            : Inside temperature.
 * `outHumidity`       : Outside humidity.
 * `pressure`          : Pressure
 * `windSpeed`         : Wind Speed
 * `windDir`           : Wind Direction
 * `windGust`          : Wind Gust (high wind speed)
 * `windGustDir`       : Wind Gust Direction
 * `day_rain_total`    : Total rainfall today.
 * `rain`              : Rain
 * `altimeter`         : Altimeter
 * `appTemp`           : Apparent Temperature Outside
 * `barometer`         : Barometer
 * `beaufort`          : Beaufort Wind Scale rating
 * `cloudbase`         : Cloudbase Elevation
 * `dewpoint`          : Dew Point
 * `heatindex`         : Heat Index
 * `humidex`           : Humidity Index
 * `maxSolarRad`       : Maximum Solar Radiation
 * `rainRate`          : Rate of Rain
 * `windchill`         : Wind Chill Factor
 * `FMT_<obs>`         : The above observations expressed as a formatted value, including
                       the units (e.g., '4.8 mph').
 * `LABEL_<obs>`       : The label for the units associted with the observation (e.g., 'mph').
                       This label also applies to the high and low fields for this observation.
 * `UNITS_<obs>`       : The units that the observation is expressed in.  Also the units
                       for the corresponding HI and LO entries.  Example: 'mile_per_hour'.
 * `LO_<obs>`          : The minimum value of the observation today.
 * `FMT_LO_<obs>`      : The low observation expressed as a formatted value, including
                       the units (e.g., '4.8 mph').
 * `T_LO<obs>`         : The time of the daily minimum observation.
 * `HI<obs>`           : The maximum value of the observation today.
 * `FMT_HI_<obs>`      : The high observation expressed as a formatted value, including
                       the units (e.g., '4.8 mph').
 * `T_HI<obs>`         : The time of the daily maximum observation.
 * `COMPASS_<obs>`     : For windDir and windGustDir, text expression for the direction
                       (.e., 'NNE').
 * `10mMaxGust`        : The maximum wind gust in the last 10m.
 * `T_10mMaxGust`      : The time of the max gust (seconds since the epoch).
 * `FMT_10mMaxGust`    : 10mMaxGust expressed as a formatted value ('8.6 mph').
 * `LABEL_10mMaxGust`  : The label of the units for 10mMaxGust (e.g., 'mph').
 * `UNITS_10mMaxGust`  : The units that 10mMaxGust is expressed in (e.g., 'mile_per_hour').
 * `barometerRate`     : The difference in barometer in the last 3 hours
                       (i.e., barometer_3_hours_ago - barometer_now)
 * `DESC_barometerRate`: Shipping forecast descriptions for the 3 hour change in
                       barometer readings (e.g., "Falling Slowly').
 * `FMT_barometerRate`:  Formatted baromter rate (e.g., '0.2 inHg/h').
 * `UNITS_barometerRate`:The units used in baromter rate (e.g., 'inHg_per_hour').
                       barometer readings (e.g., "Falling Slowly').
 * `LABEL_barometerRate`:The label used for baromter rate units (e.g., 'inHg/hr').
 * `windRose`          : An array of 16 directions (N,NNE,NE,ENE,E,ESE,SE,SSE,S,SSW,SW,
                       WSW,W,WNW,NW,NNW) containing the distance traveled in each 
                       direction.)
 * `LABEL_windRose`    : The label of the units for windRose (e.g., 'm')
 * `UNITS_windRose`    : The units that windrose values are expressed in (e.g., 'mile').

##Licensing

WeeWX is licensed under the GNU Public License v3.
