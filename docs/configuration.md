---
title: Configuration
layout: default
nav_order: 3
---

# Configuration

All configuration lives in the `[LoopData]` section of weewx.conf, which the
installer creates.  This page documents every option; the
[sample configuration](#sample-configuration) at the bottom is exactly what a
fresh install writes.

## `[[FileSpec]]`

| Option | Meaning |
|---|---|
| `loop_data_dir` | The directory into which the loop data file should be written.  If a relative path is specified, it is relative to the `target_report` directory. |
| `filename` | The name of the loop data file to write (`loop-data.txt` by default). |

The file is written atomically (a temp file in the same directory, then a
rename), so a reader can never see a partial write.

## `[[Formatting]]`

| Option | Meaning |
|---|---|
| `target_report` | The WeeWX report to target.  LoopData uses this report to determine the units to use and the formatting to apply.  Also, if `loop_data_dir` is a relative path, it is relative to the directory of `target_report`.  When LoopData is first installed, `target_report` is set to the sample report included with the extension: `LoopDataReport`. |

Because every value is converted and formatted per the target report, page
javascript can drop values straight into HTML — they are already in the
correct units and format.  Individual fields can override the unit or
formatting; see the [field reference](field-reference.html).

## `[[LoopFrequency]]`

| Option | Meaning |
|---|---|
| `seconds` | The frequency of loop packets emitted by your device.  This is needed to give the proper weight to accumulator entries.  For example, this value is `2.0` for Davis Vantage Pro 2 and RainWise CC3000 devices. |

{: .important }
It is crucial to specify the correct loop frequency.  For vantage and cc3000,
this will be 2 seconds.

## `[[RsyncSpec]]`

Only needed if you push loop-data.txt to a remote webserver — see
[Syncing to a remote server](rsync.html) for the how and why.

| Option | Meaning |
|---|---|
| `enable` | Set to `true` to rsync the loop data file to `remote_server`. |
| `remote_server` | The server to which the loop data file will be copied.  Passwordless ssh using public/private key must be configured from the user account weewx runs under to the account on the remote machine with write access to `remote_dir`. |
| `remote_user` | The userid on `remote_server` with write permission to `remote_dir`. |
| `remote_dir` | The directory on `remote_server` where `filename` will be copied. |
| `compress` | `True` to compress the file before sending.  Default is `False`. |
| `log_success` | `True` to write success-with-timing messages to the log (for debugging).  Default is `False`. |
| `ssh_options` | Extra options for the ssh transport (e.g., a key file or port).  Whether or not this is set, LoopData appends safety bounds for any keyword you don't set yourself: `-o ConnectTimeout=<timeout>`, `-o ServerAliveInterval=<timeout>`, `-o ServerAliveCountMax=2` and `-o BatchMode=yes`, so a dead or hanging remote cannot stall the loop processing thread (an option you set always wins). |
| `timeout` | I/O timeout.  Default is `1` (when sending, time out in 1 second).  Also used for the ssh `ConnectTimeout` and `ServerAliveInterval` bounds described under `ssh_options`.  `0` disables all time bounds. |
| `skip_if_older_than` | Don't bother to rsync if the data is older than this number of seconds.  Default is `3`.  (Skip it and move on to the next packet rather than shipping stale data late.) |

## `[[Include]]`

| Option | Meaning |
|---|---|
| `fields` | The fields to include in the json file — a bare comma-separated list.  Each entry is a report tag with the `$` removed; each becomes a key in loop-data.txt.  The complete grammar is in the [field reference](field-reference.html). |

The json file will only include the fields specified here (plus the
automatic `windrose.bands` key when any [windrose](windrose.html) field is
configured).

{: .note }
A field entry containing a comma — a formatting call with two arguments, or
an almanac tag with two keywords — must be quoted, or ConfigObj will split
the entry at the comma into two bogus fields:
`fields = ..., 'day.rain.sum.format("%.2f", add_label=False)', ...`

## `[[BarometerTrendDescriptions]]`

The descriptions associated with `trend.barometer.desc`.  Localize as
necessary:

```
[LoopData]
    [[BarometerTrendDescriptions]]
        RISING_VERY_RAPIDLY = Rising Very Rapidly
        RISING_QUICKLY = Rising Quickly
        RISING = Rising
        RISING_SLOWLY = Rising Slowly
        STEADY = Steady
        FALLING_SLOWLY = Falling Slowly
        FALLING = Falling
        FALLING_QUICKLY = Falling Quickly
        FALLING_VERY_RAPIDLY = Falling Very Rapidly
```

`trend.barometer.code` (an integer from `-4` through `4`) is the
language-neutral companion; see
[Special fields](field-reference.html#special-fields).

## `windrose_bands`

Overrides the wind-speed band edges used by the
[windrose observation](windrose.html).  Ascending edges, in the target
report's windSpeed unit; the first edge doubles as the calm threshold:

```
[LoopData]
    windrose_bands = 1, 4, 8, 13, 19, 25
```

The default is the classic WRPLOT/NOAA bands (0.5, 2.1, 3.6, 5.7, 8.8 and
11.1 m/s, converted to the report's unit).  See
[Choosing your bands](windrose.html#choosing-your-bands) for how to pick
edges suited to your site.

## The trend window (`time_delta`)

The trend window *cannot* be changed on a case-by-case basis, but it can be
changed for the entire target report, using the standard WeeWX
customization — in the target report's configuration:

```
    [[[Units]]]
        [[[[Trend]]]]
            time_delta = 86400    # for a 24 hour trend.
```

The default is 10800 (3 hours) — a WeeWX default.  If `time_delta` is greater
than 259200 (3 days), LoopData caps it at 259200.

## Sample configuration

Fresh installs add the following `[LoopData]` section to weewx.conf.  The
`fields` line is exactly the fields the
[sample report's instrument panel](sample-skin.html) reads.  Upgrading
installs keep whatever `fields` line is already in weewx.conf — to adopt the
panel, replace your `fields` line with this one (appending any fields other
pages of yours use).

```
[LoopData]
    [[FileSpec]]
        loop_data_dir = .
        filename = loop-data.txt
    [[Formatting]]
        target_report = LoopDataReport
    [[LoopFrequency]]
        seconds = 2.0
    [[RsyncSpec]]
        enable = false
        remote_server = foo.bar.com
        remote_user = root
        remote_dir = /var/www/html
        compress = False
        log_success = False
        timeout = 1
        skip_if_older_than = 3
    [[Include]]
        fields = current.dateTime.raw, current.outTemp, current.outTemp.raw, day.outTemp.min.raw, day.outTemp.max.raw, day.outTemp.min.formatted, day.outTemp.max.formatted, current.outHumidity, current.outHumidity.raw, day.outHumidity.min.raw, day.outHumidity.max.raw, current.windSpeed, current.windSpeed.raw, current.windDir.raw, current.windDir.ordinal_compass, 10m.windGust.max, 10m.wind.gustdir.raw, 10m.wind.gustdir.ordinal_compass, current.barometer, current.barometer.raw, trend.barometer.raw, trend.barometer.desc, current.rainRate, current.rainRate.raw, day.rain.sum, day.rain.sum.raw, day.rainRate.max, day.rainRate.max.raw, current.dewpoint, current.dewpoint.raw, day.dewpoint.min.raw, day.dewpoint.max.raw, day.dewpoint.min.formatted, day.dewpoint.max.formatted, current.appTemp, current.appTemp.raw, day.appTemp.min.raw, day.appTemp.max.raw, day.appTemp.min.formatted, day.appTemp.max.formatted, current.UV, current.UV.raw, day.UV.max, current.radiation, current.radiation.raw, day.radiation.max, current.pm2_5, current.pm2_5_aqi.raw, current.pm2_5_aqi.formatted, day.windrose.banded, day.windrose.calm, unit.label.outTemp, unit.label.barometer, unit.label.rain, unit.label.rainRate, unit.label.windSpeed
    [[BarometerTrendDescriptions]]
        RISING_VERY_RAPIDLY = Rising Very Rapidly
        RISING_QUICKLY = Rising Quickly
        RISING = Rising
        RISING_SLOWLY = Rising Slowly
        STEADY = Steady
        FALLING_SLOWLY = Falling Slowly
        FALLING = Falling
        FALLING_QUICKLY = Falling Quickly
        FALLING_VERY_RAPIDLY = Falling Very Rapidly
```
