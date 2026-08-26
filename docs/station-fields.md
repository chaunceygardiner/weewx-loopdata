---
title: Station fields
layout: default
nav_order: 10
---

# Station fields

[weewx-loopdata manual](https://chaunceygardiner.github.io/weewx-loopdata/) · [weewx-loopdata on GitHub](https://github.com/chaunceygardiner/weewx-loopdata) · [Report an issue](https://github.com/chaunceygardiner/weewx-loopdata/issues)

---

Any `$station` report tag can be listed as a field (6.3), written with the
`$` removed.  The values are evaluated against the exact object behind the
report tag (`weewx.station.Station`), so they render as the report would
render them.

Examples:

```
station.uptime.raw                WeeWX uptime in seconds
station.uptime.long_form()        e.g., 25 days, 21 hours, 15 minutes
station.os_uptime.raw             server uptime in seconds
station.os_uptime.long_form()
station.version                   the WeeWX version, e.g., 5.4.0
station.python_version
station.hardware                  e.g., Vantage
station.location                  [Station] location from weewx.conf
station.altitude                  e.g., 700 feet
station.altitude.meter.raw        unit conversions work as in report tags
station.latitude                  ["37", "24.00", "N"] — the same (degrees,
                                  minutes, hemisphere) parts the report tag
                                  exposes, as a json array
```

Those are examples, not the whole set.  `$station` is WeeWX's own
`Station` object, so anything it exposes can be a field.  In current WeeWX
that is `uptime`, `os_uptime`, `version`, `python_version`, `altitude`,
`latitude`, `longitude`, `rain_year_str`, and — reached through the station
info it carries — `hardware`, `location`, `station_url`, `webpath`,
`week_start`, `rain_year_start`, `altitude_vt`, `latitude_f` and
`longitude_f`.  If a `$station` tag works in a Cheetah template, it works
here.

## The point: a restart-correct live uptime

The point of station fields is `uptime` and `os_uptime`: they are recomputed
on every loop packet, so a live uptime readout is correct within a packet or
two of a weewxd or server restart.

The report-cycle alternative — shipping `$station.uptime.raw` in a template
and extrapolating in javascript — is wrong after every restart: it shows the
pre-restart uptime still climbing until the next report cycle runs.

Every other `$station` attribute is constant for the life of the weewxd
process; loopdata computes those once and repeats the value in every write,
so a page that is not itself a WeeWX report can still show them.

## Notes

* Station fields are current-only: no period prefix, no aggregate.
* `round(n)` and the format specs/calls work as on observation and almanac
  fields: `.raw` on `uptime`/`os_uptime`/`altitude` (ValueHelpers), identity
  on plain values (`station.week_start.raw`); `.formatted`, `format(...)`,
  `nolabel(...)`, `string(...)`, `long_form(...)` on the ValueHelpers only.
* Unit conversions chain as in report tags: `station.altitude.meter.raw`.
* A `$station` attribute whose value is a tuple of scalars (e.g.
  `station.latitude`'s degrees/minutes/hemisphere) is emitted as a json
  array.
* The json key is the field entry verbatim, so element ids can match keys
  as usual.
