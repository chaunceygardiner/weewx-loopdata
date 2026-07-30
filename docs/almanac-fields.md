---
title: Almanac fields
layout: default
nav_order: 6
---

# Almanac fields

Any WeeWX report almanac tag can be listed as a field: write the tag as it
would appear in a Cheetah template, with the `$` removed.  The values are
computed with whatever almanac WeeWX has registered —
[weewx-skyfield](https://github.com/chaunceygardiner/weewx-skyfield) for the
full Skyfield experience, PyEphem if installed, or WeeWX's built-in fallback
(sunrise, sunset and moon phase only) — and are converted and formatted per
the target report, exactly as the report tag would render.  New in 5.0.

Examples:

```
almanac.sunrise                                      06:47 (formatted per the report)
almanac.sunrise.raw                                  1593611225 (unix epoch seconds)
almanac.sunset.raw
almanac.moon_phase                                   Waxing gibbous
almanac.moon_index                                   the 0-7 moon phase index
almanac.moon.phase                                   percent of the moon illuminated
almanac.sun.az                                       sun azimuth in decimal degrees
almanac.sun.alt                                      sun altitude in decimal degrees
almanac.sun.transit.raw
almanac.sun.visible.raw                              length of daylight, in the report's units
almanac.sun.visible.second.raw                       length of daylight, pinned to seconds
almanac.sunrise.unix_epoch.raw                       sunrise, pinned to epoch seconds
almanac.moon.rise.raw
almanac.mars.earth_distance                          in AU, as in reports
almanac.next_full_moon.raw
almanac.next_solstice.raw
almanac(horizon=-6).sun(use_center=1).rise.raw       civil dawn
almanac(horizon=-6).sun(use_center=1).set.raw        civil dusk
almanac(horizon=-12).sun(use_center=1).rise.raw      nautical dawn
almanac(horizon=-18).sun(use_center=1).rise.raw      astronomical dawn
```

## A complete example: a live sky

[weewx-celestial](https://github.com/chaunceygardiner/weewx-celestial) is a
whole page built from these fields: its Geocentric panel — every body (sun,
moon, the eight planets, Proxima Centauri) placed by compass bearing and
distance, the moon at its true phase, odometer distance readouts ticking
between loop packets — is drawn entirely from loopdata almanac fields.  It
runs no service and computes nothing itself; loopdata evaluates the almanac
tags on every loop packet, and the page reads loop-data.txt.  See it live at
[www.paloaltoweather.com/celestial/](https://www.paloaltoweather.com/celestial/).

## The `days=±N` extension

One loopdata extension to the report grammar: `almanac(days=±N)` evaluates
the almanac at the same wall-clock time N *local calendar* days away.  For
example, `almanac(days=1).sunrise.raw` is tomorrow's sunrise and
`almanac(days=-1).sun.visible.raw` is yesterday's length of day.  (Reports
express this with `$almanac(almanac_time=$time_ts+86400)`, which needs
Cheetah variables that a config line doesn't have; `days=` is also
DST-correct where ±86400 is not.)

## Pinning units

By default almanac values are converted per the target report, exactly as the
report tag would render — including any `[Units]` `[[Groups]]` overrides the
report carries.  That is right for formatted values, but it means a `.raw`
field can change meaning with the report's settings: a target report that sets
`group_deltatime = hour` makes `almanac.sun.visible.raw` emit hours instead of
seconds.

A unit segment pins the unit regardless — the same override
[observation fields](field-reference.html#overriding-the-unit-of-a-field)
(`day.outTemp.avg.degree_C`) and [station fields](station-fields.html)
(`station.altitude.meter.raw`) already take:

```
almanac.sunrise.unix_epoch.raw               epoch seconds, always
almanac.sun.visible.second.raw               seconds of daylight, always
almanac.sun.visible.hour.round(2).raw        hours, rounded — the unit sits
                                             before round(n) and the format spec
```

Pin the unit on any `.raw` field your javascript consumes numerically.

## Notes

* Almanac fields are current-only: they take no period prefix and no
  aggregate (`10m.almanac...` and `almanac.sunrise.max` are not valid).
* `.raw`/`.formatted`/`.ordinal_compass` work on tags that return formatted
  values (times, and angles like `almanac.sun.azimuth`); `.raw` on a plain
  number (e.g., `almanac.moon_index.raw`) is allowed and returns the number
  unchanged.
* The formatting calls and `round(n)` work here too, exactly as on report
  almanac tags: `almanac.sunrise.format("%H:%M")`,
  `almanac.sun.az.format("%.1f", add_label=False)`,
  `almanac.sun.az.round(1).raw`.
* The json key is the field entry verbatim, so element ids can match keys
  as usual.
* A call with more than one keyword contains a comma, so the entry must be
  quoted in weewx.conf:
  `fields = ..., "almanac(pressure=0, horizon=-8).sun.rise.raw", ...`.
  None of the standard entries above need quoting.
* A field whose endpoint is a tuple of scalars is emitted as a json array —
  e.g. a field ending in `moon_phases`.
* Temperature and pressure for refraction come from the current loop packet
  (the report analog: Cheetah uses the latest archive record).

## Cost is managed automatically

Almanac fields touch no accumulators and are evaluated on LoopData's own
thread, off the WeeWX engine thread, with per-field caching so only values
that actually change are recomputed on every loop packet:

| Cache tier | What | Recomputed |
|---|---|---|
| continuous | positions (az/alt), distances, moon phase | every loop packet |
| day | `rise`, `set`, `transit`, `visible`, `sunrise`, `sunset` | once per local day |
| event | `next_*` / `previous_*` events | computed once and kept until the local day advances past the event (so a page can show today's event for the rest of its day) |

For this reason prefer `almanac.sun.rise` over `almanac.sun.next_rising` in
loop data.

## Migrating from weewx-celestial's loop fields

weewx-celestial ≤ 5.x injected celestial loop fields via a service; since
loopdata 5.0 (and celestial 6.0) those are almanac fields instead.  Every
`current.<field>` it emitted has an almanac equivalent:

| Old celestial loop field | Almanac field |
|---|---|
| `current.sunrise.raw` | `almanac.sunrise.unix_epoch.raw` |
| `current.civilTwilightStart.raw` | `almanac(horizon=-6).sun(use_center=1).rise.unix_epoch.raw` |
| `current.daylightDur.raw` | `almanac.sun.visible.second.raw` |
| `current.tomorrowSunrise.raw` | `almanac(days=1).sunrise.unix_epoch.raw` |

The pinned unit segments keep the old fields' fixed meanings (epoch seconds,
seconds of daylight) no matter how the target report's units are set —
celestial's own loop fields never followed report units, so a faithful
migration pins.

The only derivation left to the page is waxing/waning: the moon is waxing
when `almanac.next_full_moon.unix_epoch.raw` <
`almanac.next_new_moon.unix_epoch.raw` (pinned, as for any `.raw` field the
page compares numerically).  Note that distances arrive in AU (as reports
show them) rather than miles/km.
