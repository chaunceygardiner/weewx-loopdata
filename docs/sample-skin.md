---
title: The sample skin
layout: default
nav_order: 11
---

# The sample skin

[weewx-loopdata manual](https://chaunceygardiner.github.io/weewx-loopdata/) · [weewx-loopdata on GitHub](https://github.com/chaunceygardiner/weewx-loopdata) · [Report an issue](https://github.com/chaunceygardiner/weewx-loopdata/issues)

---

A sample skin (`skins/LoopData`, registered as the report `LoopDataReport`)
is included with the extension.  After installing and restarting, and after
waiting for a report cycle, it can be found at `<weewx-url>/loopdata/`.
The skin declares the fields the panel reads in its own `skin.conf`, so
the panel works out of the box on a fresh install.

![The LoopData sample report: a live instrument panel](images/LoopDataReport.png)

## The instrument panel

The page is a NOAA windrose plus eleven canvas gauges, drawn by a few
hundred lines of dependency-free javascript — and every needle, petal and
readout redraws on every loop packet:

* Temperature, dew point, feels-like and humidity dials wear today's
  min–max as a band.
* The wind compass carries a second ghost needle at the 10-minute gust
  direction.
* The barometer draws the 3-hour trend as an arc, with a chevron showing
  the direction of travel.
* Rain and rain-rate dials rescale themselves on a big day.
* The windrose is the NOAA banded kind, drawn from `day.windrose.banded`
  and `day.windrose.calm`.
* UV, solar radiation and air quality (weewx-purple's `pm2_5_aqi`) gauges —
  and the feels-like dial where appTemp is not computed — hide themselves
  when the station doesn't report the observation, and reappear if the
  field shows up in loop-data.txt.

The division of labor is the loopdata pattern in miniature: the `.raw`
fields drive the geometry, report-formatted fields supply the readouts, and
`unit.label` fields pick the dial scales — so the panel follows this
report's units and formatting (metric or US) like any other loopdata page.
The gauges scale with the window: the engine draws in a 240-unit coordinate
system stretched to the css size, so geometry and fonts grow together on a
wide display (faces cap at 480px).

The fields the panel reads are declared in `skins/LoopData/skin.conf`,
one group per gauge (see [Declaring fields](declaring-fields.html)), and
the page reads its own report's entry in `loop-data.txt` — so it works
whatever else the file carries, and a second copy of the skin under
another report name gets its own entry.

## What each gauge reads

Gauge by gauge, in the order the page lays them out.  A gauge whose
formatted field is missing shows `--`; one whose `.raw` field is missing
draws no needle, band or petal.

| Gauge | Fields |
|:--|:--|
| Today's Windrose | `day.windrose.banded`, `day.windrose.calm` (and the automatic `windrose.bands`) |
| Wind | `current.windSpeed`, `current.windSpeed.raw`, `current.windDir.raw`, `current.windDir.ordinal_compass`, `10m.windGust.max`, `10m.wind.gustdir.raw`, `10m.wind.gustdir.ordinal_compass` |
| Temperature | `current.outTemp`, `current.outTemp.raw`, `day.outTemp.min.raw`, `day.outTemp.max.raw`, `day.outTemp.min.formatted`, `day.outTemp.max.formatted` |
| Dew Point | `current.dewpoint`, `current.dewpoint.raw`, `day.dewpoint.min.raw`, `day.dewpoint.max.raw`, `day.dewpoint.min.formatted`, `day.dewpoint.max.formatted` |
| Humidity | `current.outHumidity`, `current.outHumidity.raw`, `day.outHumidity.min.raw`, `day.outHumidity.max.raw` |
| Barometer | `current.barometer`, `current.barometer.raw`, `trend.barometer.raw`, `trend.barometer.desc` |
| Rain | `day.rain.sum`, `day.rain.sum.raw`, `current.rainRate`, `current.rainRate.raw` |
| Rain Rate | `current.rainRate`, `current.rainRate.raw`, `day.rainRate.max`, `day.rainRate.max.raw` |
| Feels Like | `current.appTemp`, `current.appTemp.raw`, `day.appTemp.min.raw`, `day.appTemp.max.raw`, `day.appTemp.min.formatted`, `day.appTemp.max.formatted` |
| UV Index | `current.UV`, `current.UV.raw`, `day.UV.max` |
| Solar Radiation | `current.radiation`, `current.radiation.raw`, `day.radiation.max` |
| Air Quality | `current.pm2_5`, `current.pm2_5_aqi.raw`, `current.pm2_5_aqi.formatted` |

`current.dateTime.raw` drives the timestamp and the LIVE indicator, and
`unit.label.outTemp`, `unit.label.barometer`, `unit.label.rain`,
`unit.label.rainRate` and `unit.label.windSpeed` pick the dial scales.

{: .note }
To turn the sample page off, set `enable = false` on `[[LoopDataReport]]`
rather than deleting or renaming the section: a relative `loop_data_dir`
is measured from its directory, and the installer would put the section
back on the next upgrade anyway.

{: .note }
The declaration in `skin.conf` is overwritten by every upgrade, as the
rest of the skin is.  To add a field for a customization of your own,
declare it under the report's stanza in `weewx.conf` instead — see
[Adding to a declaration from weewx.conf](declaring-fields.html#adding-to-a-declaration-from-weewxconf).

## Translations

As of 6.4 the page is translatable through WeeWX lang files, and eight
translations ship (a ninth lang file, `en.conf`, is the English reference
dictionary).  `lang = de` on the report's stanza selects German; the full
list is in [Translations](i18n.html).  Two languages meet on a
loopdata page — the page's labels follow this report's `lang` at
generation time, the live values follow it on every packet — see
[Translations](i18n.html).

## Skin options

In the skin's `[Extras]` — some in `skins/LoopData/skin.conf`, some
written into `weewx.conf` by the installer, which is the copy that wins:

* `loop_data_file` — the URL the page polls for the json file (default
  `loop-data.txt`; relative values are relative to this report's
  `HTML_ROOT`, and the default `loop_data_dir = .` writes the file beside
  this page).
* `refresh_rate` — seconds between polls (default `2`).  Set it to your
  station's loop frequency; polling faster than the file is rewritten just
  re-reads the same json.
* `expiration_time` — hours after which the page stops polling (default
  `4`), so abandoned browser tabs don't poll forever.  A click restarts it.
* `page_update_pwd` — loading the page as `?pageUpdate=<page_update_pwd>`
  exempts it from expiration (for a kiosk display).  Note the URL parameter
  is `pageUpdate`, while the option that sets its expected value is
  `page_update_pwd`.  This password is visible to anyone reading the page
  source.
* `googleAnalyticsId` — a Google Analytics measurement id.  Set it and the
  page loads Google's `gtag.js` and reports to that id; leave it empty, or
  leave the option out, and the page loads nothing and reports nothing.
  The installer writes it empty, so a fresh install sends no analytics
  until you fill it in.
* `analytics_host` — report only when the page is served from this
  hostname, which keeps a copy you are testing locally out of your
  figures.  Empty, or absent, means report from wherever the page is
  served.  It does nothing unless `googleAnalyticsId` is set.

{: .note }
Before 6.11.3 both options were tested for *presence* rather than for a
value.  Because the installer writes them present but empty, a default
install fetched `gtag.js` with an empty id on every page view, and anyone
who set an id but left `analytics_host` empty had the page compare its
hostname against `""` — never true — and report nothing.  Both now test
the value, as described above.

## Files to crib from

* `index.html.tmpl` — the page skeleton: a `<canvas>` per gauge, the
  palette, and the translated strings Cheetah hands to the javascript.  It
  renders no readings itself; every value on the page arrives by poll.
* `realtime_updater.inc` — the polling javascript: the fetch loop, the
  LIVE/OFFLINE/NO DATA/BAD DATA indicator, the expiration timer, and the
  canvas gauge and windrose rendering.

The palette lives in two places that must be kept in step: the `:root`
custom properties in `index.html.tmpl` for the html, and the `C` and `RAMP`
literals in `realtime_updater.inc` for the canvases, which cannot read css
variables.  Retune it freely — the windrose and the dials share a face
radius, so they read as one size whatever you do to the colors.

[Building a live page](build-a-live-page.html) walks through the same
pattern for your own skin.
