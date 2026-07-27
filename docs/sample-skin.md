---
title: The sample skin
layout: default
nav_order: 9
---

# The sample skin

A sample skin (`skins/LoopData`, registered as the report `LoopDataReport`)
is included with the extension.  After installing and restarting, and after
waiting for a report cycle, it can be found at `<weewx-url>/loopdata/`.
LoopData is initially configured with `target_report = LoopDataReport`, so
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
`unit.label` fields pick the dial scales — so the panel follows the target
report's units and formatting (metric or US) like any other loopdata page.
The gauges scale with the window: the engine draws in a 240-unit coordinate
system stretched to the css size, so geometry and fonts grow together on a
wide display (faces cap at 480px).

The fields line the panel reads is exactly the
[sample configuration](configuration.html#sample-configuration)'s `fields`
line — fresh installs get it by default.

{: .important }
**Upgrading from a pre-6.0 loopdata:** upgrading replaces the sample skin's
page with the instrument panel but keeps your existing `[LoopData]` fields
line, and the panel reads fields the old default never included (`.raw`
geometry, `unit.label` scales, `day.windrose.*`).  With an old fields line
the page serves half-dead: text readouts but no needles, min–max bands or
windrose.  Replace the fields line with the sample configuration's (append
any fields other pages of yours use) and restart weewxd.

## Skin options

In the skin's `[Extras]` (see `skins/LoopData/skin.conf`):

* `loop_data_file` — the URL the page polls for the json file (default
  `../loop-data.txt`, matching the default `loop_data_dir = .`).
* `expiration_time` — hours after which the page stops polling (so
  abandoned browser tabs don't poll forever).
* `page_update_pwd` — loading the page as `?pageUpdate=<page_update_pwd>`
  exempts it from expiration (for a kiosk display).

## Files to crib from

* `index.html.tmpl` — element ids match the json keys, and Cheetah renders
  the report-time values so the page is populated before the first poll.
* `realtime_updater.inc` — the polling javascript: the fetch loop, the
  LIVE/OFFLINE/NO DATA/BAD DATA indicator, the expiration timer, and the
  canvas gauge and windrose rendering.

[Building a live page](build-a-live-page.html) walks through the same
pattern for your own skin.
