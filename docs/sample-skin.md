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

In the skin's `[Extras]`.  Most of them have a value in
`skins/LoopData/skin.conf`, which is replaced on every upgrade; a copy in
the report's `weewx.conf` stanza, which is not, overrides it.  A fresh
install now writes only `page_update_pwd` there as a live setting and
leaves the rest commented out, so that the skin's own value answers
unless something else in weewx.conf supplies one.  The two analytics
options are the exception: neither file sets them, because their default
is to be absent — see below.  A station installed before that change has
live copies of `loop_data_file` and `expiration_time` in `weewx.conf`,
and those still win — edit them there:

* `loop_data_file` — the URL the page polls for the json file (default
  `loop-data.txt`; relative values are relative to this report's
  `HTML_ROOT`, and the default `loop_data_dir = .` writes the file beside
  this page).
* `refresh_rate` — seconds between polls (default `2`).  Set it to your
  station's loop frequency; polling faster than the file is rewritten just
  re-reads the same json.  A `0`, a negative, or anything that is not a
  number, means the default.
* `expiration_time` — hours after which the page stops polling (default
  `24`), so abandoned browser tabs don't poll forever.  A click restarts it.
  Set it to `0` and the page never expires — the same thing `?pageUpdate=`
  buys a kiosk, without the URL.  A negative, or anything that is not a
  number, means the default.
* `theme` — which theme the page wears: `auto` (the default), `dark` or
  `light`.  Anything else is treated as `auto`.  See
  [Themes](#themes) below, which is also where to look if your page
  turned light when you upgraded.
* `page_update_pwd` — loading the page as `?pageUpdate=<page_update_pwd>`
  exempts it from expiration (for a kiosk display).  Note the URL parameter
  is `pageUpdate`, while the option that sets its expected value is
  `page_update_pwd`.  Any characters work — spaces, quotes, accents — and
  the URL value is compared both as typed and percent-decoded, so a URL
  that already worked goes on working.  Set the option empty and no URL
  exempts the page; it expires like any other.  This password is visible
  to anyone reading the page source.
* `googleAnalyticsId` — a Google Analytics measurement id.  Set it and the
  page loads Google's `gtag.js` and reports to that id; leave it empty, or
  leave the option out, and the page loads nothing and reports nothing.
  Neither the skin nor a fresh install sets it — the installer writes a
  commented-out example to fill in.  (Installs from 6.11.3 through 7.0 have
  it in `weewx.conf` as an empty value, which also reports nothing.)
* `analytics_host` — report only when the page is served from this
  hostname, which keeps a copy you are testing locally out of your
  figures.  Empty, or absent, means report from wherever the page is
  served.  It does nothing unless `googleAnalyticsId` is set.

{: .note }
Before 6.11.3 both options were tested for *presence* rather than for a
value.  Because the installer wrote them present but empty, a default
install fetched `gtag.js` with an empty id on every page view, and anyone
who set an id but left `analytics_host` empty had the page compare its
hostname against `""` — never true — and report nothing.  Both now test
the value, as described above.

## Themes

The page ships two: **dark**, navy dials on near-black, which is what this
skin has always looked like, and **light**, deep navy instruments on warm
paper.  `theme` in the report's `[Extras]` chooses between them.

**`auto` is the default, and it follows the viewer, not the station.**  The
browser reports whatever the person looking at the page has set on their
own computer — the macOS or Windows appearance setting, Android or iOS
night mode, often on a schedule that turns over at sunset — and the page
follows it.  Two people looking at the same page at the same moment can
see different themes, and a viewer who changes the setting while the page
is open sees it change under them without a reload.

### If your page turned light when you upgraded

That is `auto` doing its job, and one line puts it back.  **A browser with
no preference set reports *light*, not "no preference"** — that value was
removed from the standard — so a machine where nobody has chosen a theme
gets the light page.  To pin the dark page for everyone regardless of
their setting, put this in the report's stanza in `weewx.conf`:

```
[StdReport]
    [[LoopDataReport]]
        [[[Extras]]]
            theme = dark
```

A fresh install writes that line commented out, as `#theme = auto`;
uncomment it and change the value.  An install from before this release
has no such line at all — add it.  **Then restart weewxd.**  WeeWX's
report engine reads `weewx.conf` once, at startup, and re-reads only
`skin.conf` and the language file on each report cycle — so a running
weewxd will not notice this edit, however long you wait.  (`weectl report
run` does read it fresh, which is why the change shows there.)  Your
browser may also need a reload past its cache.

`theme = light` pins the light page the same way, for a station that wants
it regardless of who is looking.

### Retuning the colors

The palette is a set of CSS custom properties at the top of
`index.html.tmpl`, and since 7.3 that is the only place those colors are
written: `realtime_updater.inc` reads the properties rather than repeating
them, so changing one there changes both the page and the canvases.  The
one exception is the windrose's six speed-band shades, which are still the
`RAMP` array in `realtime_updater.inc` — they are drawn on the dial face,
which is dark in both themes, so they do not change with the theme.  The dark
values sit on `:root` and the light ones are named `--light-*` beside
them, applied by a `prefers-color-scheme` media query and by the
`[data-theme]` attribute the `theme` option sets.

The shipped numbers were measured, not picked: each gauge's range band
clears its track by 3:1, which is what makes an arc read at a glance, and
the six windrose shades in `RAMP` hold their separation against the dial
face.  If
you retune, keep those relationships or the dials get harder to read at
the size they are actually drawn.  Note also that a skin edit is replaced
on the next upgrade — `weewx.conf` is not.

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

`RAMP`'s six shades are stops on a curve rather than one color per band:
`bandColor` samples them for however many speed bands your
`windrose_bands` produces, so the calmest band is always the first stop
and the windiest always the last, and six bands get the six stops
exactly.

[Building a live page](build-a-live-page.html) walks through the same
pattern for your own skin.
