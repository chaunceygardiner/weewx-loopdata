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

The page is a NOAA windrose, a wind compass and ten dials, drawn as SVG
by a few hundred lines of dependency-free javascript — and every needle,
petal and reading redraws on every loop packet.  Each instrument is a card:
the drawing, then under it the reading, a line saying what it means, and
today's high above its low.

* Temperature, dew point, feels-like and humidity dials draw today's
  low-to-high as an arc inside the ticks.  Feels like says how far it is
  from the air temperature.
* The wind compass points the dials' red arm at the side the wind comes
  from, with a lighter, shorter needle for the strongest gust of the last
  ten minutes, and today's prevailing direction as a short arc.  Under it:
  "from WNW · gusting 14 mph from W", today's peak and the prevailing
  direction in words.
* The barometer draws the 3-hour trend as an arc, headed in the direction
  of travel, once the change is big enough to draw — about 1.3 hPa
  (0.04 inHg) — and says it in words (`trend.barometer.desc`) either
  way.
* UV and air quality wear their EPA category colors on the rim, and say
  the category: "High" at a UV index of 6, "Good" at an AQI of 38.  EPA
  names the UV index rounded, so the page does too.  The air quality dial
  runs to 500, the top of EPA's Hazardous band.
* UV, solar radiation and rain rate draw today's peak as an arc from the
  floor.  Rain and rain-rate dials rescale themselves on a big day.
* The windrose is the NOAA banded kind, drawn from `day.windrose.banded`
  and `day.windrose.calm`.
* UV, solar radiation and air quality (weewx-purple's `pm2_5_aqi`) gauges —
  and the feels-like dial where appTemp is not computed — hide themselves
  when the station doesn't report the observation, and reappear if the
  field shows up in loop-data.txt.

The division of labor is the loopdata pattern in miniature: the `.raw`
fields drive the geometry, `.formatted` fields supply each number and
`unit.label` fields the unit beside it and the dial scales — so the panel
follows this report's units and formatting (metric or US, and its decimal
point) like any other loopdata page.  The numerals on the dials use the
same decimal point.  The panel scales with the window: four cards to a
row, three below 1080 pixels, two below 800 and one below 540.  Each
drawing is a square stretched to its card, so its lines and words grow
together (drawings cap at 480px), and the words under it grow with the
card.  Every gauge's face, the windrose's included, comes out the same
size.

The fields the panel reads are declared in `skins/LoopData/skin.conf`,
one group per gauge (see [Declaring fields](declaring-fields.html)), and
the page reads its own report's entry in `loop-data.txt` — so it works
whatever else the file carries, and a second copy of the skin under
another report name gets its own entry.

## What each gauge reads

Gauge by gauge, in the order the page lays them out.  A gauge whose
formatted field is missing shows `--`; one whose `.raw` field is missing
draws no needle, arc or petal.

| Gauge | Fields |
|:--|:--|
| Today's Windrose | `day.windrose.banded`, `day.windrose.calm` (and the automatic `windrose.bands`) |
| Wind | `current.windSpeed.formatted`, `current.windSpeed.raw`, `current.windDir.raw`, `current.windDir.ordinal_compass`, `10m.windGust.max`, `10m.windGust.max.raw`, `10m.wind.gustdir.raw`, `10m.wind.gustdir.ordinal_compass`, `day.wind.max`, `day.wind.max.raw`, `day.wind.vecdir.raw`, `day.wind.vecdir.ordinal_compass` |
| Temperature | `current.outTemp.formatted`, `current.outTemp.raw`, `day.outTemp.min.raw`, `day.outTemp.max.raw`, `day.outTemp.min.formatted`, `day.outTemp.max.formatted` |
| Dew Point | `current.dewpoint.formatted`, `current.dewpoint.raw`, `day.dewpoint.min.raw`, `day.dewpoint.max.raw`, `day.dewpoint.min.formatted`, `day.dewpoint.max.formatted` |
| Humidity | `current.outHumidity.formatted`, `current.outHumidity.raw`, `day.outHumidity.min.raw`, `day.outHumidity.max.raw`, `day.outHumidity.min.formatted`, `day.outHumidity.max.formatted` |
| Barometer | `current.barometer.formatted`, `current.barometer.raw`, `trend.barometer.raw`, `trend.barometer.desc`, `day.barometer.min.formatted`, `day.barometer.max.formatted` |
| Rain | `day.rain.sum.formatted`, `day.rain.sum.raw`, `current.rainRate`, `current.rainRate.raw` |
| Rain Rate | `current.rainRate.formatted`, `current.rainRate.raw`, `day.rainRate.max`, `day.rainRate.max.raw` |
| Feels Like | `current.appTemp.formatted`, `current.appTemp.raw`, `day.appTemp.min.raw`, `day.appTemp.max.raw`, `day.appTemp.min.formatted`, `day.appTemp.max.formatted` |
| UV Index | `current.UV.formatted`, `current.UV.raw`, `day.UV.max`, `day.UV.max.raw` |
| Solar Radiation | `current.radiation.formatted`, `current.radiation.raw`, `day.radiation.max`, `day.radiation.max.raw` |
| Air Quality | `current.pm2_5`, `current.pm2_5_aqi.raw`, `current.pm2_5_aqi.formatted` |

`current.dateTime.raw` drives the timestamp and the LIVE indicator.
`unit.label.outTemp`, `unit.label.outHumidity`, `unit.label.barometer`,
`unit.label.rain`, `unit.label.rainRate`, `unit.label.windSpeed` and
`unit.label.radiation` supply the unit beside each reading and pick the
dial scales.

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

The page ships two: **dark**, deep dials on navy cards, and **light**,
pale dials on white cards.  In both, each gauge has one red arm, as an
analog gauge does.
`theme` in the report's `[Extras]` chooses between them.

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
`index.html.tmpl`, and that is the only place the colors are written: the
instruments are SVG whose every line and fill is a css class, so the
javascript names no color at all.  The dark values sit on `:root` and the
light ones are named `--light-*` beside them, applied by a
`prefers-color-scheme` media query and by the `[data-theme]` attribute the
`theme` option sets.  The one set that is the same in both themes is the
UV and air quality rims, which are EPA's own colors.

The shipped numbers were measured, not picked: every color clears the
surface it is drawn on — 4.5:1 for text, 3:1 for a needle, arc or tick —
and the six windrose shades (`--rose1` to `--rose6`) step evenly from the
calmest band to the windiest against their own theme.  If you retune, keep
those relationships or the dials get harder to read at the size they are
actually drawn.  Note also that a skin edit is replaced on the next
upgrade — `weewx.conf` is not.

## Files to crib from

* `index.html.tmpl` — the page skeleton: a card per gauge with an empty
  slot for its drawing and one for its reading, the palette, and the
  styles that dress the drawings.  It renders no readings itself; every
  value on the page arrives by poll.
* `realtime_updater.inc` — the polling javascript: the fetch loop, the
  LIVE/OFFLINE/NO DATA/BAD DATA indicator, the expiration timer, and the
  SVG drawing of the dials, the compass and the windrose.  It writes class
  names only, and rewrites a drawing only when it changed.

The six `--rose` shades are stops on a curve rather than one color per
band: the windrose samples them, with css `color-mix`, for however many
speed bands your `windrose_bands` produces, so the calmest band is always
the first stop and the windiest always the last, and six bands get the six
stops exactly.

[Building a live page](build-a-live-page.html) walks through the same
pattern for your own skin.
