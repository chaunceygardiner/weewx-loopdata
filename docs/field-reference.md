---
title: Field reference
layout: default
nav_order: 4
---

# Field reference

Every entry on the `fields` line of `[LoopData] [[Include]]` is a WeeWX
report tag with the `$` removed, and becomes a key in loop-data.txt — the
key is the field entry verbatim.  Generally, if you can specify a field in a
Cheetah template, and that field begins with `$current`, `$trend`, `$hour`,
`$day`, `$week`, `$month`, `$year`, or `$rainyear`, you can specify it here
(without the dollar sign).  For all time, use `alltime`.  Rolling-window
periods are also available: any number of minutes from `1m` through `1440m`
and any number of hours from `1h` through `24h`.

Three field families follow their own grammars and have their own pages:
[almanac fields](almanac-fields.html) (`almanac.sunrise`, ...),
[station fields](station-fields.html) (`station.uptime.raw`, ...), and the
[windrose aggregates](windrose.html) (`day.windrose.banded`, ...).
Everything else — ordinary observation fields — is on this page.

## The grammar at a glance

Every observation field has this shape (brackets mark optional slots):

```
period.obstype[.agg_type][.unit][.round(n)][.format_spec]
```

* **period**: `current`, `trend`, `hour`, `day`, `week`, `month`, `year`,
  `rainyear`, `alltime`, `1m`–`1440m`, or `1h`–`24h`.
* **obstype**: any observation in the loop packet (`outTemp`, `barometer`,
  `rain`, `windSpeed`, ...), the composite `wind`, or the composite
  [`windrose`](windrose.html).
* **agg_type**: required for every period except `current` and `trend`:
  * every observation: `min`, `mintime`, `max`, `maxtime`, `sum`, `count`,
    `avg`
  * `wind` only (the composite of windSpeed/windDir/windGust/windGustDir),
    additionally: `gustdir`, `rms`, `vecavg`, `vecdir`
  * `windrose` only (not valid for `current`/`trend`): exactly `sum`,
    `time`, `banded`, `calm` — array/matrix projections, see
    [the windrose page](windrose.html)
  * observation types registered with WeeWX's `firstlast` accumulator
    (string-valued types), on the rolling periods: `first`, `last`,
    `firsttime`, `lasttime`
* **unit**: an optional unit override — see
  [Overriding the unit of a field](#overriding-the-unit-of-a-field).
* **round(n)**: an optional rounding transform, exactly as report tags allow
  (`$day.outTemp.max.round(1).raw`): the value is rounded to n digits, then
  the format spec renders the rounded value.  Most useful with `.raw`, to
  publish a number with limited digits (`29.93` instead of
  `29.927100000000002`).
* **format_spec**: optional; just like in a report, it specializes the
  rendering:
  * No format spec: converted and formatted per the report, with a label
    (e.g., `64.7°F`).
  * `.raw`: converted per the report, but not formatted (e.g., `64.711`).
  * `.formatted`: converted and formatted per the report, no label (e.g.,
    `64.7`).
  * `.ordinal_compass`: for directional observations, the value as text
    (e.g., `SW`).
  * `.format(...)` / `.nolabel(...)` / `.string(...)` / `.long_form(...)`:
    the report tags' formatting calls, with the same arguments — see
    [Formatting a field with arguments](#formatting-a-field-with-arguments-call-syntax).

## Periods

| Period | Window |
|---|---|
| `current` | The latest loop packet.  No aggregate. |
| `trend` | Change over the target report's trend window (`[Units][Trend] time_delta`, default 3 hours, capped at 3 days — see [Configuration](configuration.html#the-trend-window-time_delta)).  No aggregate. |
| `1m` – `1440m` | Rolling window of that many minutes, e.g. `2m`, `10m`, `90m`. |
| `1h` – `24h` | Rolling window of that many hours, e.g. `8h`, `24h`. |
| `hour` | The current clock hour. |
| `day` | Today (local time). |
| `week` | This week (honors the report's `week_start`). |
| `month` | This calendar month. |
| `year` | This calendar year. |
| `rainyear` | This rain year. |
| `alltime` | Everything in the archive. |

The rolling periods act just like `day`, `week`, `month`, `year`, `rainyear`
and `alltime`: they take the same aggregates and format specs.

## Examples

Current outside temperature:

* `current.outTemp.formatted` might yield `79.2`
* `current.outTemp` might yield `79.2°F`
* `current.outTemp.raw` might yield `79.175`

Maximum wind in the last 30 minutes:

* `30m.wind.max.formatted` might yield `7.1`
* `30m.wind.max` might yield `7.1 mph`
* `30m.wind.max.raw` might yield `7.12`

Average outside temperature over the last 3 hours:

* `3h.outTemp.avg.formatted` might yield `32.4`
* `3h.outTemp.avg` might yield `32.4°`
* `3h.outTemp.avg.raw` might yield `32.41`

Average inside temperature this clock hour:

* `hour.inTemp.avg.formatted` might yield `68.1`
* `hour.inTemp.avg` might yield `68.1°`
* `hour.inTemp.avg.raw` might yield `68.12`

Day average of outside temperature:

* `day.outTemp.avg.formatted` might yield `64.7`
* `day.outTemp.avg` might yield `64.7°`
* `day.outTemp.avg.raw` might yield `64.711`

Wind speed average for this week:

* `week.windSpeed.avg.formatted` might yield `2.7`
* `week.windSpeed.avg` might yield `2.7 mph`
* `week.windSpeed.avg.raw` might yield `2.74`

Minimum dewpoint this month, and when:

* `month.dewpoint.min` might yield `43.7°`
* `month.dewpoint.mintime` might yield `08/01/2020 03:27:00 AM`

Maximum wind speed this year, and when:

* `year.wind.max` might yield `29.6 mph`
* `year.wind.maxtime` might yield `02/26/2020 07:40:00 PM`

Total rain this rain year:

* `rainyear.rain.sum.formatted` might yield `7.1`
* `rainyear.rain.sum` might yield `7.1 in`
* `rainyear.rain.sum.raw` might yield `7.13`

All-time high outside temperature:

* `alltime.outTemp.max.formatted` might yield `107.3`
* `alltime.outTemp.max` might yield `107.3°`
* `alltime.outTemp.max.raw` might yield `107.29`

## Time-of-event fields

Time-of-event fields (`maxtime`, `mintime`, `firsttime`, `lasttime`) are
formatted exactly as WeeWX report tags format them: the field's period is
the time context, so the target report's `[Units][TimeFormats]` entry for
that period applies.  With the standard settings: `hour` is `%H:%M`, `day`
is `%X`, `week` is `%X (%A)`, and `month`/`year`/`rainyear` are `%x %X`.
`alltime` uses the `year` format, as WeeWX's `$alltime` tag does.  Rolling
periods (`1m`–`1440m`, `1h`–`24h`) use the `current` format.  For example:

* `day.outTemp.maxtime` might yield `12:00:00`
* `hour.outTemp.maxtime` might yield `12:00`

To take full control of the rendering, use a formatting call with a strftime
format: `day.outTemp.maxtime.format("%H:%M")`.

## Overriding the unit of a field

By default every field is converted to the unit the target report calls for.
A field may instead name an explicit unit, exactly as WeeWX report tags allow
(e.g. `$current.outTemp.degree_C`).  The unit goes right after the
aggregation, before the optional `round(n)` and format spec.  Any unit WeeWX
knows for the observation's unit group is accepted.  For example, regardless
of the report's configured units:

* `current.windSpeed.beaufort` might yield `5`
* `current.windSpeed.beaufort.formatted` might yield `5`
* `day.outTemp.avg.degree_C` might yield `18.3°C`
* `day.outTemp.avg.degree_C.raw` might yield `18.33`
* `day.outTemp.avg.degree_F.raw` might yield `64.99`
* `10m.windGust.max.knot.raw` might yield `6.18`
* `trend.barometer.mbar.formatted` might yield `2.4`

This is handy for gauges that expect a fixed unit (for example a Beaufort
wind gauge) no matter what units the rest of the report uses.  The override
applies to value fields only; the `unit.label` prefix form has no override
(matching WeeWX, whose `$unit.label` is obstype-only).  If the named unit is
incompatible with the observation's group (e.g. `day.outTemp.avg.beaufort`),
the field is simply omitted from loop-data.txt.

{: .note }
A unit must already be registered with WeeWX when LoopData starts: all of
WeeWX's own units (including `beaufort`) always are, but a unit registered by
another extension is recognized only if that extension initializes before
LoopData.

## Formatting a field with arguments (call syntax)

The formatting methods a WeeWX report tag can call are also available as
format specs, with the same names, arguments and output as the report tag:

* `format(format_string, None_string, add_label, localize)` — all arguments
  optional
* `nolabel(format_string, None_string)` — like `format()`, but with no label
* `string(None_string)` — the report's default formatting, with control over
  missing data
* `long_form(format_string, None_string)` — delta times spelled out (e.g.
  sunshine duration)

For example:

* `day.outTemp.maxtime.format("%H:%M")` might yield `12:05`
* `current.outTemp.format(add_label=False)` might yield `79.2`
* `day.windGust.max.nolabel("%.0f")` might yield `9`
* `day.rain.sum.format("%.2f", add_label=False)` might yield `0.31`
* `day.sunshineDur.sum.long_form()` might yield
  `6 hours, 25 minutes, 10 seconds`

Exactly as in a report, a time-of-event field's `format_string` is a
strftime format, and a numeric field's is a %-format.  Arguments must be
literals; positional arguments bind exactly as the report tag's do.  A bare
spec name (`day.rain.sum.string`) is a zero-argument call, just as Cheetah
renders `$day.rain.sum.string`.  The unit override and `round(n)` compose as
usual: `day.outTemp.avg.degree_C.nolabel("%.2f")`,
`day.barometer.max.mbar.round(1).raw`.

{: .important }
A call containing a comma must be quoted in weewx.conf, or ConfigObj will
split the entry at the comma into two bogus fields:
`fields = ..., 'day.rain.sum.format("%.2f", add_label=False)', ...`
Calls without a comma (e.g. `day.outTemp.maxtime.format("%H:%M")`) need no
quoting.  The json key is the field entry verbatim (without the outer
quotes).

## Missing data

If a field is requested but the data is missing — say a trend before enough
packets have arrived, or an observation the station doesn't report — the
field will not be present in loop-data.txt.  Your javascript should expect
absent keys and react accordingly (see
[Building a live page](build-a-live-page.html#handle-missing-fields)).

The exception: `string()`, or an explicit `None_string` argument to any
formatting call, overrides the omission.  The field is then always emitted,
rendering missing data exactly as the report tag would (`N/A` unless a
`None_string` says otherwise).  For example, `trend.outTemp.string("n/a")`
is present from the very first packet — handy for pages that want every key
present from the start.

## Special fields

### `unit.label.<obs>`

The target report's unit label for an observation, e.g. `unit.label.outTemp`
might yield `°F`.  Useful for drawing dial scales and legends that follow
the report's units automatically.  (`unit.label.windrose` yields the
distance unit label used by `windrose.sum` — see
[the windrose page](windrose.html).)

### `trend.barometer.desc` and `trend.barometer.code`

`trend.barometer.desc` provides a text version of the barometer rate (e.g.,
`Falling Slowly`); the texts translate through the target report's `[Texts]` —
see [Translating trend.barometer.desc](configuration.html#translating-trendbarometerdesc).
`trend.barometer.code` provides an integer of value `-4`, `-3`, `-2`, `-1`,
`0`, `1`, `2`, `3` or `4`, corresponding to `Falling Very Rapidly`,
`Falling Quickly`, `Falling`, `Falling Slowly`, `Steady`, `Rising Slowly`,
`Rising`, `Rising Quickly` and `Rising Very Rapidly`, respectively.

## Aggregates via xtypes are not supported

If an aggregate is implemented via xtypes, it will be ignored by loopdata.
For example, the weewx-purple extension implements `pm2_5_aqi` via xtypes.
If, say, `week.pm2_5_aqi.max` were specified on the fields line, it would be
ignored — there is no database entry from which to look up the weekly high
for `pm2_5_aqi`.

The rule is: if an observation is not stored in the database, you can't
specify aggregates for it.  LoopData will still report *current* values if
you specify them (e.g. `current.pm2_5_aqi.raw` works — the value is in the
loop packet).

## What report tags can do that fields cannot

For rendering values, the fields grammar is at parity with report tags: any
period tag with a standard aggregate, converted to any unit, rounded, and
formatted with the full set of formatting calls.  What remains report-only:

* Aggregates computed by xtypes (`$day.heatdeg.sum`, `$year.growdeg.sum`,
  ...) — see above.
* Offset periods: `$yesterday`, `$day($days_ago=1)`, `$month($months_ago=1)`,
  `$rainyear($years_ago=1)` and the arbitrary `$span(...)`.
* Series tags (`$day.outTemp.series(...)`).
* The introspection helpers `.json`, `.exists` and `.has_data` (a field with
  missing data is simply absent from loop-data.txt — or always emitted, via
  `string()`).
* Non-observation tags: `$latitude`, `$longitude`, `$altitude`, `$Extras`,
  `$gettext`, `$obs`, and the like.  The exceptions are `$unit.label.<obs>`,
  supported as `unit.label.<obs>` above, and `$station`, supported as
  [station fields](station-fields.html) — `$latitude` et al. have `$station`
  equivalents.
