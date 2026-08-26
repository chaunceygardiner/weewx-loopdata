---
title: Declaring fields
layout: default
nav_order: 5
---

# Declaring fields

[weewx-loopdata manual](https://chaunceygardiner.github.io/weewx-loopdata/) · [weewx-loopdata on GitHub](https://github.com/chaunceygardiner/weewx-loopdata) · [Report an issue](https://github.com/chaunceygardiner/weewx-loopdata/issues)

---

A report declares the fields it needs in its own skin's `skin.conf`, and
LoopData writes them into `loop-data.txt` under that report's name —
converted, formatted and worded exactly as that report would render them.
Every report that declares fields is served this way, each in its own
units and language, off one shared set of accumulators.  An extension
that has a live page can install its declaration with it, so nobody need
maintain a list of fields by hand.

This page is the declaration, what it buys, and how the older
`[LoopData] [[Include]] fields` line fits in.

## The declaration

In the skin's `skin.conf`, a `[LoopData]` section with a `[[fields]]`
sub-section of named groups:

```
[LoopData]
    [[fields]]
        clock       = current.dateTime.raw
        temperature = current.outTemp, current.outTemp.raw, day.outTemp.max
        barometer   = current.barometer, trend.barometer.desc
        labels      = unit.label.outTemp, unit.label.barometer
```

Each group is a comma-separated line of fields in the usual grammar — see
the [field reference](field-reference.html) — and LoopData takes the union
of every group, in order.  The group names are yours.  They exist because
ConfigObj has no line continuation for lists, so a single `fields =` line
would be one very long line; name the groups by gauge, by panel, by
whatever keeps the list readable.  A field listed in two groups counts
once.  The shipped sample skin's
[`skin.conf`](https://github.com/chaunceygardiner/weewx-loopdata/blob/master/skins/LoopData/skin.conf)
declares one group per gauge.

{: .note }
A field entry containing a comma — a formatting call with two arguments,
or an almanac tag with two keywords — must be quoted, or ConfigObj will
split the entry at the comma into two bogus fields:
`rain = day.rain.sum, 'day.rain.sum.format("%.2f", add_label=False)'`

{: .important }
Close every quote you open.  An unbalanced quote on a long group line does
not make ConfigObj raise; it makes it backtrack, for minutes, and since
LoopData reads every report's `skin.conf` at startup, weewxd hangs
silently before its first packet.  If weewxd stops starting after an edit
to a declaration, a quote is the first thing to check.

Only enabled reports are read.  A report with `enable = false` declares
nothing, and its entry disappears from the file.

## Each declaring report is its own target

Conversions are decided by the report, not by the units stored in the
database.  For every declaring report LoopData uses that report's own
converter and formatter, its `[Texts]` (for
[`trend.barometer.desc`](configuration.html#translating-trendbarometerdesc)),
its `[Almanac]` names and moon phases, its `$station`, its
[trend window](configuration.html#the-trend-window-time_delta) and its
[windrose bands](#windrose_bands-per-report).  The same observation
declared by two reports — a US report and a metric one on the same station,
say — is written twice, differently, off the one accumulator:

```json
{
  "LoopDataReport": {"current.outTemp": "63.4°F", "day.outTemp.max": "71.2°F", ...},
  "MetricReport":   {"current.outTemp": "17.4°C", "day.outTemp.max": "21.8°C", ...}
}
```

The keys are **report** names — the `[StdReport]` section names — never
skin names.  One skin can be listed under two reports (in two languages,
or two unit systems), and each gets its own entry: both declare the same
fields, from the skin's `skin.conf`, and differ by whatever their stanzas
in `weewx.conf` say about units and `lang`.

Accumulation is shared.  One set of accumulators feeds every report, and
each packet is accumulated once however many reports there are; only the
rendering is per report.  Two report settings reach the accumulators
rather than the renderers — the trend window and the windrose band edges —
so a report resolves its own and LoopData keeps one accumulator per
distinct value: reports that agree share it, and a report that differs
gets its own.  See [How LoopData works](how-it-works.html).

## Reading your report's entry

The page reads its own key.  In a Cheetah template `$REPORT_NAME` is the
report's name (WeeWX 4.6 or later), so the fetch in
[Building a live page](build-a-live-page.html#4-poll-loop-datatxt-and-fill-the-elements)
is:

```js
const data = (await response.json())[$json.dumps($REPORT_NAME)];
```

(with `#import json` at the top of the template).  `json.dumps` turns the
name into a quoted javascript string whatever it contains — a report is
any `[StdReport]` section name, apostrophes and accents included, and
written bare inside quotes such a name would break the script or miss
the key.

## Adding to a declaration from weewx.conf

LoopData reads the report's *merged* configuration — the skin's
`skin.conf`, then `[StdReport] [[Defaults]]`, then the report's own stanza
in `weewx.conf` — so a `[[[LoopData]]]` section under the report's stanza
adds to or replaces a group and leaves the skin's other groups alone:

```
[StdReport]
    [[CelestialReport]]
        [[[LoopData]]]
            [[[[fields]]]]
                satellites = almanac.ISS.next_pass.rise, almanac.ISS.next_pass.set
```

The merge is group by group: a group named here replaces the skin's group
of the same name; a new name adds a group.  This is how an extension can
declare fields that depend on your configuration — weewx-celestial's
satellite and comet fields, say, which a shipped `skin.conf` cannot know
because they follow the satellites you configured.  It is also how you add
a field of your own to a page you did not write, without editing a skin
file that the next upgrade overwrites.

Declarations belong to a report.  `[StdReport] [[Defaults]]` is merged
into every report, so a declaration written there would reach every
report, uploaders included, and each would render those fields on every
packet — declare on the report that needs the fields instead.

## `windrose_bands` per report

The [windrose](windrose.html) band edges default to the classic WRPLOT
bands, converted to the report's windSpeed unit.  `windrose_bands` is a
report option, and where it is written says what unit it is in:

* On the report's stanza in `weewx.conf`, in **that report's** windSpeed
  unit — for one report:

  ```
  [StdReport]
      [[LoopDataReport]]
          windrose_bands = 1, 4, 8, 13, 19, 25
  ```

* Under `[StdReport] [[Defaults]]`, in the **Defaults'** windSpeed unit —
  for every report at once.  A report whose unit differs gets the same
  physical edges converted to its unit and rounded to one decimal, so a
  mph rose and a km/h rose on one station band the same wind the same way.

* In the skin's `skin.conf`, in that report's unit — at the top, or inside
  its `[LoopData]` section beside `[[fields]]` — a skin author's choice,
  which the two above override.

Precedence is WeeWX's own for a report option: the stanza, then
`[StdReport]`, then `skin.conf`.  With none of them a report takes the
WRPLOT defaults — except `target_report`, which falls back to the
deprecated
[`[LoopData] windrose_bands`](configuration.html#windrose_bands-deprecated-here)
while that is still set, since before 7.0 that value banded exactly one
rose and it was that report's.  Whichever applies, the edges a report's
windrose was banded with are written beside its fields as
`windrose.bands`, so a legend never hardcodes them.

## Fields for scripts and other non-report consumers

Not everything that reads `loop-data.txt` is a WeeWX report.  A shell
script, an SNMP extension, a monitoring check — none of them has a skin,
and none of them should be parked on a report that belongs to somebody
else: the fields would sit in an extension's entry, that extension's own
tests would see a field its pages never read, and its next upgrade
overwrites the `skin.conf` you added them to.

LoopData installs a report for them, disabled: **`ScriptData`**, whose
skin generates nothing.  Enable it and declare your fields under its
stanza in `weewx.conf`:

```
[StdReport]
    [[ScriptData]]
        enable = true
        skin = ScriptData
        [[[LoopData]]]
            [[[[fields]]]]
                snmp-ambient = current.extraTemp2.raw
                hardware-check = 24h.UV.max.raw, 24h.radiation.max.raw
```

Its values arrive under `ScriptData` like any other report's, in the units
and formats `[StdReport] [[Defaults]]` gives it (override them on the
stanza as you would for a page):

```json
{"ScriptData": {"current.extraTemp2.raw": 68.2}, "LoopDataReport": {...}}
```

so a script reads `d["ScriptData"]["current.extraTemp2.raw"]`.  A `.raw`
field is usually what a script wants: a bare number, with no unit label to
strip.

{: .important }
Declare them in `weewx.conf`, as above — **not** in
`skins/ScriptData/skin.conf`.  That file is replaced on every upgrade of
weewx-loopdata, like every skin file, and anything you add to it is lost;
your report's stanza in `weewx.conf` is never touched.

Two things worth knowing about how this works, both of them WeeWX's rules
rather than LoopData's:

* **A report must name a skin directory that exists.**  WeeWX changes
  directory into a report's skin before running its generators, and does
  so without checking — a report with no `skin`, or one naming a directory
  that isn't there, raises out of the report cycle and takes every report
  after it down with it.  That is the only reason `skins/ScriptData`
  exists; it holds no templates.
* **`enable = true` is required.**  A disabled report declares nothing,
  exactly as a disabled page does — which is why the shipped stanza is
  disabled and does nothing until you turn it on.  Leaving it enabled
  costs nothing: with no `[Generators]` in its skin, WeeWX runs no
  generator for it.

{: .note }
Nothing stops you from adding more such reports, or naming them for their
consumers (`Monitoring`, `Dashboard`) — each needs a skin directory of its
own, which can be a copy of `skins/ScriptData`.  This is also where a
field belongs when nothing reads it from a page at all: a value you want
in the file for a graph or an experiment.

## The `[[Include]]` fields line

Before 7.0 there was one list of fields for the whole station — the
`fields` line of `[LoopData] [[Include]]` in `weewx.conf` — rendered through
the one report named by `[[Formatting]] target_report`, and written as flat
keys at the top level of the file.  Both settings, and the flat keys they
produce, are deprecated together and still work exactly as before: an
upgraded station keeps writing them, and the per-report entries appear in
the same file beside them, so every page keeps reading what it always read.
A fresh install writes neither.

{: .important }
**Do not edit the `fields` line by hand.**  An extension declares its own
fields when it is installed; the line exists only for pages that have not
been updated to read their report's entry.  LoopData logs a warning at
startup while the line is present, and a later release removes the line
and `target_report` once every extension that used the line declares its
fields — WeeWX backs up `weewx.conf` before an extension install, and the
installer prints what it removed.

A live page of your own that reads the flat keys should move to a
declaration: declare its fields under its report as above, and have it
read its report's entry.  From that moment the page is independent of the
`fields` line.  A page that is not a WeeWX report — a hand-written HTML
file, say — can declare through any enabled report's stanza in
`weewx.conf`, even one whose skin it never uses, and read that report's
key.

## Finishing the migration

**The line does not remove itself, and you should remove it.**  While it
is there, LoopData renders it in addition to every report's own entry, so
the fields on it that no report covers are computed a second time on every
loop packet, and the flat keys are written to the file every time.
LoopData ships a command that finishes the job:

```
python3 -m user.loopdata
```

Run it once every extension whose pages read the loop-data file has been
upgraded to a version that declares its fields.  It changes nothing by
itself: it prints which reports declare their own fields, then goes
through the `fields` line entry by entry and says which report now covers
each one, and which are covered by nobody.

Almost always the uncovered ones are simply **cruft** — entries a page
asked for years ago and no longer reads, or that an extension appended for
a version since changed.  If nothing outside WeeWX reads your loop-data
file, that is the whole story: delete them from the line and run the
command again.  The two other possibilities are unusual, and the command
spells them out: a page of your own still reading a flat key (declare
under its report), or something outside WeeWX reading the file — a script,
an SNMP check (declare under
[`ScriptData`](#fields-for-scripts-and-other-non-report-consumers)).

Once nothing is uncovered, run it again with `--apply`:

```
python3 -m user.loopdata --apply
```

which backs up `weewx.conf`, deletes `[[Include]]` and `[[Formatting]]`,
moves `[LoopData] windrose_bands` onto the stanza of the report whose rose
it bands — `target_report`, in the unit the value is already written in —
prints every change, and leaves you to restart weewxd.  If that report
already gets its bands from somewhere nearer, its own stanza or its skin,
the deprecated value was banding nothing and is simply dropped.  After
that the loop-data file is nothing but report entries.

A relative `loop_data_dir` is measured from `target_report`'s directory,
so removing `target_report` would move the file.  It does not let that
happen: it writes the path the file is at today into `loop_data_dir`
first, and says so, leaving every page that polls it reading the same URL
as before.  There is also nothing to stop you running it with the fields
line already deleted by hand — `target_report` and
`[LoopData] windrose_bands` are deprecated too, and it removes those on
their own.

### Running it

It has to be run by the Python that runs WeeWX, with WeeWX's `user`
directory importable.  That directory is `bin/user` under the
`WEEWX_ROOT` named at the top of your `weewx.conf`, so what goes on
`PYTHONPATH` is `WEEWX_ROOT/bin` — which install method you used decides
the rest:

| Install method | Command |
|:--|:--|
| pip, into a virtual environment | `PYTHONPATH=~/weewx-data/bin ~/weewx-venv/bin/python -m user.loopdata` |
| Debian, Red Hat or openSUSE package | `PYTHONPATH=/etc/weewx/bin:/usr/share/weewx python3 -m user.loopdata` |
| WeeWX 4, installed with `setup.py` | `PYTHONPATH=/home/weewx/bin python3 -m user.loopdata` |

A package install needs `/usr/share/weewx` on the path as well, because
that is where the package puts WeeWX itself; a pip install has WeeWX in
the virtual environment already, and the interpreter inside it is the one
to use.  If your `WEEWX_ROOT` is somewhere else — it is whatever the top
of your `weewx.conf` says — substitute it.

`weewx.conf` is found the same way WeeWX finds it, looking in
`~/weewx-data`, `/etc/weewx` and `/home/weewx`.  Add
`--config /path/to/weewx.conf` if yours is elsewhere, or if you run more
than one station on the machine.

Add `--apply` to the same command line to make the change once the report
comes back clean.

`target_report` keeps one small job while it exists: a relative
[`loop_data_dir`](configuration.html#filespec) is relative to that
report's directory.  With no `target_report` that is `LoopDataReport`, the
sample report, which is where a fresh install's page looks for the file —
so keep that section (turn the page off with `enable = false` rather than
deleting it).

## What the log says

At startup LoopData logs one line per declaring report — how many fields
it declared, how many of them are almanac and station fields, its trend
window and its band edges — followed by the accumulators those settings
resolved to (`trend accumulators : [...]`, `windrose bands ...`).  A second
trend accumulator there is a report whose `time_delta` differs from the
others'.

A field none of the parsers accept is logged as
`Ignoring unrecognized field <field> (report <name>)` and dropped — a typo
in a skin's declaration no longer vanishes without trace.  A `fields =`
line written directly under `[LoopData]` in a `skin.conf`, rather than as a
`[[fields]]` section of groups, is refused with `Ignoring [LoopData] fields
in report <name>: declare fields as named groups`.
