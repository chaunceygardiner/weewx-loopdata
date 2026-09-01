---
title: Upgrading
layout: default
nav_order: 3
---

# Upgrading

[weewx-loopdata manual](https://chaunceygardiner.github.io/weewx-loopdata/) · [weewx-loopdata on GitHub](https://github.com/chaunceygardiner/weewx-loopdata) · [Report an issue](https://github.com/chaunceygardiner/weewx-loopdata/issues)

---

Upgrading is the same command as installing — the installer updates the
extension in place and leaves your `[LoopData]` section alone:

```
weectl extension install weewx-loopdata.zip
```

(WeeWX 4: `sudo wee_extension --install weewx-loopdata.zip`, or the full path on a
setup.py install.)
Restart weewxd afterwards.  Full steps are on the
[Installation](installation.html) page.

Because your existing configuration is preserved, an upgrade can leave you
running new code against an old `weewx.conf`.  Everything below is a case
where that matters.  Read the entries newer than the version you are coming
from; if you are already on 7.0, there is nothing to do.

{: .note }
`weectl extension install` overwrites `skins/LoopData/` on every upgrade,
including its `lang` files.  Customizations belong in the report's stanza in
weewx.conf (`[[[Texts]]]` entries, skin `[Extras]` overrides and
`[[[LoopData]]] [[[[fields]]]]` groups — see
[Adding to a declaration from weewx.conf](declaring-fields.html#adding-to-a-declaration-from-weewxconf)
— survive upgrades); edits made directly to the shipped skin files do not.

## Action required

### 7.0 — reports declare their own fields

Affects you only if you wrote a live page of your own that reads
`loop-data.txt`.  The sample page needs nothing.  weewx-celestial and
weewx-weatherboard keep working as they are, through the fields line,
until each ships a release that declares its fields; upgrade to those when
they come, and their installers take care of it.

Each report now declares the fields it needs in its own `skin.conf`, and
LoopData writes them under the report's name, in that report's units and
language — see [Declaring fields](declaring-fields.html).  The
station-wide `[LoopData] [[Include]]` fields line and `[[Formatting]]`
target_report are deprecated together with the flat top-level keys they
produce.  All three still work exactly as before, so your page keeps
reading what it always read, and the per-report entries appear in the
same file beside the flat keys.  A fresh install writes neither.

Before a later release removes the line, move your page over: declare its
fields under its report — in its skin's `skin.conf`, or under the report's
stanza in `weewx.conf` — and change its fetch to take the report's entry:

```js
const data = (await response.json())[$json.dumps($REPORT_NAME)];
```

(`#import json` at the top of the template.)  From then on the page is
independent of the fields line.

**Then finish the migration.**  The fields line does not remove itself.
Once every extension whose pages read the loop-data file has been upgraded
to a version that declares its fields, run

```
PYTHONPATH=<WEEWX_ROOT>/bin <the python that runs weewx> -m user.loopdata
```

which reports what the line still holds and, once every entry on it is
accounted for, removes it with `--apply` — along with `target_report`,
moving `[LoopData] windrose_bands` somewhere it still applies.  The exact
command line depends on how WeeWX was installed;
[Running it](declaring-fields.html#running-it) gives it for each.  Until you
do, LoopData keeps honoring the line, which costs a little on every packet
and keeps the flat keys in the file.  See
[Finishing the migration](declaring-fields.html#finishing-the-migration).

`windrose_bands` moved with it.  It is a report option now — on the
report's stanza in `weewx.conf` in that report's windSpeed unit, or under
`[StdReport] [[Defaults]]` for every report.  A `windrose_bands` still
under `[LoopData]` bands `target_report`'s rose and no other's, which is
the one rose it banded before 7.0; every other report takes the WRPLOT
defaults.  Finishing the migration moves it onto that report's stanza; see
[`windrose_bands` per report](declaring-fields.html#windrose_bands-per-report).

{: .note }
A **fresh** install of 7.0 writes no fields line, and weewx-celestial 8.4's
installer adds its fields only to a line that already exists.  A fresh
station therefore needs weewx-celestial 9.0 or later, which declares its
own fields, for the Celestial page to have live values.  Upgraded stations
keep their line and are unaffected.

### 6.4 — `[[BarometerTrendDescriptions]]` was removed

Affects you only if you customized that section.

The nine `trend.barometer.desc` descriptions are gettext-style keys in
the **report's** `[Texts]` — the report that declares the field, or
`target_report` for the old fields line — so they translate through the
same lang files as everything else.  The old `[LoopData]`
`[[BarometerTrendDescriptions]]` section is gone and is now ignored — delete
it from weewx.conf.  A custom wording moves to the report:

```
[StdReport]
    [[LoopDataReport]]
        [[[Texts]]]
            "Steady" = "Holding steady"
```

See [Translating `trend.barometer.desc`](configuration.html#translating-trendbarometerdesc).

### 6.0 — the sample panel reads fields the old default never listed

Nothing to do on 7.0 or later: the sample skin declares its own fields
and reads its own entry, whatever the fields line says.  Between 6.0 and
6.11 the panel read the station-wide fields line, and an old line served
it half-dead — text readouts, but no needles, min–max bands or windrose —
until the line was replaced with the sample configuration's.

### 6.0 — `windrun_<direction>` was removed

Affects you only if you listed those fields.

The experimental `windrun_N` … `windrun_NNW` observation types are gone,
replaced by the first-class [`windrose`](windrose.html) observation; fields
naming them are now ignored.  They were always documented as experimental
and likely to change.

The mapping is by compass order: `day.windrun_N.sum` is element 0 of
`day.windrose.sum`, `day.windrun_NNE.sum` element 1, and so on clockwise
through `NNW` (element 15).  Unlike `windrun_<dir>`, windrose seeds every
period from the archive at startup, so the buckets are no longer empty after
a restart.  Details, and the aggregates the new type adds, are under
[Upgrading from `windrun_<direction>` fields](windrose.html#upgrading-from-windrun_direction-fields).

## Worth knowing, but nothing to do

* **7.0.1** — a *fresh* install now writes eight of the options it puts in
  `weewx.conf` commented out, showing the value that applies: four in
  `[[RsyncSpec]]` (`compress`, `log_success`, `timeout`,
  `skip_if_older_than`) and four in the sample report's `[Extras]`
  (`loop_data_file`, `expiration_time`, and examples for
  `googleAnalyticsId` and `analytics_host`).  Left commented, LoopData's
  own value answers, so a later release's better default reaches that
  station; written live, it would be frozen for ever, since the installer
  fills in absent keys and never rewrites one.  **Your `weewx.conf` is not
  touched** — those options are live settings on your station, with the
  same values, and they keep governing.  There is nothing to comment out
  by hand.  Also fixed: deleting `enable`, `compress` or `log_success`
  from `[[RsyncSpec]]` — or deleting the whole section — used to stop
  LoopData at startup with `Unknown boolean specifier: 'None'`.  Each now
  defaults to `false`, which is what the installer has always written.

  Also fixed: `weectl report run` used to leave a zero-byte `LoopData…`
  file in the directory the loop-data file is written to — inside your
  web-served report tree, unless you moved it — one per run.  Nothing
  cleans up the ones already there: they are all zero bytes, and
  `loop-data.txt` itself does not match `LoopData*`, so they can be
  deleted.

  One behavior does reach a station without an edit: the sample page's
  `expiration_time` default is now 24 hours where it was 4, so the page
  polls six times longer before it gives up and waits for a click.  That
  applies only where `weewx.conf` does not set the option itself — a fresh
  install since 6.11.3, where it is written commented out.  If your
  `weewx.conf` carries `expiration_time = 4` as a live setting, as every
  install made before that does, it still wins and nothing changes; delete
  or edit that line to take the new default.  `expiration_time = 0` now
  means the page never expires, where it used to expire the page before
  its first packet arrived.
* **7.0** — a field none of the parsers accept is logged
  (`Ignoring unrecognized field`), naming the report that declared it;
  it was dropped silently.  A trend window or windrose bands that differ
  between reports each get their own accumulator; reports that agree
  share one, so a station whose reports inherit `[[Defaults]]` runs
  exactly the accumulators it ran before.
* **6.11.2** — the sample skin ships `loop_data_file = loop-data.txt`
  where it shipped `../loop-data.txt` before.  Nothing about your
  installation changes: `weewx.conf` carries its own copy of the option
  and that copy has always won, so the old value in `skin.conf` was never
  read.  The manual also gains [Where the loop-data file should
  live](configuration.html#where-the-loop-data-file-should-live), for
  anyone who would rather keep the file on a memory filesystem outside
  the web root.
* **6.11.1** — the sample report no longer prints the loopdata field names
  under each gauge.  They read as a template that had failed to render;
  the mapping is now in
  [What each gauge reads](sample-skin.html#what-each-gauge-reads).
* **6.11** — the sample report's dark theme is higher contrast: white
  readouts, a visible recessed track on every dial, a windrose whose calmest
  band no longer disappears into the disc.  Nothing to configure — if you
  installed the sample skin, the next report cycle looks different.  A skin
  you copied and customized keeps whatever palette you gave it; the values
  live in `:root` in `index.html.tmpl` and in the `C` and `RAMP` literals in
  `realtime_updater.inc`, which must be kept in step.
* **6.10** — almanac fields work again on WeeWX earlier than 5.3.  If yours
  stopped writing `loop-data.txt` and left a `TypeError` traceback in the
  log, this release is the fix; see
  [Troubleshooting](troubleshooting.html#loop-datatxt-stopped-being-written-and-the-log-shows-a-traceback).
* **6.9** — a `next_*` almanac field now expires when its event's instant
  passes rather than at midnight, so an in-progress satellite pass rolls to
  the following one the moment it sets.  A day- or event-tier field that
  evaluates to no data is no longer cached until midnight, so a newly added
  satellite picks up values as soon as its elements download.  See
  [the caching tiers](almanac-fields.html#cost-is-managed-automatically).
* **6.4 and later** — the sample report is translatable, and eight
  translations now ship.  Nothing changes unless you set `lang`; see
  [Translations](i18n.html).
* **6.3** — `$station` report tags can be listed as fields, including a
  restart-correct live uptime.  See [Station fields](station-fields.html).
* **4.0** — the `sortedcontainers` package is no longer required.  Versions
  3.0 through 3.9 needed it; you may uninstall it if nothing else on the
  machine uses it.

## After upgrading

Navigate to `<weewx-url>/loopdata/` once a report cycle has run.  The
indicator should read LIVE and the needles should move with your station.
If something looks wrong, [Troubleshooting](troubleshooting.html) starts
with the symptoms an upgrade most often produces.
