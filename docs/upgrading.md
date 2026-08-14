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

(WeeWX 4: `sudo /home/weewx/bin/wee_extension --install weewx-loopdata.zip`.)
Restart weewxd afterwards.  Full steps are on the
[Installation](installation.html) page.

Because your existing configuration is preserved, an upgrade can leave you
running new code against an old `weewx.conf`.  Everything below is a case
where that matters.  Read the entries newer than the version you are coming
from; if you are already on 6.4 or later, there is nothing to do.

{: .note }
`weectl extension install` overwrites `skins/LoopData/` on every upgrade,
including its `lang` files.  Customizations belong in the report's stanza in
weewx.conf (`[[[Texts]]]` entries and skin `[Extras]` overrides survive
upgrades); edits made directly to the shipped skin files do not.

## Action required

### 6.4 — `[[BarometerTrendDescriptions]]` was removed

Affects you only if you customized that section.

The nine `trend.barometer.desc` descriptions are now gettext-style keys in
the **target report's** `[Texts]`, so they translate through the same lang
files as everything else.  The old `[LoopData]`
`[[BarometerTrendDescriptions]]` section is gone and is now ignored — delete
it from weewx.conf.  A custom wording moves to the target report:

```
[StdReport]
    [[LoopDataReport]]
        [[[Texts]]]
            "Steady" = "Holding steady"
```

See [Translating `trend.barometer.desc`](configuration.html#translating-trendbarometerdesc).

### 6.0 — the sample panel needs a new `fields` line

Affects you only if you publish the sample report.

Upgrading replaces the sample skin's page with the
[instrument panel](sample-skin.html) but keeps your existing `fields` line,
and the panel reads fields the old default never included (`.raw` geometry,
`unit.label` scales, `day.windrose.*`).  With an old fields line the page
serves half-dead: text readouts, but no needles, no min–max bands and no
windrose.

Replace the `fields` line under `[LoopData] [[Include]]` with the
[sample configuration](configuration.html#sample-configuration)'s, appending
any fields your own pages use, and restart weewxd.  Fresh installs get it by
default.

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
