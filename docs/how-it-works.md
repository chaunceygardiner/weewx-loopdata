---
title: How LoopData works
layout: default
nav_order: 12
---

# How LoopData works

You don't need this page to use LoopData — it's here for the curious, and
for anyone weighing what a big fields line costs.

## Two threads, one queue

LoopData is a WeeWX `StdService`.  On the engine thread it does its setup:
parse the `[LoopData]` configuration, resolve the target report (whose
Formatter and Converter drive all unit conversion and formatting), and parse
the fields line.  It then spawns a separate daemon thread — the loop
processor — and the two are joined by a simple queue.  Every arriving loop
packet is put on the queue; all per-packet work (accumulation, rendering,
writing, rsync) happens on the processor thread, off the WeeWX engine
thread.

Once the thread starts and the accumulators are built, LoopData never
touches the database and never consults WeeWX's accumulators.  Its only
connection to the WeeWX main thread is that `NEW_LOOP_PACKET` is bound to
queue each loop packet.

## Only what you ask for

LoopData gathers at startup only what is needed to prime its accumulators
for the fields you listed.  For example, if a week field is included in the
fields line (`week.rain.sum`), daily summaries for the week are read to
prime the week accumulator.  If no week field is included, no work is done —
ditto for `alltime`, `rainyear`, `year`, `month`, `hour`, `1h`–`24h` and
`1m`–`1440m` accumulators.  They are populated only if they are used.

Likewise, only the necessary observation types are tracked: if no form of
`month.barometer` is specified on the fields line, the month accumulator
does not accumulate barometer readings.  Packets are pruned to the needed
obstypes before accumulation.

Accumulator priming happens on the first loop packet, not earlier — day
accumulators come from the database's daily summary, the longer spans from
day summaries plus archive records, and one continuous accumulator per
rolling/trend period.

## Continuous accumulators

The rolling windows (`1m`–`1440m`, `1h`–`24h`, and `trend`, which is just a
rolling window sized by the report's `time_delta`) are backed by LoopData's
core invention: continuous accumulators.  Each keeps running sums plus a
minimal homegrown min/max-tracking mapping (`MinMaxDict` — two heaps with
lazy deletion, mapping value → timestamps).  As entries age out of the
window, their contributions are subtracted and min/max stay O(log n)
amortized — values are never recomputed from scratch, no matter how large
the window.  (Versions 3.0–3.9 used the `sortedcontainers` package for
this; 4.0 replaced it with `MinMaxDict`, which is faster at every key count
and dropped the extension's last third-party dependency.)

`wind` is a composite observation assembled from
windSpeed/windDir/windGust/windGustDir, which is what gives the extra
aggregates `gustdir`, `rms`, `vecavg` and `vecdir`.

## Almanac and station fields

[Almanac fields](almanac-fields.html) follow the same
stay-off-the-engine-thread pattern: they touch no accumulators and are
evaluated on the processor thread, with per-field caching so only values
that actually change (positions, distances, phase) are recomputed on every
loop packet — see
[the caching table](almanac-fields.html#cost-is-managed-automatically).
[Station fields](station-fields.html) similarly compute the constant
attributes once, and recompute only `uptime`/`os_uptime` per packet.

## Atomic writes

The json file is written as a temp file in the same directory and then
renamed over the target, so a reader can never see a partial file.  If
rsync is enabled, the upload happens after the write, with time bounds that
keep a dead remote from stalling the processor thread and a staleness check
(`skip_if_older_than`) that drops old packets rather than shipping them
late — see [Syncing to a remote server](rsync.html).
