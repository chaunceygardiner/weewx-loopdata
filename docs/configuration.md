---
title: Configuration
layout: default
nav_order: 4
---

# Configuration

[weewx-loopdata manual](https://chaunceygardiner.github.io/weewx-loopdata/) · [weewx-loopdata on GitHub](https://github.com/chaunceygardiner/weewx-loopdata) · [Report an issue](https://github.com/chaunceygardiner/weewx-loopdata/issues)

---

Most configuration lives in the `[LoopData]` section of weewx.conf, which the
installer creates, and this page documents every option in it — the
[sample configuration](#sample-configuration) at the bottom is exactly what a
fresh install writes (which since 6.11.3 arrives commented; the block below
prints the values alone).

The fields themselves are not in `[LoopData]`: each report declares the
fields it needs in its own `skin.conf`, and is served in its own units,
formats and language — see [Declaring fields](declaring-fields.html).
Two settings LoopData obeys are likewise ordinary WeeWX report settings
that it simply follows, per report: the wording of
[`trend.barometer.desc`](#translating-trendbarometerdesc) and the
[trend window](#the-trend-window-time_delta).  Both are at the end of this
page.

## `[[FileSpec]]`

| Option | Meaning |
|---|---|
| `loop_data_dir` | The directory into which the loop data file should be written.  A relative path is relative to the directory of the report named by `target_report` — `LoopDataReport`, the sample report, when that option is absent — so the default writes the file beside the sample report's page.  Keep the `[[LoopDataReport]]` section for that reason: to turn the sample page off, set `enable = false` on it rather than deleting or renaming it (the installer would put it back on the next upgrade anyway).  Should the section be missing, a relative path is relative to `[StdReport]` `HTML_ROOT`, and LoopData warns at startup, since a page expecting the file beside itself will not find it there. |
| `filename` | The name of the loop data file to write (`loop-data.txt` by default). |

The file is written atomically (a temp file in the same directory, then a
rename), so a reader can never see a partial write.

## Where the loop-data file should live

The default puts the file inside your reports tree, in the sample
report's own directory.  That is where most stations leave it, and it
works — nothing in this section is a repair.

If you are comfortable editing your web server's configuration, there is
a tidier place for it: a memory filesystem outside the web root.  Two
reasons it is worth knowing about.

**It is rewritten on every loop packet.**  Nothing needs it to survive a
reboot, so if your station runs from an SD card you may prefer those
writes to land in RAM.

**Your report sync copies it, every cycle.**  A report sync pushes the
whole HTML tree, and a loop-data file anywhere inside that tree goes
along for the ride.  It cannot be skipped the way an unchanged page is
skipped — rewritten that often, the file is always newer than the last
upload.  If your pages are served from the machine WeeWX runs on, that
upload buys nothing at all: the page reads the file where it is written,
and the copy on the far end is read by nobody.

If your reports go out by `RsyncGenerator` and LoopData is *also* sending
the file at loop cadence with its own [rsync](rsync.md), it is worth
checking whether those two end up in the same place — configuration alone
will not tell you, since the two name their destinations separately and
an alias or a symlink defeats comparing the strings.  If they do land
together, two writers share one destination file: the report cycle's copy
is opened moments before it is sent, so it is barely older, but it can
still arrive after a fresher one and set the page's timestamp back for a
second or so.  Nothing is damaged, and the next loop packet corrects it —
it is just a puzzling thing to watch happen.

One move answers both: put the file on a memory filesystem outside the
web root, and let your web server serve it from there.  A page reading it
cannot tell the difference — it fetches a URL either way.

`/dev/shm` is already a memory filesystem on any Linux, so it needs no
mounting:

```
[LoopData]
    [[FileSpec]]
        loop_data_dir = /dev/shm/weewx
```

LoopData creates that directory at startup, which matters because
`/dev/shm` is empty after every reboot — as is the file, which the next
loop packet rewrites seconds later.  If you would rather have a mount of
your own, with a size cap and permissions you set, use an `/etc/fstab`
line instead and put that path everywhere `/dev/shm/weewx` appears below:

```
tmpfs /var/loop-data tmpfs defaults,size=4M,mode=0755,uid=weewx,gid=weewx 0 0
```

Two details `/dev/shm` spares you, because it is already mounted and mode
`1777` — world-writable, so anyone can create a subdirectory in it and
LoopData makes its own:

* Create the mount point first — `sudo mkdir /var/loop-data`.  Otherwise
  `mount -a`, and every boot after it, fails with `mount point
  /var/loop-data does not exist`.
* Give it to the user weewxd runs as, with `uid=`/`gid=` as above (or a
  `chown` afterwards).  A root-owned `mode=0755` tmpfs is not writable by
  a weewxd running as anyone else, and the failure is unhelpful: the
  directory already exists so LoopData's `makedirs` succeeds, and it is
  the temp file it opens next that raises `PermissionError` — during
  service init, so weewxd does not start.  Leave them off only if weewxd
  runs as root.

Then serve it.  For Apache, in your site's configuration — or a file of
your own under `conf-available/`, enabled with `a2enconf`:

```
Alias /loop-data/ /dev/shm/weewx/
<Directory /dev/shm/weewx>
    Require all granted
    Header set Cache-Control "no-store"
</Directory>
```

{: .important }
`Header` comes from `mod_headers`, which Debian and Ubuntu do **not**
enable by default.  Without it Apache refuses to start at all — `Invalid
command 'Header'`, which `apache2ctl configtest` will tell you.  Enable it
first (`restart`, not `reload`: a server that failed to start has nothing
to reload):

```
sudo a2enmod headers
sudo systemctl restart apache2
```

The directory itself appears when weewxd next starts with the
`loop_data_dir` above; until then the URL simply 404s (a `<Directory>`
naming a path that does not exist yet is not an error to Apache).

For nginx:

```
location /loop-data/ {
    alias /dev/shm/weewx/;
    add_header Cache-Control "no-store";
}
```

The `Cache-Control` header is not decoration: a file this small, fetched
this often, is exactly what an intermediate proxy likes to cache, and a
cached loop-data file shows up on a live page as a timestamp that never
catches up.

Finally, tell the page that reads it where to look.  Every live page has
an option naming the URL it polls — the sample skin's `loop_data_file`,
and the same option in weewx-celestial — and it has to be the URL your
alias serves, absolute now that the file no longer shares a tree with the
page:

```
[StdReport]
    [[LoopDataReport]]
        [[[Extras]]]
            loop_data_file = /loop-data/loop-data.txt
```

{: .important }
**None of this helps if the report sync is the only way to reach the
machine serving your pages** — as it is when you publish by FTP.  The
loop-data file then crosses only when the report cycle runs, so between
cycles it is minutes old: the sample panel, which reads LIVE only while
the packet is no older than three times its `refresh_rate` (six seconds,
by default) and otherwise shows the age instead, would be reporting
minutes almost all the time, and a page of your own has nothing fresher
to read.
LoopData needs a loop-cadence transport to whatever machine serves the
page — its own [rsync](rsync.md), which goes over ssh, or serving the
pages from the machine WeeWX runs on.

## `[[Formatting]]` (deprecated)

| Option | Meaning |
|---|---|
| `target_report` | The report the [`[[Include]]` fields line](#include-deprecated) is rendered through — its units, formatting and language — and the report whose directory a relative `loop_data_dir` is relative to.  `LoopDataReport`, the sample report, when absent.  Deprecated together with the fields line; a fresh install writes neither. |

Why does LoopData need a report at all?  Because conversions are decided
by the report, not by the units stored in the database: if the database
is metric but the report specifies US units, `day.outTemp.avg` arrives as
`68.2°F`.  On every loop packet LoopData applies the report's converters
and formatters, so each value lands in `loop-data.txt` exactly as the
WeeWX reporting cycle would have rendered it — page javascript can drop
values straight into HTML.  Since 7.0 every report that
[declares fields](declaring-fields.html) is its own target, served under
its own name in its own units; `target_report` names the one report the
old station-wide fields line still renders through.  Individual fields
can override the unit or formatting; see the
[field reference](field-reference.html).

A report also supplies the *language* of its values: the
`trend.barometer.desc` descriptions, moon phases, compass ordinates,
almanac body and constellation names, and hemisphere letters all follow
the report's lang file — see [Translations](i18n.html).

## `[[LoopFrequency]]`

| Option | Meaning |
|---|---|
| `seconds` | How often your station emits loop packets.  LoopData weights its accumulator entries with it, and gives each packet an `interval` of `seconds / 60`.  `2.0` is the shipped default and is right for a Davis Vantage. |

{: .note }
Set this correctly.  What a wrong value costs depends on which value you
are reading.

Every period that outlives the moment weewxd started — `hour`, `day`,
`week`, `month`, `year`, `rainyear`, `alltime`, and the rolling windows —
is seeded at startup from what is already in your database, and those
seeded values carry the weights the archive gives them.  Only the loop
packets arriving afterwards are weighted with `seconds`, so a wrong value
changes the ratio between the seeded part of a period and the live part.
Set too high it over-counts the live period and pulls an average toward
the present; set too low, toward the archived history.  A station emitting
every two seconds but configured for four counts its evening twice over:
restart at 18:00 on a 20 °C day that cools to 10 °C, and
`day.outTemp.avg` reads 16.0 where it should read 17.5.  The skew lasts
only while both parts are in the accumulator — until the period rolls, or
until the seeded records age out of a rolling window — so it is bounded by
the period's own length.  Minima and maxima never use the weight at all,
and once an accumulator holds nothing but live packets the value cancels
out of the average exactly.

`windrun` and `windrose` are the ones that do not recover.  Both are wind
speed multiplied by this interval, so they scale straight with the error
and no roll-over cleans them: a value half your real cadence halves every
windrun, and halves every distance and every band time the windrose
reports — and `day.windrose.banded` is what the sample panel draws.

A Davis Vantage emits a loop packet about every two seconds, which is what
the shipped `2.0` reflects, and there is nothing to set on the station
side.  Other drivers vary, and many have a polling interval you chose
yourself — use that number.  If you are unsure of either, time the
`dateTime` in `loop-data.txt` over a few minutes and use the average you
see.

## `[[RsyncSpec]]`

Only needed if you push loop-data.txt to a remote webserver — see
[Syncing to a remote server](rsync.html) for the how and why.

| Option | Meaning |
|---|---|
| `enable` | Set to `true` to rsync the loop data file to `remote_server`. |
| `remote_server` | The server to which the loop data file will be copied.  Passwordless ssh using public/private key must be configured from the user account weewx runs under to the account on the remote machine with write access to `remote_dir`. |
| `remote_port` | The ssh port on `remote_server`, if it is not the default 22.  Unset by default, in which case ssh's own default applies.  (A port set here and a port set in `ssh_options` are two ways to the same end; pick one.) |
| `remote_user` | The userid on `remote_server` with write permission to `remote_dir`. |
| `remote_dir` | The directory on `remote_server` where `filename` will be copied. |
| `compress` | `True` to compress the file before sending.  Default is `False`. |
| `log_success` | `True` to write success-with-timing messages to the log (for debugging).  Default is `False`. |
| `ssh_options` | Extra options for the ssh transport (e.g., a key file or port).  Whether or not this is set, LoopData appends safety bounds for any keyword you don't set yourself: `-o ConnectTimeout=<timeout>`, `-o ServerAliveInterval=<timeout>`, `-o ServerAliveCountMax=2` and `-o BatchMode=yes`, so a dead or hanging remote cannot stall the loop processing thread (an option you set always wins). |
| `timeout` | I/O timeout.  Default is `1` (when sending, time out in 1 second).  Also used for the ssh `ConnectTimeout` and `ServerAliveInterval` bounds described under `ssh_options`.  `0` disables all time bounds. |
| `skip_if_older_than` | Don't bother to rsync if the data is older than this number of seconds.  Default is `3`.  (Skip it and move on to the next packet rather than shipping stale data late.) |

## `[[Include]]` (deprecated)

| Option | Meaning |
|---|---|
| `fields` | The station-wide list of fields that pre-7.0 versions wrote — a bare comma-separated list, rendered through `target_report` and written as flat keys at the top level of the file.  Still honored exactly as before, beside the per-report entries; not written by a fresh install. |

Reports declare their own fields now — see
[Declaring fields](declaring-fields.html), which also says what to do
about a page of your own that reads the flat keys.

{: .important }
Do not edit this line by hand.  An extension declares its own fields when
installed, and a later release removes the line and `target_report` once
every extension that used it declares its fields.  LoopData logs a warning
at startup while the line is present.

{: .note }
A field entry containing a comma — a formatting call with two arguments, or
an almanac tag with two keywords — must be quoted, or ConfigObj will split
the entry at the comma into two bogus fields:
`fields = ..., 'day.rain.sum.format("%.2f", add_label=False)', ...`

## `windrose_bands` (deprecated here)

The wind-speed band edges of the [windrose observation](windrose.html)
are a *report* option since 7.0, written where the report's other options
are: on the report's stanza in `weewx.conf`, in that report's windSpeed
unit, or under `[StdReport] [[Defaults]]` for every report at once, in the
Defaults' unit — see
[`windrose_bands` per report](declaring-fields.html#windrose_bands-per-report).

A `windrose_bands` under `[LoopData]` is the pre-7.0 spelling, in
`target_report`'s windSpeed unit.  Before 7.0 it banded the one rose there
was — the one in the flat file, rendered through `target_report` — and it
still bands that report's rose and no other's, so an upgraded station's
rose keeps its bands and every other report's takes the defaults until
someone chooses otherwise.  It is deprecated; the value belongs on the
stanza of the report whose rose it bands, which is where
[finishing the migration](declaring-fields.html#finishing-the-migration)
moves it.

```
[LoopData]
    windrose_bands = 1, 4, 8, 13, 19, 25
```

The default is the classic WRPLOT/NOAA bands (0.5, 2.1, 3.6, 5.7, 8.8 and
11.1 m/s, converted to the report's unit), and
[Choosing your bands](windrose.html#choosing-your-bands) says how to pick
edges suited to your site.

## Translating `trend.barometer.desc`

As of 6.4 the descriptions served for `trend.barometer.desc` (`Falling
Slowly`, `Steady`, …) are gettext-style keys into the **report's**
`[Texts]` — the report that declared the field, or `target_report` for the
fields line.  Translate them in the report's lang file (every lang file
the sample report ships already carries all nine), or override a
description without touching any skin file:

```
[StdReport]
    [[LoopDataReport]]
        [[[Texts]]]
            "Steady" = "Holding steady"
```

The pre-6.4 `[LoopData]` `[[BarometerTrendDescriptions]]` section is gone
and now ignored — delete it from weewx.conf.

`trend.barometer.code` (an integer from `-4` through `4`) is the
language-neutral companion; see
[Special fields](field-reference.html#special-fields).

## The trend window (`time_delta`)

The trend window *cannot* be changed on a case-by-case basis, but it can be
changed for a whole report, using the standard WeeWX customization — in
the report's configuration:

```
    [[[Units]]]
        [[[[Trend]]]]
            time_delta = 86400    # for a 24 hour trend.
```

Each declaring report gets the window its own configuration asks for (the
fields line gets `target_report`'s); reports with the same window share
one trend accumulator, and a report with a different one gets its own.
The default is 10800 (3 hours) — a WeeWX default.  If `time_delta` is
greater than 259200 (3 days), LoopData caps it at 259200.

## Sample configuration

Fresh installs add the following `[LoopData]` section to weewx.conf.  Since
6.11.3 each section and option in it arrives with a comment saying what it is
for; the block below strips those, so it is the values a fresh install writes,
not the shape of the text.  There is no fields line: the
[sample report's instrument panel](sample-skin.html) declares the fields it
reads in its own `skin.conf`, as every live page does — see
[Declaring fields](declaring-fields.html).  Upgrading installs keep whatever
`[[Include]]` fields line and `[[Formatting]]` target_report are already in
weewx.conf, and those keep working as before.

```
[LoopData]
    [[FileSpec]]
        loop_data_dir = .
        filename = loop-data.txt
    [[LoopFrequency]]
        seconds = 2.0
    [[RsyncSpec]]
        enable = false
        remote_server = www.foobar.com
        remote_user = root
        remote_dir = /home/weewx/loop-data
        compress = False
        log_success = False
        timeout = 1
        skip_if_older_than = 3
```
