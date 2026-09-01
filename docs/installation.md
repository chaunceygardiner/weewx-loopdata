---
title: Installation
layout: default
nav_order: 2
---

# Installation

[weewx-loopdata manual](https://chaunceygardiner.github.io/weewx-loopdata/) · [weewx-loopdata on GitHub](https://github.com/chaunceygardiner/weewx-loopdata) · [Report an issue](https://github.com/chaunceygardiner/weewx-loopdata/issues)

---

**Requirements:** Python 3.7 or later and WeeWX 4.6 or later.  No third-party
Python packages are needed.  (Versions 3.0 through 3.9 required
`sortedcontainers`; as of 4.0 it is no longer used.)  Python 3.7 is the
floor because LoopData's code carries type annotations, which do not work
under Python 2 nor under earlier versions of Python 3.

{: .important }
LoopData is targeted at drivers that report loop packets on a regular
cadence, with all observations present in every packet.  It has been tested
with the WeeWX vantage and cc3000 drivers and will likely work with any
other driver of that kind.  Drivers that report irregularly or send partial
packets are untested: time-weighted aggregates assume the steady cadence set
in `[[LoopFrequency]] seconds`.

## WeeWX 5

1. Download the latest release,
   [weewx-loopdata.zip](https://github.com/chaunceygardiner/weewx-loopdata/releases/latest/download/weewx-loopdata.zip).

1. Install the loopdata extension.

   On a pip install `weectl` lives in the virtual environment, so
   activate it first (yours may sit elsewhere; `~/weewx-venv` is the usual
   place):

   ```
   source ~/weewx-venv/bin/activate
   weectl extension install weewx-loopdata.zip
   ```

   On a Debian or Red Hat package install there is no environment to
   activate and `weectl` is already on the path:

   ```
   weectl extension install weewx-loopdata.zip
   ```

   No `sudo`: that install put your account in the `weewx` group, which
   owns the files.  If you installed WeeWX in this same login session, log
   out and back in first so the group membership takes effect.

1. Adjust the `[LoopData]` section the install added to weewx.conf (see
   [First-time configuration](#first-time-configuration) below).

1. Restart WeeWX.

## WeeWX 4

1. Download the latest release,
   [weewx-loopdata.zip](https://github.com/chaunceygardiner/weewx-loopdata/releases/latest/download/weewx-loopdata.zip).

1. Install the loopdata extension:

   ```
   sudo wee_extension --install weewx-loopdata.zip
   ```

   Note: a package install has `wee_extension` on the path, as above.  On
   a setup.py install use the full path, e.g.
   `/home/weewx/bin/wee_extension`.

1. Adjust the `[LoopData]` section the install added to weewx.conf (see
   [First-time configuration](#first-time-configuration) below).

1. Restart WeeWX.

{: .note }
Already running an older loopdata?  The command is the same, and your
`[LoopData]` section is preserved — but some releases need a change to it.
See [Upgrading](upgrading.html).

## First-time configuration

The install creates a `[LoopData]` section in weewx.conf (the full section is
shown in [Configuration](configuration.html#sample-configuration)).  Adjust
the values accordingly.  In particular:

* Specify `seconds` with how often your device writes loop records — `2.0`
  is the shipped default and is right for a Davis Vantage.  It is the
  weight given to each live packet in the accumulators, so it is worth
  getting right; see
  [`[[LoopFrequency]]`](configuration.html#loopfrequency) for
  what a wrong value costs.
* Specify the `loop_data_dir` where the loop-data.txt file should be written.
  If `loop_data_dir` is a relative path, it is interpreted as relative to the
  sample report's directory (`LoopDataReport`, or the report named by
  `target_report` if you set one).  The default works and is what most stations
  keep; see
  [Where the loop-data file should live](configuration.html#where-the-loop-data-file-should-live)
  if you would rather have the file on a memory filesystem outside the web
  root.
* There is no list of fields to fill in: the sample report declares the
  fields its panel reads in its own `skin.conf`, and a page of your own
  declares its fields the same way — see
  [Declaring fields](declaring-fields.html), later, after you are sure
  LoopData is running correctly.  Another extension with a live page
  declares its own when installed — weewx-celestial does so from 9.0
  (8.4 and earlier only add their fields to a line that already exists,
  and a fresh install of loopdata 7.0 or later writes none).
* If you need the loop-data.txt file pushed to a remote webserver, you will
  also need to fill in the `RsyncSpec` fields; but one can fill that in
  later, after LoopData is up and running.  See
  [Syncing to a remote server](rsync.html).

## Checking for a properly running installation

After a reporting cycle runs, navigate to `<weewx-url>/loopdata/` in your
browser to see the default loopdata report — the
[sample instrument panel](sample-skin.html).  (Reports typically run every 5
minutes.)  Once the page loads, its indicator should read LIVE and the
needles should move with your station.

If something is off, see [Troubleshooting](troubleshooting.html).

## Optional: SSH control master multiplexing

This applies only if you rsync loop-data.txt to another machine (see
[Syncing to a remote server](rsync.html)).  If you are rsync'ing loopdata to
another machine every 2 seconds, inevitably some of these rsyncs will fail —
perhaps on the order of 3 to 10 per day on the author's systems.  This is
totally fine and is not noticeable, but there is an easy way to make the
rsyncs lightweight and have none of them fail: create a `.ssh/config` file
under the home directory of the user running WeeWX, with the contents below.
The `Host` entered must match exactly the `remote_server` value entered in
the `RsyncSpec` section of `[LoopData]` in weewx.conf.

```
Host www.paloaltoweather.com   # <-- CHANGE TO YOUR remote_server!
    ControlMaster auto
    ControlPath ~/.ssh/control-%r@%h:%p
    ControlPersist 10m
    ServerAliveInterval 15
    ServerAliveCountMax 3
```
