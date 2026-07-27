---
title: Installation
layout: default
nav_order: 2
---

# Installation

**Requirements:** Python 3.7 or later and WeeWX 4 or 5.  No third-party
Python packages are needed.  (Versions 3.0 through 3.9 required
`sortedcontainers`; as of 4.0 it is no longer used.)

{: .important }
This extension has been tested with the WeeWX vantage and cc3000 drivers.  It
will likely also work with other drivers that, like the two drivers tested,
report loop packets on a regular basis and report all observations on every
loop packet.  Use loopdata with drivers that report loop packets on an
irregular basis and/or report partial observations, at your own risk.

## WeeWX 5

1. Download the latest release,
   [weewx-loopdata.zip](https://github.com/chaunceygardiner/weewx-loopdata/releases/latest/download/weewx-loopdata.zip).

1. Install the loopdata extension:

   ```
   weectl extension install weewx-loopdata.zip
   ```

1. Adjust the `[LoopData]` section the install added to weewx.conf (see
   [First-time configuration](#first-time-configuration) below).

1. Restart WeeWX.

## WeeWX 4

1. Download the latest release,
   [weewx-loopdata.zip](https://github.com/chaunceygardiner/weewx-loopdata/releases/latest/download/weewx-loopdata.zip).

1. Install the loopdata extension:

   ```
   sudo /home/weewx/bin/wee_extension --install weewx-loopdata.zip
   ```

   Note: this command assumes weewx is installed in `/home/weewx`.  If it's
   installed elsewhere, adjust the path of `wee_extension` accordingly.

1. Adjust the `[LoopData]` section the install added to weewx.conf (see
   [First-time configuration](#first-time-configuration) below).

1. Restart WeeWX.

## First-time configuration

The install creates a `[LoopData]` section in weewx.conf (the full section is
shown in [Configuration](configuration.html#sample-configuration)).  Adjust
the values accordingly.  In particular:

* Specify `seconds` with how often your device writes loop records (e.g.,
  `2.0` for Davis Vantage Pro 2 and RainWise CC3000).  This value matters:
  it is the weight given to each packet in the accumulators.
* Specify the `target_report` for the report you wish to use for formatting
  and units.
* Specify the `loop_data_dir` where the loop-data.txt file should be written.
  If `loop_data_dir` is a relative path, it is interpreted as relative to the
  target report's directory.
* You will eventually need to update the `fields` line with the fields you
  actually need for the report you are targeting.  Change this line later,
  after you are sure LoopData is running correctly.
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
