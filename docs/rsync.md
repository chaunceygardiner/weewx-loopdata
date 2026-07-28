---
title: Syncing to a remote server
layout: default
nav_order: 11
---

# Syncing to a remote server

If weewxd and your web server are the same machine, you don't need any of
this — just write loop-data.txt somewhere the web server serves.  If your
pages live on a remote server, LoopData can push loop-data.txt there on
every loop packet with rsync.  Rsync is the *only* mechanism provided.

Configuration is the `[[RsyncSpec]]` block — every option is documented in
[Configuration](configuration.html#rsyncspec).  The design principle
throughout: a slow or dead remote must never stall loop processing.  Better
to drop a packet's upload than to queue a backlog of stale data.

## Prerequisites

LoopData uses WeeWX's `weeutil.rsyncupload.RsyncUpload` utility.  If you
have rsync working for WeeWX to push your web pages to a remote server,
loopdata's rsync is likely to work too.  First get WeeWX working with rsync
before you try to get loopdata working with rsync.

Passwordless ssh (public/private key) must be configured from the user
account weewx runs under on the weewx machine to the account on the remote
machine that has write access to `remote_dir`.

## Recommended: SSH control master multiplexing

With a 2-second loop, setting up a fresh ssh connection for every packet
means a few rsyncs a day will fail — harmless, but avoidable.  A
`.ssh/config` entry with `ControlMaster` keeps one connection open and makes
every rsync lightweight; see
[the installation page](installation.html#optional-ssh-control-master-multiplexing)
for the exact block.  With multiplexing in place, the timeout errors below
essentially disappear.

## Keep loop-data.txt out of WeeWX's html tree

It's probably better to put loop-data.txt outside of WeeWX's html tree so
that WeeWX's rsync and loopdata's rsync don't both write the file.  If
you're up for configuring your webserver to serve it from elsewhere (e.g.,
`/home/weewx/loopdata/loop-data.txt`), you should do so.  If not, it's
probably OK — there just *might* be the rare complaint in the log because
the WeeWX main thread and the LoopData thread both tried to sync the same
file at the same time.

## About those rsync errors in the log

If one is using rsync, especially with a short loop interval (e.g. 2s), it
is expected that there will be log entries for connection timeouts,
transmit timeouts, write errors and skipped packets.  By default only one
second is allowed to connect or transmit the data (`timeout = 1`), and if
the loop data is older than 3 seconds it is skipped
(`skip_if_older_than = 3`).  With these settings the remote server may miss
some loop-data packets, but it won't get caught behind trying to send a
backlog of old loop data.

Examples of a connection timeout, a transmission timeout, a write error and
a skipped packet — all fine in moderation:

```
Jul  1 04:12:03 charlemagne weewx[1126] ERROR weeutil.rsyncupload: [['rsync', '--archive', '--stats', '--timeout=1', '-e ssh -o ConnectTimeout=1', '/home/weewx/gauge-data/loop-data.txt', 'root@www.paloaltoweather.com:/home/weewx/gauge-data/loop-data.txt']] reported errors: ssh: connect to host www.paloaltoweather.com port 22: Connection timed out. rsync: connection unexpectedly closed (0 bytes received so far) [sender]. rsync error: unexplained error (code 255) at io.c(235) [sender=3.1.3]
Jun 30 20:51:48 charlemagne weewx[1126] ERROR weeutil.rsyncupload: [['rsync', '--archive', '--stats', '--timeout=1', '-e ssh -o ConnectTimeout=1', '/home/weewx/gauge-data/loop-data.txt', 'root@www.paloaltoweather.com:/home/weewx/gauge-data/loop-data.txt']] reported errors: [sender] io timeout after 1 seconds -- exiting. rsync error: timeout in data send/receive (code 30) at io.c(204) [sender=3.1.3]
Jun 27 10:18:37 charlemagne weewx[17982] ERROR weeutil.rsyncupload: [['rsync', '--archive', '--stats', '--timeout=1', '-e ssh -o ConnectTimeout=1', '/home/weewx/gauge-data/loop-data.txt', 'root@www.paloaltoweather.com:/home/weewx/gauge-data/loop-data.txt']] reported errors: rsync: [sender] write error: Broken pipe (32). rsync error: error in socket IO (code 10) at io.c(829) [sender=3.1.3]
Jun 27 23:15:53 charlemagne weewx[10156] INFO user.loopdata: skipping packet (2020-06-27 23:15:50 PDT (1593324950)) with age: 3.348237
```

If too many packets are timing out, implement ssh control master
multiplexing (above), or try raising the `timeout` value.

## How the time bounds work (6.1 and later)

`timeout` bounds more than rsync's own I/O.  rsync's `--timeout` only covers
rsync protocol I/O — not the phases ssh owns: a black-holed TCP connect, a
stalled handshake, or an auth prompt could each block the loop-processing
thread for minutes.  Since 6.1, LoopData folds ssh-side bounds into the
transport: `ConnectTimeout=<timeout>`, `ServerAliveInterval=<timeout>`,
`ServerAliveCountMax=2` and `BatchMode=yes` (an interactive prompt can never
be answered under weewxd).  Each is applied only if your own `ssh_options`
doesn't set that keyword — anything you set wins.  Setting `timeout = 0`
disables the time bounds (BatchMode is still applied).
