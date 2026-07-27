---
title: Building a live page
layout: default
nav_order: 8
---

# Building a live page

The recipe for using LoopData in your own skin, demonstrated in full by the
included [sample skin](sample-skin.html) (`skins/LoopData`):

## 1. List the fields your page needs

Put every field your page needs on the `fields` line of
`[LoopData] [[Include]]` in weewx.conf.  The
[field reference](field-reference.html) documents everything available.

## 2. Point LoopData at your report

Set `target_report` to your report, so values arrive already in that
report's units and formatting, and set `loop_data_dir`/`filename` so the
json file lands somewhere your web server serves.  By default,
`loop_data_dir` is relative to the target report's HTML directory, so the
page can fetch the file with a relative URL.

## 3. Give elements ids matching the json keys

In your page, give an id to each HTML element that should show a value.  The
simplest convention is to make the id the json key itself:

```html
Outside Temperature: <span id="current.outTemp"></span>
Today's High: <span id="day.outTemp.max"></span>
```

Better yet, have Cheetah render the report-time value inside the element
(`<span id="current.outTemp">$current.outTemp</span>`), so the page is
correct even before the first poll — the javascript then keeps it fresh.

## 4. Poll loop-data.txt and fill the elements

Add javascript that fetches loop-data.txt on an interval matching your loop
frequency and fills in the elements.  A minimal version:

```html
<script>
  async function updateLoopData() {
    try {
      const response = await fetch('loop-data.txt', {cache: 'no-store'});
      const data = await response.json();
      for (const key in data) {
        const element = document.getElementById(key);
        if (element) element.innerHTML = data[key];
      }
    } catch (e) {
      // File unreachable; try again next interval.
    }
  }
  updateLoopData();
  setInterval(updateLoopData, 2000);  // match your loop frequency
</script>
```

## 5. Handle missing fields

A field missing from the packet is missing from the json (see
[Missing data](field-reference.html#missing-data)).  The loop above simply
leaves the old value in place; the sample skin's gauges instead draw the
dial with no needle and a `--` readout — choose what suits your page.  (Or
force a field to always be present with `string()` — see the field
reference.)

## Production niceties

The sample skin's `realtime_updater.inc` shows the touches that separate a
demo from a page you can leave running for months:

* **A LIVE/age indicator** driven by `current.dateTime.raw` — the packet's
  own timestamp, so the page knows how fresh its data really is.  The sample
  skin distinguishes the failure modes: a rejected fetch (server
  unreachable) shows OFFLINE; an HTTP error shows
  `NO DATA (HTTP 404) — check loop_data_file` (the classic cause: loopdata
  writing outside the web server's tree); a 200 whose body isn't json shows
  `BAD DATA — check loop_data_file`.  A later successful poll rewrites the
  indicator to LIVE.
* **A page-expiration timer** that stops polling in abandoned browser tabs.
  In the sample skin this is the `expiration_time` Extras option (in hours);
  loading the page with `?pageUpdate=<page_update_pwd>` exempts it (for a
  kiosk display that should never stop).
* **Keep rendering errors out of the poll loop.**  If your page draws
  (canvas gauges, charts), catch drawing errors separately from fetch
  errors, so a drawing bug cannot stop the polling.

{: .note }
There is no need to guard against reading loop-data.txt mid-write: LoopData
writes the file atomically (temp file plus rename).  A fetch either gets the
old complete file or the new complete file.
