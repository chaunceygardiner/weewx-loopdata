## Copyright (C)2022-2026 by John A Kline (john@johnkline.com)
## Distributed under the terms of the GNU Public License (GPLv3)
## See LICENSE for your rights.

#errorCatcher Echo
<script>
  #if $Extras.get('page_update_pwd')
    // json.dumps, not a hand-quoted literal: it escapes a quote or a
    // backslash that would otherwise end the string early and kill the
    // whole script, and its \u escapes survive the html_entities encoding
    // that would turn a non-ASCII password into character references.
    // json.dumps leaves the slash alone, so the closing-tag sequence is
    // escaped after it: the HTML parser ends this script block at a
    // literal closing script tag, in a javascript string or not.  (This
    // comment says it in words for the same reason.)
    // chr(92) rather than a backslash literal, and NOT a tidy-up to undo:
    // this file sets #errorCatcher Echo, so Cheetah does not compile the
    // placeholder inline -- it eval()s the expression's source out of a
    // triple-quoted string.  A backslash in the template is consumed by
    // that outer literal, leaving an invalid escape in the eval'd source,
    // which warns on EVERY render under Python 3.12+ and is slated to
    // become a SyntaxError.
    page_update_pwd = $json.dumps($Extras.page_update_pwd).replace('</', '<' + chr(92) + '/');
  #else
    // No password configured, so no URL exempts the page.  Not a sentinel
    // word: that one is in the shipped source for anyone to read.
    page_update_pwd = null;
  #end if
  // Quoted by json.dumps for the same reasons as the password above: a
  // quote in either value used to end the string early and stop the whole
  // script.  Number() of a string that is not a number is NaN, which the
  // fallback below catches.
  #if $Extras.get('refresh_rate')
    refresh_rate = Number($json.dumps($Extras.refresh_rate).replace('</', '<' + chr(92) + '/'));
  #else
    refresh_rate = 2;
  #end if
  #if $Extras.get('expiration_time')
    expiration_time = Number($json.dumps($Extras.expiration_time).replace('</', '<' + chr(92) + '/'));
  #else
    expiration_time = 24;
  #end if
  // ConfigObj yields strings, so a '0' passes the tests above.  A negative
  // or non-numeric setting means the default -- a negative is a number, so
  // isNaN alone would let one through, and for expiration_time it would
  // then read as never expiring.  expiration_time = 0 IS never; refresh_rate
  // has no such reading, since a page that never polls is a dead page.
  if (!(refresh_rate > 0)) {
    refresh_rate = 2;
  }
  if (isNaN(expiration_time) || expiration_time < 0) {
    expiration_time = 24;
  }
  // The report's language drives toLocaleTimeString (the last-update
  // stamp); an unknown tag must not break every render.
  LOCALE = '$lang';
  try {
    new Date().toLocaleString(LOCALE);
  } catch (e) {
    console.log('bad lang "' + LOCALE + '", using en-US');
    LOCALE = 'en-US';
  }
  ## Everything this script composes is translated at generation time and
  ## fed here: compass cardinals from the report formatter's ordinates
  ## (the same source as .ordinal_compass fields when this report is the
  ## loopdata target), and the badge/readout strings from [Texts] via
  ## $gettext (each key spelled out literally so the tests can tie the
  ## include to lang/en.conf).  Both ride through json.dumps, which
  ## \u-escapes every non-ASCII character, so html_entities encoding can
  ## never touch them; the drawing code escapes them again on the way into
  ## the markup it writes.
  #import json
  #set $ords = $almanac.formatter.ordinate_names
  CARDINALS = $json.dumps([str($ords[0]), str($ords[4]), str($ords[8]), str($ords[12])]);
  #set $t = {}
  #silent $t.update({'LIVE': $gettext('LIVE')})
  #silent $t.update({'{age}s ago': $gettext('{age}s ago')})
  #silent $t.update({'NO DATA (HTTP {status}) — check loop_data_file': $gettext('NO DATA (HTTP {status}) — check loop_data_file')})
  #silent $t.update({'BAD DATA — check loop_data_file': $gettext('BAD DATA — check loop_data_file')})
  #silent $t.update({'OFFLINE': $gettext('OFFLINE')})
  #silent $t.update({'CLICK-ME': $gettext('CLICK-ME')})
  #silent $t.update({'calm': $gettext('calm')})
  #silent $t.update({'from {dir}': $gettext('from {dir}')})
  #silent $t.update({'gusting {gust}': $gettext('gusting {gust}')})
  #silent $t.update({'gusting {gust} from {dir}': $gettext('gusting {gust} from {dir}')})
  #silent $t.update({'prevailing {dir}': $gettext('prevailing {dir}')})
  #silent $t.update({'peak {peak}': $gettext('peak {peak}')})
  #silent $t.update({'today': $gettext('today')})
  #silent $t.update({'Dry so far today': $gettext('Dry so far today')})
  #silent $t.update({'rate {rate}': $gettext('rate {rate}')})
  #silent $t.update({'Same as the air': $gettext('Same as the air')})
  #silent $t.update({'{n}° warmer than the air': $gettext('{n}° warmer than the air')})
  #silent $t.update({'{n}° cooler than the air': $gettext('{n}° cooler than the air')})
  #silent $t.update({'Low': $gettext('Low')})
  #silent $t.update({'Moderate': $gettext('Moderate')})
  #silent $t.update({'High': $gettext('High')})
  #silent $t.update({'Very High': $gettext('Very High')})
  #silent $t.update({'Extreme': $gettext('Extreme')})
  #silent $t.update({'Good': $gettext('Good')})
  #silent $t.update({'Unhealthy for Sensitive Groups': $gettext('Unhealthy for Sensitive Groups')})
  #silent $t.update({'Unhealthy': $gettext('Unhealthy')})
  #silent $t.update({'Very Unhealthy': $gettext('Very Unhealthy')})
  #silent $t.update({'Hazardous': $gettext('Hazardous')})
  #silent $t.update({'pm2.5 {pm25}': $gettext('pm2.5 {pm25}')})
  T = $json.dumps($t);
  ## The report's decimal point, for the numerals printed on the dials:
  ## the readings come from .formatted fields, which loopdata renders in
  ## the report's locale, and a dial reading "29.5" under a "29,9" would
  ## disagree with itself.  WeeWX generates a report with the report's
  ## locale set, so the running locale IS the report's here -- and a lang
  ## that does not resolve falls back the same way loopdata's own
  ## rendering does.
  #import locale
  DECIMAL_POINT = $json.dumps($locale.localeconv()['decimal_point']);
  // Composed translations: look the English key up in T (falling back to
  // the key, so a missing entry renders English) and fill the {named}
  // placeholders.  Javascript key literals must spell non-ASCII with \u
  // escapes to match json.dumps' escaping of the generated object.
  function fmt(key, params) {
    var s = T[key] || key;
    for (var k in params) {
      s = s.replace('{' + k + '}', params[k]);
    }
    return s;
  }
  function addLoadEvent(func) {
    var oldonload = window.onload;
    if (typeof window.onload != 'function') {
      window.onload = func;
    } else {
      window.onload = function() {
        if (oldonload) {
          oldonload();
        }
        func();
      }
    }
  }
  function getUrlParam(paramName) {
      var name, regexS, regex, results;
      name = paramName.replace(/(\[|\])/g, '\\$1');
      regexS = '[\\?&]' + name + '=([^&#]*)';
      regex = new RegExp(regexS);
      results = regex.exec(window.location.href);
      if (results === null) {
          return '';
      }
      return results[1];
  }
  // The URL value arrives percent-encoded, so compared raw a password
  // holding any character the browser encodes could never match -- but a
  // password that itself contains a percent-escape DID match raw before
  // that was fixed, and those kiosk URLs must go on working.  Either
  // reading is accepted.  A '+' is left alone: this is a hand-written URL,
  // not a form submission.
  function urlParamMatches(given, expected) {
      if (expected === null) {
          return false;                  // no password: nothing exempts
      }
      if (given === expected) {
          return true;
      }
      try {
          return decodeURIComponent(given) === expected;
      } catch (e) {
          return false;                  // malformed escape: raw was its chance
      }
  }
  // The SERVER's clock, read off the response that carried the record.
  // Date.parse only decodes the header's string form; it never consults
  // the viewer's clock.  That is the whole point: a station timestamp
  // subtracted from the browser's wall clock charges whatever the viewer's
  // device is set wrong by straight to the reported age, and with a LIVE
  // window of 3 x refresh_rate -- six seconds at the default -- a tablet
  // ten seconds out never shows LIVE over a perfectly healthy feed.
  // Add back any Age a proxy reports, or a cached copy reads as young as
  // when it was stored.  NaN when the header is absent or unparseable
  // (Date is not CORS-safelisted, so a cross-origin fetch needs
  // Access-Control-Expose-Headers: Date); the caller then falls back to
  // the monotonic backstop.
  function serverNowFromHeaders(getHeader) {
    var serverNow = Date.parse(getHeader('Date'));
    if (isNaN(serverNow)) {
      return NaN;
    }
    var cacheAge = Number(getHeader('Age'));
    if (isFinite(cacheAge) && cacheAge > 0) {
      serverNow += cacheAge * 1000;
    }
    return serverNow;
  }

  // Every write to the two status elements goes through these: a page
  // adapted from this one need not carry them, and a missing element must
  // not throw out of the poll -- which would take the gauge drawing with
  // it and leave the panel dead.
  function setIndicator(text) {
    var el = document.getElementById("live-label");
    if (el !== null) {
      el.innerHTML = text;
    }
  }
  function setStamp(text) {
    var el = document.getElementById("last-update");
    if (el !== null) {
      el.innerHTML = text;
    }
  }

  // When the feed last carried a NEW record, on the monotonic clock.
  var lastStampSeen = null;
  var lastChangeAt = null;

  var pageTimedOut = false;
  function expirePage() {
    pageTimedOut = true;
  }
  function setUpExpiredClickListener() {
    var liveLabel = document.getElementById("live-label");
    if (liveLabel === null) {
      return;
    }
    if (liveLabel.textContent !== fmt('CLICK-ME')) {
      liveLabel.innerHTML = fmt('CLICK-ME');
      // set an onclick event on live-label to restart everything
      liveLabel.addEventListener("click", clickListener);
    }
  }
  function clickListener() {
    // disable the onClick event again
    var liveLabel = document.getElementById("live-label");
    liveLabel.removeEventListener('click', clickListener);
    liveLabel.innerHTML = "";
    // restart everything
    pageTimedOut = false;
    // restart the page timeout
    setPageExpirationTimer();
  }
  function setPageExpirationTimer() {
    if (expiration_time <= 0) {
      return;                          // 0 hours: the page never expires
    }
    if (!urlParamMatches(getUrlParam('pageUpdate'), page_update_pwd)) {
      // Expire in N hours, clamped to the browser's int32 timer-delay
      // ceiling (~24.8 days): past 2147483647 ms the delay overflows and
      // the timer fires at once, so an expiration_time over ~596 hours --
      // someone reaching for "effectively never" -- would expire the page
      // immediately instead.  Zero is how you say never.
      setTimeout(expirePage,
                 Math.min(1000 * 60 * 60 * expiration_time, 2147483647));
    }
  }
  setPageExpirationTimer();
  setInterval(updateCurrent, refresh_rate * 1000);
  addLoadEvent(updateCurrent);

  // ---- The instrument panel: a windrose, a compass and ten dials. ---------
  // Values are read straight from loop-data.txt: .raw fields drive the
  // geometry (needles, arcs, petals), .formatted fields and unit.label
  // fields supply every reading, and the unit.label fields pick the dial
  // scales, so the panel obeys this report's units and formatting like any
  // other loopdata page.
  //
  // SVG, built as markup and written only when it changed.  NOTHING HERE
  // NAMES A COLOR: every stroke and fill is a class that index.html.tmpl's
  // css dresses, so a theme -- the media query or the [data-theme] pin --
  // reaches the dials through the cascade with no redraw.  Each drawing is
  // a 200-unit square, scaled by css to its card, so its lines and its
  // words grow together.

  function esc(s) {
    return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;')
                    .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }
  function num(v) {
    var n = parseFloat(v);
    return isNaN(n) ? undefined : n;
  }
  function f2(n) { return n.toFixed(2); }
  // A point at radius r, deg degrees clockwise from north, about (100,100).
  function pt(r, deg) {
    var a = deg * Math.PI / 180;
    return [100 + r * Math.sin(a), 100 - r * Math.cos(a)];
  }
  function xy(p) { return f2(p[0]) + ' ' + f2(p[1]); }
  // An arc from a0 to a1 in either direction.  A sliver too short to see
  // is widened so a range whose low and high agree still shows.
  function arcD(r, a0, a1) {
    var sweep = a1 >= a0 ? 1 : 0;
    if (Math.abs(a1 - a0) < 0.3) {
      a1 = a0 + (sweep ? 0.3 : -0.3);
    }
    return 'M' + xy(pt(r, a0)) + 'A' + r + ' ' + r + ' 0 ' +
           (Math.abs(a1 - a0) > 180 ? 1 : 0) + ' ' + sweep + ' ' + xy(pt(r, a1));
  }
  function line(cls, a, b) {
    return '<line class="' + cls + '" x1="' + f2(a[0]) + '" y1="' + f2(a[1]) +
           '" x2="' + f2(b[0]) + '" y2="' + f2(b[1]) + '"/>';
  }
  function svgOpen(viewBox, cls) {
    return '<svg' + (cls ? ' class="' + cls + '"' : '') + ' viewBox="' +
           (viewBox || '0 0 200 200') + '" aria-hidden="true" focusable="false">';
  }

  // Dial scales by the report's unit, keyed off the unit.label fields.
  function tempScale(label) {
    if (label.indexOf('C') >= 0) {
      return { min: -20, max: 50, major: 10, minor: 5, dec: 0 };
    }
    return { min: 0, max: 120, major: 20, minor: 5, dec: 0 };
  }
  function baroScale(label) {
    if (label.indexOf('inHg') >= 0) {
      return { min: 28.5, max: 31, major: 0.5, minor: 0.1, dec: 1 };
    }
    if (label.indexOf('mmHg') >= 0) {
      return { min: 720, max: 790, major: 10, minor: 2, dec: 0 };
    }
    if (label.indexOf('kPa') >= 0) {
      return { min: 96, max: 105, major: 2, minor: 0.5, dec: 0 };
    }
    return { min: 960, max: 1050, major: 20, minor: 5, dec: 0 };  // mbar/hPa
  }
  function rainScale(label, value) {
    // A metric top of 20, not 25: the majors fall at quarters, and a
    // quarter of 25 put 6.25 beside a numeral written "6".  20 and every
    // doubling of it divide by four into whole millimeters.
    var metric = label.indexOf('mm') >= 0 || label.indexOf('cm') >= 0;
    var max = metric ? 20 : 1;
    while (value !== undefined && value > max) { max *= 2; }
    return { min: 0, max: max, major: max / 4, minor: max / 20,
             dec: metric ? 0 : 2 };
  }
  var HUM_SCALE = { min: 0, max: 100, major: 20, minor: 5, dec: 0 };
  var UV_SCALE = { min: 0, max: 14, major: 2, minor: 1, dec: 0 };
  var RAD_SCALE = { min: 0, max: 1400, major: 400, minor: 100, dec: 0 };
  var AQI_SCALE = { min: 0, max: 500, major: 100, minor: 25, dec: 0 };

  // THE CATEGORY RIMS, only where a published scale exists that no unit
  // setting changes: EPA's UV Index and EPA's Air Quality Index.  Each
  // band is [upper bound, class].  EPA names the UV index ROUNDED (0-2
  // Low, 3-5 Moderate, 6-7 High, 8-10 Very High, 11+ Extreme), so a UV
  // band ends at the half, and a reading is rounded before it is named:
  // 2.5 is Moderate, 5.5 is High.  The AQI is a whole number already.
  var UV_RIM = [[2.5, 'uv-1'], [5.5, 'uv-2'], [7.5, 'uv-3'], [10.5, 'uv-4'], [14, 'uv-5']];
  var AQI_RIM = [[50, 'aqi-1'], [100, 'aqi-2'], [150, 'aqi-3'], [200, 'aqi-4'],
                 [300, 'aqi-5'], [500, 'aqi-6']];
  var UV_WORDS = [[2, fmt('Low')], [5, fmt('Moderate')], [7, fmt('High')],
                  [10, fmt('Very High')], [Infinity, fmt('Extreme')]];
  var AQI_WORDS = [[50, fmt('Good')], [100, fmt('Moderate')],
                   [150, fmt('Unhealthy for Sensitive Groups')],
                   [200, fmt('Unhealthy')], [300, fmt('Very Unhealthy')],
                   [Infinity, fmt('Hazardous')]];
  function categoryWord(words, v) {
    if (v === undefined) {
      return '';
    }
    v = Math.round(v);
    for (var i = 0; i < words.length; i++) {
      if (v <= words[i][0]) {
        return words[i][1];
      }
    }
    return '';
  }

  // THE DIAL.  A 270-degree sweep with the gap at the bottom.  The face
  // fills the drawing and the numerals sit INSIDE it, so the face is as
  // large as the card allows; the needle runs out to the ticks and is
  // drawn over a numeral when it points at one, as a gauge's needle is.
  // Radii are fractions of the face's: numerals centered at 0.58, and
  // at the sides (where a long one would otherwise reach back into the
  // range arc) anchored by the outer end at 0.72; today's range just
  // inside the ticks, its inner edge at 0.78.  A little further out and
  // a four-character numeral ("31.0", "1.00") touched the arc at the
  // corners of the dial.
  var RF = 92, A0 = -135, A1 = 135;
  var R_NUM = 53, R_NUM_SIDE = 66, R_ARC = 74, R_RIM = 87;
  // opts: span [lo, hi] | trend (where the reading was trend-window ago) |
  // rim (category bands)
  function dialSvg(scale, value, opts) {
    var frac = function (v) {
      return Math.max(0, Math.min(1, (v - scale.min) / (scale.max - scale.min)));
    };
    var ang = function (v) { return A0 + (A1 - A0) * frac(v); };
    var out = svgOpen() + '<circle class="face" cx="100" cy="100" r="' + RF + '"/>';
    if (opts.rim) {
      var prev = scale.min;
      opts.rim.forEach(function (band) {
        out += '<path class="rim ' + band[1] + '" d="' +
               arcD(R_RIM, ang(prev), ang(band[0])) + '"/>';
        prev = band[0];
      });
    }
    if (opts.span && opts.span[0] !== undefined && opts.span[1] !== undefined) {
      out += '<path class="span" d="' + arcD(R_ARC, ang(opts.span[0]), ang(opts.span[1])) + '"/>';
    }
    // The barometer's trend: from where the pressure was one trend window
    // ago to where it is now, with a head at the present end.  Compared as
    // ANGLES: a pressure off the scale pins both ends to the same stop,
    // and a trend there has no length to draw.
    var t0 = opts.trend === undefined ? undefined : ang(opts.trend);
    var t1 = value === undefined ? undefined : ang(value);
    if (t0 !== undefined && t1 !== undefined && Math.abs(t1 - t0) > 0.01) {
      var dir = t1 > t0 ? 1 : -1;
      var neck = t1 - dir * 4;
      if ((neck - t0) * dir < 0) {
        neck = t0;
      }
      out += '<path class="trend" d="' + arcD(R_ARC, t0, neck) + '"/>' +
             '<path class="trend-head" d="M' + xy(pt(R_ARC, t1)) + 'L' +
             xy(pt(R_ARC - 6, neck)) + 'L' + xy(pt(R_ARC + 6, neck)) + 'Z"/>';
    }
    var steps = Math.round((scale.max - scale.min) / scale.minor);
    for (var k = 0; k <= steps; k++) {
      var v = scale.min + k * scale.minor;
      var q = (v - scale.min) / scale.major;
      var major = Math.abs(q - Math.round(q)) < 1e-6;
      var a = ang(v);
      out += line(major ? 'tick major' : 'tick', pt(RF - 2, a), pt(RF - (major ? 13 : 7), a));
      if (major) {
        var side = Math.sin(a * Math.PI / 180), anchor = 'middle', lp = pt(R_NUM, a);
        if (Math.abs(side) > 0.85) {
          anchor = side > 0 ? 'end' : 'start';
          lp = pt(R_NUM_SIDE, a);
        }
        out += '<text class="num" x="' + f2(lp[0]) + '" y="' + f2(lp[1] + 4.5) +
               '" text-anchor="' + anchor + '">' +
               esc(v.toFixed(scale.dec).replace('.', DECIMAL_POINT)) + '</text>';
      }
    }
    if (value !== undefined) {
      var na = ang(value);
      out += line('needle', pt(-18, na), pt(RF - 10, na));
    }
    return out + '<circle class="hub" cx="100" cy="100" r="6.5"/></svg>';
  }

  // THE COMPASS: letters inside the ring, the needle's head at the side
  // the wind comes FROM, the last ten minutes' gust as a lighter, shorter
  // needle, and today's prevailing direction as a short arc inside the
  // ticks.  Both needles are drawn after the letters, so an arrow covers
  // the letter it reaches rather than being read through it.
  var CL = { letters: 64, arc: 76, arcHalf: 10,
             gust: [-12, 44, 62, 9], wind: [-16, 54, 72, 9] };
  function arrow(cls, deg, n) {
    return line(cls, pt(n[0], deg), pt(n[1], deg)) +
           '<path class="' + cls + '-head" d="M' + xy(pt(n[2], deg)) + 'L' +
           xy(pt(n[1], deg - n[3])) + 'L' + xy(pt(n[1], deg + n[3])) + 'Z"/>';
  }
  function compassSvg(w) {
    var out = svgOpen() + '<circle class="face" cx="100" cy="100" r="' + RF + '"/>';
    if (w.prevailing !== undefined) {
      out += '<path class="span" d="' +
             arcD(CL.arc, w.prevailing - CL.arcHalf, w.prevailing + CL.arcHalf) + '"/>';
    }
    for (var d = 0; d < 360; d += 10) {
      var major = d % 30 === 0;
      out += line(major ? 'tick major' : 'tick', pt(RF - 2, d), pt(RF - (major ? 13 : 7), d));
    }
    CARDINALS.forEach(function (c, i) {
      var p = pt(CL.letters, i * 90);
      out += '<text class="letter' + (i === 0 ? ' north' : '') + '" x="' + f2(p[0]) +
             '" y="' + f2(p[1]) + '" text-anchor="middle" dominant-baseline="central">' +
             esc(c) + '</text>';
    });
    if (w.gust > 0 && w.gustDir !== undefined) {
      out += arrow('gust', w.gustDir, CL.gust);
    }
    if (w.speed > 0 && w.dir !== undefined) {
      out += arrow('needle', w.dir, CL.wind);
    }
    return out + '<circle class="hub" cx="100" cy="100" r="7"/></svg>';
  }

  // THE WINDROSE: petals by direction, each stacked outward by speed band,
  // scaled so the windiest direction reaches the outer ring; the share of
  // the day that was calm in the hub.
  //
  // A band's fill for ANY number of bands.  The six --rose tokens are
  // stops on a curve, not one color per band: six bands take the six
  // stops exactly, and any other count samples the same curve with css
  // color-mix, so the calmest band is always the first stop and the
  // windiest always the last.  (Indexing six colors directly gave a
  // seventh band no color at all.)
  function bandFill(i, n) {
    if (n === 6 || n <= 1) {
      return 'var(--rose' + Math.min(i + 1, 6) + ')';
    }
    var x = i / (n - 1) * 5, lo = Math.floor(x), t = x - lo;
    if (t < 1e-6 || lo >= 5) {
      return 'var(--rose' + (Math.min(lo, 5) + 1) + ')';
    }
    return 'color-mix(in srgb, var(--rose' + (lo + 1) + ') ' +
           Math.round((1 - t) * 100) + '%, var(--rose' + (lo + 2) + '))';
  }
  // The rose sits on a face of its own, with the letters inside it, and
  // its drawing is cropped wider than 200 so that face comes out exactly
  // the size of the dials' faces beside it: a face of ROSE_FACE shown as a
  // dial shows one of RF.
  var ROSE_FACE = 106, ROSE_HALF = ROSE_FACE * 100 / RF;
  var ROSE_VIEW = [100 - ROSE_HALF, 100 - ROSE_HALF, 2 * ROSE_HALF, 2 * ROSE_HALF]
      .map(function (v) { return v.toFixed(2); }).join(' ');
  function roseSvg(banded, calm) {
    var R1 = 84, r0 = 27;
    var out = svgOpen(ROSE_VIEW, 'rose') + '<circle class="face" cx="100" cy="100" r="' + ROSE_FACE + '"/>';
    [0.25, 0.5, 0.75, 1].forEach(function (k) {
      out += '<circle class="ring" cx="100" cy="100" r="' + (r0 + (R1 - r0) * k) + '"/>';
    });
    var total = calm || 0, maxDir = 0;
    banded.forEach(function (row) {
      var t = row.reduce(function (x, y) { return x + y; }, 0);
      total += t;
      maxDir = Math.max(maxDir, t);
    });
    if (total > 0 && maxDir > 0) {
      var scale = (R1 - r0) / (maxDir / total);
      banded.forEach(function (row, i) {
        var a = i * 22.5, acc = 0;
        row.forEach(function (sec, band) {
          if (!(sec > 0)) {
            return;
          }
          var ra = r0 + acc / total * scale, rb = r0 + (acc + sec) / total * scale;
          acc += sec;
          out += '<path class="petal" style="fill: ' + bandFill(band, row.length) +
                 '" d="M' + xy(pt(ra, a - 9)) + 'L' + xy(pt(rb, a - 9)) + 'A' + f2(rb) +
                 ' ' + f2(rb) + ' 0 0 1 ' + xy(pt(rb, a + 9)) + 'L' + xy(pt(ra, a + 9)) +
                 'A' + f2(ra) + ' ' + f2(ra) + ' 0 0 0 ' + xy(pt(ra, a - 9)) + 'Z"/>';
        });
      });
    }
    CARDINALS.forEach(function (c, i) {
      var p = pt(R1 + 10, i * 90);
      out += '<text class="letter' + (i === 0 ? ' north' : '') + '" x="' + f2(p[0]) +
             '" y="' + f2(p[1]) + '" text-anchor="middle" dominant-baseline="central">' +
             esc(c) + '</text>';
    });
    // A translation of "calm" longer than the hub has room for is set
    // smaller rather than run over its edge.
    var calmWord = fmt('calm');
    return out + '<circle class="hub-disc" cx="100" cy="100" r="25"/>' +
           '<text class="hub-pct" x="100" y="100" text-anchor="middle">' +
           (total > 0 ? Math.round((calm || 0) / total * 100) + '%' : '\u2014') + '</text>' +
           '<text class="hub-calm" x="100" y="114" text-anchor="middle"' +
           (calmWord.length > 6 ? ' style="font-size: ' + (80 / calmWord.length).toFixed(1) + 'px"' : '') +
           '>' + esc(calmWord) + '</text></svg>';
  }
  function legendHtml(edges, unit) {
    var html = '';
    edges.forEach(function (edge, i) {
      html += '<span><i style="background: ' + bandFill(i, edges.length) + '"></i>' +
              esc(i < edges.length - 1 ? edge + '\u2013' + edges[i + 1] : edge + '+') + '</span>';
    });
    return html + '<span>' + esc(unit.trim()) + '</span>';
  }

  // ---- the lines under a dial: the reading, what it means, today's
  // high above its low.  The number is the field's .formatted and the
  // unit its unit.label, both as this report renders them.
  function readingHtml(number, unit) {
    return '<div class="reading"><b>' + esc(number === undefined ? '--' : number) + '</b>' +
           (unit && number !== undefined ? '<span class="unit">' + esc(unit) + '</span>' : '') +
           '</div>';
  }
  function wordHtml(html) {
    return '<div class="word">' + (html || '&nbsp;') + '</div>';
  }
  // notes: further items for the same line (a peak, the prevailing wind).
  function rangeHtml(hi, lo, notes) {
    var out = '<div class="range">';
    if (hi !== undefined) {
      out += '<span class="hi">\u25b2 ' + esc(hi) + '</span>';
    }
    if (lo !== undefined) {
      out += '<span class="lo">\u25bc ' + esc(lo) + '</span>';
    }
    (notes || []).forEach(function (note) {
      out += '<span>' + esc(note) + '</span>';
    });
    return out + '</div>';
  }
  // A formatted value with its unit label, or undefined when the value is
  // missing.
  function withUnit(value, unit) {
    return value === undefined ? undefined : value + (unit || '');
  }

  // Each element is rewritten only when its markup changed.
  var written = {};
  function put(id, html) {
    if (written[id] === html) {
      return;
    }
    var el = document.getElementById(id);
    if (el !== null) {
      el.innerHTML = html;
      written[id] = html;
    }
  }

  // Cells for observations a station may not report (no sensor, or the
  // xtype/appTemp not computed) hide entirely instead of showing a dead
  // dial; they reappear if the field shows up in loop-data.txt.
  function showCell(id, show) {
    var el = document.getElementById(id);
    if (el !== null) {
      el.style.display = show ? '' : 'none';
    }
  }

  // Temperature, dew point, feels like and humidity: a dial with today's
  // range as its arc, and today's high above its low.
  function ranged(r, id, obs, scale, unit, word) {
    var v = num(r['current.' + obs + '.raw']);
    var lo = num(r['day.' + obs + '.min.raw']), hi = num(r['day.' + obs + '.max.raw']);
    put('gauge-' + id, dialSvg(scale, v, {span: [lo, hi]}));
    put('read-' + id, readingHtml(r['current.' + obs + '.formatted'], unit) + wordHtml(esc(word)) +
        rangeHtml(withUnit(r['day.' + obs + '.max.formatted'], unit),
                  withUnit(r['day.' + obs + '.min.formatted'], unit)));
  }
  // UV, solar radiation and rain rate: the arc runs from the floor to
  // today's peak.
  function peaked(r, id, obs, scale, unit, word, rim) {
    var v = num(r['current.' + obs + '.raw']), peak = num(r['day.' + obs + '.max.raw']);
    put('gauge-' + id, dialSvg(scale, v, {span: peak > scale.min ? [scale.min, peak] : null,
                                          rim: rim}));
    put('read-' + id, readingHtml(r['current.' + obs + '.formatted'], unit) + wordHtml(esc(word)) +
        rangeHtml(undefined, undefined,
                  peak > scale.min && r['day.' + obs + '.max'] !== undefined
                      ? [fmt('peak {peak}', {peak: r['day.' + obs + '.max']})] : []));
  }

  function updateGauges(result) {
    var r = result;
    var tLabel = r['unit.label.outTemp'] || '';

    if (r['day.windrose.banded'] !== undefined) {
      put('gauge-rose', roseSvg(r['day.windrose.banded'], num(r['day.windrose.calm'])));
      if (r['windrose.bands'] !== undefined) {
        put('rose-legend', legendHtml(r['windrose.bands'], r['unit.label.windSpeed'] || ''));
      }
    }

    var speed = num(r['current.windSpeed.raw']), gust = num(r['10m.windGust.max.raw']);
    var windFrom = r['current.windDir.ordinal_compass'];
    var gustFrom = r['10m.wind.gustdir.ordinal_compass'];
    put('gauge-wind', compassSvg({speed: speed, dir: num(r['current.windDir.raw']),
                                  gust: gust, gustDir: num(r['10m.wind.gustdir.raw']),
                                  prevailing: num(r['day.wind.vecdir.raw'])}));
    // "from NNE · gusting 7 mph", naming the gust's bearing only where it
    // is not the wind's; no speed is no reading, not a calm.
    var said = [];
    if (speed > 0) {
      if (windFrom !== undefined) {
        // The bearing is set bold inside the translated sentence: fill a
        // marker, escape the sentence, then swap the marker for the markup.
        said.push(esc(fmt('from {dir}', {dir: '\u0000'}))
                  .replace('\u0000', '<b>' + esc(windFrom) + '</b>'));
      }
    } else if (speed !== undefined) {
      said.push('<b>' + esc(fmt('calm')) + '</b>');
    }
    if (gust > 0 && r['10m.windGust.max'] !== undefined) {
      said.push(esc(gustFrom !== undefined && (gustFrom !== windFrom || !(speed > 0))
          ? fmt('gusting {gust} from {dir}', {gust: r['10m.windGust.max'], dir: gustFrom})
          : fmt('gusting {gust}', {gust: r['10m.windGust.max']})));
    }
    var notes = [];
    if (num(r['day.wind.max.raw']) > 0 && r['day.wind.max'] !== undefined) {
      notes.push(fmt('peak {peak}', {peak: r['day.wind.max']}));
    }
    if (r['day.wind.vecdir.ordinal_compass'] !== undefined) {
      notes.push(fmt('prevailing {dir}', {dir: r['day.wind.vecdir.ordinal_compass']}));
    }
    put('read-wind', readingHtml(r['current.windSpeed.formatted'], r['unit.label.windSpeed']) +
        wordHtml(said.join(' \u00b7 ')) + rangeHtml(undefined, undefined, notes));

    ranged(r, 'temp', 'outTemp', tempScale(tLabel), tLabel, '');
    ranged(r, 'dew', 'dewpoint', tempScale(tLabel), tLabel, '');
    ranged(r, 'hum', 'outHumidity', HUM_SCALE, r['unit.label.outHumidity'] || '', '');

    var bLabel = r['unit.label.barometer'] || '';
    var baro = num(r['current.barometer.raw']), change = num(r['trend.barometer.raw']);
    put('gauge-baro', dialSvg(baroScale(bLabel), baro,
                              {trend: baro !== undefined && change !== undefined ? baro - change : undefined}));
    put('read-baro', readingHtml(r['current.barometer.formatted'], bLabel) +
        wordHtml(esc(r['trend.barometer.desc'] || '')) +
        rangeHtml(withUnit(r['day.barometer.max.formatted'], bLabel),
                  withUnit(r['day.barometer.min.formatted'], bLabel)));

    var rLabel = r['unit.label.rain'] || '';
    var rain = num(r['day.rain.sum.raw']), rate = num(r['current.rainRate.raw']);
    var rainWord = rain === 0 ? fmt('Dry so far today')
        : rate > 0 && r['current.rainRate'] !== undefined ? fmt('rate {rate}', {rate: r['current.rainRate']})
        : fmt('today');
    put('gauge-rain', dialSvg(rainScale(rLabel, rain), rain, {}));
    put('read-rain', readingHtml(r['day.rain.sum.formatted'], rLabel) + wordHtml(esc(rainWord)) +
        rangeHtml());

    // Scale off the day's peak too, so the dial does not rescale as a
    // burst passes.
    var rrLabel = r['unit.label.rainRate'] || '';
    var rateMax = num(r['day.rainRate.max.raw']);
    peaked(r, 'rrate', 'rainRate', rainScale(rrLabel, Math.max(rate || 0, rateMax || 0)),
           rrLabel, '', null);

    var feel = num(r['current.appTemp.raw']);
    showCell('cell-feel', feel !== undefined);
    if (feel !== undefined) {
      var temp = num(r['current.outTemp.raw']), diff = temp === undefined ? undefined : feel - temp;
      var feelWord = diff === undefined ? ''
          : Math.abs(diff) < 1 ? fmt('Same as the air')
          : diff > 0 ? fmt('{n}\u00b0 warmer than the air', {n: Math.round(diff)})
          : fmt('{n}\u00b0 cooler than the air', {n: Math.round(-diff)});
      ranged(r, 'feel', 'appTemp', tempScale(tLabel), tLabel, feelWord);
    }

    var uv = num(r['current.UV.raw']);
    showCell('cell-uv', uv !== undefined);
    if (uv !== undefined) {
      peaked(r, 'uv', 'UV', UV_SCALE, '', categoryWord(UV_WORDS, uv), UV_RIM);
    }

    var solar = num(r['current.radiation.raw']);
    showCell('cell-rad', solar !== undefined);
    if (solar !== undefined) {
      peaked(r, 'rad', 'radiation', RAD_SCALE, r['unit.label.radiation'] || '', '', null);
    }

    var aqi = num(r['current.pm2_5_aqi.raw']);
    showCell('cell-aqi', aqi !== undefined);
    if (aqi !== undefined) {
      var aqiWord = categoryWord(AQI_WORDS, aqi);
      if (r['current.pm2_5'] !== undefined) {
        aqiWord += ' \u00b7 ' + fmt('pm2.5 {pm25}', {pm25: r['current.pm2_5']});
      }
      put('gauge-aqi', dialSvg(AQI_SCALE, aqi, {rim: AQI_RIM}));
      put('read-aqi', readingHtml(r['current.pm2_5_aqi.formatted'], '') + wordHtml(esc(aqiWord)) +
          rangeHtml());
    }
  }

  async function updateCurrent() {
    if (pageTimedOut) {
      setUpExpiredClickListener();
      return false;
    }
    var response;
    try {
      response = await fetch('$Extras.loop_data_file', {cache: 'no-store'});
    } catch (e) {
      // Network-level failure (server unreachable, request blocked): no
      // status to show.  A later successful poll rewrites the indicator.
      setIndicator(fmt('OFFLINE'));
      console.log(e);
      return;
    }
    if (!response.ok) {
      // The server answered but not with the file.  Almost always
      // loop_data_file not resolving to where loopdata writes -- the
      // classic being a 404 page because the file lives outside HTML_ROOT
      // (say /dev/shm) with nothing on the web server serving it.  Say so
      // in the indicator: the old behavior (a console error only a
      // debugging user ever finds) left the panel silently dead.
      setIndicator(fmt('NO DATA (HTTP {status}) \u2014 check loop_data_file',
                       {status: response.status}));
      return;
    }
    var result;
    try {
      // Since loopdata 7.0 the file carries each declaring report's fields
      // under the report's name (the [StdReport] section, not the skin: one
      // skin can be listed twice).  This page reads its own.  A file
      // without the key is an older loopdata, or this report not declaring
      // its fields; BAD DATA covers it -- the file is there, this page's
      // data is not.  The name goes through json.dumps like every other
      // string Cheetah hands this script: a report name is any [StdReport]
      // section name, quotes and non-ASCII included.
      result = (await response.json())[$json.dumps($REPORT_NAME)];
      if (result === undefined) {
        throw new Error('no ' + $json.dumps($REPORT_NAME) + ' in loop_data_file');
      }
    } catch (e) {
      // A 200 with a non-json body, or json without this report's key:
      // loop_data_file points at something, but not at loopdata's output
      // for this page.
      setIndicator(fmt('BAD DATA \u2014 check loop_data_file'));
      console.log(e);
      return;
    }
    try {
      var stamp = result["current.dateTime.raw"];
      if (stamp === undefined || stamp === null || stamp === ''
          || isNaN(Number(stamp))) {
        // This report declared fields but not the clock group, so there is
        // no packet timestamp to age.  null and '' are tested before
        // Number(), which maps both to 0 -- an epoch-0 stamp would put an
        // age of half a century on the label rather than saying the data
        // is unusable.  Draw FIRST and say so after: on a page adapted
        // without these two elements, writing them first would throw and
        // the gauges would never draw at all.
        updateGauges(result);
        setIndicator(fmt('BAD DATA \u2014 check loop_data_file'));
        setStamp("");
        console.log("no current.dateTime.raw in this report's loop data");
        return;
      }
      // The LIVE indicator, from the packet's own timestamp aged against
      // the SERVER's clock -- never the viewer's.
      if (stamp !== lastStampSeen) {
        lastStampSeen = stamp;
        lastChangeAt = performance.now();
      }
      var age = null;
      var serverNow = serverNowFromHeaders(function (name) {
        return response.headers.get(name);
      });
      if (!isNaN(serverNow)) {
        age = Math.round(serverNow / 1000 - Number(stamp));
      } else {
        // No usable Date -- a cross-origin fetch without
        // Access-Control-Expose-Headers: Date, say.  Fall back to the
        // viewer's clock: a poorer estimate, and the very thing this
        // change stopped trusting, but it is the ONLY measure that can see
        // data which was already stale when the page loaded.  Without it
        // the backstop below starts at zero and an abandoned feed reads
        // LIVE for as long as the tab stays open.
        age = Math.round(Date.now() / 1000 - Number(stamp));
      }
      // Backstop: elapsed since the data last advanced is always a LOWER
      // bound on the true age, so it can only win where the header reads
      // too young -- a response served from cache, whose Date is frozen
      // with its body.  performance.now() is monotonic, so an NTP step on
      // the viewer's machine cannot make this jump.
      var elapsed = Math.round((performance.now() - lastChangeAt) / 1000);
      age = (age === null) ? elapsed : Math.max(age, elapsed);
      if (age < 0) {
        age = 0;
      }
      // The stamp still renders in the viewer's own local time below --
      // that is a display of when the packet was taken, a different job
      // from measuring how old it is, and correct against their clock.
      var lastUpdate = new Date(Number(stamp) * 1000);
      // Same shape as the no-clock branch above: draw FIRST, then write the
      // labels, and guard both lookups.  A page adapted without them --
      // which the manual invites -- would otherwise throw here and never
      // draw a gauge, the very fault that branch was fixed for.
      updateGauges(result);
      setIndicator((age <= 3 * refresh_rate)
          ? fmt('LIVE') : fmt('{age}s ago', {age: age}));
      setStamp(lastUpdate.toLocaleTimeString(
          LOCALE, {hour: '2-digit', minute: '2-digit', second: '2-digit'}));
    } catch (e) {
      // A rendering error must not stop the polling; try again next
      // interval.
      console.log(e);
    }
  }
</script>
