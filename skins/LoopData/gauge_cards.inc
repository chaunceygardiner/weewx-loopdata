## Copyright (C)2022-2026 by John A Kline (john@johnkline.com)
## Distributed under the terms of the GNU Public License (GPLv3)
## See LICENSE for your rights.

#errorCatcher Echo
<script>
  // ---- THE GAUGE CARDS (8.0): a card for every gauge, opened by a click
  // or a tap. ----------------------------------------------------------
  //
  // Everything a card says comes from loop-data.txt, the same poll the
  // panel makes: the card is another view of the same record, rewritten
  // on every packet while it is open.  There is no service file, no
  // archive read and no outside service, so what a card can say is what a
  // loopdata field can say -- a period's low and high and when each fell,
  // a sum, an average, the change over the trend window, a windrose.
  // The fields are declared in skin.conf, one <gauge>_card group per card.
  //
  // The frame: one native <dialog>, built once and filled per gauge; a
  // sticky head with the gauge's name, "n of 12" and the panel's LIVE
  // pill, and previous / next / close at the right; a full-screen sheet
  // under 600px (the css in index.html.tmpl).  A click anywhere on a
  // gauge's card opens it, and the drawing is the keyboard target (Enter
  // or Space).  Escape, the x, the arrow keys, and a press that starts
  // AND ends on the backdrop -- a drag out of the card, or a click on its
  // scrollbar, does not close it.  Focus goes to the title, never to the
  // dialog: Safari scrolls a focused dialog back to its top whenever its
  // content changes, which is every packet.
  //
  // This file follows realtime_updater.inc and leans on it: fmt(), esc(),
  // num(), withUnit(), roseSvg(), the category words and rims, put()'s
  // convention of writing an element only when its markup changed, and
  // updateGauges(), which it wraps so the open card redraws after the
  // panel does.  As there, NOTHING HERE NAMES A COLOR except EPA's
  // category bands, which are the same in both themes.
  ## The strings, translated at generation time through the report's
  ## [Texts] as the panel's are (one literal $gettext per key, so the
  ## tests can tie this file to lang/en.conf), and merged into the T
  ## table realtime_updater.inc built.
  ## Imported here as well: an #include does not share the updater's
  ## imports, and under #errorCatcher Echo a $json that does not resolve
  ## is written out as text and kills the script.
  #import json
  #set $t = {}
  #silent $t.update({'Previous gauge': $gettext('Previous gauge')})
  #silent $t.update({'Next gauge': $gettext('Next gauge')})
  #silent $t.update({'Close': $gettext('Close')})
  #silent $t.update({'{n} of {total}': $gettext('{n} of {total}')})
  #silent $t.update({'Open the {gauge} card': $gettext('Open the {gauge} card')})
  #silent $t.update({'nothing yet': $gettext('nothing yet')})
  #silent $t.update({'now': $gettext('now')})
  #silent $t.update({'low to high': $gettext('low to high')})
  #silent $t.update({'Since {year}': $gettext('Since {year}')})
  #silent $t.update({'This hour': $gettext('This hour')})
  #silent $t.update({'Last hour': $gettext('Last hour')})
  #silent $t.update({'Last 24 hours': $gettext('Last 24 hours')})
  #silent $t.update({'Today': $gettext('Today')})
  #silent $t.update({'This week': $gettext('This week')})
  #silent $t.update({'This month': $gettext('This month')})
  #silent $t.update({'This year': $gettext('This year')})
  #silent $t.update({'Rain year': $gettext('Rain year')})
  #silent $t.update({'All time': $gettext('All time')})
  #silent $t.update({'High today': $gettext('High today')})
  #silent $t.update({'Low today': $gettext('Low today')})
  #silent $t.update({'Last {hours} hours': $gettext('Last {hours} hours')})
  #silent $t.update({'change': $gettext('change')})
  #silent $t.update({'average': $gettext('average')})
  #silent $t.update({'Where it sits': $gettext('Where it sits')})
  #silent $t.update({'the low and high of each period': $gettext('the low and high of each period')})
  #silent $t.update({'Peaks': $gettext('Peaks')})
  #silent $t.update({'{change} in the last {hours} hours': $gettext('{change} in the last {hours} hours')})
  #silent $t.update({'Today from {lo} at {lot} to {hi} at {hit}, averaging {avg}': $gettext('Today from {lo} at {lot} to {hi} at {hit}, averaging {avg}')})
  #silent $t.update({'Dew point {dew}, humidity {hum}': $gettext('Dew point {dew}, humidity {hum}')})
  #silent $t.update({'{n}° below the air: the air can cool that far before dew or fog forms': $gettext('{n}° below the air: the air can cool that far before dew or fog forms')})
  #silent $t.update({'Dry under 50°F (10°C), comfortable to 60 (16), sticky to 65 (18), muggy to 70 (21), oppressive above.': $gettext('Dry under 50°F (10°C), comfortable to 60 (16), sticky to 65 (18), muggy to 70 (21), oppressive above.')})
  #silent $t.update({'Dew point {dew}, {word}': $gettext('Dew point {dew}, {word}')})
  #silent $t.update({'Relative humidity is how close the air is to holding all the water it can at its temperature; the dew point says how much water is there.': $gettext('Relative humidity is how close the air is to holding all the water it can at its temperature; the dew point says how much water is there.')})
  #silent $t.update({'{desc}, {change} in the last {hours} hours': $gettext('{desc}, {change} in the last {hours} hours')})
  #silent $t.update({'Sea-level pressure.  Falling steadily, it brings weather; falling fast, it brings it soon.': $gettext('Sea-level pressure.  Falling steadily, it brings weather; falling fast, it brings it soon.')})
  #silent $t.update({'Force {n} on the Beaufort scale, {word}': $gettext('Force {n} on the Beaufort scale, {word}')})
  #silent $t.update({'Calm': $gettext('Calm')})
  #silent $t.update({'Light air': $gettext('Light air')})
  #silent $t.update({'Light breeze': $gettext('Light breeze')})
  #silent $t.update({'Gentle breeze': $gettext('Gentle breeze')})
  #silent $t.update({'Moderate breeze': $gettext('Moderate breeze')})
  #silent $t.update({'Fresh breeze': $gettext('Fresh breeze')})
  #silent $t.update({'Strong breeze': $gettext('Strong breeze')})
  #silent $t.update({'Near gale': $gettext('Near gale')})
  #silent $t.update({'Gale': $gettext('Gale')})
  #silent $t.update({'Strong gale': $gettext('Strong gale')})
  #silent $t.update({'Storm': $gettext('Storm')})
  #silent $t.update({'Violent storm': $gettext('Violent storm')})
  #silent $t.update({'Hurricane': $gettext('Hurricane')})
  #silent $t.update({'Averaging {avg} today, {run} of wind run': $gettext('Averaging {avg} today, {run} of wind run')})
  #silent $t.update({'Averaging {avg} today': $gettext('Averaging {avg} today')})
  #silent $t.update({'Last hour: averaging {avg}, gusting {gust} from {dir}': $gettext('Last hour: averaging {avg}, gusting {gust} from {dir}')})
  #silent $t.update({'Peak gust today': $gettext('Peak gust today')})
  #silent $t.update({'Prevailing today': $gettext('Prevailing today')})
  #silent $t.update({'vector average {v}': $gettext('vector average {v}')})
  #silent $t.update({'Wind run today': $gettext('Wind run today')})
  #silent $t.update({'distance the wind traveled': $gettext('distance the wind traveled')})
  #silent $t.update({'Strongest gusts': $gettext('Strongest gusts')})
  #silent $t.update({'with the side each came from': $gettext('with the side each came from')})
  #silent $t.update({'wind now': $gettext('wind now')})
  #silent $t.update({'Wind mostly from the {dir} today': $gettext('Wind mostly from the {dir} today')})
  #silent $t.update({'{pct}% of the day from that direction, {calm}% calm': $gettext('{pct}% of the day from that direction, {calm}% calm')})
  #silent $t.update({'Each petal is the share of the day the wind came from that direction, its bands the share at each speed; the windiest direction reaches the outer ring.': $gettext('Each petal is the share of the day the wind came from that direction, its bands the share at each speed; the windiest direction reaches the outer ring.')})
  #silent $t.update({'By period': $gettext('By period')})
  #silent $t.update({'the same rose over longer spans': $gettext('the same rose over longer spans')})
  #silent $t.update({'The calm share sits in each hub.  The year rose is the station’s climate; today’s is its weather.': $gettext('The calm share sits in each hub.  The year rose is the station’s climate; today’s is its weather.')})
  #silent $t.update({'{amt} so far today': $gettext('{amt} so far today')})
  #silent $t.update({'{amt} this rain year, since {start}': $gettext('{amt} this rain year, since {start}')})
  #silent $t.update({'{h1} in the last hour, {h24} in the last 24': $gettext('{h1} in the last hour, {h24} in the last 24')})
  #silent $t.update({'since {start}': $gettext('since {start}')})
  #silent $t.update({'Rain by period': $gettext('Rain by period')})
  #silent $t.update({'each bar against the year': $gettext('each bar against the year')})
  #silent $t.update({'{amt} since {year}, everything the station has measured.': $gettext('{amt} since {year}, everything the station has measured.')})
  #silent $t.update({'Not raining': $gettext('Not raining')})
  #silent $t.update({'Light rain': $gettext('Light rain')})
  #silent $t.update({'Moderate rain': $gettext('Moderate rain')})
  #silent $t.update({'Heavy rain': $gettext('Heavy rain')})
  #silent $t.update({'Peaked at {rate} today at {t}': $gettext('Peaked at {rate} today at {t}')})
  #silent $t.update({'The rate is how hard it is raining this moment, not how much has fallen.  Light rain is under 0.10 in/h (2.5 mm/h), moderate to 0.30 (7.6), heavy above that.': $gettext('The rate is how hard it is raining this moment, not how much has fallen.  Light rain is under 0.10 in/h (2.5 mm/h), moderate to 0.30 (7.6), heavy above that.')})
  #silent $t.update({'Heaviest rate': $gettext('Heaviest rate')})
  #silent $t.update({'the hardest it rained in each period': $gettext('the hardest it rained in each period')})
  #silent $t.update({'Air {t}, humidity {h}, wind {w}': $gettext('Air {t}, humidity {h}, wind {w}')})
  #silent $t.update({'Steadman’s apparent temperature, as WeeWX computes it: the air temperature, plus what the humidity adds and less what the wind takes away, for a person in the shade.': $gettext('Steadman’s apparent temperature, as WeeWX computes it: the air temperature, plus what the humidity adds and less what the wind takes away, for a person in the shade.')})
  #silent $t.update({'Air now': $gettext('Air now')})
  #silent $t.update({'measured': $gettext('measured')})
  #silent $t.update({'The sun is {alt} above the horizon; highest at {transit}, sets {set}': $gettext('The sun is {alt} above the horizon; highest at {transit}, sets {set}')})
  #silent $t.update({'EPA names the index rounded, so 2.5 is Moderate.  The peak comes at solar noon, not clock noon, on a clear day.': $gettext('EPA names the index rounded, so 2.5 is Moderate.  The peak comes at solar noon, not clock noon, on a clear day.')})
  #silent $t.update({'Peak today': $gettext('Peak today')})
  #silent $t.update({'Sun up': $gettext('Sun up')})
  #silent $t.update({'to {set}, {len}': $gettext('to {set}, {len}')})
  #silent $t.update({'to {set}': $gettext('to {set}')})
  #silent $t.update({'the highest of each period, in EPA’s colors': $gettext('the highest of each period, in EPA’s colors')})
  #silent $t.update({'EPA’s scale': $gettext('EPA’s scale')})
  #silent $t.update({'The sun is {alt} above the horizon, {az} around from north': $gettext('The sun is {alt} above the horizon, {az} around from north')})
  #silent $t.update({'Highest at {transit}; it sets at {set} and civil twilight ends at {civil}': $gettext('Highest at {transit}; it sets at {set} and civil twilight ends at {civil}')})
  #silent $t.update({'Sunlight on a level surface, in watts per square meter.  A clear noon in summer reaches about 1000; haze, cloud and a low sun take it down.': $gettext('Sunlight on a level surface, in watts per square meter.  A clear noon in summer reaches about 1000; haze, cloud and a low sun take it down.')})
  #silent $t.update({'Daylight': $gettext('Daylight')})
  #silent $t.update({'{d} minutes on yesterday': $gettext('{d} minutes on yesterday')})
  #silent $t.update({'the brightest moment of each period': $gettext('the brightest moment of each period')})
  #silent $t.update({'PM2.5 {now} now, {h1} over the last hour, {h24} over the last 24': $gettext('PM2.5 {now} now, {h1} over the last hour, {h24} over the last 24')})
  #silent $t.update({'The index is EPA’s, from the PM2.5 reading this moment; EPA’s own AQI is a 24-hour figure, and its daily standard is 35 µg/m³.': $gettext('The index is EPA’s, from the PM2.5 reading this moment; EPA’s own AQI is a 24-hour figure, and its daily standard is 35 µg/m³.')})
  #silent $t.update({'24-hour PM2.5': $gettext('24-hour PM2.5')})
  #silent $t.update({'Worst air': $gettext('Worst air')})
  #silent $t.update({'the highest index of each period, in EPA’s colors': $gettext('the highest index of each period, in EPA’s colors')})
  #silent $t.update({'Each period’s index is EPA’s for its highest PM2.5, as weewx-purple computes it.': $gettext('Each period’s index is EPA’s for its highest PM2.5, as weewx-purple computes it.')})
  Object.assign(T, $json.dumps($t));
  ## The sixteen compass points in the report's language, for naming the
  ## windrose's leading direction (CARDINALS carries only the four the
  ## drawings letter).
  #set $ords = list(map(str, list($almanac.formatter.ordinate_names)[:16]))
  ORDINATES = $json.dumps($ords);
  ## The report's own trend window, in seconds: [Units][Trend] time_delta,
  ## which is the window loopdata's trend.* fields for this report use.
  ## Read through the $trend tag, present since WeeWX 4.6, so the sentence
  ## "in the last 3 hours" says what the report's window is.
  TREND_SECONDS = Number($json.dumps($trend.time_delta.raw));

  (function () {
    'use strict';

    var TREND_HOURS = TREND_SECONDS > 0 ? Math.round(TREND_SECONDS / 360) / 10 : 3;
    var hoursWord = String(TREND_HOURS).replace('.', DECIMAL_POINT);

    // ---- the periods a card ranks a reading against --------------------
    // [field prefix, label key, the declared when-field suffix].  Day and
    // week times are the report's own [Units][TimeFormats]; month, year and
    // all time are the two date forms the skin declares.
    var DATE = '.format("%b %-d")', DATE_YEAR = '.format("%b %-d, %Y")';
    var CAL = [
      ['1h', fmt('Last hour'), null],
      ['24h', fmt('Last 24 hours'), null],
      ['day', fmt('Today'), ''],
      ['week', fmt('This week'), ''],
      ['month', fmt('This month'), DATE],
      ['year', fmt('This year'), DATE],
      ['alltime', fmt('All time'), DATE_YEAR]
    ];
    var PEAKS = CAL.filter(function (p) { return p[2] !== null; });
    function periodLabel(p, r) {
      if (p[0] === 'alltime' && r['alltime.start.format("%Y")'] !== undefined) {
        return fmt('Since {year}', {year: r['alltime.start.format("%Y")']});
      }
      return p[1];
    }
    function fmtNum(v, dec) { return v.toFixed(dec === undefined ? 0 : dec).replace('.', DECIMAL_POINT); }

    // ---- THE LADDER: one row per period, each a bar from the period's
    // low to its high on a shared axis, the reading now as a line through
    // every row.  A single-ended ladder (peaks, gusts, rain) runs every bar
    // from zero.  rows: [{label, lo, hi, loText, hiText, loWhen, hiWhen,
    // cls}] with lo/hi in the report's unit (.raw); now: the reading.
    function ladder(rows, now, unit, opts) {
      opts = opts || {};
      var lo = Infinity, hi = -Infinity;
      rows.forEach(function (w) {
        if (w.lo !== undefined) { lo = Math.min(lo, w.lo); }
        if (w.hi !== undefined) { hi = Math.max(hi, w.hi); }
      });
      if (now !== undefined) { lo = Math.min(lo, now); hi = Math.max(hi, now); }
      if (opts.single) { lo = 0; }
      if (opts.axis) { lo = opts.axis[0]; hi = opts.axis[1]; }
      // No row and no reading: an observation the feed lacks altogether.
      // Every row says so, and there is no axis to draw.
      if (!isFinite(lo) || !isFinite(hi)) {
        return '<div class="ladder' + (opts.single ? ' single' : '') + '">' + rows.map(function (w) {
          return '<span class="lab">' + esc(w.label) + '</span><span class="none">' + esc(fmt('nothing yet')) + '</span>';
        }).join('') + '</div>';
      }
      if (!(hi > lo)) { hi = lo + 1; }
      var pad = opts.single || opts.axis ? 0 : (hi - lo) * 0.04;
      var a0 = lo - pad, a1 = hi + pad;
      var pct = function (v) { return ((v - a0) / (a1 - a0) * 100).toFixed(2) + '%'; };
      var out = '<div class="ladder' + (opts.single ? ' single' : '') + '">';
      rows.forEach(function (w) {
        out += '<span class="lab">' + esc(w.label) + '</span>';
        if (w.hi === undefined || (!opts.single && w.lo === undefined)) {
          out += '<span class="none">' + esc(fmt('nothing yet')) + '</span>';
          return;
        }
        if (!opts.single) {
          out += '<span class="end lo">' + esc(w.loText) +
                 (w.loWhen ? '<small>' + esc(w.loWhen) + '</small>' : '') + '</span>';
        }
        var l = opts.single ? 0 : w.lo;
        out += '<span class="track"><span class="bar' + (w.cls ? ' ' + w.cls : '') +
               '" style="left:' + pct(l) + ';width:' +
               ((w.hi - l) / (a1 - a0) * 100).toFixed(2) + '%"></span>' +
               (now !== undefined ? '<span class="mark" style="left:' + pct(now) + '"></span>' : '') +
               '</span>';
        out += '<span class="end hi' + (opts.single ? ' plain' : '') + '">' + esc(w.hiText) +
               (w.hiWhen ? '<small>' + esc(w.hiWhen) + '</small>' : '') + '</span>';
      });
      out += '<span class="axis"><span>' + esc(fmtNum(a0, opts.dec) + unit) + '</span><span>' +
             esc(fmtNum(a1, opts.dec) + unit) + '</span></span></div>';
      return out;
    }
    // A ranged observation: a min and a max per period.
    function rangeLadder(r, obs, unit, dec, periods, axis) {
      var rows = (periods || CAL).map(function (p) {
        var w = p[2] || '';
        return {label: periodLabel(p, r),
                lo: num(r[p[0] + '.' + obs + '.min.raw']), hi: num(r[p[0] + '.' + obs + '.max.raw']),
                loText: withUnit(r[p[0] + '.' + obs + '.min.formatted'], unit),
                hiText: withUnit(r[p[0] + '.' + obs + '.max.formatted'], unit),
                loWhen: p[2] === null ? undefined : r[p[0] + '.' + obs + '.mintime' + w],
                hiWhen: p[2] === null ? undefined : r[p[0] + '.' + obs + '.maxtime' + w]};
      });
      return ladder(rows, num(r['current.' + obs + '.raw']), unit, {dec: dec, axis: axis}) +
             '<div class="key"><i></i>' + esc(fmt('now')) + ' &nbsp; <i class="bar"></i>' +
             esc(fmt('low to high')) + '</div>';
    }
    // A peak per period, from zero; classify colors each bar by category.
    function peakLadder(r, obs, unit, dec, periods, classify) {
      var rows = periods.map(function (p) {
        var v = num(r[p[0] + '.' + obs + '.max.raw']);
        return {label: periodLabel(p, r), hi: v, hiText: r[p[0] + '.' + obs + '.max'],
                hiWhen: p[2] === null ? undefined : r[p[0] + '.' + obs + '.maxtime' + p[2]],
                cls: classify && v !== undefined ? classify(v) : undefined};
      });
      return ladder(rows, num(r['current.' + obs + '.raw']), unit, {single: true, dec: dec});
    }

    // ---- words -----------------------------------------------------------
    // The named parts set bold inside the translated sentence: fill each
    // with a marker, escape the sentence, then swap the markers for markup
    // (the same move the panel makes for "from {dir}").
    function bold(key, params) {
      var marked = {};
      Object.keys(params).forEach(function (k) { marked[k] = '\u0000' + k + '\u0000'; });
      var s = esc(fmt(key, marked));
      Object.keys(params).forEach(function (k) {
        s = s.replace('\u0000' + k + '\u0000', '<b>' + esc(params[k] === undefined ? '\u2014' : params[k]) + '</b>');
      });
      return s;
    }
    function p(html, cls) { return '<p' + (cls ? ' class="' + cls + '"' : '') + '>' + html + '</p>'; }
    function facts(items) {
      var out = '<div class="facts">';
      items.forEach(function (it) {
        if (it[1] === undefined) { return; }
        out += '<div><span class="fl">' + esc(it[0]) + '</span><b>' + esc(it[1]) + '</b>' +
               (it[2] ? '<span class="fs">' + esc(it[2]) + '</span>' : '') + '</div>';
      });
      return out + '</div>';
    }
    function section(title, aside, html, note) {
      return '<div class="sec"><div class="sec-head"><h3>' + esc(title) + '</h3>' +
             (aside ? '<span class="aside">' + esc(aside) + '</span>' : '') + '</div>' + html +
             (note ? '<p>' + note + '</p>' : '') + '</div>';
    }
    // A change as loopdata formats it, with an explicit plus.
    function signed(text) {
      if (text === undefined) { return undefined; }
      return /^-/.test(text) ? text : '+' + text;
    }
    function todayFacts(r, obs, unit) {
      return facts([
        [fmt('High today'), withUnit(r['day.' + obs + '.max.formatted'], unit), r['day.' + obs + '.maxtime']],
        [fmt('Low today'), withUnit(r['day.' + obs + '.min.formatted'], unit), r['day.' + obs + '.mintime']],
        [fmt('Last {hours} hours', {hours: hoursWord}), withUnit(signed(r['trend.' + obs + '.formatted']), unit), fmt('change')]
      ]);
    }
    // The Beaufort force from the speed in the report's unit: the scale's
    // knot thresholds, the reading converted to knots by its unit label.
    var KNOT_PER = {'mph': 0.868976, 'km/h': 0.539957, 'm/s': 1.943844, 'knot': 1};
    // The scale's lower bounds in knots, force 1 through 12, as WeeWX's
    // own wxformulas.beaufort draws them.
    var BEAUFORT_KNOTS = [1, 4, 7, 11, 17, 22, 28, 34, 41, 48, 56, 64];
    var BEAUFORT = [fmt('Calm'), fmt('Light air'), fmt('Light breeze'), fmt('Gentle breeze'),
                    fmt('Moderate breeze'), fmt('Fresh breeze'), fmt('Strong breeze'), fmt('Near gale'),
                    fmt('Gale'), fmt('Strong gale'), fmt('Storm'), fmt('Violent storm'), fmt('Hurricane')];
    function beaufort(speed, label) {
      if (speed === undefined) { return undefined; }
      var per = KNOT_PER[label.trim()];
      if (per === undefined) { return undefined; }
      var knots = speed * per, force = 0;
      while (force < BEAUFORT_KNOTS.length && knots >= BEAUFORT_KNOTS[force]) { force++; }
      return force;
    }
    // The AMS Glossary's rain-rate words: light under 0.10 in/h (2.5 mm/h),
    // moderate to 0.30 (7.6), heavy above.
    function rateWord(rate, metric) {
      if (!(rate > 0)) { return fmt('Not raining'); }
      var inph = metric ? rate / 25.4 : rate;
      return inph < 0.1 ? fmt('Light rain') : inph <= 0.3 ? fmt('Moderate rain') : fmt('Heavy rain');
    }
    function scaleTable(bands, names, value) {
      var out = '<table class="scale">', prev = 0, v = value === undefined ? undefined : Math.round(value);
      bands.forEach(function (b, i) {
        var top = b[0] === Infinity ? undefined : b[0];
        var on = v !== undefined && v >= prev && (top === undefined || v <= top);
        out += '<tr' + (on ? ' class="on"' : '') + '><td><i class="' + b[1] + '"></i></td>' +
               '<td>' + esc(names[i]) + '</td><td class="n">' +
               esc(top === undefined ? prev + '+' : prev + '\u2013' + top) + '</td></tr>';
        prev = top + 1;
      });
      return out + '</table>';
    }
    // EPA's bands as the panel names them (UV_WORDS rounds the index; the
    // AQI is whole), each with its rim class, which the css colors.
    var UV_BANDS = [[2, 'uv-1'], [5, 'uv-2'], [7, 'uv-3'], [10, 'uv-4'], [Infinity, 'uv-5']];
    var AQI_BANDS = [[50, 'aqi-1'], [100, 'aqi-2'], [150, 'aqi-3'], [200, 'aqi-4'],
                     [300, 'aqi-5'], [Infinity, 'aqi-6']];
    function classifier(bands) {
      return function (v) {
        v = Math.round(v);
        for (var i = 0; i < bands.length; i++) { if (v <= bands[i][0]) { return bands[i][1]; } }
        return bands[bands.length - 1][1];
      };
    }
    var uvClass = classifier(UV_BANDS), aqiClass = classifier(AQI_BANDS);
    // EPA's PM2.5 index from a concentration in \u00b5g/m\u00b3, the same
    // piecewise line weewx-purple computes pm2_5_aqi with (EPA's 2024
    // breakpoints).  The AQI is an xtype, computed per packet and never
    // archived, so a period's worst air is the index of its highest
    // PM2.5, which IS archived: the ladder ranks that.
    function pm25Aqi(x) {
      if (x === undefined || x < 0) { return undefined; }
      x = Math.trunc(x * 10) / 10;       // purple truncates to a tenth first
      return x <= 9.0 ? Math.round(x / 9.0 * 50)
           : x <= 35.4 ? Math.round((x - 9.1) / 26.3 * 49.0 + 51.0)
           : x <= 55.4 ? Math.round((x - 35.5) / 19.9 * 49.0 + 101.0)
           : x <= 125.4 ? Math.round((x - 55.5) / 69.9 * 49.0 + 151.0)
           : x <= 225.4 ? Math.round((x - 125.5) / 99.9 * 99.0 + 201.0)
           : Math.round((x - 225.5) / 99.9 * 199.0 + 301.0);
    }
    var NO_HOUR = CAL.filter(function (p) { return p[0] !== '1h'; });

    // ---- the cards -------------------------------------------------------
    // key -> {gauge, read | legend, body(r)}: the drawing and the read
    // lines the card copies from the panel, and the words below them.
    // The card's title is the gauge's own heading, translated when the
    // page was generated.  body() returns the sentence block, then the tiles and
    // sections from the first '<div class="sec">' or facts on.
    var CARDS = {
      rose: {gauge: 'gauge-rose', legend: 'rose-legend', body: function (r) {
        var banded = r['day.windrose.banded'], calm = num(r['day.windrose.calm']) || 0;
        if (banded === undefined) { return ''; }
        var total = calm, best = 0, bestT = 0;
        banded.forEach(function (row, i) {
          var t = row.reduce(function (x, y) { return x + y; }, 0);
          total += t;
          if (t > bestT) { bestT = t; best = i; }
        });
        var say = '';
        if (total > 0 && bestT > 0) {
          say = p(bold('Wind mostly from the {dir} today', {dir: ORDINATES[best]}), 'lead') +
                p(bold('{pct}% of the day from that direction, {calm}% calm',
                       {pct: Math.round(bestT / total * 100), calm: Math.round(calm / total * 100)}));
        }
        say += p(esc(fmt('Each petal is the share of the day the wind came from that direction, its bands the share at each speed; the windiest direction reaches the outer ring.')), 'fine');
        var roses = '<div class="roses">';
        [['hour', fmt('This hour')], ['day', fmt('Today')], ['week', fmt('This week')],
         ['month', fmt('This month')], ['year', fmt('This year')]].forEach(function (pp) {
            var b = r[pp[0] + '.windrose.banded'], c = num(r[pp[0] + '.windrose.calm']) || 0;
            if (b === undefined) { return; }
            roses += '<figure><div class="draw">' + roseSvg(b, c) + '</div><figcaption>' +
                     esc(pp[1]) + '</figcaption></figure>';
          });
        roses += '</div>';
        return say + section(fmt('By period'), fmt('the same rose over longer spans'), roses,
                             esc(fmt('The calm share sits in each hub.  The year rose is the station\u2019s climate; today\u2019s is its weather.')));
      }},
      wind: {gauge: 'gauge-wind', read: 'read-wind', body: function (r) {
        var unit = r['unit.label.windSpeed'] || '', force = beaufort(num(r['current.windSpeed.raw']), unit);
        var say = '';
        if (force !== undefined) {
          say += p(bold('Force {n} on the Beaufort scale, {word}', {n: force, word: BEAUFORT[force].toLowerCase()}), 'lead');
        }
        if (r['day.windSpeed.avg'] !== undefined) {
          say += p(r['day.windrun.sum'] !== undefined
                   ? bold('Averaging {avg} today, {run} of wind run', {avg: r['day.windSpeed.avg'], run: r['day.windrun.sum']})
                   : bold('Averaging {avg} today', {avg: r['day.windSpeed.avg']}));
        }
        if (r['1h.windGust.max'] !== undefined && num(r['1h.windGust.max.raw']) > 0) {
          say += p(bold('Last hour: averaging {avg}, gusting {gust} from {dir}',
                        {avg: r['1h.windSpeed.avg'], gust: r['1h.windGust.max'], dir: r['1h.wind.gustdir.ordinal_compass']}));
        }
        var tiles = facts([
          [fmt('Peak gust today'), r['day.wind.max'], r['day.wind.maxtime']],
          [fmt('Prevailing today'), r['day.wind.vecdir.ordinal_compass'],
           r['day.wind.vecavg'] === undefined ? undefined : fmt('vector average {v}', {v: r['day.wind.vecavg']})],
          [fmt('Wind run today'), r['day.windrun.sum'], fmt('distance the wind traveled')]]);
        var rows = CAL.map(function (pp) {
          var k = pp[2] === null ? 'windGust' : 'wind';
          var v = num(r[pp[0] + '.' + k + '.max.raw']), dir = r[pp[0] + '.wind.gustdir.ordinal_compass'];
          return {label: periodLabel(pp, r), hi: v,
                  hiText: r[pp[0] + '.' + k + '.max'] === undefined ? undefined
                          : r[pp[0] + '.' + k + '.max'] + (dir ? ' ' + dir : ''),
                  hiWhen: pp[2] === null ? undefined : r[pp[0] + '.wind.maxtime' + pp[2]]};
        });
        return say + tiles + section(fmt('Strongest gusts'), fmt('with the side each came from'),
                                     ladder(rows, num(r['current.windSpeed.raw']), unit, {single: true}) +
                                     '<div class="key"><i></i>' + esc(fmt('wind now')) + '</div>');
      }},
      temp: {gauge: 'gauge-temp', read: 'read-temp', body: function (r) {
        var unit = r['unit.label.outTemp'] || '';
        var say = p(bold('{change} in the last {hours} hours',
                         {change: withUnit(signed(r['trend.outTemp.formatted']), unit), hours: hoursWord}), 'lead') +
                  p(bold('Today from {lo} at {lot} to {hi} at {hit}, averaging {avg}', {
                    lo: withUnit(r['day.outTemp.min.formatted'], unit), lot: r['day.outTemp.mintime'],
                    hi: withUnit(r['day.outTemp.max.formatted'], unit), hit: r['day.outTemp.maxtime'],
                    avg: withUnit(r['day.outTemp.avg.formatted'], unit)})) +
                  p(bold('Dew point {dew}, humidity {hum}',
                         {dew: withUnit(r['current.dewpoint.formatted'], unit),
                          hum: withUnit(r['current.outHumidity.formatted'], r['unit.label.outHumidity'])}), 'fine');
        return say + todayFacts(r, 'outTemp', unit) +
               section(fmt('Where it sits'), fmt('the low and high of each period'), rangeLadder(r, 'outTemp', unit, 0));
      }},
      dew: {gauge: 'gauge-dew', read: 'read-dew', body: function (r) {
        var unit = r['unit.label.outTemp'] || '', dew = num(r['current.dewpoint.raw']), t = num(r['current.outTemp.raw']);
        var spread = dew !== undefined && t !== undefined ? t - dew : undefined;
        var say = p(esc(dewWord(dew, unit)), 'lead');
        if (spread !== undefined) {
          say += p(bold('{n}\u00b0 below the air: the air can cool that far before dew or fog forms',
                        {n: fmtNum(spread, 1)}));
        }
        say += p(bold('{change} in the last {hours} hours',
                      {change: withUnit(signed(r['trend.dewpoint.formatted']), unit), hours: hoursWord}));
        return say + todayFacts(r, 'dewpoint', unit) +
               section(fmt('Where it sits'), fmt('the low and high of each period'), rangeLadder(r, 'dewpoint', unit, 0),
                       esc(fmt('Dry under 50\u00b0F (10\u00b0C), comfortable to 60 (16), sticky to 65 (18), muggy to 70 (21), oppressive above.')));
      }},
      hum: {gauge: 'gauge-hum', read: 'read-hum', body: function (r) {
        var unit = r['unit.label.outHumidity'] || '', tu = r['unit.label.outTemp'] || '';
        var say = p(bold('Dew point {dew}, {word}', {dew: withUnit(r['current.dewpoint.formatted'], tu),
                                                     word: dewWord(num(r['current.dewpoint.raw']), tu).toLowerCase()}), 'lead') +
                  p(bold('{change} in the last {hours} hours',
                         {change: withUnit(signed(r['trend.outHumidity.formatted']), unit), hours: hoursWord})) +
                  p(esc(fmt('Relative humidity is how close the air is to holding all the water it can at its temperature; the dew point says how much water is there.')), 'fine');
        return say + todayFacts(r, 'outHumidity', unit) +
               section(fmt('Where it sits'), fmt('the low and high of each period'),
                       rangeLadder(r, 'outHumidity', unit, 0, undefined, [0, 100]));
      }},
      baro: {gauge: 'gauge-baro', read: 'read-baro', body: function (r) {
        var unit = r['unit.label.barometer'] || '';
        var say = p(bold('{desc}, {change} in the last {hours} hours',
                         {desc: r['trend.barometer.desc'] || '', change: withUnit(signed(r['trend.barometer.formatted']), unit),
                          hours: hoursWord}), 'lead') +
                  p(esc(fmt('Sea-level pressure.  Falling steadily, it brings weather; falling fast, it brings it soon.')), 'fine');
        return say + todayFacts(r, 'barometer', unit) +
               section(fmt('Where it sits'), fmt('the low and high of each period'),
                       rangeLadder(r, 'barometer', unit, unit.indexOf('inHg') >= 0 ? 2 : 0));
      }},
      rain: {gauge: 'gauge-rain', read: 'read-rain', body: function (r) {
        var unit = r['unit.label.rain'] || '', today = num(r['day.rain.sum.raw']);
        var say = p(today > 0 ? bold('{amt} so far today', {amt: r['day.rain.sum']}) : esc(fmt('Dry so far today')), 'lead');
        if (r['rainyear.rain.sum'] !== undefined) {
          say += p(bold('{amt} this rain year, since {start}',
                        {amt: r['rainyear.rain.sum'], start: r['rainyear.start.format("%b %-d")']}));
        }
        if (r['24h.rain.sum'] !== undefined) {
          say += p(bold('{h1} in the last hour, {h24} in the last 24', {h1: r['1h.rain.sum'], h24: r['24h.rain.sum']}));
        }
        var tiles = facts([[fmt('This month'), r['month.rain.sum']], [fmt('This year'), r['year.rain.sum']],
                           [fmt('Rain year'), r['rainyear.rain.sum'],
                            fmt('since {start}', {start: r['rainyear.start.format("%b %-d")'] || ''})]]);
        var rows = CAL.slice(0, 6).concat([['rainyear', fmt('Rain year')]]).map(function (pp) {
          return {label: pp[1], hi: num(r[pp[0] + '.rain.sum.raw']), hiText: r[pp[0] + '.rain.sum']};
        });
        var all = r['alltime.rain.sum'] === undefined ? '' :
          esc(fmt('{amt} since {year}, everything the station has measured.',
                  {amt: r['alltime.rain.sum'], year: r['alltime.start.format("%Y")'] || ''}));
        return say + tiles + section(fmt('Rain by period'), fmt('each bar against the year'),
                                     ladder(rows, undefined, unit, {single: true, dec: unit.indexOf('mm') >= 0 ? 0 : 2}), all);
      }},
      rrate: {gauge: 'gauge-rrate', read: 'read-rrate', body: function (r) {
        var unit = r['unit.label.rainRate'] || '', rate = num(r['current.rainRate.raw']), metric = unit.indexOf('mm') >= 0;
        var say = p(esc(rateWord(rate, metric)), 'lead');
        if (num(r['day.rainRate.max.raw']) > 0) {
          say += p(bold('Peaked at {rate} today at {t}', {rate: r['day.rainRate.max'], t: r['day.rainRate.maxtime']}));
        }
        say += p(esc(fmt('The rate is how hard it is raining this moment, not how much has fallen.  Light rain is under 0.10 in/h (2.5 mm/h), moderate to 0.30 (7.6), heavy above that.')), 'fine');
        return say + section(fmt('Heaviest rate'), fmt('the hardest it rained in each period'),
                             peakLadder(r, 'rainRate', unit, metric ? 0 : 2,
                                        PEAKS.filter(function (pp) { return pp[0] !== 'week'; })));
      }},
      feel: {gauge: 'gauge-feel', read: 'read-feel', body: function (r) {
        var unit = r['unit.label.outTemp'] || '', feel = num(r['current.appTemp.raw']), t = num(r['current.outTemp.raw']);
        var diff = feel !== undefined && t !== undefined ? feel - t : undefined;
        var say = p(diff === undefined ? '' : Math.abs(diff) < 1 ? esc(fmt('Same as the air'))
                    : bold(diff > 0 ? '{n}\u00b0 warmer than the air' : '{n}\u00b0 cooler than the air',
                           {n: Math.round(Math.abs(diff))}), 'lead') +
                  p(bold('Air {t}, humidity {h}, wind {w}',
                         {t: withUnit(r['current.outTemp.formatted'], unit),
                          h: withUnit(r['current.outHumidity.formatted'], r['unit.label.outHumidity']),
                          w: withUnit(r['current.windSpeed.formatted'], r['unit.label.windSpeed'])})) +
                  p(esc(fmt('Steadman\u2019s apparent temperature, as WeeWX computes it: the air temperature, plus what the humidity adds and less what the wind takes away, for a person in the shade.')), 'fine');
        return say + facts([[fmt('High today'), withUnit(r['day.appTemp.max.formatted'], unit), r['day.appTemp.maxtime']],
                            [fmt('Low today'), withUnit(r['day.appTemp.min.formatted'], unit), r['day.appTemp.mintime']],
                            [fmt('Air now'), withUnit(r['current.outTemp.formatted'], unit), fmt('measured')]]) +
               section(fmt('Where it sits'), fmt('the low and high of each period'),
                       rangeLadder(r, 'appTemp', unit, 0));
      }},
      uv: {gauge: 'gauge-uv', read: 'read-uv', body: function (r) {
        var uv = num(r['current.UV.raw']);
        var say = p(esc(categoryWord(UV_WORDS, uv)), 'lead');
        if (r['almanac.sun.altitude'] !== undefined) {
          say += p(bold('The sun is {alt} above the horizon; highest at {transit}, sets {set}',
                        {alt: r['almanac.sun.altitude'], transit: r['almanac.sun.transit'], set: r['almanac.sunset']}));
        }
        say += p(esc(fmt('EPA names the index rounded, so 2.5 is Moderate.  The peak comes at solar noon, not clock noon, on a clear day.')), 'fine');
        var sunUp = r['almanac.sunrise'] === undefined ? undefined
          : r['almanac.sun.visible.long_form()'] !== undefined
            ? fmt('to {set}, {len}', {set: r['almanac.sunset'], len: r['almanac.sun.visible.long_form()']})
            : fmt('to {set}', {set: r['almanac.sunset']});
        return say + facts([[fmt('Peak today'), r['day.UV.max'], r['day.UV.maxtime']],
                            [fmt('Sun up'), r['almanac.sunrise'], sunUp],
                            [fmt('Last hour'), r['1h.UV.avg.formatted'], fmt('average')]]) +
               section(fmt('Peaks'), fmt('the highest of each period, in EPA\u2019s colors'),
                       peakLadder(r, 'UV', '', 1, PEAKS, uvClass)) +
               section(fmt('EPA\u2019s scale'), '',
                       scaleTable(UV_BANDS, [fmt('Low'), fmt('Moderate'), fmt('High'), fmt('Very High'), fmt('Extreme')], uv));
      }},
      rad: {gauge: 'gauge-rad', read: 'read-rad', body: function (r) {
        var unit = r['unit.label.radiation'] || '';
        var say = '';
        if (r['almanac.sun.altitude'] !== undefined) {
          say += p(bold('The sun is {alt} above the horizon, {az} around from north',
                        {alt: r['almanac.sun.altitude'], az: r['almanac.sun.azimuth']}), 'lead') +
                 p(bold('Highest at {transit}; it sets at {set} and civil twilight ends at {civil}',
                        {transit: r['almanac.sun.transit'], set: r['almanac.sunset'],
                         civil: r['almanac(horizon=-6).sun(use_center=1).set']}));
        }
        say += p(esc(fmt('Sunlight on a level surface, in watts per square meter.  A clear noon in summer reaches about 1000; haze, cloud and a low sun take it down.')), 'fine');
        return say + facts([[fmt('Peak today'), r['day.radiation.max'], r['day.radiation.maxtime']],
                            [fmt('Last hour'), r['1h.radiation.avg'], fmt('average')],
                            [fmt('Daylight'), r['almanac.sun.visible.long_form()'],
                             r['almanac.sun.visible_change.minute.format("%d")'] === undefined ? undefined
                             : fmt('{d} minutes on yesterday', {d: r['almanac.sun.visible_change.minute.format("%d")']})]]) +
               section(fmt('Peaks'), fmt('the brightest moment of each period'), peakLadder(r, 'radiation', unit, 0, PEAKS));
      }},
      aqi: {gauge: 'gauge-aqi', read: 'read-aqi', body: function (r) {
        var aqi = num(r['current.pm2_5_aqi.raw']);
        var say = p(esc(categoryWord(AQI_WORDS, aqi)), 'lead');
        if (r['24h.pm2_5.avg'] !== undefined) {
          say += p(bold('PM2.5 {now} now, {h1} over the last hour, {h24} over the last 24',
                        {now: r['current.pm2_5'], h1: r['1h.pm2_5.avg'], h24: r['24h.pm2_5.avg']}));
        }
        say += p(esc(fmt('The index is EPA\u2019s, from the PM2.5 reading this moment; EPA\u2019s own AQI is a 24-hour figure, and its daily standard is 35 \u00b5g/m\u00b3.')), 'fine');
        return say + facts([[fmt('High today'), r['day.pm2_5_aqi.max'], r['day.pm2_5_aqi.maxtime']],
                            [fmt('Low today'), r['day.pm2_5_aqi.min'], r['day.pm2_5_aqi.mintime']],
                            [fmt('24-hour PM2.5'), r['24h.pm2_5.avg'], fmt('average')]]) +
               section(fmt('Worst air'), fmt('the highest index of each period, in EPA\u2019s colors'),
                       ladder(NO_HOUR.map(function (pp) {
                         var v = pm25Aqi(num(r[pp[0] + '.pm2_5.max.raw']));
                         return {label: periodLabel(pp, r), hi: v, hiText: v === undefined ? undefined : v + ' AQI',
                                 hiWhen: pp[2] === null ? undefined : r[pp[0] + '.pm2_5.maxtime' + pp[2]],
                                 cls: v === undefined ? undefined : aqiClass(v)};
                       }), aqi, '', {single: true}),
                       esc(fmt('Each period\u2019s index is EPA\u2019s for its highest PM2.5, as weewx-purple computes it.'))) +
               section(fmt('EPA\u2019s scale'), '',
                       scaleTable(AQI_BANDS, [fmt('Good'), fmt('Moderate'), fmt('Unhealthy for Sensitive Groups'),
                                              fmt('Unhealthy'), fmt('Very Unhealthy'), fmt('Hazardous')], aqi));
      }}
    };
    var ORDER = ['rose', 'wind', 'temp', 'dew', 'hum', 'baro', 'rain', 'rrate', 'feel', 'uv', 'rad', 'aqi'];

    // ---- the frame -------------------------------------------------------
    var dlg = null, cur = null, last = null, writtenCard = {};
    // The gauges on show, in page order: a hidden cell has no card.
    function shown() {
      return ORDER.filter(function (k) {
        var draw = document.getElementById(CARDS[k].gauge);
        return draw !== null && draw.parentNode.style.display !== 'none';
      });
    }
    function build() {
      dlg = document.createElement('dialog');
      dlg.className = 'card';
      dlg.innerHTML =
        '<div class="card-head"><div><h2 id="card-title" tabindex="-1"></h2>' +
        '<p class="sub"><span id="card-n"></span><span class="pill" id="card-live"></span></p></div>' +
        '<div class="card-nav"><button type="button" data-step="-1" aria-label="' + esc(fmt('Previous gauge')) + '">\u2039</button>' +
        '<button type="button" data-step="1" aria-label="' + esc(fmt('Next gauge')) + '">\u203a</button>' +
        '<button type="button" data-step="0" aria-label="' + esc(fmt('Close')) + '">\u00d7</button></div></div>' +
        '<div class="card-body"><div class="now"><div><div class="draw" id="card-gauge"></div>' +
        '<div class="read" id="card-read"></div><div class="legend" id="card-legend"></div></div>' +
        '<div class="say" id="card-say"></div></div><div id="card-more"></div></div>';
      document.body.appendChild(dlg);
      dlg.querySelectorAll('button').forEach(function (b) {
        b.addEventListener('click', function () {
          var d = Number(b.dataset.step);
          if (d === 0) { dlg.close(); } else { step(d); b.focus(); }
        });
      });
      var downOutside = false;
      function outside(e) {
        var b = dlg.getBoundingClientRect();
        return e.target === dlg && (e.clientX < b.left || e.clientX > b.right ||
                                    e.clientY < b.top || e.clientY > b.bottom);
      }
      dlg.addEventListener('pointerdown', function (e) { downOutside = outside(e); });
      dlg.addEventListener('click', function (e) { if (downOutside && outside(e)) { dlg.close(); } });
      dlg.addEventListener('close', function () {
        document.documentElement.classList.remove('card-open');
        cur = null;
        writtenCard = {};
      });
      document.addEventListener('keydown', function (e) {
        if (!dlg.open || /^(INPUT|SELECT|TEXTAREA)$/.test(e.target.tagName)) { return; }
        if (e.key === 'ArrowLeft') { step(-1); } else if (e.key === 'ArrowRight') { step(1); }
      });
    }
    function step(d) {
      var list = shown(), i = list.indexOf(cur);
      if (list.length === 0) { return; }
      openCard(list[(i + d + list.length) % list.length]);
    }
    function putCard(id, html) {
      if (writtenCard[id] === html) { return; }
      document.getElementById(id).innerHTML = html;
      writtenCard[id] = html;
    }
    // The gauge's heading, as the page rendered it.
    function titleOf(key) {
      var draw = document.getElementById(CARDS[key].gauge);
      var h = draw === null ? null : draw.parentNode.querySelector('h2');
      return h === null ? '' : h.textContent.trim();
    }
    // Copy an element's markup, or nothing when the page lacks it.
    function copyOf(id) {
      var el = id === undefined ? null : document.getElementById(id);
      return el === null ? '' : el.innerHTML;
    }
    function renderCard() {
      if (cur === null || last === null || dlg === null) { return; }
      var c = CARDS[cur], list = shown();
      putCard('card-title', esc(titleOf(cur)));
      putCard('card-n', esc(fmt('{n} of {total}', {n: list.indexOf(cur) + 1, total: list.length})));
      putCard('card-live', copyOf('live-label'));
      putCard('card-gauge', copyOf(c.gauge));
      putCard('card-read', copyOf(c.read));
      putCard('card-legend', copyOf(c.legend));
      var body = c.body(last), i = body.search(/<div class="(facts|sec)">/);
      putCard('card-say', i < 0 ? body : body.slice(0, i));
      putCard('card-more', i < 0 ? '' : body.slice(i));
    }
    function openCard(key) {
      if (dlg === null) { build(); }
      cur = key;
      renderCard();
      if (!dlg.open) {
        dlg.showModal();
        document.documentElement.classList.add('card-open');
      }
      dlg.scrollTop = 0;
      document.getElementById('card-title').focus({preventScroll: true});
    }

    // Every gauge cell opens its card; the drawing is the keyboard target.
    function wire() {
      ORDER.forEach(function (k) {
        var draw = document.getElementById(CARDS[k].gauge);
        if (draw === null) { return; }
        draw.parentNode.addEventListener('click', function () { openCard(k); });
        draw.setAttribute('role', 'button');
        draw.setAttribute('tabindex', '0');
        draw.setAttribute('aria-label', fmt('Open the {gauge} card', {gauge: titleOf(k)}));
        draw.addEventListener('keydown', function (e) {
          if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); openCard(k); }
        });
      });
    }
    // Ride the panel's poll: after the gauges draw, the open card redraws
    // from the same record.
    var panelUpdate = window.updateGauges;
    window.updateGauges = function (r) {
      panelUpdate(r);
      last = r;
      renderCard();
    };
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', wire);
    } else {
      wire();
    }
    window.LoopDataCards = {open: openCard, shown: shown, cards: CARDS};
  })();
</script>
