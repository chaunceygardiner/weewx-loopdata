## Copyright (C)2022-2026 by John A Kline (john@johnkline.com)
## Distributed under the terms of the GNU Public License (GPLv3)
## See LICENSE for your rights.

#errorCatcher Echo
<script>
  #if $Extras.has_key('page_update_pwd')
    page_update_pwd = '$Extras.page_update_pwd';
  #else
    page_update_pwd = 'foo';
  #end if
  #if $Extras.get('refresh_rate')
    refresh_rate = $Extras.refresh_rate;
  #else
    refresh_rate = 2;
  #end if
  #if $Extras.get('expiration_time')
    expiration_time = $Extras.expiration_time;
  #else
    expiration_time = 4;
  #end if
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
  ## \u-escapes every non-ASCII character -- html_entities encoding can
  ## never touch them, and canvas text takes no entities anyway.
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
  #silent $t.update({'gust {gust}': $gettext('gust {gust}')})
  #silent $t.update({'gust {gust} @ {dir}': $gettext('gust {gust} @ {dir}')})
  #silent $t.update({'today': $gettext('today')})
  #silent $t.update({'today {min} to {max}': $gettext('today {min} to {max}')})
  #silent $t.update({'max today {max}': $gettext('max today {max}')})
  #silent $t.update({'rate {rate}': $gettext('rate {rate}')})
  #silent $t.update({'pm2.5 {pm25}': $gettext('pm2.5 {pm25}')})
  T = $json.dumps($t);
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
      } else {
          return results[1];
      }
  }
  var pageTimedOut = false;
  function expirePage() {
    pageTimedOut = true;
  }
  function setUpExpiredClickListener() {
    var liveLabel = document.getElementById("live-label");
    if (liveLabel != fmt('CLICK-ME')) {
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
    if (getUrlParam('pageUpdate') !== page_update_pwd) {
      // expire in N hours
      setTimeout(expirePage, 1000 * 60 * 60 * expiration_time);
    }
  }
  setPageExpirationTimer();
  setInterval(updateCurrent, refresh_rate * 1000);
  addLoadEvent(updateCurrent);

  // ---- The instrument panel: a windrose and five dials, plain canvas. ------
  // Values are read straight from loop-data.txt: .raw fields drive the
  // geometry (needles, bands, petals), report-formatted fields supply every
  // readout, and unit.label fields pick the dial scales, so the panel obeys
  // the target report's units and formatting like any other loopdata page.

  // Mirrors the :root custom properties in index.html.tmpl -- canvases
  // cannot read css variables, so the two must be kept in step.  track is
  // the dial's recessed arc; tick is the quieter grey for minor ticks and
  // the windrose's reference rings; rim is the face disc's own edge, a
  // whisper here because the dials read as instruments without it, not
  // because anything depends on it (the rose matches the dials by
  // geometry now -- same face radius -- so a louder rim is a free choice).
  var C = {
    face: '#121a26', track: '#5c718f', tick: '#3a4a63', rim: '#1e2a3a',
    ink: '#ffffff', muted: '#c6d2e2',
    amber: '#ffb92e', steel: '#9cceff', steelDim: '#8db0d6'
  };
  var SANS = 'system-ui, -apple-system, sans-serif';
  // Sequential ramp for the windrose speed bands, dim to bright.  The floor
  // clears 3:1 against the face: the calmest band is the biggest segment of
  // most petals, and it used to disappear into the disc.
  var RAMP = ['#4a739b', '#5d89b2', '#75a3c8', '#90bcdd', '#aed4ef', '#d2ecff'];

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
    var metric = label.indexOf('mm') >= 0 || label.indexOf('cm') >= 0;
    var max = metric ? 25 : 1;
    while (value !== undefined && value > max) { max *= 2; }
    return { min: 0, max: max, major: max / 4, minor: max / 20,
             dec: metric ? 0 : 2 };
  }
  var UV_SCALE = { min: 0, max: 12, major: 2, minor: 1, dec: 0 };
  var RAD_SCALE = { min: 0, max: 1200, major: 300, minor: 60, dec: 0 };
  var AQI_SCALE = { min: 0, max: 300, major: 50, minor: 10, dec: 0 };

  // Resolution-independent canvases: CSS decides the displayed size, the
  // backing store matches it at device resolution, and all drawing happens
  // in a 240-unit coordinate system so geometry and fonts scale together.
  var LOGICAL = 240;
  var ctxs = null;
  function context(id) {
    var canvas = document.getElementById(id);
    if (canvas === null || !canvas.getContext) {
      return null;
    }
    var dpr = window.devicePixelRatio || 1;
    var size = canvas.clientWidth || LOGICAL;
    canvas.width = size * dpr;
    canvas.height = size * dpr;
    var ctx = canvas.getContext('2d');
    ctx.scale(size * dpr / LOGICAL, size * dpr / LOGICAL);
    ctx.w = LOGICAL; ctx.h = LOGICAL;
    ctx.cssSize = size;
    ctx.canvas_el = canvas;
    return ctx;
  }
  // Re-initialize on resize so the backing stores match the new displayed
  // size (the next update redraws everything anyway).
  window.addEventListener('resize', function () { ctxs = null; });
  function initCanvases() {
    if (ctxs !== null) {
      return true;
    }
    var rose = context('gauge-rose');
    if (rose === null) {
      return false;                    // page not loaded yet
    }
    ctxs = {
      rose: rose,
      wind: context('gauge-wind'),
      temp: context('gauge-temp'),
      dew: context('gauge-dew'),
      hum: context('gauge-hum'),
      baro: context('gauge-baro'),
      rain: context('gauge-rain'),
      rrate: context('gauge-rrate'),
      feel: context('gauge-feel'),
      uv: context('gauge-uv'),
      rad: context('gauge-rad'),
      aqi: context('gauge-aqi')
    };
    return true;
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

  function face(ctx, cx, cy, r) {
    ctx.beginPath();
    ctx.arc(cx, cy, r, 0, 2 * Math.PI);
    ctx.fillStyle = C.face;
    ctx.fill();
    ctx.strokeStyle = C.rim;
    ctx.lineWidth = 1;
    ctx.stroke();
  }

  // ---- The shared dial: a 270-degree meter, south-gap orientation. ---------
  // cfg: scale {min,max,major,minor,dec}, value (raw number, undefined ok),
  //      text (report-formatted readout), label, [band: [lo, hi]],
  //      [trendFrom]
  var START = 135, SWEEP = 270;                    // degrees, clockwise from +x
  function rad(deg) { return deg * Math.PI / 180; }
  function dialAngle(cfg, v) {
    var f = (v - cfg.scale.min) / (cfg.scale.max - cfg.scale.min);
    f = Math.max(0, Math.min(1, f));
    return rad(START + SWEEP * f);
  }

  function drawDial(ctx, cfg) {
    var W = ctx.w, H = ctx.h, cx = W / 2, cy = H / 2;
    var r = Math.min(W, H) / 2 - 12;
    var s = cfg.scale;
    ctx.clearRect(0, 0, W, H);
    face(ctx, cx, cy, r);
    var rt = r - 12;                               // track radius

    // Recessed track.
    ctx.beginPath();
    ctx.arc(cx, cy, rt, rad(START), rad(START + SWEEP));
    ctx.strokeStyle = C.track;
    ctx.lineWidth = 7;
    ctx.lineCap = 'round';
    ctx.stroke();

    // Range band (today's min..max) on the track.
    if (cfg.band !== undefined &&
        cfg.band[0] !== undefined && cfg.band[1] !== undefined) {
      ctx.beginPath();
      ctx.arc(cx, cy, rt, dialAngle(cfg, cfg.band[0]), dialAngle(cfg, cfg.band[1]));
      ctx.strokeStyle = C.steelDim;
      ctx.lineWidth = 7;
      ctx.stroke();
    }

    // Trend arc (barometer over the trend window) just inside the track,
    // chevron at the current end showing direction of travel.
    if (cfg.trendFrom !== undefined && cfg.value !== undefined &&
        cfg.trendFrom !== cfg.value) {
      var a0 = dialAngle(cfg, cfg.trendFrom), a1 = dialAngle(cfg, cfg.value);
      var ccw = a1 < a0;
      if (a0 !== a1) {
        ctx.beginPath();
        ctx.arc(cx, cy, rt - 10, a0, a1, ccw);
        ctx.strokeStyle = C.steel;
        ctx.lineWidth = 2.5;
        ctx.lineCap = 'butt';
        ctx.stroke();
        var tip = a1 + (ccw ? -0.045 : 0.045);
        ctx.beginPath();
        ctx.moveTo(cx + (rt - 5) * Math.cos(a1), cy + (rt - 5) * Math.sin(a1));
        ctx.lineTo(cx + (rt - 10) * Math.cos(tip), cy + (rt - 10) * Math.sin(tip));
        ctx.lineTo(cx + (rt - 15) * Math.cos(a1), cy + (rt - 15) * Math.sin(a1));
        ctx.closePath();
        ctx.fillStyle = C.steel;
        ctx.fill();
      }
    }

    // Ticks and numerals.
    ctx.font = '10px ' + SANS;
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    for (var v = s.min; v <= s.max + 1e-9; v += s.minor) {
      var a = dialAngle(cfg, v);
      var major = Math.abs(v / s.major - Math.round(v / s.major)) < 1e-6;
      var len = major ? 7 : 3.5;
      ctx.beginPath();
      ctx.moveTo(cx + (rt - 9) * Math.cos(a), cy + (rt - 9) * Math.sin(a));
      ctx.lineTo(cx + (rt - 9 - len) * Math.cos(a), cy + (rt - 9 - len) * Math.sin(a));
      ctx.strokeStyle = major ? C.muted : C.tick;
      ctx.lineWidth = major ? 1.5 : 1;
      ctx.stroke();
      if (major) {
        ctx.fillStyle = C.muted;
        ctx.fillText(v.toFixed(s.dec),
                     cx + (rt - 26) * Math.cos(a), cy + (rt - 26) * Math.sin(a));
      }
    }

    // Needle: warm amber, a hint of lamp-glow, counterweight tail.
    if (cfg.value !== undefined) {
      var na = dialAngle(cfg, cfg.value);
      ctx.save();
      ctx.shadowColor = 'rgba(255, 185, 46, 0.55)';
      ctx.shadowBlur = 6;
      ctx.beginPath();
      ctx.moveTo(cx - 14 * Math.cos(na), cy - 14 * Math.sin(na));
      ctx.lineTo(cx + (rt - 18) * Math.cos(na), cy + (rt - 18) * Math.sin(na));
      ctx.strokeStyle = C.amber;
      ctx.lineWidth = 2.5;
      ctx.lineCap = 'round';
      ctx.stroke();
      ctx.restore();
    }
    ctx.beginPath();
    ctx.arc(cx, cy, 5, 0, 2 * Math.PI);
    ctx.fillStyle = C.amber;
    ctx.fill();
    ctx.beginPath();
    ctx.arc(cx, cy, 2, 0, 2 * Math.PI);
    ctx.fillStyle = C.face;
    ctx.fill();

    // Readout in the south gap.
    ctx.fillStyle = C.ink;
    ctx.font = '600 19px ' + SANS;
    ctx.fillText(cfg.text !== undefined ? cfg.text : '--', cx, cy + r - 28);
    if (cfg.label) {
      ctx.fillStyle = C.muted;
      ctx.font = '11px ' + SANS;
      ctx.fillText(cfg.label, cx, cy + r - 9);
    }
  }

  // ---- The wind compass: full circle, direction needle + gust ghost. -------
  function compassAngle(deg) { return rad(deg - 90); }   // 0 = north = up

  function drawCompass(ctx, cfg) {
    var W = ctx.w, H = ctx.h, cx = W / 2, cy = H / 2;
    var r = Math.min(W, H) / 2 - 12;
    ctx.clearRect(0, 0, W, H);
    face(ctx, cx, cy, r);
    var rt = r - 12;

    // Ticks every 22.5, heavier at the cardinals; letters inside the rim.
    for (var i = 0; i < 16; i++) {
      var a = compassAngle(i * 22.5);
      var major = i % 4 === 0;
      ctx.beginPath();
      ctx.moveTo(cx + rt * Math.cos(a), cy + rt * Math.sin(a));
      ctx.lineTo(cx + (rt - (major ? 8 : 4)) * Math.cos(a),
                 cy + (rt - (major ? 8 : 4)) * Math.sin(a));
      ctx.strokeStyle = major ? C.muted : C.tick;
      ctx.lineWidth = major ? 1.5 : 1;
      ctx.stroke();
    }
    ctx.font = '600 12px ' + SANS;
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    CARDINALS.forEach(function (d, i) {
      var a = compassAngle(i * 90);
      ctx.fillStyle = i === 0 ? C.ink : C.muted;
      ctx.fillText(d, cx + (rt - 19) * Math.cos(a), cy + (rt - 19) * Math.sin(a));
    });

    function needle(deg, color, width, len, glow) {
      var a = compassAngle(deg);
      ctx.save();
      if (glow) {
        ctx.shadowColor = 'rgba(255, 185, 46, 0.55)';
        ctx.shadowBlur = 6;
      }
      ctx.beginPath();
      ctx.moveTo(cx - 14 * Math.cos(a), cy - 14 * Math.sin(a));
      ctx.lineTo(cx + len * Math.cos(a), cy + len * Math.sin(a));
      ctx.strokeStyle = color;
      ctx.lineWidth = width;
      ctx.lineCap = 'round';
      ctx.stroke();
      ctx.beginPath();
      ctx.moveTo(cx + (len + 9) * Math.cos(a), cy + (len + 9) * Math.sin(a));
      ctx.lineTo(cx + len * Math.cos(a + 0.10), cy + len * Math.sin(a + 0.10));
      ctx.lineTo(cx + len * Math.cos(a - 0.10), cy + len * Math.sin(a - 0.10));
      ctx.closePath();
      ctx.fillStyle = color;
      ctx.fill();
      ctx.restore();
    }
    if (cfg.gustDir !== undefined) {
      needle(cfg.gustDir, C.steel, 2, rt - 38, false);
    }
    if (cfg.dir !== undefined && cfg.speed !== undefined && cfg.speed > 0) {
      needle(cfg.dir, C.amber, 2.5, rt - 34, true);
    }

    ctx.beginPath();
    ctx.arc(cx, cy, 5, 0, 2 * Math.PI);
    ctx.fillStyle = C.amber;
    ctx.fill();
    ctx.beginPath();
    ctx.arc(cx, cy, 2, 0, 2 * Math.PI);
    ctx.fillStyle = C.face;
    ctx.fill();

    // Readouts in the lower third.
    ctx.fillStyle = C.ink;
    ctx.font = '600 18px ' + SANS;
    ctx.fillText(cfg.speedText !== undefined ? cfg.speedText : '--',
                 cx, cy + r * 0.42);
    ctx.fillStyle = C.muted;
    ctx.font = '11px ' + SANS;
    var sub = (cfg.speed !== undefined && cfg.speed > 0 &&
               cfg.dirText !== undefined) ? cfg.dirText : fmt('calm');
    if (cfg.gustText !== undefined) {
      sub += '   ' + (cfg.gustDirText !== undefined
          ? fmt('gust {gust} @ {dir}', {gust: cfg.gustText, dir: cfg.gustDirText})
          : fmt('gust {gust}', {gust: cfg.gustText}));
    }
    ctx.fillText(sub, cx, cy + r * 0.42 + 18);
  }

  // ---- The NOAA windrose: petals by direction, stacked by speed band. ------
  function drawRose(ctx, rose) {
    var W = ctx.w, H = ctx.h, cx = W / 2, cy = H / 2;
    // The rose's face is the dials' face exactly -- both 12 units inside
    // the canvas -- so the twelve instruments are one size by construction,
    // whatever the palette does.  (Through 6.10 the face was pulled in to
    // the dials' TRACK radius instead, matching the boundary the eye read
    // back when the dials' own face edge was invisible; a palette with a
    // visible face edge broke that.)  The compass letters move inside, sat
    // so their outer edge lands on the circle the dials' track ring
    // occupies, and the petals stop short of them: a petal points straight
    // at its own letter, so the clearance is purely radial.  What faces the
    // petal depends on the position, and the two tight ones are the BOTTOM
    // and the SIDES.  textBaseline 'middle' seats a cap-height glyph with
    // more of it above the anchor than below, so at the bottom of the disc
    // that surplus points inward: S clears 2.70 where N, the same glyph
    // height, clears 5.30.  At E and W the glyph's side faces inward, so
    // the widest letter is as tight as the bottom one -- W clears 2.65.
    // Worst is that S/W pair at about 3.3 units -- the letter's ink, skirt
    // included, against the petals' outer arc at rMax -- in all nine shipped
    // languages, the petals keeping the reach they have had since 6.0.
    // Measure by rendering the rose three times through this function --
    // normally, with CARDINALS blanked, with no petals -- and diffing, so
    // anti-aliased skirts count as ink rather than being thresholded away.
    // Two traps, both of which have produced a wrong answer here: sweep the
    // petal's whole +/-8.2 degree span, because a glyph with a hollow centre
    // (N, O) reads far clearer than it is on the cardinal's centre ray
    // alone; and do not take the with-minus-without-petals diff as petal
    // ink, because each segment is stroked 2px in the FACE colour and where
    // that covers a reference ring it registers as ~1.5 units of reach that
    // is invisible and eats no clearance.  weewx-liveseasons, which carries
    // this same panel, measures 3.0 to 3.4 across its responsive canvas
    // sizes; canvas size moves the figure only slightly.  Measured off the rendered canvas, not
    // estimated; re-measure before moving either radius, and note the
    // ordinates come from the TARGET report, so a custom one can serve any
    // glyph it likes.
    var rFace = Math.min(W, H) / 2 - 12;
    var rLetter = rFace - 11;
    var rMax = rFace - 20;
    var rCalm = 22;
    ctx.clearRect(0, 0, W, H);
    face(ctx, cx, cy, rFace);

    // Totals: overall (for the calm share) and the windiest direction
    // (sets the radial scale).
    var total = rose.calm, maxBin = 0;
    var binTotals = rose.banded.map(function (bands) {
      var t = bands.reduce(function (a, b) { return a + b; }, 0);
      total += t;
      if (t > maxBin) {
        maxBin = t;
      }
      return t;
    });

    // Recessive reference rings.
    ctx.strokeStyle = C.tick;
    ctx.lineWidth = 1;
    [1 / 3, 2 / 3, 1].forEach(function (f) {
      ctx.beginPath();
      ctx.arc(cx, cy, rCalm + (rMax - rCalm) * f, 0, 2 * Math.PI);
      ctx.stroke();
    });

    // Petals: bin i centered on i*22.5 deg clockwise from north; a face-color
    // stroke keeps a gap between stacked segments.
    for (var i = 0; i < rose.banded.length; i++) {
      if (binTotals[i] <= 0 || maxBin <= 0) {
        continue;
      }
      var a0 = rad(i * 22.5 - 90 - 8.2), a1 = rad(i * 22.5 - 90 + 8.2);
      var r = rCalm;
      var petal = (rMax - rCalm) * binTotals[i] / maxBin;
      for (var b = 0; b < rose.banded[i].length; b++) {
        if (rose.banded[i][b] <= 0) {
          continue;
        }
        var seg = petal * rose.banded[i][b] / binTotals[i];
        ctx.beginPath();
        ctx.arc(cx, cy, r + seg, a0, a1);
        ctx.arc(cx, cy, r, a1, a0, true);
        ctx.closePath();
        ctx.fillStyle = RAMP[b % RAMP.length];
        ctx.fill();
        ctx.strokeStyle = C.face;
        ctx.lineWidth = 2;
        ctx.stroke();
        r += seg;
      }
    }

    // Compass letters inside the rim, on the dials' track circle.
    ctx.font = '600 11px ' + SANS;
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    CARDINALS.forEach(function (d, i) {
      var a = rad(i * 90 - 90);
      ctx.fillStyle = i === 0 ? C.ink : C.muted;
      ctx.fillText(d, cx + rLetter * Math.cos(a), cy + rLetter * Math.sin(a));
    });

    // Calm share, center.
    ctx.beginPath();
    ctx.arc(cx, cy, rCalm, 0, 2 * Math.PI);
    ctx.fillStyle = C.face;
    ctx.fill();
    ctx.strokeStyle = C.track;
    ctx.lineWidth = 1;
    ctx.stroke();
    if (total > 0) {
      ctx.fillStyle = C.ink;
      ctx.font = '600 13px ' + SANS;
      ctx.fillText(Math.round(100 * rose.calm / total) + '%', cx, cy - 5);
      ctx.fillStyle = C.muted;
      ctx.font = '9px ' + SANS;
      ctx.fillText(fmt('calm'), cx, cy + 9);
    }
  }

  // Legend from windrose.bands (band edges in the report's windSpeed unit) --
  // rebuilt only when the edges change.
  var legendKey = '';
  function buildLegend(edges, unitLabel) {
    var key = edges.join(',') + unitLabel;
    if (key === legendKey) {
      return;
    }
    legendKey = key;
    var el = document.getElementById('rose-legend');
    el.innerHTML = '';
    edges.forEach(function (lo, i) {
      var span = document.createElement('span');
      var chip = document.createElement('i');
      chip.style.background = RAMP[i % RAMP.length];
      span.appendChild(chip);
      var hi = i + 1 < edges.length ? '-' + edges[i + 1] : '+' + unitLabel;
      span.appendChild(document.createTextNode(lo + hi));
      el.appendChild(span);
    });
  }

  function updateGauges(result) {
    // If layout has changed the displayed size since the backing stores
    // were built (late layout, rotation), rebuild them at the new size.
    if (ctxs !== null && ctxs.rose !== null &&
        Math.abs(ctxs.rose.canvas_el.clientWidth - ctxs.rose.cssSize) > 2) {
      ctxs = null;
    }
    if (!initCanvases()) {
      return;
    }
    if (ctxs.rose !== null && result['day.windrose.banded'] !== undefined) {
      drawRose(ctxs.rose, {
        banded: result['day.windrose.banded'],
        calm: result['day.windrose.calm'] || 0
      });
      if (result['windrose.bands'] !== undefined) {
        buildLegend(result['windrose.bands'],
                    ' ' + (result['unit.label.windSpeed'] || '').trim());
      }
    }
    if (ctxs.wind !== null) {
      drawCompass(ctxs.wind, {
        speed: result['current.windSpeed.raw'],
        dir: result['current.windDir.raw'],
        gustDir: result['10m.wind.gustdir.raw'],
        speedText: result['current.windSpeed'],
        dirText: result['current.windDir.ordinal_compass'],
        gustText: result['10m.windGust.max'],
        gustDirText: result['10m.wind.gustdir.ordinal_compass']
      });
    }
    if (ctxs.temp !== null) {
      var tLabel = '';
      if (result['day.outTemp.min.formatted'] !== undefined &&
          result['day.outTemp.max.formatted'] !== undefined) {
        tLabel = fmt('today {min} to {max}',
                     {min: result['day.outTemp.min.formatted'],
                      max: result['day.outTemp.max.formatted']});
      }
      drawDial(ctxs.temp, {
        scale: tempScale(result['unit.label.outTemp'] || ''),
        value: result['current.outTemp.raw'],
        band: [result['day.outTemp.min.raw'], result['day.outTemp.max.raw']],
        text: result['current.outTemp'],
        label: tLabel
      });
    }
    if (ctxs.baro !== null) {
      var trendFrom = undefined;
      if (result['current.barometer.raw'] !== undefined &&
          result['trend.barometer.raw'] !== undefined) {
        trendFrom = result['current.barometer.raw'] - result['trend.barometer.raw'];
      }
      drawDial(ctxs.baro, {
        scale: baroScale(result['unit.label.barometer'] || ''),
        value: result['current.barometer.raw'],
        trendFrom: trendFrom,
        text: result['current.barometer'],
        label: result['trend.barometer.desc'] || ''
      });
    }
    if (ctxs.hum !== null) {
      var hLabel = '';
      if (result['day.outHumidity.min.raw'] !== undefined &&
          result['day.outHumidity.max.raw'] !== undefined) {
        hLabel = fmt('today {min} to {max}',
                     {min: Math.round(result['day.outHumidity.min.raw']),
                      max: Math.round(result['day.outHumidity.max.raw'])});
      }
      drawDial(ctxs.hum, {
        scale: { min: 0, max: 100, major: 20, minor: 5, dec: 0 },
        value: result['current.outHumidity.raw'],
        band: [result['day.outHumidity.min.raw'], result['day.outHumidity.max.raw']],
        text: result['current.outHumidity'],
        label: hLabel
      });
    }
    if (ctxs.rain !== null) {
      var rLabel = fmt('today');
      if (result['current.rainRate.raw'] !== undefined &&
          result['current.rainRate.raw'] > 0 &&
          result['current.rainRate'] !== undefined) {
        rLabel = fmt('rate {rate}', {rate: result['current.rainRate']});
      }
      drawDial(ctxs.rain, {
        scale: rainScale(result['unit.label.rain'] || '',
                         result['day.rain.sum.raw']),
        value: result['day.rain.sum.raw'],
        text: result['day.rain.sum'],
        label: rLabel
      });
    }
    if (ctxs.dew !== null) {
      var dLabel = '';
      if (result['day.dewpoint.min.formatted'] !== undefined &&
          result['day.dewpoint.max.formatted'] !== undefined) {
        dLabel = fmt('today {min} to {max}',
                     {min: result['day.dewpoint.min.formatted'],
                      max: result['day.dewpoint.max.formatted']});
      }
      drawDial(ctxs.dew, {
        scale: tempScale(result['unit.label.outTemp'] || ''),
        value: result['current.dewpoint.raw'],
        band: [result['day.dewpoint.min.raw'], result['day.dewpoint.max.raw']],
        text: result['current.dewpoint'],
        label: dLabel
      });
    }
    if (ctxs.rrate !== null) {
      // Scale off the day's max too, so the dial does not rescale as a
      // burst passes.
      var rateMax = result['current.rainRate.raw'];
      if (result['day.rainRate.max.raw'] !== undefined &&
          (rateMax === undefined || result['day.rainRate.max.raw'] > rateMax)) {
        rateMax = result['day.rainRate.max.raw'];
      }
      var rrLabel = '';
      if (result['day.rainRate.max'] !== undefined) {
        rrLabel = fmt('max today {max}', {max: result['day.rainRate.max']});
      }
      drawDial(ctxs.rrate, {
        scale: rainScale(result['unit.label.rainRate'] || '', rateMax),
        value: result['current.rainRate.raw'],
        text: result['current.rainRate'],
        label: rrLabel
      });
    }
    var haveFeel = result['current.appTemp.raw'] !== undefined;
    showCell('cell-feel', haveFeel);
    if (haveFeel && ctxs.feel !== null) {
      var fLabel = '';
      if (result['day.appTemp.min.formatted'] !== undefined &&
          result['day.appTemp.max.formatted'] !== undefined) {
        fLabel = fmt('today {min} to {max}',
                     {min: result['day.appTemp.min.formatted'],
                      max: result['day.appTemp.max.formatted']});
      }
      drawDial(ctxs.feel, {
        scale: tempScale(result['unit.label.outTemp'] || ''),
        value: result['current.appTemp.raw'],
        band: [result['day.appTemp.min.raw'], result['day.appTemp.max.raw']],
        text: result['current.appTemp'],
        label: fLabel
      });
    }
    var haveUv = result['current.UV.raw'] !== undefined;
    showCell('cell-uv', haveUv);
    if (haveUv && ctxs.uv !== null) {
      var uLabel = '';
      if (result['day.UV.max'] !== undefined) {
        uLabel = fmt('max today {max}', {max: result['day.UV.max']});
      }
      drawDial(ctxs.uv, {
        scale: UV_SCALE,
        value: result['current.UV.raw'],
        text: result['current.UV'],
        label: uLabel
      });
    }
    var haveRad = result['current.radiation.raw'] !== undefined;
    showCell('cell-rad', haveRad);
    if (haveRad && ctxs.rad !== null) {
      var sLabel = '';
      if (result['day.radiation.max'] !== undefined) {
        sLabel = fmt('max today {max}', {max: result['day.radiation.max']});
      }
      drawDial(ctxs.rad, {
        scale: RAD_SCALE,
        value: result['current.radiation.raw'],
        text: result['current.radiation'],
        label: sLabel
      });
    }
    var haveAqi = result['current.pm2_5_aqi.raw'] !== undefined;
    showCell('cell-aqi', haveAqi);
    if (haveAqi && ctxs.aqi !== null) {
      var aLabel = '';
      if (result['current.pm2_5'] !== undefined) {
        aLabel = fmt('pm2.5 {pm25}', {pm25: result['current.pm2_5']});
      }
      drawDial(ctxs.aqi, {
        scale: AQI_SCALE,
        value: result['current.pm2_5_aqi.raw'],
        text: result['current.pm2_5_aqi.formatted'],
        label: aLabel
      });
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
      document.getElementById("live-label").innerHTML = fmt('OFFLINE');
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
      document.getElementById("live-label").innerHTML =
          fmt('NO DATA (HTTP {status}) \u2014 check loop_data_file',
              {status: response.status});
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
      document.getElementById("live-label").innerHTML =
          fmt('BAD DATA \u2014 check loop_data_file');
      console.log(e);
      return;
    }
    try {
      // The LIVE indicator, from the packet's own timestamp.
      var lastUpdate = new Date(result["current.dateTime.raw"] * 1000);
      var age = Math.round(Math.abs(new Date() - lastUpdate) / 1000);
      var element = document.getElementById("live-label");
      if (age <= 3 * refresh_rate) {
        element.innerHTML = fmt('LIVE');
      } else {
        element.innerHTML = fmt('{age}s ago', {age: age});
      }
      var activityElement = document.getElementById("last-update");
      activityElement.innerHTML = lastUpdate.toLocaleTimeString(LOCALE, {hour: '2-digit', minute:'2-digit', second:'2-digit'});

      updateGauges(result);
    } catch (e) {
      // A rendering error must not stop the polling; try again next
      // interval.
      console.log(e);
    }
  }
</script>
