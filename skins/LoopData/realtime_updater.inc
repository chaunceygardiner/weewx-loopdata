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
  #if $Extras.has_key('refresh_rate')
    refresh_rate = $Extras.refresh_rate;
  #else
    refresh_rate = 2;
  #end if
  #if $Extras.has_key('expiration_time')
    expiration_time = $Extras.expiration_time;
  #else
    expiration_time = 4;
  #end if
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
    if (liveLabel != "CLICK-ME") {
      liveLabel.innerHTML = "CLICK-ME";
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

  var C = {
    face: '#171d28', track: '#232c3b', ink: '#e8edf4', muted: '#8b96a8',
    amber: '#f5a83c', steel: '#5b87ad', steelDim: '#33506b'
  };
  var SANS = 'system-ui, -apple-system, sans-serif';
  // Sequential ramp for the windrose speed bands, dim to bright.
  var RAMP = ['#24384e', '#33506b', '#44688a', '#5b87ad', '#7ba6c9', '#a3c8e3'];

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

  // Crisp canvas on hi-dpi displays: scale the backing store, draw in css px.
  var ctxs = null;
  function context(id) {
    var canvas = document.getElementById(id);
    if (canvas === null || !canvas.getContext) {
      return null;
    }
    var dpr = window.devicePixelRatio || 1;
    var w = canvas.width, h = canvas.height;
    canvas.style.width = w + 'px';
    canvas.style.height = h + 'px';
    canvas.width = w * dpr;
    canvas.height = h * dpr;
    var ctx = canvas.getContext('2d');
    ctx.scale(dpr, dpr);
    ctx.w = w; ctx.h = h;
    return ctx;
  }
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
    ctx.strokeStyle = C.track;
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
      ctx.strokeStyle = major ? C.muted : C.track;
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
      ctx.shadowColor = 'rgba(245, 168, 60, 0.45)';
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
      ctx.strokeStyle = major ? C.muted : C.track;
      ctx.lineWidth = major ? 1.5 : 1;
      ctx.stroke();
    }
    ctx.font = '600 12px ' + SANS;
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ['N', 'E', 'S', 'W'].forEach(function (d, i) {
      var a = compassAngle(i * 90);
      ctx.fillStyle = i === 0 ? C.ink : C.muted;
      ctx.fillText(d, cx + (rt - 19) * Math.cos(a), cy + (rt - 19) * Math.sin(a));
    });

    function needle(deg, color, width, len, glow) {
      var a = compassAngle(deg);
      ctx.save();
      if (glow) {
        ctx.shadowColor = 'rgba(245, 168, 60, 0.45)';
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
               cfg.dirText !== undefined) ? cfg.dirText : 'calm';
    if (cfg.gustText !== undefined) {
      sub += '   gust ' + cfg.gustText;
      if (cfg.gustDirText !== undefined) {
        sub += ' @ ' + cfg.gustDirText;
      }
    }
    ctx.fillText(sub, cx, cy + r * 0.42 + 18);
  }

  // ---- The NOAA windrose: petals by direction, stacked by speed band. ------
  function drawRose(ctx, rose) {
    var W = ctx.w, H = ctx.h, cx = W / 2, cy = H / 2;
    var rFace = Math.min(W, H) / 2 - 12;
    var rMax = rFace - 22;                         // room for compass letters
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
    ctx.strokeStyle = C.track;
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

    // Compass letters at the rim.
    ctx.font = '600 11px ' + SANS;
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ['N', 'E', 'S', 'W'].forEach(function (d, i) {
      var a = rad(i * 90 - 90);
      ctx.fillStyle = i === 0 ? C.ink : C.muted;
      ctx.fillText(d, cx + (rMax + 12) * Math.cos(a), cy + (rMax + 12) * Math.sin(a));
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
      ctx.fillText('calm', cx, cy + 9);
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
        tLabel = 'today ' + result['day.outTemp.min.formatted'] +
                 ' to ' + result['day.outTemp.max.formatted'];
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
        hLabel = 'today ' + Math.round(result['day.outHumidity.min.raw']) +
                 ' to ' + Math.round(result['day.outHumidity.max.raw']);
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
      var rLabel = 'today';
      if (result['current.rainRate.raw'] !== undefined &&
          result['current.rainRate.raw'] > 0 &&
          result['current.rainRate'] !== undefined) {
        rLabel = 'rate ' + result['current.rainRate'];
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
        dLabel = 'today ' + result['day.dewpoint.min.formatted'] +
                 ' to ' + result['day.dewpoint.max.formatted'];
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
        rrLabel = 'max today ' + result['day.rainRate.max'];
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
        fLabel = 'today ' + result['day.appTemp.min.formatted'] +
                 ' to ' + result['day.appTemp.max.formatted'];
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
        uLabel = 'max today ' + result['day.UV.max'];
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
        sLabel = 'max today ' + result['day.radiation.max'];
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
        aLabel = 'pm2.5 ' + result['current.pm2_5'];
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
    try {
      var response = await fetch('$Extras.loop_data_file', {cache: 'no-store'});
      var result = await response.json();

      // The LIVE indicator, from the packet's own timestamp.
      var lastUpdate = new Date(result["current.dateTime.raw"] * 1000);
      var age = Math.round(Math.abs(new Date() - lastUpdate) / 1000);
      var element = document.getElementById("live-label");
      if (age <= 3 * refresh_rate) {
        element.innerHTML = "LIVE";
      } else {
        element.innerHTML = age + "s ago";
      }
      var activityElement = document.getElementById("last-update");
      activityElement.innerHTML = lastUpdate.toLocaleTimeString([], {hour: '2-digit', minute:'2-digit', second:'2-digit'});

      updateGauges(result);
    } catch (e) {
      // File unreachable or mid-write; try again next interval.
      console.log(e);
    }
  }
</script>
