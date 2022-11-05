## Copyright (C)2020 by John A Kline (john@johnkline.com)
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
  setInterval(updateCurrent, 2000);
  addLoadEvent(updateCurrent);

  function updateCurrent() {
    if (pageTimedOut) {
        setUpExpiredClickListener();
        return false;
    }
    var xhttp = new XMLHttpRequest();
    xhttp.onload = function() {
      try {
        var result = JSON.parse(this.responseText);

        // Check the date
        // "dateTime": 1578965850,
        var lastUpdate = new Date(result["current.dateTime.raw"] * 1000);
        var age = Math.round(Math.abs(new Date() - lastUpdate) / 1000);
        var element = document.getElementById("live-label");
        element.style.fontWeight = "bolder";
        if (age <= 6) {
          element.innerHTML = "LIVE&nbsp;&nbsp;";
        } else {
          element.innerHTML = age + "s ago&nbsp;&nbsp;";
        }

        // Display the time of the last update.
        var activityElement = document.getElementById("last-update");
        activityElement.innerHTML = lastUpdate.toLocaleTimeString([], {hour: '2-digit', minute:'2-digit', second:'2-digit'});

        // Trend
        if (result.hasOwnProperty("trend.outTemp")) {
          document.getElementById("trend.outTemp").innerHTML = result["trend.outTemp"];
        } else {
          document.getElementById("trend.outTemp").innerHTML = '';
        }
        if (result.hasOwnProperty("trend.barometer.desc")) {
          document.getElementById("trend.barometer.desc").innerHTML = result["trend.barometer.desc"];
        } else {
          document.getElementById("trend.barometer.desc").innerHTML = '';
        }
        // Current
        if (result.hasOwnProperty("current.outTemp.formatted")) {
          document.getElementById("current.outTemp.formatted").innerHTML = result["current.outTemp.formatted"].padStart(5,' ');
        } else {
          document.getElementById("current.outTemp.formatted").innerHTML = ''.padStart(5,' ');
        }
        if (result.hasOwnProperty("current.dewpoint.formatted")) {
          // dew point
          document.getElementById("current.dewpoint.formatted").innerHTML = result["current.dewpoint.formatted"].padStart(5,' ');
        } else {
          document.getElementById("current.dewpoint.formatted").innerHTML = ''.padStart(5,' ');
        }
        if (result.hasOwnProperty("current.windSpeed.formatted")) {
          document.getElementById("current.windSpeed.formatted").innerHTML = result["current.windSpeed.formatted"].padStart(5,' ');
          var windBearing = ''.padStart(4, ' ').padEnd(5,' ');
          if (result["current.windSpeed.formatted"] != 0.0) {
            if (result.hasOwnProperty("current.windDir.ordinal_compass")) {
              windBearing = result["current.windDir.ordinal_compass"].padStart(4, ' ').padEnd(5,' ');
            } else {
            }
          }
          document.getElementById("current.windDir.ordinal_compass").innerHTML = windBearing;
        } else {
          document.getElementById("current.windSpeed.formatted").innerHTML = ''.padStart(5,' ');
          document.getElementById("current.windDir.ordinal_compass").innerHTML = ''.padEnd(3,' ');
        }
        if (result.hasOwnProperty("current.rainRate.formatted")) {
          document.getElementById("current.rainRate.formatted").innerHTML = result["current.rainRate.formatted"].padStart(5,' ');
        } else {
          document.getElementById("current.rainRate.formatted").innerHTML = ''.padStart(5,' ');
        }
        #set $archive_data = ['2m', '10m', '24h', 'hour', 'day', 'week', 'month', 'year', 'rainyear']
        #for $archive in $archive_data
        if (result.hasOwnProperty("$(archive).outTemp.max.formatted")) {
          document.getElementById("$(archive).outTemp.max.formatted").innerHTML = result["$(archive).outTemp.max.formatted"].padStart(5,' ');
        } else {
          document.getElementById("$(archive).outTemp.max.formatted").innerHTML = ''.padStart(5,' ');
        }
        if (result.hasOwnProperty("$(archive).outTemp.min.formatted")) {
          document.getElementById("$(archive).outTemp.min.formatted").innerHTML = result["$(archive).outTemp.min.formatted"].padStart(5,' ');
        } else {
          document.getElementById("$(archive).outTemp.min.formatted").innerHTML = ''.padStart(5,' ');
        }
        if (result.hasOwnProperty("$(archive).wind.rms.formatted")) {
          document.getElementById("$(archive).wind.rms.formatted").innerHTML = result["$(archive).wind.rms.formatted"].padStart(5,' ');
        } else {
          document.getElementById("$(archive).wind.rms.formatted").innerHTML = ''.padStart(5,' ');
        }
        if (result.hasOwnProperty("$(archive).windGust.max.formatted")) {
          document.getElementById("$(archive).windGust.max.formatted").innerHTML = result["$(archive).windGust.max.formatted"].padStart(5,' ');
        } else {
          document.getElementById("$(archive).windGust.max.formatted").innerHTML = ''.padStart(5,' ');
        }
        if (result.hasOwnProperty("$(archive).rain.sum.formatted")) {
          document.getElementById("$(archive).rain.sum.formatted").innerHTML = result["$(archive).rain.sum.formatted"].padStart(5,' ');
        } else {
          document.getElementById("$(archive).rain.sum.formatted").innerHTML = '';
        }
        #end for
      } catch (e) {
        console.log(e);
      }
    }
    xhttp.onerror = function() {
      console.log(e);
    }
    try {
      xhttp.open("GET", "$Extras.loop_data_file", true);
      xhttp.send();
    } catch (e) {
      console.log(e);
    }
  }
  function decode_rgb(i) {
    var red   = i >> 16;
    var green = (i & 0xFF00) >> 8;
    var blue  = i & 0xFF;
    return `rgb(${red},${green},${blue})`
  }
</script>
