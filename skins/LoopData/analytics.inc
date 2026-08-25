## analytics module weewx skins
## Based off of code Copyright Tom Keffer, Matthew Wall
## See LICENSE.txt for your rights
#errorCatcher Echo
#encoding UTF-8

## Include the Google Analytics code if an Id has been specified.
## Test the VALUE, not just the key: weewx.conf carries googleAnalyticsId
## and analytics_host with empty values on a fresh install, and a has_key
## test on those loaded gtag.js with an empty id on every page view, and
## compared the host against "" -- which is never equal -- so nothing was
## ever reported.  Empty now means off, as both options claim.

#if $Extras.get('googleAnalyticsId')
<!-- Global site tag (gtag.js) - Google Analytics -->
<script async src="https://www.googletagmanager.com/gtag/js?id=$Extras.googleAnalyticsId"></script>
<script>
  var host = location.host;
#if $Extras.get('analytics_host')
  if (host == "$Extras.analytics_host") {
#end if
    window.dataLayer = window.dataLayer || [];
    function gtag(){dataLayer.push(arguments);}
    gtag('js', new Date());
    gtag('config', '$Extras.googleAnalyticsId');
#if $Extras.get('analytics_host')
  }
#end if
</script>
#end if
