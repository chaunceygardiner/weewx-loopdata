## analytics module weewx skins
## Based off of code Copyright Tom Keffer, Matthew Wall
## See LICENSE.txt for your rights
#errorCatcher Echo
#encoding UTF-8

## Include the Google Analytics code if an Id has been specified

#if $Extras.has_key('googleAnalyticsId')
<!-- Global site tag (gtag.js) - Google Analytics -->
<script async src="https://www.googletagmanager.com/gtag/js?id=$Extras.googleAnalyticsId"></script>
<script>
  var host = location.host;
#if $Extras.has_key('analytics_host')
  if (host == "$Extras.analytics_host") {
#end if
    window.dataLayer = window.dataLayer || [];
    function gtag(){dataLayer.push(arguments);}
    gtag('js', new Date());
    gtag('config', '$Extras.googleAnalyticsId');
#if $Extras.has_key('analytics_host')
  }
#end if
</script>
#end if
