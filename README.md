# ckpod

```
$ ./ckpod.py --help
usage: ckpod.py [-h] [-c CONFDIR] [-d DOWNLOADS] [-r] [-v]

Another (hopefully less terrible) podcast downloader

optional arguments:
  -h, --help            show this help message and exit
  -c CONFDIR, --confdir CONFDIR path of the configuration directory (default: ~/.ckpod)
  -d DOWNLOADS, --downloads DOWNLOADS number of simultaneous downloads (default: 4)
  -r, --refresh         refresh episode list only (default: False)
  -v, --verbose         increase verbosity (default: 0)
```

One key feature of `ckpod` is the ability to regex transform the download filename.
A couple of examples:

```
[go_on_air]
url = https://www.thisisdistorted.com/repository/xml/GiuseppeOttaviani1430216693.xml
# using @ as the delimiter because i'm using / and , in the pattern
# transform from
# http://audio.thisisdistorted.com/repository/audio/episodes/Giuseppe_Ottaviani_presents_GO_On_Air_Episode_229-1484302984297800712-MjM1NDgtNTk5MDY5OTk=.m4a
# to
# Giuseppe_Ottaviani_presents_GO_On_Air_Episode_229.m4a
sed = s@^(.*/)((.+?)(-[0-9]{16,})?-.{20}(.{12})?)[.]@\3.@
```

Or 

```
[war_college]
url = https://rss.acast.com/warcollege
# transform from
# https://media.acast.com/warcollege/howthemilitaryis-quietly-defyingtrumpbybattlingclimatechange/media.mp3
# to
# howthemilitaryis-quietly-defyingtrumpbybattlingclimatechange.mp3
sed = s,^.+/([^/]+)/media([.]\w+)$,\1\2,
```

### To-Do

* handle redirects. Some pods redirect through a couple of GUIDs until finally ending up at a sane filename. possibly `HEAD` the media url during RSS loading to probe the final filename? maybe add a new keyword? `requests.get` will do that and the `requests.Response.url` will contain the final resolved URL. Unfortunately some of those URLs are a mess, which is why we have sed.
* add some special formats for composing disk filenames, eg. date, episode title, ...
* fetch-only mode. don't waste time reloading the feeds, just download missing episodes
