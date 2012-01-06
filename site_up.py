#!/usr/bin/python

import sys
import time
import urllib2
import urlparse
import platform

import alerts


INTERVAL = 10  # seconds
TIMEOUT = 30  # seconds
THRESHOLD = 3  # recent failures


def monitor_site(url):
    tag = urlparse.urlparse(url).hostname
    local_name = platform.node()

    recent_failures = 0
    while True:
        try:
            urllib2.urlopen(url, timeout=TIMEOUT)
        except urllib2.URLError:
            recent_failures += 1

            if recent_failures > THRESHOLD:
                alerts.harold.alert(tag, "[%s] %s is down" % (local_name, tag))
                recent_failures = THRESHOLD
        else:
            recent_failures = max(recent_failures - 1, 0)
            time.sleep(INTERVAL)

        alerts.harold.heartbeat("monitor_%s" % tag, max(INTERVAL, TIMEOUT) * 2)


if __name__ == "__main__":
    url = sys.argv[1]
    monitor_site(url)
