#!/usr/bin/python

import csv
import urllib2
import urlparse
import collections
import time

import alerts


TIMEOUT = 3  # seconds
CONFIG_SECTION = "haproxy"


def fetch_queue_lengths_by_pool(haproxy_stats_urls):
    pools = collections.Counter()

    for url in haproxy_stats_urls:
        try:
            csv_data = urllib2.urlopen(url, timeout=TIMEOUT)
            reader = csv.reader(csv_data)

            reader.next()  # skip the header
            for row in reader:
                proxy_name, server_name, queue_length = row[:3]
                if server_name != "BACKEND":
                    continue
                pools[proxy_name] += int(queue_length)
        except urllib2.URLError:
            host = urlparse.urlparse(url).hostname
            alerts.harold.alert(host, "couldn't connect to haproxy")

    return pools


def watch_request_queues(haproxy_urls, threshold, check_interval):
    queued_pools = set()

    while True:
        # this *should* die if unable to talk to haproxy, then we'll
        # get heartbeat-failure alerts
        pools = fetch_queue_lengths_by_pool(haproxy_urls)

        # alert where necessary
        for pool, queue_length in pools.iteritems():
            if queue_length > threshold:
                if pool in queued_pools:
                    alerts.harold.alert("queuing-%s" % pool,
                                        "%s pool is queuing (%d)" %
                                        (pool, queue_length))
                queued_pools.add(pool)
            else:
                if pool in queued_pools:
                    queued_pools.remove(pool)

        # check in then sleep 'til we're needed again
        alerts.harold.heartbeat("monitor_haproxy", check_interval * 2)
        time.sleep(check_interval)


def main():
    # expects a config section like the following
    # [haproxy]
    # threshold = 200
    # interval = 30
    # url.* = url
    alerts.init()
    haproxy_urls = [value for key, value in
                    alerts.config.items(CONFIG_SECTION)
                    if key.startswith("url")]
    threshold = alerts.config.getint(CONFIG_SECTION, "threshold")
    interval = alerts.config.getint(CONFIG_SECTION, "interval")

    watch_request_queues(haproxy_urls, threshold, interval)


if __name__ == "__main__":
    main()
