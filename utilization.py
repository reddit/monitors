#!/usr/bin/python

from __future__ import division

import csv
import urllib2
import collections
import time

import alerts


def fetch_session_counts(haproxy_stats_urls):
    current = collections.Counter()
    limit = collections.Counter()
    queue = {}

    for url in haproxy_stats_urls:
        csv_data = urllib2.urlopen(url, timeout=3)
        reader = csv.reader(csv_data)

        for i, row in enumerate(reader):
            if i == 0: continue

            proxy_name, service_name, q_cur, s_cur, s_lim, status = row[0], row[1], row[2], row[4], row[6], row[17]

            if service_name == "BACKEND":
                queue[proxy_name] = int(q_cur)

            if service_name in ("FRONTEND", "BACKEND"):
                continue

            if status != "UP":
                continue

            current[proxy_name] += int(s_cur)
            limit[proxy_name] += int(s_lim)

    return [(pool, current[pool], capacity, queue[pool])
            for pool, capacity in limit.most_common()]


def notify_graphite(usage):
    values = {}
    for pool, cur, limit, queue in usage:
        values["stats.utilization.%s.current" % pool] = cur
        values["stats.utilization.%s.capacity" % pool] = limit
        values["stats.utilization.%s.queue" % pool] = queue
    alerts.graphite.send_values(values)


def pretty_print(usage):
    print "%20s%20s%10s" % ("", "sessions", "")
    print "%20s%10s%10s%10s%10s" % ("pool", "cur", "max", "% util", "queue")
    print "-" * 60
    for pool, cur, limit, queue in usage:
        print "%20s%10d%10d%10.2f%10d" % (pool, cur, limit, cur / limit * 100.0, queue)


def main():
    alerts.init()

    haproxy_urls = [value for key, value in
                    alerts.config.items("haproxy")
                    if key.startswith("url")]

    while True:
        try:
            usage_by_pool = fetch_session_counts(haproxy_urls)
        except urllib2.URLError:
            pass
        else:
            notify_graphite(usage_by_pool)
            pretty_print(usage_by_pool)

        time.sleep(1)


if __name__ == "__main__":
    main()
