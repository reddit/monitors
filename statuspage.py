#!/usr/bin/python
"""Query metrics from Graphite and send them to statuspage.io."""

import ConfigParser
import datetime
import posixpath
import requests
import time
import urllib
import urlparse


def graphite_datetime(dt):
    return dt.strftime("%H:%M_%Y%m%d")


class Graphite(object):
    def __init__(self, base_url):
        scheme, netloc, path, query, fragment = urlparse.urlsplit(base_url)

        self.scheme = scheme
        self.netloc = netloc
        self.base_path = path

        assert not query
        assert not fragment

    def query(self, queries, from_time, until_time):
        params = [
            ("format", "json"),
            ("from", graphite_datetime(from_time)),
            ("until", graphite_datetime(until_time)),
        ]

        for query in queries:
            params.append(("target", query))

        url = urlparse.urlunsplit((
            self.scheme,
            self.netloc,
            posixpath.join(self.base_path, "render"),
            urllib.urlencode(params),
            None,
        ))

        response = requests.get(url)
        response.raise_for_status()

        result = {}
        for target_data in response.json:
            result[target_data["target"]] = target_data["datapoints"]
        return result


class StatusPage(object):
    def __init__(self, page_id, api_key):
        self.page_id = page_id
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": "OAuth " + api_key,
        })

    def send_metric(self, metric_id, timestamp, value):
        path = posixpath.join(
            "/v1/pages", self.page_id, "metrics", metric_id, "data.json")
        url = urlparse.urlunsplit(
            ("https", "api.statuspage.io", path, None, None))

        response = self.session.post(url, {
            "data[timestamp]": timestamp,
            "data[value]": value,
        })
        response.raise_for_status()

        # TODO: smarter?
        time.sleep(1)


def send_metrics_to_statuspage(history):
    parser = ConfigParser.RawConfigParser()
    with open("production.ini") as f:
        parser.readfp(f)

    until_time = datetime.datetime.now().replace(second=0, microsecond=0)
    from_time = until_time - history

    graphite_url = parser.get("statuspage", "graphite_url")
    graphite = Graphite(graphite_url)
    metrics = dict(parser.items("statuspage-metrics"))
    query_results = graphite.query(metrics.values(), from_time, until_time)

    statuspage_page_id = parser.get("statuspage", "statuspage_page_id")
    statuspage_api_key = parser.get("statuspage", "statuspage_api_key")
    status_page = StatusPage(statuspage_page_id, statuspage_api_key)
    for metric_id, query in metrics.iteritems():
        for value, timestamp in query_results[query]:
            status_page.send_metric(metric_id, timestamp, int(value or 0))


if __name__ == "__main__":
    send_metrics_to_statuspage(history=datetime.timedelta(minutes=2))
