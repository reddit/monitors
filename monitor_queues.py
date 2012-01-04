#!/usr/bin/env python

import logging
import socket
import subprocess
import sys
import time
import urllib

# Alert noisiness is suppressed by these two factors. First, a queue must be in
# an alerting state continuously for at least ALERT_GRACE_PERIOD seconds.
# Second, no alert for a single queue will be repeated within ALERT_RATE_LIMIT
# seconds.
ALERT_GRACE_PERIOD = 5
ALERT_RATE_LIMIT = 15

HAROLD_BASE = 'http://127.0.0.1:8888/harold'
HAROLD_SECRET = 'secret'
HAROLD_HEARTBEAT_INTERVAL = 60  # seconds
HAROLD_HEARTBEAT_TIMEOUT_FACTOR = 3

GRAPHITE_HOST = '10.114.195.155'
GRAPHITE_PORT = 2003

POLL_INTERVAL = 1.0

QUEUE_LIMITS = dict(
    scraper_q=4000,
    newcomments_q=200,
    log_q=10000,
    usage_q=8000,
    commentstree_q=1000,
    corrections_q=500,
    spam_q=500,
    vote_comment_q=10000,
    vote_link_q=10000,
    indextank_changes=50000,
    solrsearch_changes=100000,
    ratelimit_q=sys.maxint,
)

# dict of queue name to timestamp of beginning of current overrun status
overruns = {}

# dict of queue name to time of last alert
recent_alerts = {}

# timestamp of the last time a heartbeat was sent
last_heartbeat = 0

def harold_url(command):
    return '%s/%s/%s' % (HAROLD_BASE, command, HAROLD_SECRET)

def harold_send_message(command, **data):
    return urllib.urlopen(harold_url(command), urllib.urlencode(data))

def send_heartbeat():
    global last_heartbeat
    last_heartbeat = time.time()
    interval = HAROLD_HEARTBEAT_INTERVAL * HAROLD_HEARTBEAT_TIMEOUT_FACTOR
    harold_send_message('heartbeat', tag='monitor_queues', interval=interval)

def get_queue_lengths():
    pipe = subprocess.Popen(['/usr/sbin/rabbitmqctl', 'list_queues'],
                            stdout=subprocess.PIPE)
    output = pipe.communicate()[0]
    lines = output.split('\n')[1:-2]  # throw away first line and last lines
    return dict((name, int(value)) for name, value in
                (line.split() for line in lines))

def send_graphite_message(msg):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((GRAPHITE_HOST, GRAPHITE_PORT))
    sock.send(msg + '\n')
    sock.close()

def send_queue_stats(queue_lengths):
    stat_msgs = []
    now = time.time()
    for name, length in queue_lengths.iteritems():
        stat_msgs.append('stats.queue.%s.length %d %d' % (name, length, now))
    if not stat_msgs:
        return
    send_graphite_message('\n'.join(stat_msgs))

def send_queue_alert(queue_name, queue_length, alert_threshold):
    alert = dict(
        tag=queue_name,
        message='%s is too long (%d/%d)' % (
            queue_name, queue_length, alert_threshold)
    )
    logging.warn('ALERT on %(tag)s: %(message)s' % alert)
    harold_send_message('alert', **alert)

def update_queue_status(queue_name, queue_length, alert_threshold):
    if queue_length <= alert_threshold:
        if queue_name in overruns:
            del overruns[queue_name]
        return False
    else:
        now = time.time()
        overruns.setdefault(queue_name, now)
        if (now - overruns[queue_name] >= ALERT_GRACE_PERIOD
            and recent_alerts.get(queue_name, 0) + ALERT_RATE_LIMIT <= now):
            send_queue_alert(queue_name, queue_length, alert_threshold)
            recent_alerts[queue_name] = now
            return True
        else:
            logging.warn('suppressing continued alert on %s', queue_name)
            return False

def check_queues():
    queue_lengths = get_queue_lengths()
    for name, length in queue_lengths.iteritems():
        update_queue_status(name, length, QUEUE_LIMITS.get(name, sys.maxint))
    send_queue_stats(queue_lengths)
    if time.time() - last_heartbeat >= HAROLD_HEARTBEAT_INTERVAL:
        send_heartbeat()

def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
    )
    while True:
        logging.info('checking on queues')
        try:
            check_queues()
        except:
            logging.exception('exception raised in check_queues')
        time.sleep(POLL_INTERVAL)

if __name__ == '__main__':
    main()
