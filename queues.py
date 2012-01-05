#!/usr/bin/env python

'''Provides a monitor that polls queue lengths using rabbitmqctl.

Run this as a script in the same directory as a production.ini file that looks
like this:

[harold]
host = localhost
port = 8888
secret = haroldsecret

[queues]
# check queue lengths with this frequency (seconds)
poll_interval = 1

# These two settings control alert noisiness. The grace period is how long a
# queue must show an overrun (in seconds) before an alert is raised. The rate
# limit is how long (in seconds) it must be since the last alert was raised
# before we'll raise another one (per queue).
alert_grace_period = 5
alert_rate_limit = 15

# tell harold we're alive at this frequency (seconds)
heartbeat_interval = 60

# multiply by heartbeat_interval to tell harold how long to wait before deciding
# we're "dead"
heartbeat_timeout_factor = 3

# send messages to graphite at this address
graphite_addr = localhost:2003

[queue_limits]
# list queue names with alerting thresholds for queue length here
# queues not mentioned here will still have stats recorded in graphite, but
# won't be alerted on

commentstree_q = 1000
newcomments_q = 200
vote_comment_q = 10000
vote_link_q = 10000
# etc.
'''

import logging
import socket
import subprocess
import sys
import time
import urllib

import alerts

def parse_addr(addr):
    host, port_str = addr.split(':', 1)
    return host, int(port_str)

def get_queue_lengths():
    pipe = subprocess.Popen(['/usr/sbin/rabbitmqctl', 'list_queues'],
                            stdout=subprocess.PIPE)
    output = pipe.communicate()[0]
    lines = output.split('\n')[1:-2]  # throw away first line and last lines
    return dict((name, int(value)) for name, value in
                (line.split() for line in lines))

class QueueMonitor:
    '''Polls "rabbitmqctl list_queues" to report on queue lengths.

    @attr overruns: dict, maps queue name to timestamp of when current overrun
        status began
    @attr recent_alerts: dict, maps queue name to timestamp of most recent alert
    @attr last_heartbeat: float, timestamp of last heartbeat sent to harold
    '''
    def __init__(self):
        self.config = alerts.config
        self.harold = alerts.harold
        self.overruns = {}
        self.recent_alerts = {}
        self.last_heartbeat = 0
        self._load_from_config()

    def _load_from_config(self):
        config = self.config
        self.heartbeat_interval = config.getfloat(
            'queues', 'heartbeat_interval')
        self.heartbeat_timeout_factor = config.getfloat(
            'queues', 'heartbeat_timeout_factor')
        self.graphite_host, self.graphite_port = parse_addr(
            config.get('queues', 'graphite_addr'))
        self.alert_grace_period = config.getfloat(
            'queues', 'alert_grace_period')
        self.alert_rate_limit = config.getfloat('queues', 'alert_rate_limit')
        self.poll_interval = config.getfloat('queues', 'poll_interval')
        self.queue_limits = dict((q, config.getint('queue_limits', q))
                                 for q in config.options('queue_limits'))

    def send_heartbeat(self):
        self.last_heartbeat = time.time()
        interval = self.heartbeat_interval * self.heartbeat_timeout_factor
        self.harold.heartbeat('monitor_queues', interval)

    def send_graphite_message(self, msg):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.graphite_host, self.graphite_port))
        sock.send(msg + '\n')
        sock.close()

    def send_queue_stats(self, queue_lengths):
        stat_msgs = []
        now = time.time()
        for name, length in queue_lengths.iteritems():
            stat_msgs.append('stats.queue.%s.length %d %d'
                             % (name, length, now))
        if not stat_msgs:
            return
        self.send_graphite_message('\n'.join(stat_msgs))

    def send_queue_alert(self, queue_name, queue_length, alert_threshold):
        alert = dict(
            tag=queue_name,
            message='%s is too long (%d/%d)' % (
                queue_name, queue_length, alert_threshold)
        )
        logging.warn('ALERT on %(tag)s: %(message)s' % alert)
        self.harold.alert(**alert)

    def update_queue_status(self, queue_name, queue_length, alert_threshold):
        if queue_length <= alert_threshold:
            if queue_name in self.overruns:
                del self.overruns[queue_name]
            return False
        else:
            now = time.time()
            self.overruns.setdefault(queue_name, now)
            if (now - self.overruns[queue_name] >= self.alert_grace_period
                and self.recent_alerts.get(queue_name, 0)
                    + self.alert_rate_limit <= now):
                self.send_queue_alert(queue_name, queue_length, alert_threshold)
                self.recent_alerts[queue_name] = now
                return True
            else:
                logging.warn('suppressing continued alert on %s', queue_name)
                return False

    def check_queues(self):
        queue_lengths = get_queue_lengths()
        for name, length in queue_lengths.iteritems():
            self.update_queue_status(name, length,
                                     self.queue_limits.get(name, sys.maxint))
        self.send_queue_stats(queue_lengths)
        if time.time() - self.last_heartbeat >= self.heartbeat_interval:
            self.send_heartbeat()

    def poll(self):
        while True:
            logging.info('checking on queues')
            try:
                monitor.check_queues()
            except:
                logging.exception('exception raised in check_queues')
            time.sleep(self.poll_interval)

def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
    )
    alerts.init()
    monitor = QueueMonitor()
    monitor.poll()

if __name__ == '__main__':
    main()
