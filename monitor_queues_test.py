#!/usr/bin/env python

import subprocess
import sys
import time
import unittest

import alerts
import monitor_queues
import testing

class MockPipe:
    def __init__(self, pipe_output):
        self.pipe_output = pipe_output

    def communicate(self):
        return (self.pipe_output, '')

class QueueMonitorTest(unittest.TestCase):
    HEARTBEAT_INTERVAL = 5
    HEARTBEAT_TIMEOUT_FACTOR = 2
    GRAPHITE_HOST = 'localhost'
    GRAPHITE_PORT = 22003
    ALERT_GRACE_PERIOD = 15
    ALERT_RATE_LIMIT = 60
    POLL_INTERVAL = 1

    def setUp(self):
        testing.init_alerts(
            queues=dict(
                heartbeat_interval=self.HEARTBEAT_INTERVAL,
                heartbeat_timeout_factor=self.HEARTBEAT_TIMEOUT_FACTOR,
                graphite_addr='%s:%d' % (
                    self.GRAPHITE_HOST, self.GRAPHITE_PORT),
                alert_grace_period=self.ALERT_GRACE_PERIOD,
                alert_rate_limit=self.ALERT_RATE_LIMIT,
                poll_interval=self.POLL_INTERVAL,
            ),
            queue_limits=dict(q1=1, q2=2),
        )
        self.monitor = monitor_queues.QueueMonitor()

    @testing.stub(subprocess, 'Popen')
    def test_get_queue_lengths(self):
        output = 'Listing queues ...\nA\t1\nB\t2\n...done.\n'
        subprocess.Popen = lambda cmd, stdout: MockPipe(output)
        self.assertEquals(dict(A=1, B=2), monitor_queues.get_queue_lengths())

    @testing.stub(time, 'time')
    def test_send_queue_stats(self):
        sent_messages = set()
        time.time = lambda: 1000
        self.monitor.send_graphite_message = (
            lambda msg: sent_messages.update(msg.split('\n')))
        self.monitor.send_queue_stats(dict(a=1, b=2))
        expected_messages = set([
            'stats.queue.a.length 1 1000',
            'stats.queue.b.length 2 1000',
        ])
        self.assertEquals(expected_messages, sent_messages)

    def test_send_queue_alert(self):
        self.monitor.send_queue_alert('A', 2, 1)
        self.assertEquals(
            [(['alert'], dict(tag='A', message='A is too long (2/1)'))],
            alerts.harold.post_log)

    @testing.stub(time, 'time')
    def test_update_queue_status(self):
        now = 1000
        alerts = []
        time.time = lambda: now
        self.monitor.send_queue_alert = (
            lambda q, l, t: alerts.append((q, l, t)))

        # Non-alerting conditions.
        self.assertFalse(self.monitor.update_queue_status('A', 1, 2))
        self.assertFalse(self.monitor.update_queue_status('A', 1, 1))

        # Initial overrun condition for A should not fire alert.
        self.assertFalse(self.monitor.update_queue_status('A', 9, 1))
        now += self.ALERT_GRACE_PERIOD - 1
        self.assertFalse(self.monitor.update_queue_status('A', 9, 1))

        # If overrun condition outlives the grace period, alert should raise.
        now += 1
        self.assertTrue(self.monitor.update_queue_status('A', 2, 1))
        self.assertEquals(('A', 2, 1), alerts[-1])

        # Spammy alert should be suppressed but eventually refire.
        now += self.ALERT_RATE_LIMIT - 1
        self.assertFalse(self.monitor.update_queue_status('A', 9, 1))
        now += 1
        self.assertTrue(self.monitor.update_queue_status('A', 3, 1))
        self.assertEquals(('A', 3, 1), alerts[-1])

        # Non-overrun condition should reset grace period.
        now += self.ALERT_RATE_LIMIT
        self.assertFalse(self.monitor.update_queue_status('A', 1, 1))
        self.assertFalse(self.monitor.update_queue_status('A', 9, 1))
        now += self.ALERT_GRACE_PERIOD - 1
        self.assertFalse(self.monitor.update_queue_status('A', 9, 1))
        now += 1
        self.assertTrue(self.monitor.update_queue_status('A', 4, 1))
        self.assertEquals(('A', 4, 1), alerts[-1])

    @testing.stub(time, 'time')
    @testing.stub(monitor_queues, 'get_queue_lengths')
    def test_check_queues(self):
        now = 1000
        expected_queue_lengths = dict(q1=1, q2=2, q3=3)

        time.time = lambda: now
        monitor_queues.get_queue_lengths = lambda: expected_queue_lengths

        queue_statuses = {}
        def stub_update_queue_status(n, l, t):
            queue_statuses[n] = (l, t)
        self.monitor.update_queue_status = stub_update_queue_status

        queue_lengths = {}
        self.monitor.send_queue_stats = lambda ql: queue_lengths.update(ql)

        # First run should emit heartbeat.
        self.monitor.check_queues()
        self.assertEquals(expected_queue_lengths, queue_lengths)
        self.assertEquals(
            dict(q1=(1, 1), q2=(2, 2), q3=(3, sys.maxint)), queue_statuses)
        self.assertEquals(
            [(['heartbeat'],
              dict(tag='monitor_queues',
                  interval=self.HEARTBEAT_INTERVAL
                      * self.HEARTBEAT_TIMEOUT_FACTOR))],
            alerts.harold.post_log)
        self.assertEquals(now, self.monitor.last_heartbeat)

        # Second run within heartbeat interval, no heartbeat emitted.
        last_heartbeat = now
        now += self.HEARTBEAT_INTERVAL - 1
        self.monitor.check_queues()
        self.assertEquals(last_heartbeat, self.monitor.last_heartbeat)

        # Third run when next heartbeat should be sent.
        now += 1
        self.monitor.check_queues()
        self.assertEquals(now, self.monitor.last_heartbeat)

if __name__ == '__main__':
    unittest.main()
