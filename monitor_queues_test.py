#!/usr/bin/env python

import subprocess
import time
import unittest

import monitor_queues
import testing

class MockPipe:
    def __init__(self, pipe_output):
        self.pipe_output = pipe_output

    def communicate(self):
        return (self.pipe_output, '')

class MonitorQueuesTest(unittest.TestCase):
    @testing.stub(subprocess, 'Popen')
    def test_get_queue_lengths(self):
        output = 'Listing queues ...\nA\t1\nB\t2\n...done.\n'
        subprocess.Popen = lambda cmd, stdout: MockPipe(output)
        self.assertEquals(dict(A=1, B=2), monitor_queues.get_queue_lengths())

    @testing.stub(time, 'time')
    @testing.stub(monitor_queues, 'send_graphite_message')
    def test_send_queue_stats(self):
        sent_messages = set()
        time.time = lambda: 1000
        monitor_queues.send_graphite_message = (
            lambda msg: sent_messages.update(msg.split('\n')))
        monitor_queues.send_queue_stats(dict(a=1, b=2))
        expected_messages = set([
            'stats.queue.a.length 1 1000',
            'stats.queue.b.length 2 1000',
        ])
        self.assertEquals(expected_messages, sent_messages)

    @testing.stub(monitor_queues, 'harold_send_message')
    def test_send_queue_alert(self):
        msgs = []
        monitor_queues.harold_send_message = (
            lambda command, **data: msgs.append((command, data)))
        monitor_queues.send_queue_alert('A', 2, 1)
        self.assertEquals(
            [('alert', dict(tag='A', message='A is too long (2/1)'))], msgs)

    @testing.stub(time, 'time')
    @testing.stub(monitor_queues, 'send_queue_alert')
    def test_update_queue_status(self):
        now = 1000
        alerts = []
        time.time = lambda: now
        monitor_queues.send_queue_alert = (
            lambda q, l, t: alerts.append((q, l, t)))

        # Non-alerting conditions.
        self.assertFalse(monitor_queues.update_queue_status('A', 1, 2))
        self.assertFalse(monitor_queues.update_queue_status('A', 1, 1))

        # Initial overrun condition for A should not fire alert.
        self.assertFalse(monitor_queues.update_queue_status('A', 9, 1))
        now += monitor_queues.ALERT_GRACE_PERIOD - 1
        self.assertFalse(monitor_queues.update_queue_status('A', 9, 1))

        # If overrun condition outlives the grace period, alert should raise.
        now += 1
        self.assertTrue(monitor_queues.update_queue_status('A', 2, 1))
        self.assertEquals(('A', 2, 1), alerts[-1])

        # Spammy alert should be suppressed but eventually refire.
        now += monitor_queues.ALERT_RATE_LIMIT - 1
        self.assertFalse(monitor_queues.update_queue_status('A', 9, 1))
        now += 1
        self.assertTrue(monitor_queues.update_queue_status('A', 3, 1))
        self.assertEquals(('A', 3, 1), alerts[-1])

        # Non-overrun condition should reset grace period.
        now += monitor_queues.ALERT_RATE_LIMIT
        self.assertFalse(monitor_queues.update_queue_status('A', 1, 1))
        self.assertFalse(monitor_queues.update_queue_status('A', 9, 1))
        now += monitor_queues.ALERT_GRACE_PERIOD - 1
        self.assertFalse(monitor_queues.update_queue_status('A', 9, 1))
        now += 1
        self.assertTrue(monitor_queues.update_queue_status('A', 4, 1))
        self.assertEquals(('A', 4, 1), alerts[-1])

    @testing.stub(time, 'time')
    @testing.stub(monitor_queues, 'get_queue_lengths')
    @testing.stub(monitor_queues, 'update_queue_status')
    @testing.stub(monitor_queues, 'send_queue_stats')
    @testing.stub(monitor_queues, 'QUEUE_LIMITS')
    @testing.stub(monitor_queues, 'harold_send_message')
    def test_check_queues(self):
        now = 1000
        expected_queue_lengths = dict(A=1, B=2, C=3)
        queue_lengths = {}
        queue_statuses = {}

        def stub_update_queue_status(n, l, t):
            queue_statuses[n] = (l, t)

        time.time = lambda: now
        monitor_queues.get_queue_lengths = lambda: expected_queue_lengths
        monitor_queues.update_queue_status = stub_update_queue_status
        monitor_queues.send_queue_stats = lambda ql: queue_lengths.update(ql)
        monitor_queues.QUEUE_LIMITS = dict(B=1)
        # swallow up the outgoing heartbeat message
        monitor_queues.harold_send_message = lambda command, **data: None

        # First run should emit heartbeat.
        monitor_queues.check_queues()
        self.assertEquals(expected_queue_lengths, queue_lengths)
        self.assertEquals((2, 1), queue_statuses['B'])
        self.assertEquals(now, monitor_queues.last_heartbeat)

        # Second run within heartbeat interval, no heartbeat emitted.
        last_heartbeat = now
        now += monitor_queues.HAROLD_HEARTBEAT_INTERVAL - 1
        monitor_queues.check_queues()
        self.assertEquals(last_heartbeat, monitor_queues.last_heartbeat)

        # Third run when next heartbeat should be sent.
        now += 1
        monitor_queues.check_queues()
        self.assertEquals(now, monitor_queues.last_heartbeat)


if __name__ == '__main__':
    unittest.main()
