#!/usr/bin/env python

import collections
import ConfigParser
import time
import unittest

import tallier
import testing

def build_config(**sections):
    config = ConfigParser.RawConfigParser()
    for section, data in sections.iteritems():
        config.add_section(section)
        for name, value in data.iteritems():
            config.set(section, name, value)
    return config

class MasterTest(unittest.TestCase):
    def test_from_config(self):
        # underspecified
        harold = dict(host='localhost', port=8888, secret='secret')
        graphite = dict(graphite_addr='localhost:9999')
        t = dict(port=7777, num_workers=1)
        config = build_config(harold=harold, graphite=graphite, tallier=t)
        master = tallier.Master.from_config(config, harold='stub')
        self.assertEquals('', master.iface)
        self.assertEquals(7777, master.port)
        self.assertEquals(1, master.num_workers)
        self.assertEquals(10.0, master.flush_interval)
        self.assertEquals('localhost', master.graphite_host)
        self.assertEquals(9999, master.graphite_port)
        self.assertTrue(master.harold is not None)

        # fully specified
        t.update(flush_interval=5.0, interface='localhost',
                 enable_heartbeat='true')
        config = build_config(harold=harold, graphite=graphite, tallier=t)
        master = tallier.Master.from_config(config, harold='stub')
        self.assertEquals(5.0, master.flush_interval)
        self.assertEquals('localhost', master.iface)
        self.assertTrue(master.harold is not None)

        # disable heartbeats
        t.update(enable_heartbeat='false')
        config = build_config(harold=harold, graphite=graphite, tallier=t)
        master = tallier.Master.from_config(config, harold='stub')
        self.assertTrue(master.harold is None)

    @testing.stub(time, 'time')
    def test_build_graphite_report(self):
        now = 1000
        time.time = lambda: now
        master = tallier.Master('', 0, 1, flush_interval=2.0)
        master.last_flush_time = now - 2
        master.num_stats = 0

        agg_counters = dict(a=1, b=2, c=3)
        agg_timers = dict(
            a=range(100),  # mean=49.5
        )

        m = lambda k, v: '%s %f %d' % (k, v, now)

        msgs = list(master._build_graphite_report(agg_counters, agg_timers))
        self.assertEquals(
            sorted([
                m('stats.a', 0.5),
                m('stats_counts.a', 1),
                m('stats.b', 1),
                m('stats_counts.b', 2),
                m('stats.c', 1.5),
                m('stats_counts.c', 3),
                m('stats.timers.a.lower', 0),
                m('stats.timers.a.upper', 99),
                m('stats.timers.a.upper_90', 90),
                m('stats.timers.a.mean', 49.5),
                m('stats.timers.a.count', 100),
                m('stats.timers.a.rate', 50),
                m('stats.tallier.num_workers', 1),
                m('stats.tallier.num_stats', 4),
            ]), sorted(msgs))

    def test_flush(self):
        child_reports = [
            ({'a': 1}, {'t1': [1, 2, 3]}),
            ({'b': 2}, {'t2': [2, 3, 4]}),
            ({'a': 3, 'b': 4}, {'t1': [5, 6], 't2': [6, 7]}),
        ]
        master = tallier.Master('', 0, 1)
        master._command_all = lambda cmd: child_reports
        master._build_graphite_report = lambda x, y: (
            x, dict((k, list(sorted(v))) for k, v in y.iteritems()))
        master._send_to_graphite = lambda x: x

        agg_counters, agg_timers = master._flush()
        self.assertEquals(
            {'a': 4,
             'b': 6,
             'tallier.messages.total': 0,
             'tallier.bytes.total': 0},
             agg_counters)
        self.assertEquals(
            {'t1': [1, 2, 3, 5, 6], 't2': [2, 3, 4, 6, 7]}, agg_timers)

class ListenerTest(unittest.TestCase):
    def test_handle_sample(self):
        listener = tallier.Listener(0, None)
        listener.current_samples = (collections.defaultdict(float), {})

        s = tallier.Sample(
            key='key', value=1.0, value_type=tallier.Sample.COUNTER,
            sample_rate=0.5)
        listener._handle_sample(s)
        self.assertEquals(2.0, listener.current_samples[0]['key'])

        s.sample_rate = 1.0
        listener._handle_sample(s)
        self.assertEquals(3.0, listener.current_samples[0]['key'])

        s.value_type = tallier.Sample.TIMER
        for i in xrange(3):
            s.value = i
            listener._handle_sample(s)
        self.assertEquals(range(3), listener.current_samples[1]['key'])

    def test_flush(self):
        listener = tallier.Listener(0, None)
        listener.message_count = 0
        listener.last_message_count = 0
        listener.byte_count = 0
        listener.last_byte_count = 0
        listener.current_samples = (collections.defaultdict(float), {})
        dgram = 'key:1|c:2|c:3|c:4|ms:5|ms:6|ms'
        listener._handle_datagram(dgram)
        expected_data = (
            {'key': 6},
            {'key': [4, 5, 6]})
        self.assertEquals(expected_data, listener.current_samples)
        expected_data[0]['tallier.messages.child_0'] = 1
        expected_data[0]['tallier.bytes.child_0'] = len(dgram)
        self.assertEquals(expected_data, listener.flush())
        self.assertEquals(({}, {}), listener.current_samples)

class SampleTest(unittest.TestCase):
    def test_normalize_key(self):
        n = tallier.Sample._normalize_key
        self.assertEquals('test', n('test'))
        self.assertEquals('test_1', n('test_1'))
        self.assertEquals('test_1-2', n('test 1\\2'))
        self.assertEquals('test1-2', n('[()@test#@&$^@&#*^1-2'))

    def test_parse_part(self):
        p = tallier.Sample._parse_part
        k = 'key'
        self.assertRaises(ValueError, p, k, '123')

        s = p(k, '123|c')
        self.assertEquals(k, s.key)
        self.assertEquals(123.0, s.value)
        self.assertEquals(tallier.Sample.COUNTER, s.value_type)
        self.assertEquals(1.0, s.sample_rate)

        s = p(k, '123|c@0.5')
        self.assertEquals(k, s.key)
        self.assertEquals(123.0, s.value)
        self.assertEquals(tallier.Sample.COUNTER, s.value_type)
        self.assertEquals(0.5, s.sample_rate)

        s = p(k, '123.45|ms')
        self.assertEquals(k, s.key)
        self.assertEquals(123.45, s.value)
        self.assertEquals(tallier.Sample.TIMER, s.value_type)
        self.assertEquals(1.0, s.sample_rate)

    def test_parse(self):
        self.assertEquals([], tallier.Sample.parse('a:b:c'))

        (s,) = tallier.Sample.parse('key:123.45|ms@0.5')
        self.assertEquals('key', s.key)
        self.assertEquals(123.45, s.value)
        self.assertEquals(tallier.Sample.TIMER, s.value_type)
        self.assertEquals(0.5, s.sample_rate)

        ss = tallier.Sample.parse('key1:1|c:2|c:3|c')
        self.assertEquals(3, len(ss))
        self.assertEquals('key1', ss[0].key)
        self.assertEquals('key1', ss[1].key)
        self.assertEquals(1, ss[0].value)
        self.assertEquals(2, ss[1].value)
        self.assertEquals(3, ss[2].value)

        multisample = tallier.Sample.parse('key1:1|c\nkey2:9|c')
        self.assertEquals(2, len(multisample))
        self.assertEquals('key1', multisample[0].key)
        self.assertEquals('key2', multisample[1].key)
        self.assertEquals(1, multisample[0].value)
        self.assertEquals(9, multisample[1].value)

    def test_parse_decompression(self):
        ss = tallier.Sample.parse('^03abc:1|c\n^02def:2|c\n^08:3|c')
        self.assertEquals(4, len(ss))
        self.assertEquals('abc', ss[0].key)
        self.assertEquals(1, ss[0].value)
        self.assertEquals('abdef', ss[1].key)
        self.assertEquals(2, ss[1].value)
        self.assertEquals('abdef', ss[2].key)
        self.assertEquals(2, ss[2].value)
        self.assertEquals('abdef', ss[3].key)
        self.assertEquals(3, ss[3].value)

if __name__ == '__main__':
    unittest.main()
