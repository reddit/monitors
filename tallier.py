#!/usr/bin/env python

"""
Server that aggregates stats from many clients and flushes to graphite.

This service is meant to support hundreds of clients feeding in a total of
thousands of sample points per second via UDP. It can fork into multiple
processes to utilize additional processors/cores.

The architecture comprises:

  - Master: entry point of the server, spins off server processes and polls
      them for aggregate data to flush periodically to graphite.

  - Controller: the main thread of each server process; starts up and shuts down
      the Listener; communicates with the Master; swaps out the Listener's data
      accumulation dict on flush requests, returning the former dict to the
      Master.

  - Listener: a thread of the server process; simply receives data over the
      datagram socket and accumulates it in a dict.

Configuration is through .ini file. Example:

[harold]
host = localhost
port = 8888
secret = haroldsecret

[graphite]
graphite_addr = localhost:2003

[tallier]

# receive datagrams on this port
port = 8125

# consume datagrams with this many processes
num_workers = 3

# sample stats at this frequency (seconds); this is how often data is pushed to
# graphite
flush_interval = 10
"""

from __future__ import division

import collections
import logging
import multiprocessing
import os
import re
import signal
import socket
import threading
import time
import urllib2

import alerts

FLUSH = 'flush'
SHUTDOWN = 'shutdown'

class Master:
    """Entry point of the tally service.

    Spins off server processes and polls them for aggregate data to flush
    periodically to graphite.
    """

    @classmethod
    def from_config(cls, config, harold=None):
        """Instantiate the tally service from a parsed config file."""
        if config.has_option('tallier', 'flush_interval'):
            flush_interval = config.getfloat('tallier', 'flush_interval')
        else:
            flush_interval = 10.0
        if config.has_option('tallier', 'interface'):
            iface = config.get('tallier', 'interface')
        else:
            iface = ''
        port = config.getint('tallier', 'port')
        num_workers = config.getint('tallier', 'num_workers')
        graphite_addr = config.get('graphite', 'graphite_addr')
        if (config.has_option('tallier', 'enable_heartbeat')
            and not config.getboolean('tallier', 'enable_heartbeat')):
            harold = None
        return cls(iface, port, num_workers, flush_interval=flush_interval,
                   graphite_addr=graphite_addr, harold=harold)

    def __init__(self, iface, port, num_workers, flush_interval=10,
                 graphite_addr='localhost:2003', harold=None):
        """Constructor.

        Args:
          - iface: str, address to bind to when the service starts (or '' for
                INADDR_ANY).
          - port: int, port to bind to.
          - num_workers: int, size of datagram receiving pool (processes); must
                be >= 1.
          - flush_interval: float, time (in seconds) between each flush
          - graphite_addr: str, graphite address for reporting collected samples
          - harold: wessex.Harold, optional harold client instance for sending
                heartbeats
        """
        assert num_workers >= 1
        self.iface = iface
        self.port = port
        self.num_workers = num_workers
        self.flush_interval = flush_interval
        self.graphite_host, self.graphite_port = graphite_addr.split(':')
        self.graphite_port = int(self.graphite_port)
        self.harold = harold

        # The following are all set up by the start() method.
        self.next_flush_time = None
        self.last_flush_time = None
        self.sock = None
        self.pipes = None
        self.controllers = None
        self.num_stats = None

    def _bind(self):
        assert self.sock is None, 'Master.start() should only be invoked once'
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.iface, self.port))

    def _create_controllers(self):
        assert self.controllers is None, (
            'Master.start() should only be invoked once')
        assert self.pipes is None, (
            'Master.start() should only be invoked once')
        assert self.num_workers >= 1
        self.pipes = [multiprocessing.Pipe() for _ in xrange(self.num_workers)]

        # Set up the controllers to run in child processes (but do not start
        # them up yet).
        self.controllers = [
            multiprocessing.Process(
                target=Controller.launch, args=(i, self.sock, pipe[1]))
            for i, pipe in enumerate(self.pipes)]

    def _shutdown(self):
        logging.info('Closing socket...')
        self.sock.close()
        self.sock = None
        logging.info('Sending shutdown command...')
        results = self._command_all(SHUTDOWN)
        logging.info('Messages: %r (total = %d)', results, sum(results))
        logging.info('Terminating child processes...')
        for controller in self.controllers:
            controller.terminate()
        for controller in self.controllers:
            controller.join()
        self.pipes = None
        self.controllers = None
        self.next_flush_time = None
        self.last_flush_time = None
        logging.info('Shutdown complete.')

    def _flush(self):
        results = self._command_all(FLUSH)
        agg_counters = collections.defaultdict(float)
        agg_timers = {}
        total_message_count = 0
        total_byte_count = 0
        for counters, timers in results:
            for key, value in counters.iteritems():
                agg_counters[key] += value
                if key.startswith('tallier.messages.child_'):
                    total_message_count += value
                elif key.startswith('tallier.bytes.child_'):
                    total_byte_count += value
            for key, values in timers.iteritems():
                agg_timers.setdefault(key, []).extend(values)

        agg_counters['tallier.messages.total'] = total_message_count
        agg_counters['tallier.bytes.total'] = total_byte_count

        msgs = self._build_graphite_report(agg_counters, agg_timers)
        return self._send_to_graphite(msgs)

    def _build_graphite_report(self, agg_counters, agg_timers):
        now = time.time()
        interval = now - self.last_flush_time
        self.last_flush_time = now

        for key, value in agg_counters.iteritems():
            scaled_value = value / interval
            yield 'stats.%s %f %d' % (key, scaled_value, now)
            yield 'stats_counts.%s %f %d' % (key, value, now)

        for key, values in agg_timers.iteritems():
            # TODO: make the percentile configurable; for now fix to 90
            percentile = 90
            values.sort()
            yield 'stats.timers.%s.lower %f %d' % (key, values[0], now)
            yield 'stats.timers.%s.upper %f %d' % (key, values[-1], now)
            yield ('stats.timers.%s.upper_%d %f %d'
                   % (key, percentile,
                      values[int(len(values) * percentile / 100.0)], now))
            yield ('stats.timers.%s.mean %f %d'
                   % (key, sum(values) / len(values), now))
            yield 'stats.timers.%s.count %f %d' % (key, len(values), now)
            yield ('stats.timers.%s.rate %f %d'
                   % (key, len(values) / interval, now))

        # global 'self' stats
        self.num_stats += len(agg_counters) + len(agg_timers)
        yield 'stats.tallier.num_stats %f %d' % (self.num_stats, now)
        yield 'stats.tallier.num_workers %f %d' % (self.num_workers, now)

    def _send_to_graphite(self, msgs):
        logging.info('Connecting to graphite...')
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.graphite_host, self.graphite_port))
        msg = '\n'.join(msgs) + '\n'
        sock.send(msg)
        sock.close()
        logging.info('Stats sent!')
        if self.harold:
            try:
                logging.info('Harold heartbeat.')
                self.harold.heartbeat('tallier', int(self.flush_interval * 3))
            except urllib2.URLError:
                logging.exception('Error sending heartbeat to harold!')

    def _command_all(self, cmd):
        for pipe in self.pipes:
            pipe[0].send(cmd)
        # TODO: should we worry about non-responsive children?
        return [pipe[0].recv() for pipe in self.pipes]

    def start(self):
        """Sets up and starts the tally service (does not return)."""
        self._bind()
        self._create_controllers()
        logging.info('Starting up child processes...')
        for controller in self.controllers:
            controller.daemon = True
            controller.start()
        self.last_flush_time = time.time()
        self.next_flush_time = self.last_flush_time + self.flush_interval
        self.num_stats = 0
        logging.info('Running.')
        try:
            while True:
                # sleep until next flush time
                sleep_time = self.next_flush_time - time.time()
                if sleep_time > 0:
                    time.sleep(sleep_time)
                self._flush()
                self.next_flush_time += self.flush_interval
        except KeyboardInterrupt:
            pass
        finally:
            self._shutdown()

class Controller:
    """The main thread of each server process.

    The Controller manages a Listener and communicates with the Master. On
    command from the Master it will swap out the Listener's data accumulation
    dict, returning the former dict to the Master. It will also shut down the
    Listener at the Master's request.
    """

    def __init__(self, controller_id, sock, conn):
        """Constructor.

        Args:
          - sock: socket.Socket, bound datagram socket
          - conn: multiprocessing.Connection, for communication with the Master
        """
        self.controller_id = controller_id
        self.sock = sock
        self.conn = conn

        # The following are all set up by the start() method.
        self.listener = None

    @classmethod
    def launch(cls, controller_id, sock, conn):
        """Initialize and start up a Controller (does not return)."""
        controller = cls(controller_id, sock, conn)
        controller.start()

    def _create_listener(self):
        assert self.listener is None, (
            'Controller.start() should only be invoked once')
        self.listener = Listener(self.controller_id, self.sock)

    def _flush(self):
        self.conn.send(self.listener.flush())

    def _shutdown(self):
        # TODO: clean shutdown for listener threads?
        #self.listener.stop()
        logging.info('sending back message count...')
        self.conn.send(self.listener.message_count)
        self.listener = None

    def start(self):
        """Starts the Controller (does not return)."""
        self._create_listener()
        self.listener.start()
        try:
            while True:
                cmd = self.conn.recv()
                if cmd == FLUSH:
                    self._flush()
                elif cmd == SHUTDOWN:
                    self._shutdown()
                    break
                else:
                    logging.info(
                        'controller reporting bad command from master: %s', cmd)
        except KeyboardInterrupt:
            self._shutdown()
        logging.info('controller stopped.')

class Listener(threading.Thread):
    """A thread that receives stats from a datagram socket.

    The stats are accumulated in a dict that can be swapped out at any time.
    """

    def __init__(self, listener_id, sock):
        """Constructor.

        Args:
          - sock: socket.Socket, bound datagram socket
        """
        super(Listener, self).__init__()
        self.listener_id = listener_id
        self.sock = sock

        # The following are all set up by the start() method.
        self.current_samples = None
        self.message_count = None
        self.last_message_count = None
        self.byte_count = None
        self.last_byte_count = None

    def start(self):
        """Creates the Listener thread, starts up the Listener, and returns."""
        assert self.current_samples is None, (
            'Listener.start() should only be invoked once')
        self.daemon = True
        self.current_samples = (collections.defaultdict(float), {})
        self.message_count = 0
        self.last_message_count = 0
        self.byte_count = 0
        self.last_byte_count = 0
        super(Listener, self).start()

    def run(self):
        """Runs the main loop of the Listener (does not return)."""
        while True:
            datagram, addr = self.sock.recvfrom(1024)
            self._handle_datagram(datagram)

    def _handle_datagram(self, datagram):
        samples = Sample.parse(datagram)
        for sample in samples:
            self._handle_sample(sample)
        self.message_count += 1
        self.byte_count += len(datagram)

    def _handle_sample(self, sample):
        key = sample.key
        value = sample.value
        if sample.value_type is Sample.COUNTER:
            self.current_samples[0][key] += value / sample.sample_rate
        else:
            self.current_samples[1].setdefault(key, []).append(value)

    def flush(self):
        samples, self.current_samples = (
            self.current_samples, (collections.defaultdict(float), {}))

        # Include count of messages/bytes received by this listener process
        # since the last flush.
        mc = self.message_count
        samples[0]['tallier.messages.child_%s' % self.listener_id] = (
            mc - self.last_message_count)
        self.last_message_count = mc

        bc = self.byte_count
        samples[0]['tallier.bytes.child_%s' % self.listener_id] = (
            bc - self.last_byte_count)
        self.last_byte_count = bc

        return samples

class Sample:
    """A key, value, value type, and sample rate."""

    COUNTER = 'counter'
    TIMER = 'timer'

    _VALID_CHAR_PATTERN = re.compile(r'[A-Za-z0-9._-]')

    def __init__(self, key, value, value_type, sample_rate):
        self.key = key
        self.value = value
        self.value_type = value_type
        self.sample_rate = sample_rate

    def __str__(self):
        return '%s:%f@%s|%f' % (
            self.key, self.value,
            'ms' if self.value_type is self.TIMER else 'c',
            self.sample_rate)

    @classmethod
    def parse(cls, datagram):
        """Parses a datagram into a list of Sample values."""
        samples = []
        for metric in datagram.splitlines():
            parts = metric.split(':')
            if parts:
                key = cls._normalize_key(parts.pop(0))
                for part in parts:
                    try:
                        samples.append(cls._parse_part(key, part))
                    except ValueError:
                        continue
        return samples

    @classmethod
    def _normalize_key(cls, key):
        key = '_'.join(key.split()).replace('\\', '-')
        return ''.join(cls._VALID_CHAR_PATTERN.findall(key))

    @classmethod
    def _parse_part(cls, key, part):
        # format: <value> '|' <value_type> ('@' <sample_rate>)?
        fields = part.split('|')
        if len(fields) != 2:
            raise ValueError
        value = float(fields[0])
        if '@' in fields[1]:
            fields[1], sample_rate = fields[1].split('@', 1)
            sample_rate = float(sample_rate)
            if not (0.0 < sample_rate <= 1.0):
                raise ValueError
        else:
            sample_rate = 1.0
        if fields[1] == 'ms':
            value_type = cls.TIMER
        else:
            value_type = cls.COUNTER
        return cls(key, value, value_type, sample_rate)

if __name__ == '__main__':
    alerts.init()
    master = Master.from_config(alerts.config, alerts.harold)
    logging.info('Serving...')
    master.start()
    logging.info('Done!')
