#!/usr/bin/python

import ConfigParser
import logging
import logging.handlers
import os
import sys
import time
import socket

import wessex

__all__ = ["harold", "config"]

harold = None
graphite = None
config = None

def init(config_path='production.ini'):
    global config, harold
    config = load_config(path=config_path)
    if config.has_section('logging'):
        configure_logging(config)
    if config.has_section('graphite'):
        configure_graphite(config)
    if config.has_section('harold'):
        harold = get_harold(config)

def load_config(path='production.ini'):
    config = ConfigParser.RawConfigParser()
    config.read([path])
    return config

def get_harold(config):
    harold_host = config.get('harold', 'host')
    harold_port = config.getint('harold', 'port')
    harold_secret = config.get('harold', 'secret')
    return wessex.Harold(
        host=harold_host, port=harold_port, secret=harold_secret)

class StreamLoggingFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        timestamp = time.strftime(datefmt)
        return timestamp % dict(ms=(1000 * record.created) % 1000)

def _get_logging_handler(config):
    mode = config.get('logging', 'mode')
    if mode == 'file':
        return logging.FileHandler(config.get('logging', 'file'))
    elif mode == 'stderr':
        return logging.StreamHandler()
    elif mode == 'syslog':
        return logging.handlers.SysLogHandler(
            config.get('logging', 'syslog_addr'))
    else:
        raise ValueError('unsupported logging mode: %r' % mode)

def _get_logging_formatter(config):
    mode = config.get('logging', 'mode')
    if mode == 'syslog':
        app_name = os.path.basename(sys.argv[0])
        return logging.Formatter(
            '%s: [%%(levelname)s] %%(message)s' % app_name)
    else:
        return StreamLoggingFormatter(
            '%(levelname).1s%(asctime)s: %(message)s',
            '%m%d %H:%M:%S.%%(ms)03d')

def _get_logging_level(config):
    if config.has_option('logging', 'level'):
        return config.get('logging', 'level')
    else:
        return logging.INFO

def configure_logging(config):
    ch = _get_logging_handler(config)
    ch.setFormatter(_get_logging_formatter(config))
    logger = logging.getLogger()
    logger.setLevel(_get_logging_level(config))
    logger.addHandler(ch)
    return logger

def _parse_addr(addr):
    host, port_str = addr.split(':', 1)
    return host, int(port_str)

class Graphite(object):
    """Send data to graphite in a fault-tolerant manner.

    Provides a single public method - send_values() - which adds to and
    flushes an internal queue of messages. Should delivery to graphite fail,
    messages are queued until the next invocation of send_values(). Up to
    MAX_QUEUE_SIZE messages are kept, after which the oldest are dropped. This
    class is not thread-safe.

    """
    MAX_QUEUE_SIZE = 1000

    def __init__(self, address):
        self.address = address
        self.send_queue = []

    def _send_message(self, msg):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(self.address)
        sock.send(msg + '\n')
        sock.close()

    def send_values(self, items):
        messages = []
        timestamp = str(time.time())
        for key, value in items.iteritems():
            messages.append(" ".join((key, str(value), timestamp)))
        self.send_queue.extend(messages)
        if self.send_queue:
            try:
                self._send_message("\n".join(self.send_queue))
                del self.send_queue[:]
            except socket.error as e:
                logging.warning(
                    "Error while flushing to graphite. Queue size: %d",
                    len(self.send_queue)
                )
            finally:
                if len(self.send_queue) > Graphite.MAX_QUEUE_SIZE:
                    logging.warning(
                        "Discarding %d messages",
                        len(self.send_queue) - Graphite.MAX_QUEUE_SIZE
                    )
                    self.send_queue = self.send_queue[-Graphite.MAX_QUEUE_SIZE:]


def configure_graphite(config):
    global graphite

    address_text = config.get('graphite', 'graphite_addr')
    address = _parse_addr(address_text)
    graphite = Graphite(address)
