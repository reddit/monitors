#!/usr/bin/env python

import ConfigParser
import logging
import tempfile
import unittest

import alerts

class ConfigureLoggingTest(unittest.TestCase):
    @staticmethod
    def _config(**data):
        config = ConfigParser.RawConfigParser()
        config.add_section('logging')
        for k, v in data.iteritems():
            config.set('logging', k, v)
        return config

    def test_get_logging_handler(self):
        def assertHandler(mode, expected_class):
            with tempfile.NamedTemporaryFile() as f:
                config = self._config(mode=mode, file=f.name,
                                      syslog_addr='/dev/log')
                self.assertTrue(
                    isinstance(
                        alerts._get_logging_handler(config), expected_class))

        assertHandler('file', logging.FileHandler)
        assertHandler('stderr', logging.StreamHandler)
        # we can count on the alerts module importing logging.handlers
        assertHandler('syslog', logging.handlers.SysLogHandler)

        self.assertRaises(ValueError, assertHandler, 'asdf', None)

    def test_get_logging_formatter(self):
        f = alerts._get_logging_formatter(self._config(mode='syslog'))
        self.assertFalse(isinstance(f, alerts.StreamLoggingFormatter))
        f = alerts._get_logging_formatter(self._config(mode='not-syslog'))
        self.assertTrue(isinstance(f, alerts.StreamLoggingFormatter))

    def test_get_logging_level(self):
        config = self._config()
        self.assertEquals(logging.INFO, alerts._get_logging_level(config))
        config = self._config(level='DEBUG')
        self.assertEquals('DEBUG', alerts._get_logging_level(config))

if __name__ == '__main__':
    unittest.main()
