#!/usr/bin/python

import ConfigParser

import wessex

__all__ = ["harold", "config"]

# load the configuration
config = ConfigParser.RawConfigParser()
config.read(['production.ini'])
HAROLD_HOST = config.get('harold', 'host')
HAROLD_PORT = config.getint('harold', 'port')
HAROLD_SECRET = config.get('harold', 'secret')

# create the harold object
harold = wessex.Harold(host=HAROLD_HOST, port=HAROLD_PORT, secret=HAROLD_SECRET)
