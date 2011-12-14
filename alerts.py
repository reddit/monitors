#!/usr/bin/python

import ConfigParser

import wessex

__all__ = ['harold']

# load the configuration
config = ConfigParser.RawConfigParser()
config.read(['production.ini'])
HAROLD_HOST = config.get('alerts', 'host')
HAROLD_PORT = config.getint('alerts', 'port')
HAROLD_SECRET = config.get('alerts', 'secret')

# create the harold object
harold = wessex.Harold(host=HAROLD_HOST, port=HAROLD_PORT, secret=HAROLD_SECRET)
