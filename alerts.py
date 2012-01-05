#!/usr/bin/python

import ConfigParser

import wessex

__all__ = ["harold", "config"]

harold = None
config = None

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

def init(config_path='production.ini'):
    global config, harold
    config = load_config(path=config_path)
    harold = get_harold(config)
