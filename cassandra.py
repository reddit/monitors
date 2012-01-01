#!/usr/bin/python

import sys
import time
import urllib2
import urlparse
import platform
from paramiko import SSHClient, SSHException, AutoAddPolicy

import alerts


INTERVAL  = 10  # seconds
THRESHOLD = 3   # recent failures

# The command should be forced through authorized_keys, so this is just a reference
CASS_CMD  = "/usr/local/bin/ringstat -d"

class DownedNodeException(Exception):
    def __init__(self, node):
        self.node = node
    def __str__(self):
        return "%s is down in the ring" % self.node

class CassandraMonitor():
    def __init__(self, server):
        self.server = server
        self.recent_failures = 0
    def __str__(self):
        return "CassandraMonitor for %s with %i failures" % (self.server, self.recent_failures)
    def mark_error(self, tag, message):
        self.recent_failures += 1
        if self.recent_failures > THRESHOLD:
            if tag == "fucking-cass-22":
                print "Ignoring %s" % tag
                return
            alerts.harold.alert(tag, message)
            self.recent_failures = THRESHOLD
    def clear_error(self):
        self.recent_failures = max(self.recent_failures - 1, 0)
    def start_monitor(self):
        local_name = platform.node()
        print "Starting Cassandra Monitor for %s" % self.server
        while True:
            try:
                ssh = SSHClient()
                ssh.load_system_host_keys()
                ssh.set_missing_host_key_policy(AutoAddPolicy())
                ssh.connect(self.server, timeout=5, allow_agent=False)
                stdin, stdout, stderr = ssh.exec_command(CASS_CMD)
                stdin.close()
                for line in stdout:
                    # Any line that shows up will be a downed server
                    # server datacenter rack status state load owns token
                    downed_node = line.split()
                    raise DownedNodeException(downed_node[0])
                stdout.close()
                err = stderr.read()
                if err:
                    raise Exception("Unknown error: %s" % str)
                stderr.close()
                ssh.close()
            except DownedNodeException as e:
                self.mark_error(e.node, e)
            except SSHException as e:
                self.mark_error(self.server, "%s could not connect to %s: %s" % (local_name, self.server, e))
            except Exception as e:
                self.mark_error(local_name, "Unknown error: %s" % e)
            else:
                self.clear_error()
            time.sleep(INTERVAL)

if __name__ == "__main__":
    server = sys.argv[1]
    monitor = CassandraMonitor(server)
    monitor.start_monitor()
