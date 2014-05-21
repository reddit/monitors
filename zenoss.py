import re
import json
import logging
import requests

log = logging.getLogger(__name__)

ROUTERS = {'MessagingRouter': 'messaging',
           'EventsRouter': 'evconsole',
           'ProcessRouter': 'process',
           'ServiceRouter': 'service',
           'DeviceRouter': 'device',
           'NetworkRouter': 'network',
           'TemplateRouter': 'template',
           'DetailNavRouter': 'detailnav',
           'ReportRouter': 'report',
           'MibRouter': 'mib',
           'ZenPackRouter': 'zenpack'}


class ZenossException(Exception):
    def __call__(self, *args):
        return self.__class__(*(self.args + args))


class Zenoss(object):
    def __init__(self, host, username, password, pem_path=None):
        self.__host = host
        self.__session = requests.Session()
        self.__session.auth = (username, password)
        self.__pem_path = pem_path
        self.__req_count = 0

    def __router_request(self, router, method, data=None):
        if router not in ROUTERS:
            raise Exception('Router "' + router + '" not available.')

        req_data = json.dumps([dict(
            action=router,
            method=method,
            data=data,
            type='rpc',
            tid=self.__req_count)])

        log.debug('Making request to router %s with method %s', router, method)
        uri = '%s/zport/dmd/%s_router' % (self.__host, ROUTERS[router])
        headers = {'Content-type': 'application/json; charset=utf-8'}
        response = self.__session.post(uri, data=req_data, headers=headers)
        self.__req_count += 1

        # The API returns a 200 response code even whe auth is bad.
        # With bad auth, the login page is displayed. Here I search for
        # an element on the login form to determine if auth failed.
        if re.search('name="__ac_name"', response.content):
            log.error('Request failed. Bad username/password.')
            raise ZenossException('Request failed. Bad username/password.')

        return json.loads(response.content)['result']

    def __rrd_request(self, device_uid, dsname):
        return self.__session.get('%s/%s/getRRDValue?dsname=%s' % (self.__host, device_uid, dsname)).content

    def get_devices(self, device_class='/zport/dmd/Devices', limit=None):
        """Get a list of all devices.

        """
        log.info('Getting all devices')
        return self.__router_request('DeviceRouter', 'getDevices',
                                     data=[{'uid': device_class, 'params': {}, 'limit': limit}])

    def find_device(self, device_name):
        """Find a device by name.

        """
        log.info('Finding device %s', device_name)
        all_devices = self.get_devices()

        try:
            device = [d for d in all_devices['devices'] if d['name'] == device_name][0]
            # We need to save the has for later operations
            device['hash'] = all_devices['hash']
            log.info('%s found', device_name)
            return device
        except IndexError:
            log.error('Cannot locate device %s', device_name)
            raise Exception('Cannot locate device %s' % device_name)

    def add_device(self, device_name, device_class, collector='localhost'):
        """Add a device.

        """
        log.info('Adding %s', device_name)
        data = dict(deviceName=device_name, deviceClass=device_class, model=True, collector=collector)
        return self.__router_request('DeviceRouter', 'addDevice', [data])

    def remove_device(self, device_name):
        """Remove a device.

        """
        log.info('Removing %s', device_name)
        device = self.find_device(device_name)
        data = dict(uids=[device['uid']], hashcheck=device['hash'], action='delete')
        return self.__router_request('DeviceRouter', 'removeDevices', [data])

    def move_device(self, device_name, organizer):
        """Move the device the organizer specified.

        """
        log.info('Moving %s to %s', device_name, organizer)
        device = self.find_device(device_name)
        data = dict(uids=[device['uid']], hashcheck=device['hash'], target=organizer)
        return self.__router_request('DeviceRouter', 'moveDevices', [data])

    def set_prod_state(self, device_name, prod_state):
        """Set the production state of a device.

        """
        log.info('Setting prodState on %s to %s', device_name, prod_state)
        device = self.find_device(device_name)
        data = dict(uids=[device['uid']], prodState=prod_state, hashcheck=device['hash'])
        return self.__router_request('DeviceRouter', 'setProductionState', [data])

    def set_maintenance(self, device_name):
        """Helper method to set prodState for device so that it does not alert.

        """
        return self.set_prod_state(device_name, 300)

    def set_production(self, device_name):
        """Helper method to set prodState for device so that it is back in production and alerting.

        """
        return self.set_prod_state(device_name, 1000)

    def set_product_info(self, device_name, hw_manufacturer, hw_product_name, os_manufacturer, os_product_name):
        """Set ProductInfo on a device.

        """
        log.info('Setting ProductInfo on %s', device_name)
        device = self.find_device(device_name)
        data = dict(uid=device['uid'],
                    hwManufacturer=hw_manufacturer,
                    hwProductName=hw_product_name,
                    osManufacturer=os_manufacturer,
                    osProductName=os_product_name)
        return self.__router_request('DeviceRouter', 'setProductInfo', [data])

    def set_rhel_release(self, device_name, release):
        """Sets the proper release of RedHat Enterprise Linux."""
        if type(release) is not float:
            log.error("RHEL release must be a float")
            return {u'success': False}
        log.info('Setting RHEL release on %s to %s', device_name, release)
        device = self.find_device(device_name)
        return self.set_product_info(device_name, device['hwManufacturer']['name'], device['hwModel']['name'], 'RedHat',
                                     'RHEL {}'.format(release))

    def set_device_info(self, device_name, data):
        """Set attributes on a device or device organizer.
            This method accepts any keyword argument for the property that you wish to set.

        """
        data['uid'] = self.find_device(device_name)['uid']
        return self.__router_request('DeviceRouter', 'setInfo', [data])

    def remodel_device(self, device_name):
        """Submit a job to have a device remodeled.

        """
        return self.__router_request('DeviceRouter', 'remodel', [dict(uid=self.find_device(device_name)['uid'])])

    def set_collector(self, device_name, collector):
        """Set collector for device.

        """
        device = self.find_device(device_name)
        data = dict(uids=[device['uid']], hashcheck=device['hash'], collector=collector)
        return self.__router_request('DeviceRouter', 'setCollector', [data])

    def rename_device(self, device_name, new_name):
        """Rename a device.

        """
        data = dict(uid=self.find_device(device_name)['uid'], newId=new_name)
        return self.__router_request('DeviceRouter', 'renameDevice', [data])

    def reset_ip(self, device_name, ip_address=''):
        """Reset IP address(es) of device to the results of a DNS lookup or a manually set address.

        """
        device = self.find_device(device_name)
        data = dict(uids=[device['uid']], hashcheck=device['hash'], ip=ip_address)
        return self.__router_request('DeviceRouter', 'resetIp', [data])

    def get_events(self, device=None, limit=100, component=None, event_class=None):
        """Find current events.

        """
        data = dict(start=0, limit=limit, dir='DESC', sort='severity')
        data['params'] = dict(severity=[5, 4, 3, 2], eventState=[0, 1])
        if device:
            data['params']['device'] = device
        if component:
            data['params']['component'] = component
        if event_class:
            data['params']['eventClass'] = event_class
        log.info('Getting events for %s', data)
        return self.__router_request('EventsRouter', 'query', [data])['events']

    def change_event_state(self, event_id, state):
        """Change the state of an event.

        """
        log.info('Changing eventState on %s to %s', event_id, state)
        return self.__router_request('EventsRouter', state, [{'evids': [event_id]}])

    def ack_event(self, event_id):
        """Helper method to set the event state to acknowledged.

        """
        return self.change_event_state(event_id, 'acknowledge')

    def close_event(self, event_id):
        """Helper method to set the event state to closed.

        """
        return self.change_event_state(event_id, 'close')

    def create_event_on_device(self, device_name, severity, summary, component='', evclasskey='', evclass=''):
        """Manually create a new event for the device specified.

        """
        log.info('Creating new event for %s with severity %s', device_name, severity)
        if severity not in ('Critical', 'Error', 'Warning', 'Info', 'Debug', 'Clear'):
            raise Exception('Severity %s is not valid.' % severity)
        data = dict(device=device_name, summary=summary, severity=severity, component=component, evclasskey=evclasskey, evclass=evclass)
        return self.__router_request('EventsRouter', 'add_event', [data])

    def get_load_average(self, device_name):
        """Returns the 5 minute load average for a device.
        """
        result = self.__rrd_request(self.find_device(device_name)['uid'], 'laLoadInt5_laLoadInt5')
        return round(float(result) / 100.0, 2)
