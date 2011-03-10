# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Opsource Driver
"""
from libcloud.types import NodeState, Provider, InvalidCredsError, MalformedResponseError
from libcloud.base import ConnectionUserAndKey, Response, NodeDriver, Node
from libcloud.base import NodeSize, NodeImage, NodeLocation
from libcloud.base import is_private_subnet
import base64
import socket
from xml.etree import ElementTree as ET
from xml.parsers.expat import ExpatError

from pprint import pprint

# setup a few variables to represent all of the opsource cloud namespaces
NAMESPACE_BASE       = "http://oec.api.opsource.net/schemas"
ORGANIZATION_NS      = NAMESPACE_BASE + "/organization"
SERVER_NS            = NAMESPACE_BASE + "/server"
NETWORK_NS           = NAMESPACE_BASE + "/network"
DIRECTORY_NS         = NAMESPACE_BASE + "/directory"
RESET_NS             = NAMESPACE_BASE + "/reset"
VIP_NS               = NAMESPACE_BASE + "/vip"
IMAGEIMPORTEXPORT_NS = NAMESPACE_BASE + "/imageimportexport"
DATACENTER_NS        = NAMESPACE_BASE + "/datacenter"
SUPPORT_NS           = NAMESPACE_BASE + "/support"
GENERAL_NS           = NAMESPACE_BASE + "/general"
IPPLAN_NS            = NAMESPACE_BASE + "/ipplan"
WHITELABEL_NS        = NAMESPACE_BASE + "/whitelabel"

# TODO:
#   x need to get orgId during initial connection instead of hardcoding mine into the code
#   x implement list_nodes()
#   - implement create_node()  (needs net-id and image-id to work, so we should implement those first)
#   x implement reboot()
#   - implement destroy_node()
#   - implement list_sizes()
#   - implement list_images()
#   x implement list_locations()
#   - implement .... any other standard functions missing?
#	- implement various ex_* extension functions for opsource-specific features
#       x ex_graceful_shutdown
#       x ex_start_node
#       x ex_power_off
#       x ex_list_networks
#       - ex_list_pending_nodes
#       - add pending servers to list_nodes() ?
#       x ...what else?

class OpsourceResponse(Response):
    
    def parse_body(self):
        try:
            body = ET.XML(self.body)
        except:
            raise MalformedResponseError("Failed to parse XML", body=self.body, driver=OpsourceNodeDriver)
        return body
    
    def parse_error(self):
        if self.status == 401:
            raise InvalidCredsError(self.body)
        
        if self.status == 403:
            raise InvalidCredsError(self.body)
        
        try:
            body = ET.XML(self.body)
        except:
            raise MalformedResponseError("Failed to parse XML", body=self.body, driver=OpsourceNodeDriver)
        
        try:
            if self.status == 400:
            	code = body.findtext("{%s}resultCode" % GENERAL_NS)
            	message = body.findtext("{%s}resultDetail" % GENERAL_NS)
                return "%s: %s" % (code, message)
        except:
            return self.body


class OpsourceConnection(ConnectionUserAndKey):
    """
    Connection class for the Opsource driver
    """
    
    host = 'api.opsourcecloud.net'
    api_path = '/oec'
    api_version = '0.9'
    orgId = None
    
    responseCls = OpsourceResponse
    
    def add_default_headers(self, headers):
        headers['Authorization'] = ('Basic %s'
                              % (base64.b64encode('%s:%s' % (self.user_id, self.key))))
        return headers
    
    def request(self, action, params=None, data='', headers=None, method='GET'):
        """
        This method is used to make API requests to the Opsource Cloud.
        It does some extra legwork by looking up the orgId before sending requests, since
        (most) requests to opsource cloud require the orgId to be included in the path.
        The orgId is obtained by calling /myaccount.
        
        eg:
           .request('/server/deployed')
             will result in a path like this:
           http://api.opsourcecloud.net/oec/0.9/232423-2a23-a23f-adsf2342/server/deploy
        """
        # /myaccount requests do not require the orgId in the path because this
        # is the request needed to get the orgId
        if action == '/myaccount':
            action = "%s/%s/%s" % (self.api_path, self.api_version, action)
        else:
            if self.orgId == None:
                self._get_orgId()
            action = "%s/%s/%s/%s" % (self.api_path, self.api_version, self.orgId, action)
        
        return super(OpsourceConnection, self).request(
            action=action,
            params=params, data=data,
            method=method, headers=headers
        )
    
    def _get_orgId(self):
        """
        send the /myaccount API request to opsource cloud and parse the 'orgId' from the
        XML response object.  We need the orgId to use most of the other API functions
        """
        self.orgId = self.request('/myaccount').object.findtext("{%s}orgId" % DIRECTORY_NS)

class OpsourceNetwork(object):
    """
    Opsource network with location
    """
    
    def __init__(self, id, name, description, location, privateNet, multicast):
        self.id = str(id)
        self.name = name
        self.description = description
        self.location = location
        self.privateNet = privateNet
        self.multicast = multicast

    def __repr__(self):
        return (('<OpsourceNetwork: id=%s, name=%s, description=%s, location=%s, privateNet=%s, multicast=%s>')
                % (self.id, self.name, self.description, self.location, self.privateNet, self.multicast))
                
class OpsourceNodeDriver(NodeDriver):
    """
    Opsource node driver
    """
    
    connectionCls = OpsourceConnection
    
    type = Provider.OPSOURCE
    name = 'Opsource'
    
    def list_nodes(self):
        return self._to_nodes(self.connection.request('/server/deployed').object)
    
    def list_sizes(self, location=None):
        pass
    
    def list_images(self, location=None):
        pass
    
    def list_locations(self):
        """list locations (datacenters) available for instantiating servers and
            networks.  
        """
        return self._to_locations(self.connection.request('/datacenter').object)
    
    def create_node(self, **kwargs):
        """
        notes:
            requirements:
                - node name
                - description
                - network id (net-id)
                - image id
                - admin/root password
                - isStarted = true or false
        """
        pass
    
    def reboot_node(self, node):
        object = self.connection.request('/server/%s?restart' % node.id).object
        result = object.findtext("{%s}result" % GENERAL_NS)
        return result == 'SUCCESS'
    
    def destroy_node(self, node):
        """Destroys the node"""
        pass

    def ex_start_node(self, node):
        """Powers on an existing deployed server"""
        object = self.connection.request('/server/%s?start' % node.id).object
        result = object.findtext("{%s}result" % GENERAL_NS)
        return result == 'SUCCESS'
            
    def ex_shutdown_graceful(self, node):
        """This function will attempt to "gracefully" stop a server by initiating a
	    shutdown sequence within the guest operating system. A successful response
	    on this function means the system has successfully passed the
	    request into the operating system.
        """
        object = self.connection.request('/server/%s?shutdown' % node.id).object
        result = object.findtext("{%s}result" % GENERAL_NS)
        return result == 'SUCCESS'
        
    def ex_power_off(self, node):
        """This function will abruptly power-off a server.  Unlike ex_shutdown_graceful,
        success ensures the node will stop but some OS and application configurations may
        be adversely affected by the equivalent of pulling the power plug out of the
        machine.
        """
        object = self.connection.request('/server/%s?poweroff' % node.id).object
        result = object.findtext("{%s}result" % GENERAL_NS)
        return result == 'SUCCESS'
        
    def ex_list_networks(self):
        """List networks deployed across all data center locations for your
        organization.  The response includes the location of each network.
        
        Returns a list of OpsourceNetwork objects
        """
        return self._to_networks(self.connection.request('/networkWithLocation').object)
    
    def _to_networks(self, object):
        node_elements = object.findall("{%s}network" % NETWORK_NS)
        return [ self._to_network(el) for el in node_elements ]
        
    def _to_network(self, element):
        multicast = False
        if element.findtext("{%s}multicast" % NETWORK_NS) == 'true':
            multicast = True

        location_id = element.findtext("{%s}location" % NETWORK_NS)        
        if location_id is not None:
            l = filter(lambda x: x.id == location_id, self.list_locations())
                        
        return OpsourceNetwork(id=element.findtext("{%s}id" % NETWORK_NS),
                               name=element.findtext("{%s}name" % NETWORK_NS),
                               description=element.findtext("{%s}description" % NETWORK_NS),
                               location=l,
                               privateNet=element.findtext("{%s}privateNet" % NETWORK_NS),
                               multicast=multicast)
    
    def _to_locations(self, object):
        node_elements = object.findall("{%s}datacenter" % DATACENTER_NS)
        return [ self._to_location(el) for el in node_elements ]
    
    def _to_location(self, element):
        l = NodeLocation(id=element.findtext("{%s}location" % DATACENTER_NS),
                         name=element.findtext("{%s}displayName" % DATACENTER_NS),
                         country=element.findtext("{%s}country" % DATACENTER_NS),
                         driver=self)
        return l
    
    def _to_nodes(self, object):
        node_elements = object.findall("{%s}DeployedServer" % SERVER_NS)
        return [ self._to_node(el) for el in node_elements ]
    
    def _to_node(self, element):
        if element.findtext("{%s}isStarted") == 'true':
             state = NodeState.RUNNING
        else:
            state = NodeState.TERMINATED
        
        extra = {
            'description': element.findtext("{%s}description" % SERVER_NS),
            'sourceImageId': element.findtext("{%s}sourceImageId" % SERVER_NS),
            'networkId': element.findtext("{%s}networkId" % SERVER_NS),
            'networkId': element.findtext("{%s}networkId" % SERVER_NS),
            'machineName': element.findtext("{%s}machineName" % SERVER_NS),
            'deployedTime': element.findtext("{%s}deployedTime" % SERVER_NS),
            'cpuCount': element.findtext("{%s}machineSpecification/{%s}cpuCount" % (SERVER_NS, SERVER_NS)),
            'memoryMb': element.findtext("{%s}machineSpecification/{%s}memoryMb" % (SERVER_NS, SERVER_NS)),
            'osStorageGb': element.findtext("{%s}machineSpecification/{%s}osStorageGb" % (SERVER_NS, SERVER_NS)),
            'additionalLocalStorageGb': element.findtext("{%s}machineSpecification/{%s}additionalLocalStorageGb" % (SERVER_NS, SERVER_NS)),
            'OS_type': element.findtext("{%s}machineSpecification/{%s}operatingSystem/{%s}type" % (SERVER_NS, SERVER_NS, SERVER_NS) ),
            'OS_displayName': element.findtext("{%s}machineSpecification/{%s}operatingSystem/{%s}displayName" % (SERVER_NS, SERVER_NS, SERVER_NS) ),
        }
        
        n = Node(id=element.findtext("{%s}id" % SERVER_NS),
                 name=element.findtext("{%s}name" % SERVER_NS),
                 state=state,
                 public_ip="unknown",
                 private_ip=element.findtext("{%s}privateIpAddress" % SERVER_NS),
                 driver=self.connection.driver,
                 extra=extra)
        return n
    
    def _to_sizes(self, object):
        if object.tag == 'flavor':
            return [ self._to_size(object) ]
        elements = object.findall('flavor')
        return [ self._to_size(el) for el in elements ]
    
    def _to_size(self, element):
        s = NodeSize(id=int(element.findtext('id')),
                     name=str(element.findtext('name')),
                     ram=int(element.findtext('ram')),
                     disk=None, # XXX: needs hardcode
                     bandwidth=None, # XXX: needs hardcode
                     price=float(element.findtext('price'))/(100*24*30),
                     driver=self.connection.driver)
        return s
    
    def _to_images(self, object):
        if object.tag == 'image':
            return [ self._to_image(object) ]
        elements = object.findall('image')
        return [ self._to_image(el) for el in elements ]
    
    def _to_image(self, element):
        i = NodeImage(id=int(element.findtext('id')),
                     name=str(element.findtext('name')),
                     driver=self.connection.driver)
        return i
