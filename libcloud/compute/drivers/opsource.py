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
import base64
import socket
from xml.etree import ElementTree as ET
from xml.parsers.expat import ExpatError

from libcloud.common.base import ConnectionUserAndKey, Response
from libcloud.common.types import InvalidCredsError, MalformedResponseError
from libcloud.compute.types import NodeState, Provider
from libcloud.compute.base import NodeDriver, Node, NodeAuthPassword
from libcloud.compute.base import NodeSize, NodeImage, NodeLocation

# Roadmap / TODO:
#
# 0.1 - Basic functionality:  create, delete, start, stop, reboot - servers
#                             (base OS images only, no customer images suported yet)
#   x implement list_nodes()
#   x implement create_node()  (only support Base OS images, no customer images yet)
#   x implement reboot()
#   x implement destroy_node()
#   - implement list_sizes()
#   x implement list_images()   (only support Base OS images, no customer images yet)
#   x implement list_locations()
#	- implement ex_* extension functions for opsource-specific features
#       x ex_graceful_shutdown
#       x ex_start_node
#       x ex_power_off
#       x ex_list_networks (needed for create_node())
#   x refactor:  switch to using fixxpath() from the vcloud driver for dealing with xml namespace tags
#   x refactor:  move some functionality from OpsourceConnection.request() method into new .request_with_orgId() method
#   - add OpsourceStatus object support to:
#       x _to_node()
#       x _to_network()
#
# 0.2 - Support customer images (snapshots) and server modification functions
#   - support customer-created images:
#       - list deployed customer images  (in list_images() ?)
#       - list pending customer images  (in list_images() ?)
#       - delete customer images
#       - modify customer images
#   - add "pending-servers" in list_nodes()
#	- implement various ex_* extension functions for opsource-specific features
#       - ex_modify_server()
#       - ex_add_storage_to_server()
#       - ex_snapshot_server()  (create's customer image)
# 
# 0.3 - support Network API
# 0.4 - Support VIP/Load-balancing API
# 0.5 - support Files Account API
# 0.6 - support Reports API
# 1.0 - Opsource 0.9 API feature complete, tested

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

def fixxpath(root, xpath):
    """ElementTree wants namespaces in its xpaths, so here we add them."""
    namespace, root_tag = root.tag[1:].split("}", 1)
    fixed_xpath = "/".join(["{%s}%s" % (namespace, e)
                            for e in xpath.split("/")])
    return fixed_xpath

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
            	code = body.findtext(fixxpath(body, "resultCode"))
            	message = body.findtext(fixxpath(body, "resultDetail"))
                return OpsourceAPIException(code, message)
        except:
            return self.body

class OpsourceAPIException(Exception):
    def __init__(self, code, msg):
        self.code = code
        self.msg = msg
        
    def __str__(self):
        return "%s: %s" % (self.code, self.msg)
        
    def __repr__(self):
        return "<OpsourceAPIException: code='%s', msg='%s'>" % (self.code, self.msg)

class OpsourceConnection(ConnectionUserAndKey):
    """
    Connection class for the Opsource driver
    """
    
    host = 'api.opsourcecloud.net'
    api_path = '/oec'
    api_version = '0.9'
    _orgId = None
    responseCls = OpsourceResponse
    
    def add_default_headers(self, headers):
        headers['Authorization'] = ('Basic %s'
                              % (base64.b64encode('%s:%s' % (self.user_id, self.key))))
        return headers
    
    def request(self, action, params=None, data='', headers=None, method='GET'):
        action = "%s/%s/%s" % (self.api_path, self.api_version, action)
        
        return super(OpsourceConnection, self).request(
            action=action,
            params=params, data=data,
            method=method, headers=headers
        )
        
    def request_with_orgId(self, action, params=None, data='', headers=None, method='GET'):
        action = "%s/%s" % (self.get_resource_path(), action)
        
        return super(OpsourceConnection, self).request(
            action=action,
            params=params, data=data,
            method=method, headers=headers
        )

    def get_resource_path(self):
        """this method returns a resource path which is necessary for referencing
           resources that require a full path instead of just an ID, such as
           networks, and customer snapshots.
        """
        return ("%s/%s/%s" % (self.api_path, self.api_version, self._get_orgId()))
        
    def _get_orgId(self):
        """
        send the /myaccount API request to opsource cloud and parse the 'orgId' from the
        XML response object.  We need the orgId to use most of the other API functions
        """
        if self._orgId == None:
            body = self.request('/myaccount').object
            self._orgId = body.findtext(fixxpath(body, "orgId"))
        return self._orgId

class OpsourceStatus(object):
    """
    Opsource API pending operation status class
        action, requestTime, username, numberOfSteps, updateTime, step.name, step.number,
        step.percentComplete, failureReason, 
    """
    def __init__(self, action=None, requestTime=None, userName=None, numberOfSteps=None, updateTime=None,
                step_name=None, step_number=None, step_percentComplete=None, failureReason=None):
        self.action = action
        self.requestTime = requestTime
        self.userName = userName
        self.numberOfSteps = numberOfSteps
        self.updateTime = updateTime
        self.step_name = step_name
        self.step_number = step_number
        self.step_percentComplete = step_percentComplete
        self.failureReason = failureReason
        
    def __repr__(self):
        return (('<OpsourceStatus: action=%s, requestTime=%s, userName=%s, numberOfSteps=%s, updateTime=%s, ' \
                  'step_name=%s, step_number=%s, step_percentComplete=%s, failureReason=%s')
                  % (self.action, self.requestTime, self.userName, self.numberOfSteps, self.updateTime,
                    self.step_name, self.step_number, self.step_percentComplete, self.failureReason))

class OpsourceNetwork(object):
    """
    Opsource network with location
    """
    
    def __init__(self, id, name, description, location, privateNet, multicast, status):
        self.id = str(id)
        self.name = name
        self.description = description
        self.location = location
        self.privateNet = privateNet
        self.multicast = multicast
        self.status = status

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
    
    features = {"create_node": ["password"]}
    
    def list_nodes(self):
        nodes = self._to_nodes(self.connection.request_with_orgId('/server/deployed').object)
        nodes.extend(self._to_nodes(self.connection.request_with_orgId('/server/pendingDeploy').object))
        return nodes
    
    # def list_sizes(self, location=None):
    #     pass
    
    def list_images(self, location=None):
        """return a list of available images
            Currently only returns the default 'base OS images' provided by opsource.
            Customer images (snapshots) are not yet supported.
        """
        return self._to_base_images(self.connection.request('/base/image').object)
    
    def list_locations(self):
        """list locations (datacenters) available for instantiating servers and
            networks.  
        """
        return self._to_locations(self.connection.request_with_orgId('/datacenter').object)
    
    def create_node(self, **kwargs):
        """Create a new opsource node

        Standard keyword arguments from L{NodeDriver.create_node}:
        @keyword    name:   String with a name for this new node (required)
        @type       name:   str

        @keyword    image:  OS Image to boot on node. (required)
        @type       image:  L{NodeImage}

        @keyword    auth:   Initial authentication information for the node (required)
        @type       auth:   L{NodeAuthPassword}
        
        Non-standard keyword arguments:
        @keyword    ex_description:  description for this node (required)
        @type       ex_description:  C{str}
        
        @keyword    ex_network:  Network to create the node within (required)
        @type       ex_network: L{OpsourceNetwork}
        
        @keyword    ex_isStarted:  Start server after creation? default true (required)
        @type       ex_isStarted:  C{bool}
        
        @return: The newly created L{Node}. NOTE: Opsource does not provide a way to 
                 determine the ID of the server that was just created, so the returned
                 L{Node} is not guaranteed to be the same one that was created.  This
                 is only the case when multiple nodes with the same name exist.
        """
        name = kwargs['name']
        image = kwargs['image']
        # XXX:  Node sizes can be adjusted after a node is created, but cannot be 
        #       set at create time because size is part of the image definition.
        size = NodeSize(id=0,
                     name='',
                     ram=0,
                     disk=None,
                     bandwidth=None,
                     price=0,
                     driver=self.connection.driver)
                
        password = None
        if kwargs.has_key('auth'):
            auth = kwargs['auth']
            if isinstance(auth, NodeAuthPassword):
                password = auth.password
            else:
                raise ValueError('auth must be of NodeAuthPassword type')
        
        ex_description = kwargs['ex_description']
        ex_isStarted = kwargs['ex_isStarted']
        ex_network = kwargs['ex_network']        
        vlanResourcePath = "%s/%s" % (self.connection.get_resource_path(), ex_network.id)

        imageResourcePath = None
        if image.extra.has_key('resourcePath'):
            imageResourcePath = image.extra['resourcePath']
        else:
            imageResourcePath = "%s/%s" % (self.connection.get_resource_path(), image.id)
        
        server_elm = ET.Element('Server', {'xmlns': SERVER_NS})
        ET.SubElement(server_elm, "name").text = name
        ET.SubElement(server_elm, "description").text = ex_description        
        ET.SubElement(server_elm, "vlanResourcePath").text = vlanResourcePath        
        ET.SubElement(server_elm, "imageResourcePath").text = imageResourcePath
        ET.SubElement(server_elm, "administratorPassword").text = password
        ET.SubElement(server_elm, "isStarted").text = str(ex_isStarted)

        data = self.connection.request_with_orgId('/server',
                                                  method='POST',
                                                  data=ET.tostring(server_elm)
                                                  ).object
        # XXX: return the last node in the list that has a matching name.  this
        #      is likely, but not guaranteed, to be the node we just created
        #      because opsource allows multiple nodes to have the same name
        return filter(lambda x: x.name == name, self.list_nodes())[-1]
    
    def reboot_node(self, node):
        """reboots the node"""
        body = self.connection.request_with_orgId('/server/%s?restart' % node.id).object
        result = body.findtext(fixxpath(body, "result"))
        return result == 'SUCCESS'

    def destroy_node(self, node):
        """Destroys the node"""
        body = self.connection.request_with_orgId('/server/%s?delete' % node.id).object
        result = body.findtext(fixxpath(body, "result"))
        return result == 'SUCCESS'
    
    def ex_start_node(self, node):
        """Powers on an existing deployed server"""
        body = self.connection.request_with_orgId('/server/%s?start' % node.id).object
        result = body.findtext(fixxpath(body, "result"))
        return result == 'SUCCESS'
            
    def ex_shutdown_graceful(self, node):
        """This function will attempt to "gracefully" stop a server by initiating a
	    shutdown sequence within the guest operating system. A successful response
	    on this function means the system has successfully passed the
	    request into the operating system.
        """
        body = self.connection.request_with_orgId('/server/%s?shutdown' % node.id).object
        result = body.findtext(fixxpath(body, "result"))
        return result == 'SUCCESS'
        
    def ex_power_off(self, node):
        """This function will abruptly power-off a server.  Unlike ex_shutdown_graceful,
        success ensures the node will stop but some OS and application configurations may
        be adversely affected by the equivalent of pulling the power plug out of the
        machine.
        """
        body = self.connection.request_with_orgId('/server/%s?poweroff' % node.id).object
        result = body.findtext(fixxpath(body, "result"))
        return result == 'SUCCESS'
        
    def ex_list_networks(self):
        """List networks deployed across all data center locations for your
        organization.  The response includes the location of each network.
        
        Returns a list of OpsourceNetwork objects
        """
        return self._to_networks(self.connection.request_with_orgId('/networkWithLocation').object)

    def _to_networks(self, object):
        node_elements = object.findall(fixxpath(object, "network"))
        return [ self._to_network(el) for el in node_elements ]
        
    def _to_network(self, element):
        multicast = False
        if element.findtext(fixxpath(element, "multicast")) == 'true':
            multicast = True

        status = self._to_status(element.find(fixxpath(element, "status")))

        location_id = element.findtext(fixxpath(element, "location"))
        if location_id is not None:
            location = filter(lambda x: x.id == location_id, self.list_locations())
        else:
            location = None
        
        return OpsourceNetwork(id=element.findtext(fixxpath(element, "id")),
                               name=element.findtext(fixxpath(element, "name")),
                               description=element.findtext(fixxpath(element, "description")),
                               location=location,
                               privateNet=element.findtext(fixxpath(element, "privateNet")),
                               multicast=multicast,
                               status=status)
    
    def _to_locations(self, object):
        node_elements = object.findall(fixxpath(object, "datacenter"))
        return [ self._to_location(el) for el in node_elements ]
    
    def _to_location(self, element):
        l = NodeLocation(id=element.findtext(fixxpath(element, "location")),
                         name=element.findtext(fixxpath(element, "displayName")),
                         country=element.findtext(fixxpath(element, "country")),
                         driver=self)
        return l
    
    def _to_nodes(self, object):
        node_elements = object.findall(fixxpath(object, "DeployedServer"))
        node_elements.extend(object.findall(fixxpath(object, "PendingDeployServer")))
        return [ self._to_node(el) for el in node_elements ]
    
    def _to_node(self, element):
        if element.findtext(fixxpath(element, "isStarted")) == 'true':
             state = NodeState.RUNNING
        else:
            state = NodeState.TERMINATED

        status = self._to_status(element.find(fixxpath(element, "status")))
            
        extra = {
            'description': element.findtext(fixxpath(element, "description")),
            'sourceImageId': element.findtext(fixxpath(element,"sourceImageId")),
            'networkId': element.findtext(fixxpath(element, "networkId")),
            'networkId': element.findtext(fixxpath(element, "networkId")),
            'machineName': element.findtext(fixxpath(element, "machineName")),
            'deployedTime': element.findtext(fixxpath(element, "deployedTime")),
            'cpuCount': element.findtext(fixxpath(element, "machineSpecification/cpuCount")),
            'memoryMb': element.findtext(fixxpath(element, "machineSpecification/memoryMb")),
            'osStorageGb': element.findtext(fixxpath(element, "machineSpecification/osStorageGb")),
            'additionalLocalStorageGb': element.findtext(fixxpath(element, "machineSpecification/additionalLocalStorageGb")),
            'OS_type': element.findtext(fixxpath(element, "machineSpecification/operatingSystem/type")),
            'OS_displayName': element.findtext(fixxpath(element, "machineSpecification/operatingSystem/displayName")),
            'status': status,
        }
        
        n = Node(id=element.findtext(fixxpath(element, "id")),
                 name=element.findtext(fixxpath(element, "name")),
                 state=state,
                 public_ip="unknown",
                 private_ip=element.findtext(fixxpath(element, "privateIpAddress")),
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
    
    def _to_base_images(self, object):
        node_elements = object.findall(fixxpath(object, "ServerImage"))
        return [ self._to_base_image(el) for el in node_elements ]
    
    def _to_base_image(self, element):
        ## place ##
        ## probably need multiple _to_image() functions that parse <ServerImage> differently
        ## than <DeployedImage>
        location_id = element.findtext(fixxpath(element, "location"))
        if location_id is not None:
            location = filter(lambda x: x.id == location_id, self.list_locations())
        else:
            location = None
        
        extra = {
            'description': element.findtext(fixxpath(element, "description")),
            'OS_type': element.findtext(fixxpath(element, "operatingSystem/type")),
            'OS_displayName': element.findtext(fixxpath(element, "operatingSystem/displayName")),
            'cpuCount': element.findtext(fixxpath(element, "cpuCount")),
            'resourcePath': element.findtext(fixxpath(element, "resourcePath")),
            'memory': element.findtext(fixxpath(element, "memory")),
            'osStorage': element.findtext(fixxpath(element, "osStorage")),
            'additionalStorage': element.findtext(fixxpath(element, "additionalStorage")),
            'created': element.findtext(fixxpath(element, "created")),
            'location': location,
        }
        
        i = NodeImage(id=str(element.findtext(fixxpath(element, "id"))),
                     name=str(element.findtext(fixxpath(element, "name"))),
                     extra=extra,
                     driver=self.connection.driver)
        return i

    def _to_status(self, element):
        if element == None:
            return OpsourceStatus()
        s = OpsourceStatus(action=element.findtext(fixxpath(element, "action")),
                          requestTime=element.findtext(fixxpath(element, "requestTime")),
                          userName=element.findtext(fixxpath(element, "userName")),
                          numberOfSteps=element.findtext(fixxpath(element, "numberOfSteps")),
                          step_name=element.findtext(fixxpath(element, "step/name")),
                          step_number=element.findtext(fixxpath(element, "step/number")),
                          step_percentComplete=element.findtext(fixxpath(element, "step/percentComplete")),
                          failureReason=element.findtext(fixxpath(element, "failureReason")))
        return s
