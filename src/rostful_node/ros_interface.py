from __future__ import absolute_import

import roslib
import rospy
from rospy.service import ServiceManager
import rosservice, rostopic
import actionlib_msgs.msg
import string

from importlib import import_module
from collections import deque

import json
import sys
import re
from StringIO import StringIO

from rosinterface import message_conversion as msgconv
from rosinterface import definitions, util
from rosinterface.util import ROS_MSG_MIMETYPE, request_wants_ros, get_query_bool

import os
import urlparse
import ast

from rosinterface import ActionBack
from rosinterface import ServiceBack
from rosinterface import TopicBack

from .ros_watcher import ROSWatcher

import unicodedata

CONFIG_PATH = '_rosdef'
SRV_PATH = '_srv'
MSG_PATH = '_msg'
ACTION_PATH = '_action'

def get_suffix(path):
    suffixes = '|'.join([re.escape(s) for s in [CONFIG_PATH,SRV_PATH,MSG_PATH,ACTION_PATH]])
    match = re.search(r'/(%s)$' % suffixes, path)
    return match.group(1) if match else ''

def response(start_response, status, data, content_type):
    content_length = 0
    if data is not None:
        content_length = len(data)
    headers = [('Content-Type', content_type), ('Content-Length', str(content_length))]
    start_response(status, headers)
    return data
#TODO clean this
def response_200(start_response, data='', content_type='application/json'):
    return response(start_response, '200 OK', data, content_type)

def response_404(start_response, data='Invalid URL!', content_type='text/plain'):
    return response(start_response, '404 Not Found', data, content_type)

def response_405(start_response, data=[], content_type='text/plain'):
    return response(start_response, '405 Method Not Allowed', data, content_type)

def response_500(start_response, error, content_type='text/plain'):
    e_str = '%s: %s' % (str(type(error)), str(error))
    return response(start_response, '500 Internal Server Error', e_str, content_type)

class ActionNotExposed(Exception):
    def __init__(self, action_name):
        self.action_name = action_name
    pass


"""
Interface with ROS.
"""
class RosInterface(object):
    # dict of allowed matching characters, and their corresponding replacement regex
    # strings.
    REGEX_CHARS = {'*' : '.*'}
    
    def __init__(self):
        #current services topics and actions exposed
        self.services = {}
        self.topics = {}
        self.actions = {}
        #current services topics and actions we are waiting for
        self.services_waiting = []
        self.topics_waiting = []
        self.actions_waiting = []
        #last requested services topics and actions to be exposed
        self.services_args = []
        self.topics_args = []
        self.actions_args = []
        #services, topics or actions which contain one or more match characters
        #as defined by REGEX_CHARS
        self.services_match = []
        self.topics_match = []
        self.actions_match = []
        #current topics waiting for deletion ( still contain messages )
        self.topics_waiting_del = {}

        all_topics = ast.literal_eval(rospy.get_param('~topics', "[]"))
        for topic in all_topics:
            if any(char in topic for char in self.REGEX_CHARS):
                self.topics_match.append(topic)
            
        all_services = ast.literal_eval(rospy.get_param('~services', "[]"))
        for service in all_services:
            if any(char in service for char in self.REGEX_CHARS):
                self.services_match.append(service)
            
        all_actions = ast.literal_eval(rospy.get_param('~actions', "[]"))
        for action in all_actions:
            if any(char in action for char in self.REGEX_CHARS):
                self.actions_match.append(action)

        #self.expose_topics(topics_args)
        #self.expose_services(services_args)
        #self.expose_actions(actions_args)

        self.ros_watcher = ROSWatcher(self.topics_change_cb, self.services_change_cb, self.actions_change_cb)
        self.ros_watcher.start()

    def regexify_match_string(self, match):
        new_match = match
        # replace all occurrences of each possible match char in the string with
        # the corresponding replacement regex character
        for key in self.REGEX_CHARS:
            new_match = string.replace(new_match, key, self.REGEX_CHARS[key])
        return new_match

        
    ##
    # @param key The topic, action or service name to check against the strings
    # that we have in the list of matchable candidates
    # @param match_candidates list of match candidates that we should try to match against
    def is_regex_match(self, key, match_candidates):
        for cand in match_candidates:
            pattern = re.compile(self.regexify_match_string(cand))
            if pattern.match(key):
                return True
        return False
        
    ##
    # This callback is called when dynamic_reconfigure gets an update on
    # parameter information. Topics which are received through here will be
    # added to the list of topics which are monitored and added to or removed
    # from the view on the REST interface
    def reconfigure(self, config, level):
        rospy.logwarn("""ROSInterface Reconfigure Request: \ntopics : {topics} \nservices : {services} \nactions : {actions}""".format(**config))
        # In here, we receive data from the dynamic_reconfigure which uses the
        # raw information from the parameters on the rosparam server, so the
        # regex-containing items that we filtered in the initialisation will be
        # added here if we do not make sure that they are excluded. However,
        # there might be new topics with match characters that are added by this
        # function as well, coming from the dynamic reconfiguration, so we need
        # to consider those as well.
        # Ugh, repetition...
        new_topics = []
        for topic in ast.literal_eval(config["topics"]):
            # add any topic with regex chars in it to the list of match topics (so long as it isn't already there)
            if any(char in topic for char in self.REGEX_CHARS) and not topic in self.topics_match:
                self.topics_match.append(topic)
                continue
            # put any topic not in the match topic list into the new topic list
            if topic not in self.topics_match:
                new_topics.append(topic)
        
        self.expose_topics(new_topics)

        new_services = []
        for service in ast.literal_eval(config["services"]):
            # add any service with regex chars in it to the list of match services (so long as it isn't already there)
            if any(char in service for char in self.REGEX_CHARS) and not service in self.services_match:
                self.services_match.append(service)
                continue
                # put any service not in the match service list into the new service list 
            if service not in self.services_match:
                new_services.append(service)
                
        self.expose_services(new_services)

        new_actions = []
        for action in ast.literal_eval(config["actions"]):
            # add any action with regex chars in it to the list of action topics (so long as it isn't already there)
            if any(char in action for char in self.REGEX_CHARS) and not action in self.actions_match:
                self.actions_match.append(action)
                continue
            # put any action not in the match action list into the new action list
            if action not in self.actions_match:
                new_actions.append(action)

        self.expose_actions(new_actions)

        return config

    def add_service(self, service_name, ws_name=None, service_type=None):
        resolved_service_name = rospy.resolve_name(service_name)
        if service_type is None:
            try:
                service_type = rosservice.get_service_type(resolved_service_name)
                if not service_type:
                    rospy.logwarn('Cannot Expose unknown service %s' % service_name)
                    self.services_waiting.append(service_name)
                    return False
            except rosservice.ROSServiceIOException, e:
                rospy.logwarn('Error trying to Expose service {name} : {error}'.format(name=service_name, error=e))
                self.services_waiting.append(service_name)
                return False

        if ws_name is None:
            ws_name = service_name
        if ws_name.startswith('/'):
            ws_name = ws_name[1:]

        self.services[ws_name] = ServiceBack(service_name, service_type)
        return True
 
    def del_service(self, service_name, ws_name=None):
        print("deleting service", service_name)
        if ws_name is None:
            ws_name = service_name
        if ws_name.startswith('/'):
            ws_name = ws_name[1:]

        if not self.services.pop(ws_name, None):
            self.services_waiting.remove(service_name)
        return True

    """
    This exposes a list of services as REST API. services not listed here will be removed from the API
    """
    def expose_services(self, service_names):
        rospy.loginfo('Exposing services : %r', service_names)
        if not service_names:
            return
        for service_name in service_names:
            if not service_name in self.services_args:
                ret = self.add_service(service_name)
                #if ret: rospy.loginfo( 'Exposed Service %s', service_name )

        for service_name in self.services_args:
            if not service_name in service_names:
                ret = self.del_service(service_name)
                #if ret: rospy.loginfo ( 'Removed Service %s', service_name )

        #Updating the list of services
        self.services_args = service_names

    def get_service(self, service_name, ws_name=None):
        if ws_name is None:
            ws_name = service_name
        if ws_name.startswith('/'):
            ws_name = ws_name[1:]

        if ws_name in self.services.keys():
            service = self.services[ws_name]
            return service
        else:
            return None  # service not exposed


    def add_topic(self, topic_name, ws_name=None, topic_type=None, allow_pub=True, allow_sub=True):
        resolved_topic_name = rospy.resolve_name(topic_name)
        if topic_type is None:
            try:
                topic_type, _, _ = rostopic.get_topic_type(resolved_topic_name)
                if not topic_type:
                    rospy.logwarn('Cannot Expose unknown topic %s' % topic_name)
                    self.topics_waiting.append(topic_name)
                    return False
            except rosservice.ROSServiceIOException, e:
                rospy.logwarn('Error trying to Expose topic {name} : {error}'.format(name=topic_name, error=e))
                self.topics_waiting.append(topic_name)
                return False

        if ws_name is None:
            ws_name = topic_name
        if ws_name.startswith('/'):
            ws_name = ws_name[1:]

        if ws_name in self.topics_waiting_del.keys() > 0:
            # here the intent is obviously to erase the old homonym topic data
            self.topics_waiting_del.pop(ws_name, None)

        self.topics[ws_name] = TopicBack(topic_name, topic_type, allow_pub=allow_pub, allow_sub=allow_sub)
        return True

    def del_topic(self, topic_name, ws_name=None, noloss=False):
        if ws_name is None:
            ws_name = topic_name
        if ws_name.startswith('/'):
            ws_name = ws_name[1:]

        if ws_name in self.topics:
            if noloss and self.topics.get(ws_name).unread() > 0:
                # we want to delete it later, after last message has been consumed
                # we make a copy of the topic to still be able to access it
                self.topics_waiting_del[ws_name] = (self.topics.get(ws_name))

            t = self.topics.pop(ws_name, None)
            if t:
                self.topics_waiting.append(topic_name)
            else:
                self.topics_waiting.remove(topic_name)

        elif not noloss and ws_name in self.topics_waiting_del:  # in this case we want to actually remove it completely
            self.topics_waiting_del.pop(ws_name, None)

        return True

    """
    This exposes a list of topics as REST API. topics not listed here will be removed from the API
    """
    def expose_topics(self, topic_names, allow_pub=True, allow_sub=True):
        rospy.loginfo('Exposing topics : %r', topic_names)
        if not topic_names:
            return
        # Adding missing ones
        for topic_name in topic_names:
            if not topic_name in self.topics_args:
                ret = self.add_topic(topic_name, allow_pub=allow_pub, allow_sub=allow_sub)
                # if ret: rospy.loginfo('Exposed Topic %s Pub %r Sub %r', topic_name, allow_pub, allow_sub)

        # Removing extra ones
        for topic_name in self.topics_args:
            if not topic_name in topic_names:
                ret = self.del_topic(topic_name)
                # if ret: rospy.loginfo('Removed Topic %s', topic_name)

        # Updating the list of topics
        self.topics_args = topic_names

    def get_topic(self, topic_name, ws_name=None):
        #normalizing names... ( somewhere else ?)
        if isinstance(topic_name, unicode):
            topic_name = unicodedata.normalize('NFKD', topic_name).encode('ascii', 'ignore')
        if isinstance(ws_name, unicode):
            ws_name = unicodedata.normalize('NFKD', ws_name).encode('ascii', 'ignore')

        #topic is raw str from here
        if ws_name is None:
            ws_name = topic_name
        if ws_name.startswith('/'):
            ws_name = ws_name[1:]

        #hiding topics waiting for deletion with no messages waiting to be read.
        if ws_name in self.topics.keys():
            topic = self.topics[ws_name]
            return topic
        elif ws_name in self.topics_waiting_del.keys() and self.topics_waiting_del[ws_name].unread() > 0:
            topic = self.topics_waiting_del[ws_name]
            return topic
        else:
            return None  # topic not exposed

    def add_action(self, action_name, ws_name=None, action_type=None):
        if action_type is None:
            resolved_topic_name = rospy.resolve_name(action_name + '/result')
            topic_type, _, _ = rostopic.get_topic_type(resolved_topic_name)
            if not topic_type:
                rospy.logwarn( 'Cannot Expose unknown action %s', action_name )
                self.actions_waiting.append(action_name)
                return False
            action_type = topic_type[:-len('ActionResult')]

        if ws_name is None:
            ws_name = action_name
        if ws_name.startswith('/'):
            ws_name = ws_name[1:]

        self.actions[ws_name] = ActionBack(action_name, action_type)
        return True

    def del_action(self, action_name, ws_name=None):
        if ws_name is None:
            ws_name = action_name
        if ws_name.startswith('/'):
            ws_name = ws_name[1:]

        if not self.actions.pop(ws_name,None) :
            self.actions_waiting.remove(action_name)
        return True

    """
    This exposes a list of actions as REST API. actions not listed here will be removed from the API
    """
    def expose_actions(self, action_names):
        #rospy.loginfo('Exposing actions : %r', action_names)
        if not action_names:
            return
        for action_name in action_names:
            if not action_name in self.actions_args:
                ret = self.add_action(action_name)
                #if ret: rospy.loginfo( 'Exposed Action %s', action_name)

        for action_name in self.actions_args:
            if not action_name in action_names:
                ret = self.del_action(action_name)
                #if ret: rospy.loginfo ( 'Removed Action %s', action_name)

        # Updating the list of actions
        self.actions_args = action_names

    def get_action(self, action_name):
        if action_name in self.actions:
            action = self.actions[action_name]
            return action
        else:
            raise ActionNotExposed(action_name)
            
    ##
    # This callback is called when the ros_watcher receives information about
    # new topics, or topics which dropped off the ros network.
    def topics_change_cb(self, new_topics, lost_topics):
        rospy.logwarn('new topics : %r, lost topics : %r', new_topics, lost_topics)
        topics_lst = [t for t in new_topics if t in self.topics_waiting or self.is_regex_match(t, self.topics_match)]
        print("topics lst ", topics_lst)
        if len(topics_lst) > 0:
            # rospy.logwarn('exposing new topics : %r', topics_lst)
            # Adding missing ones
            for topic_name in topics_lst:
                ret = self.add_topic(topic_name)

        topics_lst = [t for t in lost_topics if t in self.topics_args]
        if len(topics_lst) > 0:
            # rospy.logwarn('hiding lost topics : %r', topics_lst)
            # Removing extra ones
            for topic_name in topics_lst:
                ret = self.del_topic(topic_name)

        #taking the opportunity to try cleaning the topics that have been emptied
        # TODO : think about a clean way to link that to the topic.get() method
        if len(self.topics_waiting_del) > 0:
            cleanup = []
            for ws_name in self.topics_waiting_del.keys():
                if 0 == self.topics_waiting_del.get(ws_name).unread():  # FIXME : careful about the difference between topic_name and ws_name
                    cleanup.append(ws_name)
            for ws_name in cleanup:
                self.topics_waiting_del.pop(ws_name, None)
                #TODO : cleaner way by calling self.del_topic ?

    def services_change_cb(self, new_services, lost_services):
        # rospy.logwarn('new services : %r, lost services : %r', new_services, lost_services)
        svc_list = [s for s in new_services if s in self.services_waiting or self.is_regex_match(s, self.services_match)]
        if len(svc_list) > 0:
            # rospy.logwarn('exposing new services : %r', svc_list)
            for svc_name in svc_list:
                self.add_service(svc_name)

        svc_list = [s for s in lost_services if s in self.services_args]
        if len(svc_list) > 0:
            # rospy.logwarn('hiding lost services : %r', svc_list)
            for svc_name in svc_list:
                self.del_service(svc_name)

    def actions_change_cb(self, new_actions, lost_actions):
        # rospy.logwarn('new actions : %r, lost actions : %r', new_actions, lost_actions)
        act_list = [a for a in new_actions if a in self.actions_waiting or self.is_regex_match(a, self.actions_match)]
        if len(act_list) > 0:
            # rospy.logwarn('exposing new actions : %r', act_list)
            for act_name in act_list:
                self.add_action(act_name)

        act_list = [a for a in lost_actions if a in self.actions_args]
        if len(act_list) > 0:
            # rospy.logwarn('hiding lost actions : %r', act_list)
            for act_name in act_list:
                self.del_action(act_name)

