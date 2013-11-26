#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
subscribers functionality

$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from zope import component
from zope.lifecycleevent import interfaces as lce_interfaces

from zope.intid.interfaces import IIntIdRemovedEvent

from nti.contentsearch import discriminators

from nti.dataserver import interfaces as nti_interfaces
from nti.dataserver.contenttypes.forums import interfaces as frm_interfaces

from . import queue

def queue_added(obj):
	iid = discriminators.get_uid(obj)
	queue().add(iid)

def queue_modified(obj):
	iid = discriminators.get_uid(obj)
	queue().update(iid)

def queue_remove(obj):
	iid = discriminators.get_uid(obj)
	queue().remove(iid)

@component.adapter(nti_interfaces.INote, IIntIdRemovedEvent)
def _modeled_removed(modeled, event):
	queue_remove(modeled)

@component.adapter(nti_interfaces.IModeledContent, lce_interfaces.IObjectAddedEvent)
def _modeled_added(modeled, event):
	queue_added(modeled)

@component.adapter(nti_interfaces.IModeledContent, lce_interfaces.IObjectModifiedEvent)
def _modeled_modified(modeled, event):
	queue_modified(modeled)

@component.adapter(frm_interfaces.ITopic, lce_interfaces.IObjectAddedEvent)
def _topic_added(topic, event):
	queue_added(topic)

@component.adapter(frm_interfaces.ITopic, lce_interfaces.IObjectModifiedEvent)
def _topic_modified(topic, event):
	queue_modified(topic)

@component.adapter(frm_interfaces.ITopic, IIntIdRemovedEvent)
def _topic_removed(topic, event):
	queue_remove(topic)
