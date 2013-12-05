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

from zope.intid.interfaces import IIntIdRemovedEvent, IIntIdAddedEvent

from nti.contentsearch import discriminators
from nti.contentsearch import interfaces as search_interfaces

from nti.dataserver import interfaces as nti_interfaces

from . import search_queue

def is_indexable(obj):
	return component.queryAdapter(obj, search_interfaces.IContentResolver) is not None

def queue_added(obj):
	if is_indexable(obj):
		iid = discriminators.query_uid(obj)
		if iid is not None:
			search_queue().add(iid)

def queue_modified(obj):
	if is_indexable(obj):
		iid = discriminators.query_uid(obj)
		if iid is not None:
			search_queue().update(iid)
		else:
			logger.debug("Could not find iid for %r", obj)

def queue_remove(obj):
	if is_indexable(obj):
		iid = discriminators.query_uid(obj)
		if iid is not None:
			search_queue().remove(iid)

@component.adapter(nti_interfaces.INote, IIntIdRemovedEvent)
def _modeled_removed(modeled, event):
	queue_remove(modeled)

@component.adapter(nti_interfaces.IModeledContent, IIntIdAddedEvent)
def _modeled_added(modeled, event):
	queue_added(modeled)

@component.adapter(nti_interfaces.IModeledContent, lce_interfaces.IObjectModifiedEvent)
def _modeled_modified(modeled, event):
	if nti_interfaces.IDeletedObjectPlaceholder.providedBy(modeled):
		queue_remove(modeled)
	else:
		queue_modified(modeled)

