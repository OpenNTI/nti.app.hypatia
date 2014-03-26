#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
subscribers functionality

$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import gevent
import functools
import transaction

from zope import component
from zope.lifecycleevent import interfaces as lce_interfaces

from pyramid.events import ApplicationCreated

from nti.contentsearch import discriminators
from nti.contentsearch.constants import acl_

from nti.dataserver.users import Entity
from nti.dataserver import interfaces as nti_interfaces

from . import is_indexable
from . import search_queue
from . import process_queue
from . import search_catalog

def add_2_queue(obj):
	iid = discriminators.query_uid(obj)
	if iid is not None:
		__traceback_info__ = iid
		search_queue().add(iid)
		return True
	return False

def queue_added(obj):
	if is_indexable(obj):
		try:
			return add_2_queue(obj)
		except TypeError:
			pass
	return False

def queue_modified(obj):
	if is_indexable(obj):
		iid = discriminators.query_uid(obj)
		if iid is not None:
			__traceback_info__ = iid
			try:
				search_queue().update(iid)
				return True
			except TypeError:
				pass
	return False

def queue_remove(obj):
	if is_indexable(obj):
		iid = discriminators.query_uid(obj)
		if iid is not None:
			__traceback_info__ = iid
			search_queue().remove(iid)

# IIntIdRemovedEvent
def _object_removed(modeled, event):
	queue_remove(modeled)

# IIntIdAddedEvent
def _object_added(modeled, event):
	queue_added(modeled)

# IObjectModifiedEvent
def _object_modified(modeled, event):
	if nti_interfaces.IDeletedObjectPlaceholder.providedBy(modeled):
		queue_remove(modeled)
	else:
		queue_modified(modeled)

def delete_userdata(username):
	catalog = search_catalog()
	kwIndex = catalog[acl_]
	docids = kwIndex.remove_word(username)
	for docid in docids or ():
		search_queue().remove(docid)
	return len(docids)

@component.adapter(nti_interfaces.IUser, lce_interfaces.IObjectRemovedEvent)
def _user_deleted(user, event):
	username = user.username
	def _process_event():
		transaction_runner = \
			component.getUtility(nti_interfaces.IDataserverTransactionRunner)
		func = functools.partial(delete_userdata, username=username)
		transaction_runner(func)
		return True

	transaction.get().addAfterCommitHook(
					lambda success: success and gevent.spawn(_process_event))

# on change listener

_changeType_events = (nti_interfaces.SC_CREATED,
					  nti_interfaces.SC_SHARED,
					  nti_interfaces.SC_MODIFIED)

def onChange(datasvr, change, target=None, broadcast=None, **kwargs):
	changeType, changeObject = change.type, change.object
	entity = Entity.get_entity(str(target)) \
			 if not nti_interfaces.IEntity.providedBy(target) else target

	should_process = nti_interfaces.IUser.providedBy(entity)
	if should_process:
		if not broadcast and changeType in _changeType_events:
			should_process = changeObject.isSharedDirectlyWith(entity)

	if should_process:
		if changeType != nti_interfaces.SC_DELETED:
			queue_added(changeObject)

	return should_process

@component.adapter(ApplicationCreated)
def _set_change_listener(database_event):
	dataserver = component.queryUtility(nti_interfaces.IDataserver)
	if dataserver is not None:
		dataserver.add_change_listener(onChange)

# test mode

from pyramid.interfaces import INewRequest

@component.adapter(INewRequest)
def requestIndexation(event):
	def _process_event():
		transaction_runner = \
			component.getUtility(nti_interfaces.IDataserverTransactionRunner)
		func = functools.partial(process_queue, limit=-1)
		transaction_runner(func)
		return True

	transaction.get().addAfterCommitHook(
					lambda success: success and gevent.spawn(_process_event))
