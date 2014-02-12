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

from zope.intid.interfaces import IIntIdRemovedEvent, IIntIdAddedEvent

from nti.contentsearch import discriminators
from nti.contentsearch.constants import acl_

from nti.dataserver import interfaces as nti_interfaces
from nti.dataserver.contenttypes.forums import interfaces as forums_interfaces

from . import is_indexable
from . import search_queue
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
			logger.exception("Error adding object to queue")
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
				logger.exception("Error adding object to queue for update")
		else:
			logger.debug("Could not find iid for %r", obj)
	return False

def queue_remove(obj):
	if is_indexable(obj):
		iid = discriminators.query_uid(obj)
		if iid is not None:
			__traceback_info__ = iid
			search_queue().remove(iid)

@component.adapter(nti_interfaces.IModeledContent, IIntIdRemovedEvent)
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

@component.adapter(forums_interfaces.IGeneralForum, IIntIdRemovedEvent)
def _forum_removed(forum, event):
	queue_remove(forum)

@component.adapter(forums_interfaces.IGeneralForum, IIntIdAddedEvent)
def _forum_added(forum, event):
	queue_added(forum)

@component.adapter(forums_interfaces.IGeneralForum, lce_interfaces.IObjectModifiedEvent)
def _forum_modified(forum, event):
	queue_modified(forum)

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

	transaction.get().addAfterCommitHook(
					lambda success: success and gevent.spawn(_process_event))
