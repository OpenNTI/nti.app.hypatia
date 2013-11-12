#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
graphdb modeled content related functionality

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

from nti.dataserver import interfaces as nti_interfaces

from nti.externalization import externalization

from nti.ntiids import ntiids

from . import get_graph_db
from . import relationships
from . import interfaces as graph_interfaces

def to_external_ntiid_oid(obj):
	return externalization.to_external_ntiid_oid(obj)

def _get_inReplyTo_PK(obj):
	key, value = (None, None)
	if obj is not None:
		author = obj.creator
		rel_type = relationships.Reply()
		adapted = component.queryMultiAdapter((author, obj, rel_type),
											  graph_interfaces.IUniqueAttributeAdapter)
		key = adapted.key if adapted is not None else None
		value = adapted.value if adapted is not None else None
	return key, value

# note removed

def remove_modeled(db, key, value):
	node = db.get_indexed_node(key, value)
	if node is not None:
		db.delete_node(node)
		logger.debug("Node %s,%s deleted" % (key, value))
		return True
	return False

def remove_note(db, key, value, inReplyTo_key=None, inReplyTo_value=None):
	if inReplyTo_key and inReplyTo_value:
		db.delete_indexed_relationship(inReplyTo_key, inReplyTo_value)
	remove_modeled(db, key, value)

def _proces_note_removed(note, event):
	db = get_graph_db()
	irt_key, irt_value = _get_inReplyTo_PK(note)
	adapted = graph_interfaces.IUniqueAttributeAdapter(note)
	func = functools.partial(remove_note, db=db, key=adapted.key, value=adapted.value,
							 inReplyTo_key=irt_key, inReplyTo_value=irt_value)
	transaction.get().addAfterCommitHook(lambda success: success and gevent.spawn(func))

@component.adapter(nti_interfaces.INote, lce_interfaces.IObjectRemovedEvent)
def _note_removed(note, event):
	_proces_note_removed(note, event)

# note added

def add_inReplyTo_relationship(db, oid):
	note = ntiids.find_object_with_ntiid(oid)
	in_replyTo = note.inReplyTo if note is not None else None
	if in_replyTo:
		author = note.creator
		rel_type = relationships.Reply()
		# get the key/value to id the inReplyTo relationship
		key, value = _get_inReplyTo_PK(note)
		# create a relationship between author and the node being replied to
		adapted = component.getMultiAdapter((author, note, rel_type),
											graph_interfaces.IPropertyAdapter)
		result = db.create_relationship(author, in_replyTo, rel_type,
										properties=adapted.properties(),
										key=key, value=value)
	if result is not None:
		logger.debug("replyTo relationship %s retrived/created" % result)

def _process_note_inReplyTo(note):
	db = get_graph_db()
	oid = to_external_ntiid_oid(note)
	def _process_event():
		transaction_runner = \
			component.getUtility(nti_interfaces.IDataserverTransactionRunner)
		func = functools.partial(add_inReplyTo_relationship, db=db, oid=oid)
		transaction_runner(func)
	transaction.get().addAfterCommitHook(
							lambda success: success and gevent.spawn(_process_event))

@component.adapter(nti_interfaces.INote, lce_interfaces.IObjectAddedEvent)
def _note_added(note, event):
	if note.inReplyTo:
		_process_note_inReplyTo(note)

# utils

def install(db):

	from zope.generations.utility import findObjectsProviding

	dataserver = component.getUtility(nti_interfaces.IDataserver)
	_users = nti_interfaces.IShardLayout(dataserver).users_folder
	rel_type = relationships.Reply()

	result = 0
	for user in _users.itervalues():
		if not nti_interfaces.IUser.providedBy(user):
			continue
		
		objs = [user]
		for note in findObjectsProviding(user, nti_interfaces.INote):
			in_replyTo = note.inReplyTo
			if in_replyTo:
				objs.append(in_replyTo)

		# create nodes in batch
		nodes = db.create_nodes(*objs)
		assert len(nodes) == len(objs)

		rels = []
		for i, n in enumerate(nodes[1:]):
			note = objs[i + 1]
			properties = component.getMultiAdapter((user, note, rel_type),
													graph_interfaces.IPropertyAdapter)
			key, value = _get_inReplyTo_PK(note)
			rels.append((nodes[0], rel_type, n, properties, key, value))

		# create relationships in batch
		rels = db.create_relationships(*rels)
		result += len(rels)

	return result
