#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
graphdb friendship relationship

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

from nti.dataserver import users
from nti.dataserver import interfaces as nti_interfaces

from nti.externalization import externalization

from nti.ntiids import ntiids

from . import get_graph_db
from . import relationships
from . import interfaces as graph_interfaces

class _Relationship(object):

	def __init__(self, _from, _to, **kwargs):
		self._to = _to
		self._from = _from
		self.__dict__.update(kwargs)

	def __eq__(self, other):
		try:
			return self is other or (self._from == other._from
									 and self._to == other._to)
		except AttributeError:
			return NotImplemented

	def __hash__(self):
		xhash = 47
		xhash ^= hash(self._from)
		xhash ^= hash(self._to)
		return xhash

def _get_entity(entity):
	if not nti_interfaces.IEntity.providedBy(entity):
		entity = users.User.get_user(str(entity))
	return entity

def _get_graph_connections(db, entity, rel_type):
	result = set()
	rels = db.match(start=entity, rel_type=rel_type)
	for rel in rels:
		end = db.get_node(rel.end)
		username = end.properties.get('username') if end is not None else u''
		friend = users.User.get_user(username or u'')
		if friend is not None:
			result.add(_Relationship(entity, friend, rel=rel))
	return result

def _update_connections(db, entity, graph_relations_func, db_relations_func, rel_type):
	# computer db/graph relationships
	entity = _get_entity(entity)
	stored_db_relations = db_relations_func(entity)
	current_graph_relations = graph_relations_func(db, entity)
	to_add = stored_db_relations - current_graph_relations
	to_remove = current_graph_relations - stored_db_relations

	# remove old relationships
	if to_remove:
		db.delete_relationships(*[x.rel for x in to_remove])

	# create nodes
	to_create = set()
	for fship in to_add:
		to_create.add(fship._to)
		to_create.add(fship._from)
	if to_create:
		to_create = list(to_create)
		db.create_nodes(*to_create)

	# add new relationships
	result = []
	for fship in to_add:
		_to = fship._to
		_from = fship._from
		rel = db.create_relationship(_from, _to, rel_type)
		result.append(rel)
	return result

# friendship

def graph_friends(db, entity):
	result = _get_graph_connections(db, entity, rel_type=relationships.FriendOf())
	return result

def db_friends(entity):
	result = set()
	friendlists = getattr(entity, 'friendsLists', {}).values()
	for fnd_list in friendlists:
		for friend in fnd_list:
			result.add(_Relationship(entity, friend))
	return result

def update_friendships(db, entity):
	result = _update_connections(db, entity,
								 graph_friends,
								 db_friends,
								 relationships.FriendOf())
	return result

def _process_friendslist_event(db, obj, event):
	if nti_interfaces.IDynamicSharingTargetFriendsList.providedBy(obj):
		return # pragma no cover

	username = getattr(obj.creator, 'username', obj.creator)
	def _process_relationships():
		logger.info("Updating friendships for %s" % username)
		transaction_runner = \
				component.getUtility(nti_interfaces.IDataserverTransactionRunner)
		updater = functools.partial(update_friendships, db=db, entity=username)
		transaction_runner(updater)

	transaction.get().addAfterCommitHook(
				lambda success: success and gevent.spawn(_process_relationships))

@component.adapter(nti_interfaces.IFriendsList, lce_interfaces.IObjectAddedEvent)
def _friendslist_added(obj, event):
	db = get_graph_db()
	if db is not None:
		_process_friendslist_event(db, obj, event)

@component.adapter(nti_interfaces.IFriendsList, lce_interfaces.IObjectModifiedEvent)
def _friendslist_modified(obj, event):
	db = get_graph_db()
	if db is not None:
		_process_friendslist_event(db, obj, event)

@component.adapter(nti_interfaces.IFriendsList, lce_interfaces.IObjectRemovedEvent)
def _friendslist_deleted(obj, event):
	db = get_graph_db()
	if db is not None:
		_process_friendslist_event(db, obj, event)

# membership

def graph_memberships(db, entity):
	result = _get_graph_connections(db, entity, rel_type=relationships.MemberOf())
	return result

def db_memberships(entity):
	result = set()
	everyone = users.Entity.get_entity('Everyone')
	memberships = getattr(entity, 'dynamic_memberships', ())
	for x in memberships:
		if x != everyone:
			result.add(_Relationship(entity, x))
	return result

def update_memberships(db, entity):
	result = _update_connections(db, entity,
								 graph_memberships,
								 db_memberships,
								 relationships.MemberOf())
	return result

def process_start_membership(db, source, target):
	source = users.Entity.get_entity(source)
	target = ntiids.find_object_with_ntiid(target)
	if source and target:
		rel = db.create_relationship(source, target, relationships.MemberOf())
		return rel
	return None

def process_stop_membership(db, source, target):
	source = users.Entity.get_entity(source)
	target = ntiids.find_object_with_ntiid(target)
	if source and target:
		rels = db.match(start=source, end=target, rel_type=relationships.MemberOf())
		if rels:
			db.delete_relationships(*rels)
			return True
	return False

def _process_membership_event(db, event):
	source, target = event.object, event.target
	everyone = users.Entity.get_entity('Everyone')
	if target == everyone:
		return # pragma no cover

	source = source.username
	target = externalization.to_external_ntiid_oid(target)
	start_membership = nti_interfaces.IStartDynamicMembershipEvent.providedBy(event)
	def _process_relationships():
		transaction_runner = \
				component.getUtility(nti_interfaces.IDataserverTransactionRunner)
		func = process_start_membership if start_membership else process_stop_membership
		func = functools.partial(func, db=db, source=source, target=target)
		transaction_runner(func)

	transaction.get().addAfterCommitHook(
				lambda success: success and gevent.spawn(_process_relationships))

@component.adapter(nti_interfaces.IStartDynamicMembershipEvent)
def _start_dynamic_membership_event(event):
	db = get_graph_db()
	if db is not None:
		_process_membership_event(db, event)

@component.adapter(nti_interfaces.IStopDynamicMembershipEvent)
def _stop_dynamic_membership_event(event):
	db = get_graph_db()
	if db is not None:
		_process_membership_event(db, event)

def _do_membership_deletions(db, keyref):

	def _process_relationships():
		for key, value in keyref.items():
			db.delete_index_relationship(key, value)

	transaction.get().addAfterCommitHook(
			lambda success: success and gevent.spawn(_process_relationships))

@component.adapter(nti_interfaces.IDynamicSharingTargetFriendsList,
				  lce_interfaces.IObjectRemovedEvent)
def _dfl_deleted(obj, event):
	db = get_graph_db()
	if db is not None:
		result = {}
		rel_type = relationships.MemberOf()
		for user in obj:
			adapted = component.queryMultiAdapter(
										(user, obj, rel_type),
										graph_interfaces.IUniqueAttributeAdapter)
			if adapted:
				result[adapted.key] = adapted.value
		if result:
			_do_membership_deletions(db, result)

# follow/unfollow

def process_follow(db, source, followed):
	source = users.Entity.get_entity(source)
	followed = users.Entity.get_entity(followed)
	if source and followed:
		rel = db.create_relationship(source, followed, relationships.Follow())
		return rel
	return None

def process_unfollow(db, source, followed):
	source = users.Entity.get_entity(source)
	followed = users.Entity.get_entity(followed)
	if source and followed:
		rels = db.match(start=source, end=followed, rel_type=relationships.Follow())
		if rels:
			db.delete_relationships(*rels)
			return True
	return False

def _process_follow_event(db, event):
	source = getattr(event.object, 'username', event.object)
	stop_following = nti_interfaces.IStopFollowingEvent.providedBy(event)
	followed = event.not_following if stop_following else event.now_following
	followed = getattr(followed, 'username', followed)
	
	def _process_relationships():
		transaction_runner = \
				component.getUtility(nti_interfaces.IDataserverTransactionRunner)
		func = process_unfollow if stop_following else process_follow
		func = functools.partial(func, db=db, source=source, followed=followed)
		transaction_runner(func)

	transaction.get().addAfterCommitHook(
				lambda success: success and gevent.spawn(_process_relationships))

@component.adapter(nti_interfaces.IEntityFollowingEvent)
def _start_following_event(event):
	db = get_graph_db()
	if db is not None:
		_process_follow_event(db, event)

@component.adapter(nti_interfaces.IStopFollowingEvent)
def _stop_following_event(event):
	db = get_graph_db()
	if db is not None:
		_process_follow_event(db, event)

def graph_following(db, entity):
	result = _get_graph_connections(db, entity, rel_type=relationships.Follow())
	return result

def db_following(entity):
	result = set()
	entities_followed = getattr(entity, 'entities_followed', ())
	for followed in entities_followed:
		result.add(_Relationship(entity, followed))
	return result

def update_following(db, entity):
	result = _update_connections(db, entity,
								 graph_following,
								 db_following,
								 relationships.Follow())
	return result

# utils

def install(db, usernames=()):

	if not usernames:
		dataserver = component.getUtility(nti_interfaces.IDataserver)
		_users = nti_interfaces.IShardLayout(dataserver).users_folder
		usernames = _users.iterkeys()

	result = 0
	for username in usernames:
		user = users.Entity.get_entity(username)
		if not nti_interfaces.IUser.providedBy(user):
			continue

		for func in (update_friendships, update_memberships, update_following):
			rels = func(db, user)
			result += len(rels)

	return result
