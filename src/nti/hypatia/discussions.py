#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
graphdb discussion topics

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

from nti.appserver import interfaces as app_interfaces

from nti.dataserver import users
from nti.dataserver import interfaces as nti_interfaces
from nti.dataserver.contenttypes.forums import interfaces as frm_interfaces

from nti.externalization import externalization

from nti.ntiids import ntiids

from . import utils
from . import get_graph_db
from . import relationships
from . import interfaces as graph_interfaces

PrimaryKey = utils.UniqueAttribute

def to_external_ntiid_oid(obj):
	return externalization.to_external_ntiid_oid(obj)

def get_primary_key(obj):
	adapted = graph_interfaces.IUniqueAttributeAdapter(obj)
	return PrimaryKey(adapted.key, adapted.value)

# topics

def _add_authorship_relationship(db, topic):
	creator = topic.creator
	rel_type = relationships.Author()
	properties = component.getMultiAdapter(
								(creator, topic, rel_type),
								graph_interfaces.IPropertyAdapter)
	result = db.create_relationship(creator, topic, rel_type, properties=properties)
	logger.debug("authorship relationship %s created" % result)
	return result

def add_topic_node(db, oid, key, value):
	result = None
	node = db.get_indexed_node(key, value)
	topic = ntiids.find_object_with_ntiid(oid)
	if topic is not None and node is None:
		result = db.create_node(topic)
		logger.debug("topic node %s created" % result)
		_add_authorship_relationship(db, topic)
	return result, topic

def modify_topic_node(db, oid, key, value):
	node, topic = add_topic_node(db, oid, key, value)
	if topic is not None:
		labels = graph_interfaces.ILabelAdapter(topic)
		properties = graph_interfaces.IPropertyAdapter(topic)
		db.update_node(node, labels, properties)
		logger.debug("properties updated for node %s" % node)
	return node, topic

def delete_topic_node(db, key, value):
	node = db.get_indexed_node(key, value)
	if node is not None:
		db.delete_node(node)
		logger.debug("topic node %s deleted" % node)
		return True
	return False

def _process_topic_add_mod_event(db, topic, event):
	oid = to_external_ntiid_oid(topic)
	adapted = graph_interfaces.IUniqueAttributeAdapter(topic)
	key, value = adapted.key, adapted.value

	def _process_event():
		transaction_runner = \
			component.getUtility(nti_interfaces.IDataserverTransactionRunner)

		if event == graph_interfaces.ADD_EVENT:
			func = add_topic_node
		else:
			func = modify_topic_node

		func = functools.partial(func, db=db, oid=oid, key=key, value=value)
		transaction_runner(func)

	transaction.get().addAfterCommitHook(
					lambda success: success and gevent.spawn(_process_event))

@component.adapter(frm_interfaces.ITopic, lce_interfaces.IObjectAddedEvent)
def _topic_added(topic, event):
	db = get_graph_db()
	if db is not None:
		_process_topic_add_mod_event(db, topic, graph_interfaces.ADD_EVENT)

@component.adapter(frm_interfaces.ITopic, lce_interfaces.IObjectModifiedEvent)
def _topic_modified(topic, event):
	db = get_graph_db()
	if db is not None:
		_process_topic_add_mod_event(db, topic, graph_interfaces.MODIFY_EVENT)

def _process_topic_remove_event(db, primary_keys=()):

	def _process_event():
		result = db.delete_nodes(*primary_keys)
		logger.debug("%s node(s) deleted", result)

	transaction.get().addAfterCommitHook(
			lambda success: success and gevent.spawn(_process_event))
	
@component.adapter(frm_interfaces.ITopic, lce_interfaces.IObjectRemovedEvent)
def _topic_removed(topic, event):
	db = get_graph_db()
	if db is not None:
		primary_keys = [get_primary_key(topic)]
		for comment in topic.values():
			primary_keys.append(get_primary_key(comment))
		_process_topic_remove_event(db, primary_keys)

# comments

def add_comment_relationship(db, oid, comment_rel_pk):
	result = None
	comment = ntiids.find_object_with_ntiid(oid)
	if comment is not None:
		# comment are special case. we build a relationship between the comment-user and
		# the topic. We force key/value to identify the relationship
		# Note we don't create a comment node.
		author = comment.creator
		topic = comment.__parent__
		rel_type = relationships.CommentOn()
		properties = component.getMultiAdapter(
									(author, comment, rel_type),
									graph_interfaces.IPropertyAdapter)
		result = db.create_relationship(author, topic, rel_type,
										properties=properties,
										key=comment_rel_pk.key,
										value=comment_rel_pk.value)
		logger.debug("comment-on relationship %s created" % result)
	return result

def delete_comment(db, comment_pk, comment_rel_pk):
	node = db.get_indexed_node(comment_pk.key, comment_pk.value) # check for comment node
	if node is not None:
		db.delete_node(node)
		logger.debug("comment-on node %s deleted" % comment_pk)
	if db.delete_indexed_relationship(comment_rel_pk.key, comment_rel_pk.value):
		logger.debug("comment-on relationship %s deleted" % comment_rel_pk)
		return True
	return False

def _process_comment_event(db, comment, event):
	oid = to_external_ntiid_oid(comment)
	comment_pk = get_primary_key(comment)
	comment_rel_pk = get_comment_relationship_PK(comment)

	def _process_event():
		transaction_runner = \
				component.getUtility(nti_interfaces.IDataserverTransactionRunner)

		if event == graph_interfaces.ADD_EVENT:
			func = functools.partial(add_comment_relationship, db=db,
									 oid=oid,
									 comment_rel_pk=comment_rel_pk)
		elif event == graph_interfaces.REMOVE_EVENT:
			func = functools.partial(delete_comment, db=db,
									 comment_pk=comment_pk,
									 comment_rel_pk=comment_rel_pk)
		else:
			func = None

		if func is not None:
			transaction_runner(func)

	transaction.get().addAfterCommitHook(
				lambda success: success and gevent.spawn(_process_event))


def get_comment_relationship_PK(comment):
	author = comment.creator
	rel_type = relationships.CommentOn()
	adapted = component.getMultiAdapter(
							(author, comment, rel_type),
							graph_interfaces.IUniqueAttributeAdapter)
	return PrimaryKey(adapted.key, adapted.value)

@component.adapter(frm_interfaces.IPersonalBlogComment, lce_interfaces.IObjectAddedEvent)
def _add_personal_blog_comment(comment, event):
	db = get_graph_db()
	if db is not None:
		_process_comment_event(db, comment, graph_interfaces.ADD_EVENT)

@component.adapter(frm_interfaces.IGeneralForumComment, lce_interfaces.IObjectAddedEvent)
def _add_general_forum_comment(comment, event):
	db = get_graph_db()
	if db is not None:
		_process_comment_event(db, comment, graph_interfaces.ADD_EVENT)

@component.adapter(frm_interfaces.IPersonalBlogComment,
				   lce_interfaces.IObjectModifiedEvent)
def _modify_personal_blog_comment(comment, event):
	db = get_graph_db()
	if db is not None and app_interfaces.IDeletedObjectPlaceholder.providedBy(comment):
		_process_comment_event(db, comment, graph_interfaces.REMOVE_EVENT)

@component.adapter(frm_interfaces.IGeneralForumComment,
				   lce_interfaces.IObjectModifiedEvent)
def _modify_general_forum_comment(comment, event):
	_modify_personal_blog_comment(comment, event)

# utils

def install(db, usernames=()):

	if not usernames:
		dataserver = component.getUtility(nti_interfaces.IDataserver)
		_users = nti_interfaces.IShardLayout(dataserver).users_folder
		usernames = _users.iterkeys()

	def _record_author(topic):
		oid = to_external_ntiid_oid(topic)
		adapted = graph_interfaces.IUniqueAttributeAdapter(topic)
		add_topic_node(db, oid, adapted.key, adapted.value)
		_record_author.counter += 1
	_record_author.counter = 0

	def _record_comment(comment):
		oid = to_external_ntiid_oid(comment)
		comment_rel_pk = get_comment_relationship_PK(comment)
		add_comment_relationship(db, oid, comment_rel_pk)
		_record_comment.counter += 1
	_record_comment.counter = 0

	for username in usernames:
		entity = users.Entity.get_entity(username)
		if nti_interfaces.IUser.providedBy(entity):
			blog = frm_interfaces.IPersonalBlog(entity)
			for topic in blog.values():
				_record_author(topic)
				for comment in topic.values():
					_record_comment(comment)

		elif nti_interfaces.ICommunity.providedBy(entity):
			board = frm_interfaces.ICommunityBoard(entity)
			for forum in board.values():
				for topic in forum.values():
					_record_author(topic)
					for comment in topic.values():
						_record_comment(comment)

	result = _record_comment.counter + _record_author.counter
	return result
