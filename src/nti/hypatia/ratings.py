#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
graphdb ratings

$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import gevent
import functools
import transaction

from zope import component

from pyramid.security import authenticated_userid
from pyramid.threadlocal import get_current_request

from contentratings.interfaces import IObjectRatedEvent

from nti.dataserver import users
from nti.dataserver.rating import IObjectUnratedEvent
from nti.dataserver import interfaces as nti_interfaces
from nti.dataserver.contenttypes.forums import interfaces as frm_interfaces

from nti.externalization import externalization

from nti.ntiids import ntiids

from . import get_graph_db
from . import relationships

LIKE_CAT_NAME = 'likes'
RATING_CAT_NAME = 'rating'

def get_current_user():
	request = get_current_request()
	username = authenticated_userid(request) if request else None
	return username

def _add_relationship(db, username, oid, rel_type, properties=None):
	result = None
	author = users.User.get_user(username)
	obj = ntiids.find_object_with_ntiid(oid)
	if obj is not None and author is not None:
		result = db.create_relationship(author, obj, rel_type, properties=properties)
		logger.debug("%s relationship %s created", rel_type, result)
	return result

def _remove_relationship(db, username, oid, rel_type):
	result = False
	author = users.User.get_user(username)
	obj = ntiids.find_object_with_ntiid(oid)
	if obj is not None and author is not None:
		match = db.match(author, obj, rel_type)
		if match and db.delete_relationships(match[0]):
			logger.debug("%s relationship %s deleted", rel_type, match[0])
			result = True
	return result

def add_like_relationship(db, username, oid):
	result = _add_relationship(db, username, oid, relationships.Like())
	return result

def remove_like_relationship(db, username, oid):
	result = _remove_relationship(db, username, oid, relationships.Like())
	return result

def _process_like_event(db, username, oid, like=True):

	def _process_event():
		transaction_runner = \
				component.getUtility(nti_interfaces.IDataserverTransactionRunner)
		if like:
			func = functools.partial(add_like_relationship, db=db, username=username,
									 oid=oid)
		else:
			func = functools.partial(remove_like_relationship, db=db, username=username,
									 oid=oid)
		transaction_runner(func)

	transaction.get().addAfterCommitHook(
					lambda success: success and gevent.spawn(_process_event))

def add_rate_relationship(db, username, oid, rating):
	result = _add_relationship(db, username, oid, relationships.Rate(),
							   properties={"rating":int(rating)})
	return result

def remove_rate_relationship(db, username, oid):
	result = _remove_relationship(db, username, oid, relationships.Rate())
	return result

def _process_rate_event(db, username, oid, rating=None, is_rate=True):

	def _process_event():
		transaction_runner = \
				component.getUtility(nti_interfaces.IDataserverTransactionRunner)
		if is_rate:
			func = functools.partial(add_rate_relationship, db=db, username=username,
									 oid=oid, rating=rating)
		else:
			func = functools.partial(remove_rate_relationship, db=db, username=username,
									 oid=oid)
		transaction_runner(func)

	transaction.get().addAfterCommitHook(
					lambda success: success and gevent.spawn(_process_event))


@component.adapter(nti_interfaces.IModeledContent, IObjectRatedEvent)
def _object_rated(modeled, event):
	db = get_graph_db()
	username = get_current_user()
	if username and db is not None:
		oid = externalization.to_external_ntiid_oid(modeled)
		if event.category == LIKE_CAT_NAME:
			like = event.rating != 0
			_process_like_event(db, username, oid, like)
		elif event.category == RATING_CAT_NAME:
			rating = getattr(event, 'rating', None)
			is_rate = not IObjectUnratedEvent.providedBy()
			_process_rate_event(db, username, oid, rating, is_rate)

@component.adapter(frm_interfaces.ITopic, IObjectRatedEvent)
def _topic_rated(topic, event):
	_object_rated(topic, event)

# utils

def install(db, usernames=()):

	from zope.annotation import interfaces as an_interfaces
	from zope.generations.utility import findObjectsProviding

	from contentratings.category import BASE_KEY
	from contentratings.storage import UserRatingStorage

	dataserver = component.getUtility(nti_interfaces.IDataserver)
	_users = nti_interfaces.IShardLayout(dataserver).users_folder
	if not usernames:
		usernames = _users.iterkeys()

	def _get_like_storage(context, cat_name=LIKE_CAT_NAME):
		key = getattr(UserRatingStorage, 'annotation_key', BASE_KEY)
		key = str(key + '.' + cat_name)
		annotations = an_interfaces.IAnnotations(context, {})
		storage = annotations.get(key)
		return storage

	def _add_like_relationship(db, username, oid):
		add_like_relationship(db, username, oid)
		_add_like_relationship.counter += 1
	_add_like_relationship.counter = 0

	def _record_likeable(obj):
		storage = _get_like_storage(obj)
		if storage is not None:
			oid = externalization.to_external_ntiid_oid(obj)
			for rating in storage.all_user_ratings():
				if rating.userid and rating.userid in _users:
					_add_like_relationship(db, rating.userid, oid)

	for username in usernames:
		entity = users.Entity.get_entity(username)
		if nti_interfaces.IUser.providedBy(entity):

			# stored objects
			for likeable in findObjectsProviding(entity, nti_interfaces.ILikeable):
				_record_likeable(likeable)

			# check blogs
			blog = frm_interfaces.IPersonalBlog(entity)
			for topic in blog.values():
				_record_likeable(topic)
				for comment in topic.values():
					_record_likeable(comment)

		elif nti_interfaces.ICommunity.providedBy(entity):
			board = frm_interfaces.ICommunityBoard(entity)
			for forum in board.values():
				for topic in forum.values():
					_record_likeable(topic)
					for comment in topic.values():
						_record_likeable(comment)

	result = _add_like_relationship.counter
	return result
