#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
hypatia utils

$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import zope.intid
from zope import component
from zope.generations.utility import findObjectsMatching

from nti.contentsearch import interfaces as search_interfaces

from nti.dataserver import interfaces as nti_interfaces
from nti.dataserver.contenttypes.forums import interfaces as forum_interfaces

from nti.hypatia import is_indexable

def get_user_indexable_objects(user):

	def condition(x):
		result = is_indexable(x) and not forum_interfaces.ICommentPost.providedBy(x)
		return result

	for obj in findObjectsMatching(user, condition):
		yield obj

	# personal blog
	forum = forum_interfaces.IPersonalBlog(user, {})
	for topic in forum.values():
		yield topic  # get all topics

		for comment in topic.values():
			yield comment  # get all comments

	for membership in getattr(user, 'dynamic_memberships', ()):
		if nti_interfaces.ICommunity.providedBy(membership):
			board = forum_interfaces.IBoard(membership, {})
			for forum in board.values():
				for topic in forum.values():
					creator = getattr(topic, 'creator', None)
					if getattr(creator, 'username', creator) == user.username:
						yield topic

					for comment in topic.values():
						creator = getattr(comment, 'creator', None)
						if getattr(creator, 'username', creator) == user.username:
							yield comment


def all_indexable_objects_iids(users=()):
	intids = component.getUtility(zope.intid.IIntIds)
	usernames = {getattr(user, 'username', user) for user in users or ()}
	for uid, obj in intids.items():
		creator = getattr(obj, 'creator', None)
		if 	is_indexable(obj) and \
			(not usernames or getattr(creator, 'username', creator) in usernames):
			yield uid
