#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
hypatia utils

$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from zope.generations.utility import findObjectsMatching

from nti.contentsearch import interfaces as search_interfaces

from nti.dataserver import interfaces as nti_interfaces
from nti.dataserver.contenttypes.forums import interfaces as forum_interfaces

from nti.hypatia import is_indexable

def get_user_indexable_objects(user):

	for obj in findObjectsMatching(user, is_indexable):
		yield obj

	# personal blog
	forum = forum_interfaces.IPersonalBlog(user, {})
	for topic in forum.values():
		yield topic  # get all topics

		for comment in topic.values():
			yield comment  # get all comments

	for membership in getattr(user, 'dynamic_memberships', ()):
		if not nti_interfaces.ICommunity.providedBy(membership):
			continue
		
		board = forum_interfaces.IBoard(membership, {})
		for forum in board.values():
			for topic in forum.values():
				if getattr(topic, 'creator', None) == user:
					yield topic

				for comment in topic.values():
					if getattr(comment, 'creator', None) == user:
						yield comment
