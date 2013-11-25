#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
graphdb relationships

$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

from zope import interface

from . import interfaces as graph_interfaces

class _Singleton(object):
	_instances = {}
	def __new__(cls, *args, **kwargs):
		if cls not in cls._instances:
			cls._instances[cls] = super(_Singleton, cls).__new__(cls, *args, **kwargs)
		return cls._instances[cls]

@interface.implementer(graph_interfaces.IFriendOf)
class FriendOf(_Singleton):

	def __str__(self):
		return "IS_FRIEND_OF"
	__repr__ = __str__

@interface.implementer(graph_interfaces.IMemberOf)
class MemberOf(_Singleton):

	def __str__(self):
		return "IS_MEMBER_OF"
	__repr__ = __str__

@interface.implementer(graph_interfaces.IFollow)
class Follow(_Singleton):

	def __str__(self):
		return "FOLLOWS"
	__repr__ = __str__

@interface.implementer(graph_interfaces.ICommentOn)
class CommentOn(_Singleton):

	def __str__(self):
		return "HAS_COMMENTED_ON"
	__repr__ = __str__

@interface.implementer(graph_interfaces.ITakeAssessment)
class TakeAssessment(_Singleton):

	def __str__(self):
		return "HAS_TAKEN"
	__repr__ = __str__

@interface.implementer(graph_interfaces.ILike)
class Like(_Singleton):

	def __str__(self):
		return "LIKES"
	__repr__ = __str__

@interface.implementer(graph_interfaces.IRate)
class Rate(_Singleton):

	def __str__(self):
		return "HAS_RATED"
	__repr__ = __str__

@interface.implementer(graph_interfaces.IReply)
class Reply(_Singleton):

	def __str__(self):
		return "HAS_REPLIED_TO"
	__repr__ = __str__

@interface.implementer(graph_interfaces.IAuthor)
class Author(_Singleton):

	def __str__(self):
		return "HAS_AUTHORED"
	__repr__ = __str__
