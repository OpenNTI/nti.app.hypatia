#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
hypatia adapters

$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from zope import component
from zope import interface

from nti.contentsearch import interfaces as search_interfaces

from nti.dataserver import interfaces as nti_interfaces

from . import subscribers

@component.adapter(nti_interfaces.IEntity)
@interface.implementer(search_interfaces.IEntityIndexManager)
class _HypatiaEntityIndexManager(object):

	@property
	def entity(self):
		return self.__parent__

	@property
	def username(self):
		return self.entity.username

	def index_content(self, data, *args, **kwargs):
		subscribers.queue_added(data)
	
	def update_content(self, data, *args, **kwargs):
		subscribers.queue_modified(data)

	def delete_content(self, data, *args, **kwargs):
		subscribers.queue_remove(data)

	def search(self, query):
		search_interfaces.ISearchQuery(query)

	def suggest(self, query):
		pass

	def suggest_and_search(self, query):
		pass
