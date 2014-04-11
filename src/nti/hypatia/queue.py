#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
.. $Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import BTrees

from zope import interface
from zope.container import contained

from zc.catalogqueue.queue import CatalogQueue
from zc.catalogqueue.CatalogEventQueue import CatalogEventQueue

from . import interfaces as hypatia_interfaces

@interface.implementer(hypatia_interfaces.ISearchCatalogEventQueue)
class SearchCatalogEventQueue(CatalogEventQueue, contained.Contained):
	pass

@interface.implementer(hypatia_interfaces.ISearchCatalogQueue)
class SearchCatalogQueue(CatalogQueue, contained.Contained):

	def __init__(self, buckets=1009):
		CatalogQueue.__init__(self, buckets=0)
		self._queues = list()
		self._buckets = buckets
		for i in xrange(buckets):
			queue = SearchCatalogEventQueue()
			queue.__name__ = str(i)
			queue.__parent__ = self
			self._queues.append(queue)

	@property
	def buckets(self):
		return self._buckets

	def eventQueueLength(self):
		result = 0
		for queue in self._queues:
			result += len(queue)
		return result
	event_queue_length = eventQueueLength

	def syncQueue(self):
		try:
			length = self._length
		except AttributeError:
			length = self._length = BTrees.Length.Length()
		old = length.value
		new = self.eventQueueLength()
		result = old != new
		if result:  # only set if different
			length.set(new)
		return result
	sync = sync_queue = syncQueue

	changeLength = CatalogQueue._change_length

	def __getitem__(self, idx):
		return self._queues[idx]

	def __iter__(self):
		return iter(self._queues)

	def __str__(self):
		return "%s(%s)" % (self.__class__.__name__, self._buckets)
	__repr__ = __str__
