#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
$Id$
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
		CatalogQueue.__init__(self, 0)
		self._queues = []
		self._buckets = buckets
		for i in range(buckets):
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

	def syncLength(self):
		try:
			length = self._length
		except AttributeError:
			length = self._length = BTrees.Length.Length()
		length.set(self.eventQueueLength())
	sync = sync_length = syncLength

	def __getitem__(self, idx):
		return self._queues[idx]
