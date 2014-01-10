#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import os
import gevent
import random

from zope import component
from zope import interface

from ZODB import loglevels

import zope.intid

from nti.dataserver import interfaces as nti_interfaces

from . import search_queue
from . import interfaces as hypatia_interfaces

def process_queue(limit=hypatia_interfaces.DEFAULT_QUEUE_LIMIT):
	ids = component.getUtility(zope.intid.IIntIds)
	catalog = component.getUtility(hypatia_interfaces.ISearchCatalog)
	queue = search_queue()
	logger.log(loglevels.TRACE, "indexing %s object(s)", min(limit, len(queue)))
	queue.process(ids, (catalog,), limit)

@interface.implementer(hypatia_interfaces.IIndexReactor)
class IndexReactor(object):

	stop = False
	sleep_min_wait_time = 25
	sleep_max_wait_time = 50
	lockname = u"nti/hypatia/indexlock"

	def __init__(self):
		self.processor = self._spawn_index_processor()

	def halt(self):
		self.stop = True

	def _spawn_index_processor(self):
		random.seed()
		def process():
			pid = os.getpid()
			while not self.stop:
				# wait for idx ops
				secs = random.randint(self.sleep_min_wait_time, self.sleep_max_wait_time)
				gevent.sleep(seconds=secs)
				if not self.stop:
					try:
						self.process_index_msgs()
					except component.ComponentLookupError:
						logger.error("process %s could not get component", pid)
						break

		result = gevent.spawn(process)
		return result

	def process_index_msgs(self):
		redis = component.getUtility(nti_interfaces.IRedisClient)
		lock = redis.lock(self.lockname)
		try:
			aquired = lock.acquire(blocking=False)
		except TypeError:
			aquired = lock.acquire()

		try:
			if aquired:
				transaction_runner = \
						component.getUtility(nti_interfaces.IDataserverTransactionRunner)
				try:
					transaction_runner(process_queue, retries=3)
				except Exception:
					logger.exception('Cannot process index messages')
		finally:
			if aquired:
				lock.release()
