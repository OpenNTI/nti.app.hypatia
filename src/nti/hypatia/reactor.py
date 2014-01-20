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
import functools

from zope import component
from zope import interface

from ZODB import loglevels

import zope.intid

from nti.dataserver import interfaces as nti_interfaces

from nti.hypatia import LOCK_NAME
from nti.hypatia import search_queue
from nti.hypatia import interfaces as hypatia_interfaces

MIN_INTERVAL = 5
MAX_INTERVAL = 120
DEFAULT_INTERVAL = 30
DEFAULT_QUEUE_LIMIT = hypatia_interfaces.DEFAULT_QUEUE_LIMIT

def process_queue(limit=DEFAULT_QUEUE_LIMIT):
	ids = component.getUtility(zope.intid.IIntIds)
	catalog = component.getUtility(hypatia_interfaces.ISearchCatalog)
	queue = search_queue()
	queue_size = len(queue)
	if queue_size >= limit:
		logger.info("Processing %s index event(s) out of %s", limit, queue_size)
	else:
		logger.log(loglevels.TRACE, "Processing %s index event(s)",
				   min(limit, queue_size))
	queue.process(ids, (catalog,), limit)

def process_index_msgs(lockname, limit=DEFAULT_QUEUE_LIMIT):
	redis = component.getUtility(nti_interfaces.IRedisClient)
	try:
		lock = redis.lock(lockname, MAX_INTERVAL + 30)
		aquired = lock.acquire(blocking=False)
	except TypeError:
		lock = redis.lock(lockname)
		aquired = lock.acquire()

	try:
		if aquired:
			transaction_runner = \
					component.getUtility(nti_interfaces.IDataserverTransactionRunner)
			try:
				runner = functools.partial(process_queue, limit=limit) \
						 if limit != DEFAULT_QUEUE_LIMIT else process_queue
				transaction_runner(runner, retries=3)
			except Exception:
				logger.exception('Cannot process index messages')
	finally:
		if aquired:
			lock.release()
							
@interface.implementer(hypatia_interfaces.IIndexReactor)
class IndexReactor(object):

	stop = False
	min_wait_time = 25
	max_wait_time = 50
	limit = DEFAULT_QUEUE_LIMIT

	processor = pid = None

	def __init__(self, min_time=None, max_time=None, limit=None):
		if min_time:
			self.min_wait_time = min_time
		if max_time:
			self.max_wait_time = max_time
		if limit:
			self.limit = limit

	def __repr__(self):
		return "(%s)" % self.pid

	def halt(self):
		self.stop = True

	def start(self):
		if self.processor is None:
			self.processor = self._spawn_index_processor()
		return self
	
	def run(self, sleep=gevent.sleep):
		random.seed()
		self.stop = False
		self.pid = os.getpid()
		try:
			logger.info("Index reactor started (%s)", self.pid)
			while not self.stop:
				secs = random.randint(self.min_wait_time, self.max_wait_time)
				try:
					sleep(secs)
					if not self.stop:
						process_index_msgs(LOCK_NAME, self.limit)
				except component.ComponentLookupError:
					logger.error("process %s could not get component", self.pid)
					break
				except KeyboardInterrupt:
					break
		finally:
			self.processor = None

	__call__ = run

	def _spawn_index_processor(self):
		result = gevent.spawn(self.run)
		return result

from zope.processlifetime import IDatabaseOpenedWithRoot
	
@component.adapter(IDatabaseOpenedWithRoot)
def _start_reactor(database_event):
	reactor = IndexReactor().start()
	component.provideUtility(reactor, hypatia_interfaces.IIndexReactor)
