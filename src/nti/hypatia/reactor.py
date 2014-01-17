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

from nti.hypatia import LOCK_NAME
from nti.hypatia import search_queue
from nti.hypatia import interfaces as hypatia_interfaces

DEFAULT_INTERVAL = 30

def process_queue(limit=hypatia_interfaces.DEFAULT_QUEUE_LIMIT):
	ids = component.getUtility(zope.intid.IIntIds)
	catalog = component.getUtility(hypatia_interfaces.ISearchCatalog)
	queue = search_queue()
	logger.log(loglevels.TRACE, "indexing %s object(s)", min(limit, len(queue)))
	queue.process(ids, (catalog,), limit)

def process_index_msgs(lockname):
	redis = component.getUtility(nti_interfaces.IRedisClient)
	lock = redis.lock(lockname)
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
							
@interface.implementer(hypatia_interfaces.IIndexReactor)
class IndexReactor(object):

	stop = False
	min_wait_time = 25
	max_wait_time = 50
	processor = pid = None

	def __init__(self, poll_interval=None):
		if poll_interval:
			self.min_wait_time = self.max_wait_time = poll_interval

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
			while not self.stop:
				secs = random.randint(self.min_wait_time, self.max_wait_time)
				try:
					sleep(secs)
					if not self.stop:
						process_index_msgs(LOCK_NAME)
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
