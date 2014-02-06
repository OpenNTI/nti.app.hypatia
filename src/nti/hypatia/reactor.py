#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import os
import time
import gevent
import random
import functools

from zope import component
from zope import interface

from ZODB import loglevels
from ZODB.POSException import ConflictError

import zope.intid

from redis.connection import ConnectionError

from nti.dataserver import interfaces as nti_interfaces

from nti.hypatia import LOCK_NAME
from nti.hypatia import search_queue
from nti.hypatia import interfaces as hypatia_interfaces

MIN_INTERVAL = 5
MAX_INTERVAL = 120
MIN_BATCH_SIZE = 10
DEFAULT_INTERVAL = 30
DEFAULT_QUEUE_LIMIT = hypatia_interfaces.DEFAULT_QUEUE_LIMIT

def queue_length(queue):
	try:
		result = len(queue)
	except ValueError:
		result = queue.__len__()
		logger.error("Could not compute queue length. Using __len__ method (%s)", result)
	return result

def process_queue(limit=DEFAULT_QUEUE_LIMIT):
	ids = component.getUtility(zope.intid.IIntIds)
	catalog = component.getUtility(hypatia_interfaces.ISearchCatalog)
	queue = search_queue()
	queue_size = queue_length(queue)

	limit = queue_size if limit == -1 else limit
	if queue_size >= limit:
		logger.info("Processing %s index event(s) out of %s", limit, queue_size)
	elif queue_size > 0:
		logger.info("Processing %s index event(s)", queue_size)

	to_process = min(limit, queue_size)
	queue.process(ids, (catalog,), to_process)
	return to_process

def process_index_msgs(lockname, limit=DEFAULT_QUEUE_LIMIT, use_trx_runner=True):
	redis = component.getUtility(nti_interfaces.IRedisClient)
	try:
		lock = redis.lock(lockname, MAX_INTERVAL)
		aquired = lock.acquire(blocking=False)
	except TypeError:
		lock = redis.lock(lockname)
		aquired = lock.acquire()

	result = 0
	try:
		if aquired:
			try:
				runner = functools.partial(process_queue, limit=limit) \
						 if limit != DEFAULT_QUEUE_LIMIT else process_queue
				if use_trx_runner:
					transaction_runner = \
						component.getUtility(nti_interfaces.IDataserverTransactionRunner)
					result = transaction_runner(runner, retries=1, sleep=1)
				else:
					result = runner()
			except ConflictError as e:
				logger.error(e)
				result = -1
			except Exception:
				logger.exception('Cannot process index messages')
				result = -2
	finally:
		if aquired:
			lock.release()
	return result

@interface.implementer(hypatia_interfaces.IIndexReactor)
class IndexReactor(object):

	stop = False
	min_wait_time = 10
	max_wait_time = 30
	limit = DEFAULT_QUEUE_LIMIT

	processor = pid = None

	def __init__(self, min_time=None, max_time=None, limit=None):
		if min_time:
			self.min_wait_time = min_time
		if max_time:
			self.max_wait_time = max_time
		if limit and limit != DEFAULT_QUEUE_LIMIT:
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
			batch_size = self.limit
			while not self.stop:
				start = time.time()
				try:
					if not self.stop:
						result = process_index_msgs(LOCK_NAME, batch_size)
						duration = time.time() - start
						if result == 0: # no work
							batch_size = self.limit  # reset to default
							secs = random.randint(self.min_wait_time, self.max_wait_time)
							duration = secs / 2.0
						elif result < 0:  # conflict error
							factor = 0.33 if result == -1 else 0.2
							batch_size = max(MIN_BATCH_SIZE, int(batch_size * factor))
						elif duration < MAX_INTERVAL:
							batch_size = int(batch_size * 1.5)
						else:
							half = batch_size * .5
							batch_size = max(MIN_BATCH_SIZE, int(half / duration))
							
						duration = duration * 2.0
						logger.log(loglevels.TRACE, "Sleeping %s(secs). Batch size %s", duration, batch_size)
						sleep(duration)
				except component.ComponentLookupError:
					logger.error("process %s could not get component", self.pid)
					break
				except KeyboardInterrupt:
					break
				except ConnectionError:
					logger.exception("%s could not connect to redis", self.pid)
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
