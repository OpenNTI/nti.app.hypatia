#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

from redis.connection import ConnectionError

from nti.dataserver import interfaces as nti_interfaces

from nti.hypatia import LOCK_NAME
from nti.hypatia import process_queue
from nti.hypatia import interfaces as hypatia_interfaces

MIN_INTERVAL = 5
MAX_INTERVAL = 60
MIN_BATCH_SIZE = 10
DEFAULT_INTERVAL = 30
DEFAULT_QUEUE_LIMIT = hypatia_interfaces.DEFAULT_QUEUE_LIMIT

class _MockLockingClient(object):

	__slots__ = ()

	def lock(self, *args, **kwargs):
		return self

	def acquire(self, *args, **kwargs):
		return True

	def release(self, *args, **kwargs):
		pass

def process_index_msgs(lockname, limit=DEFAULT_QUEUE_LIMIT, use_trx_runner=True,
					   client=None):
	client = client if client is not None else _MockLockingClient()
	try:
		lock = client.lock(lockname, MAX_INTERVAL)
		aquired = lock.acquire(blocking=False)
	except TypeError:
		lock = client.lock(lockname)
		aquired = lock.acquire()

	result = 0
	try:
		if aquired:
			try:
				runner = functools.partial(process_queue, limit=limit)
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
	start_time = 0
	min_wait_time = 10
	max_wait_time = 30
	limit = DEFAULT_QUEUE_LIMIT

	processor = pid = None

	def __init__(self, min_time=None, max_time=None, limit=None, use_redis=False):
		if min_time:
			self.min_wait_time = min_time
		if max_time:
			self.max_wait_time = max_time
		if limit and limit != DEFAULT_QUEUE_LIMIT:
			self.limit = limit
		# get locking client
		self.lock_client = component.getUtility(nti_interfaces.IRedisClient) \
						   if use_redis else _MockLockingClient()

	def __repr__(self):
		return "%s" % (self.__class__.__name__.lower())

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
		self.start_time = time.time()
		result = 0
		try:
			logger.info("Index reactor started")
			batch_size = self.limit
			while not self.stop:
				start = time.time()
				try:
					if not self.stop:
						result = process_index_msgs(LOCK_NAME, batch_size,
													client=self.lock_client)
						duration = time.time() - start
						if result == 0: # no work
							batch_size = self.limit  # reset to default
							secs = random.randint(self.min_wait_time, self.max_wait_time)
							duration = secs
						elif result < 0:  # conflict error/exception
							factor = 0.33 if result == -1 else 0.2
							batch_size = max(MIN_BATCH_SIZE, int(batch_size * factor))
							duration = min(duration * 2.0, MAX_INTERVAL * 3.0)
						elif duration < MAX_INTERVAL:
							batch_size = int(batch_size * 1.5)
							half = int(duration / 2.0)
							secs = random.randint(self.min_wait_time,
												  max(self.min_wait_time, half))
							duration = secs
						else:
							half = batch_size * .5
							batch_size = max(MIN_BATCH_SIZE, int(half / duration))
							secs = random.randint(self.min_wait_time, self.max_wait_time)
							duration = secs
							
						logger.log(loglevels.TRACE, "Sleeping %s(secs). Batch size %s",
								   duration, batch_size)
						sleep(duration)
				except component.ComponentLookupError:
					result = 99
					logger.error("process could not get component", self.pid)
					break
				except KeyboardInterrupt:
					break
				except ConnectionError:
					result = 66
					logger.exception("%s could not connect to redis", self.pid)
					break
				except:
					logger.exception("Unhandled exception")
					raise
		finally:
			self.processor = None
		return result

	__call__ = run

	def _spawn_index_processor(self):
		result = gevent.spawn(self.run)
		return result
