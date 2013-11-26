#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import gevent
import random

from zope import component
from zope import interface

import zope.intid

from nti.dataserver import interfaces as nti_interfaces

from . import search_queue
from . import interfaces as hypatia_interfaces

def process_queue(limit=hypatia_interfaces.DEFAULT_QUEUE_LIMIT):
	ids = component.getUtility(zope.intid.IIntIds)
	catalog = component.getUtility(hypatia_interfaces.ISearchCatalog)
	search_queue().process(ids, (catalog,), limit)

@interface.implementer(hypatia_interfaces.IIndexReactor)
class IndexReactor(object):

	stop = False
	sleep_min_wait_time = 35
	sleep_max_wait_time = 60
	lockname = u"hypatia-lock"

	def __init__(self):
		self.processor = self._spawn_index_processor()

	def halt(self):
		self.stop = True

	def _spawn_index_processor(self):

		def process():
			while not self.stop:
				# wait for idx ops
				secs = random.randint(self.sleep_min_wait_time, self.sleep_max_wait_time)
				gevent.sleep(seconds=secs)
				if not self.stop:
					self.process_index_msgs()

		result = gevent.spawn(process)
		return result

	def process_index_msgs(self):
		redis = component.getUtility(nti_interfaces.IRedisClient)
		with redis.lock(self.lockname):
			transaction_runner = \
					component.getUtility(nti_interfaces.IDataserverTransactionRunner)
			try:
				transaction_runner(process_queue, retries=3)
			except Exception:
				logger.exception('Cannot process index messages')
