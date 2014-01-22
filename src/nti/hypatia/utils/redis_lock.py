#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
Copyright (c) 2011, Ionel Cristian Maries
All rights reserved.

$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from os import urandom

from ZODB import loglevels

UNLOCK_SCRIPT = b"""
	if redis.call("get", KEYS[1]) == ARGV[1] then
		redis.call("lpush", KEYS[2], 1)
		return redis.call("del", KEYS[1])
	else
		return 0
	end
"""

class Lock(object):

	_tok = None
	_expire = None
	
	def __init__(self, redis_client, name, expire=None):
		self._client = redis_client
		self._name = 'lock:' + name
		self._signal = 'lock-signal:' + name
		if expire is not None:
			self._expire= int(expire)

	def __enter__(self, blocking=True):
		logger.log(loglevels.TRACE, "Getting %r ...", self._name)

		if self._tok is None:
			self._tok = urandom(16) if self._expire else 1
		else:
			raise RuntimeError("Already aquired from this Lock instance.")

		busy = True
		while busy:
			busy = not self._client.set(self._name, self._tok, nx=True, ex=self._expire)
			if busy:
				if blocking:
					self._client.blpop(self._signal, self._expire or 0)
				else:
					logger.log(loglevels.TRACE, "Failed to get %r.", self._name)
					return False

		logger.log(loglevels.TRACE, "Got lock for %r.", self._name)

		self._client.delete(self._signal)
		return True

	acquire = __enter__

	def __exit__(self, exc_type=None, exc_value=None, traceback=None):
		logger.log(loglevels.TRACE, "Releasing %r.", self._name)
		self._client.eval(UNLOCK_SCRIPT, 2, self._name, self._signal, self._tok)

	release = __exit__
