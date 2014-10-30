#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import functools
import transaction

from zope import component

from pyramid.interfaces import INewRequest

from nti.dataserver.interfaces import IDataserverTransactionRunner

from nti.hypatia import process_queue

@component.adapter(INewRequest)
def requestIndexation(event):
	def _process_event():
		transaction_runner = \
			component.getUtility(IDataserverTransactionRunner)
		func = functools.partial(process_queue, limit=-1)
		transaction_runner(func)
		return True

	transaction.get().addAfterCommitHook(
					lambda success: success and _process_event())
