#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
hypatia keyword index

$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from zope import interface

from BTrees.Length import Length

from hypatia.field import FieldIndex

from nti.zodb.containers import time_to_64bit_int

from .interfaces import ITimeFieldIndex

@interface.implementer(ITimeFieldIndex)
class TimeFieldIndex(FieldIndex):

	def reset(self):
		"""
		Initialize forward and reverse mappings.
		"""
		# The forward index maps indexed values to a sequence of docids
		self._fwd_index = self.family.IO.BTree()
		# The reverse index maps a docid to its index value
		# use a II tree since we are storing time as ints
		self._rev_index = self.family.II.LLBTree()
		self._num_docs = Length(0)
		self._not_indexed = self.family.II.TreeSet()

	def discriminate(self, obj, default):
		value = super(TimeFieldIndex, self).discriminate(obj, default)
		assert type(value) in (float, int)
		if type(value) == float:  # auto-covert
			value = time_to_64bit_int(value)
		return value
