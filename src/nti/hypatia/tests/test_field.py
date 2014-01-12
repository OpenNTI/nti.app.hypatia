#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import is_
from hamcrest import is_not
from hamcrest import contains
from hamcrest import assert_that
does_not = is_not

import unittest

import BTrees

from nti.hypatia.field import TimeFieldIndex
from nti.hypatia.field import time_to_64bit_int

_marker = object()

def to_int(value):
	return time_to_64bit_int(value)

class TestModeled(unittest.TestCase):

	def _makeOne(self):
		def discriminator(obj, default):
			if obj is _marker:
				return default
			if type(obj) == float:
				obj = to_int(obj)
			return obj

		family = BTrees.family64
		return TimeFieldIndex(discriminator=discriminator, family=family)
	
	def _populate(self, index):
		index.index_doc(5, 1.0)  # docid, obj
		index.index_doc(2, 2.0)
		index.index_doc(1, 3.0)
		index.index_doc(3, 4.0)
		index.index_doc(4, 5.0)
		index.index_doc(8, 6.0)
		index.index_doc(9, 7.0)
		index.index_doc(7, 8.0)
		index.index_doc(6, 9.0)
		index.index_doc(11, 10.0)
		index.index_doc(10, 11.0)
	
	def test_index_doc_existing_same_value(self):
		index = self._makeOne()
		index.index_doc(1, 1.0)
		index.index_doc(1, 1.0)
		assert_that(index.indexed_count(), is_(1))
		assert_that(index.word_count(), is_(1))
		assert_that(index._rev_index, contains(1))
		ival = to_int(1.0)
		assert_that(index._fwd_index, contains(ival))
		assert_that(list(index._fwd_index[ival]), is_([1]))
		assert_that(list(index.unique_values()), is_([ival]))

	def test_index_doc_existing_new_value(self):
		index = self._makeOne()
		index.index_doc(1, 1.0)
		index.index_doc(1, 2.0)
		assert_that(index.indexed_count(), is_(1))
		assert_that(index.word_count(), is_(1))
		assert_that(index._rev_index, contains(1))

		one_ival = to_int(1.0)
		two_ival = to_int(2.0)

		assert_that(index._fwd_index, does_not(contains(one_ival)))
		assert_that(index._fwd_index, contains(two_ival))
		assert_that(list(index._fwd_index[two_ival]), is_([1]))
		assert_that(list(index.unique_values()), is_([two_ival]))
