#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import is_
from hamcrest import has_length
from hamcrest import assert_that

import unittest

import BTrees

from nti.hypatia.keyword import SearchKeywordIndex

_marker = object()

class TestModeled(unittest.TestCase):

	def _makeOne(self):
		def discriminator(obj, default):
			if obj is _marker:
				return default
			return obj

		family = BTrees.family64
		return SearchKeywordIndex(discriminator=discriminator, family=family)
	
	def _populate(self, index):
		index.index_doc(1, ('zope', 'CMF', 'Zope3'))
		index.index_doc(2, ('the', 'quick', 'brown', 'FOX'))
		index.index_doc(3, ('Zope',))
		index.index_doc(4, ())
		index.index_doc(5, ('cmf',))

	def test_get_words(self):
		index = self._makeOne()
		self._populate(index)
		assert_that(index.get_words(3), is_(('Zope',)))
		assert_that(index.get_words(2), has_length(4))
		assert_that(index.get_words(1), has_length(3))
