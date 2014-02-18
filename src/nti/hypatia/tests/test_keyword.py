#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import is_
from hamcrest import none
from hamcrest import is_in
from hamcrest import is_not
from hamcrest import contains
from hamcrest import has_length
from hamcrest import assert_that
does_not = is_not

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

	def test_strictEQ(self):
		index = self._makeOne()
		index.index_doc(1, ('zope', 'zope3'))
		index.index_doc(2, ('zope',))
		index.index_doc(3, ('zope', 'zope3', 'zope4'))
		index.index_doc(4, ('zope', 'zope3'))

		s = index.strictEq('zope')
		assert_that(s, has_length(1))
		assert_that(2, is_in(s))

		s = index.strictEq(['zope', 'zope3'])
		assert_that(s, has_length(2))
		assert_that(1, is_in(s))
		assert_that(4, is_in(s))

		s = index.strictEq(('zope3', 'zope', 'zope4'))
		assert_that(s, has_length(1))
		assert_that(3, is_in(s))

		s = index.strictEq(('zope3', 'zope5'))
		assert_that(s, has_length(0))

	def test_remove_word(self):
		index = self._makeOne()
		index.index_doc(1, ('zope', 'zope3'))
		index.index_doc(2, ('zope',))
		index.index_doc(3, ('zope', 'zope3', 'zope4'))
		index.index_doc(4, ('zope', 'zope3'))

		s = index.remove_word('zope')
		assert_that(s, has_length(1))
		assert_that(2, is_in(s))

		assert_that(index.get_words(2), is_(()))
		assert_that(index.get_words(1), is_(('zope3',)))
		assert_that(index.get_words(4), is_(('zope3',)))
		assert_that(sorted(index.get_words(3)), is_(['zope3', 'zope4']))

	def test_replace_word(self):
		index = self._makeOne()
		index.index_doc(1, ('zope', 'zope3'))
		index.index_doc(2, ('zope',))
		index.index_doc(3, ('zope', 'zope3', 'zope4'))
		index.index_doc(4, ('zope', 'zope3'))

		s = index.replace_word('notfound', 'zope')
		assert_that(s, is_(none()))

		s = index.replace_word('zope', 'zope3')
		assert_that(s, is_(none()))

		s = index.replace_word('zope', 'zope5')
		assert_that(s, has_length(4))

		for x in range(1, 5):
			assert_that(index.get_words(x), does_not(contains('zope')))

		s = index.replace_word('zope4', 'zope6')
		assert_that(s, has_length(1))
