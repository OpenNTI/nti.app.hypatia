#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

import unittest

import BTrees

from hamcrest import is_
from hamcrest import less_than
from hamcrest import assert_that
from hamcrest import greater_than

from nti.hypatia.text import SourceOkapiIndex
from nti.hypatia.text import SourceCosineIndex
from nti.hypatia.lexicon import defaultLexicon

class TestIndex(unittest.TestCase):

	def _makeCosineIndex(self):
		lexicon = defaultLexicon()
		return SourceCosineIndex(lexicon, family=BTrees.family64)

	def _makeSourceOkapiIndex(self):
		lexicon = defaultLexicon()
		return SourceOkapiIndex(lexicon, family=BTrees.family64)

	def test_cosine_index(self):
		index = self._makeCosineIndex()
		index.index_doc(1, 'one one two three one')
		assert_that(index.query_weight(['one']), less_than(1.0))
		assert_that(index.query_weight(['one']), greater_than(0.0))
		assert_that(index.text_source(1), is_('one one two three one'))

	def test_okapi_index(self):
		index = self._makeSourceOkapiIndex()
		index.index_doc(1, 'one one two three one')
		assert_that(index.query_weight(['one']), greater_than(0.0))
		assert_that(index.text_source(1), is_('one one two three one'))

	def test_unindex(self):
		index = self._makeCosineIndex()
		index.index_doc(1, 'one one two three one')
		assert_that(index.text_source(1), is_('one one two three one'))
		index.unindex_doc(1)
		try:
			index.text_source(1)
			self.fail()
		except KeyError:
			pass

	def test_reindex(self):
		index = self._makeCosineIndex()
		index.index_doc(1, 'one one two three one')
		assert_that(index.text_source(1), is_('one one two three one'))
		index.reindex_doc(1, 'ichigo')
		assert_that(index.text_source(1), is_('ichigo'))
