#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
hypatia keyword index

.. $Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import six

from zope import interface

from hypatia.keyword import KeywordIndex

from .interfaces import ISearchKeywordIndex

@interface.implementer(ISearchKeywordIndex)
class SearchKeywordIndex(KeywordIndex):

	def get_words(self, docid):
		result = ()
		if docid in self._rev_index:
			result = tuple(self._rev_index[docid])
		return result

	@property
	def num_docs(self):
		return self._num_docs.value
		
	def index_doc(self, docid, obj):
		__traceback_info__ = docid, obj
		result = super(SearchKeywordIndex, self).index_doc(docid, obj)
		return result

	def strictEq(self, query):
		if isinstance(query, six.string_types):
			query = [query]

		Set = self.family.IF.Set
		query = self.normalize(query)
		norm = len(query)

		sets = []
		for word in query:
			docids = Set(self._fwd_index.get(word, Set()))
			sets.append(docids)

		for doc_set in sets:
			for docid in list(doc_set):
				ooset = self._rev_index[docid]
				if len(ooset) != norm:
					doc_set.remove(docid)

		sets.sort(key=len)
		result = None
		for if_set in sets:
			result = self.family.IF.intersection(result, if_set)
			if not result:
				break

		if result:
			return result
		else:
			return Set()

	def removeWord(self, word):
		result = []
		docids = self._fwd_index.get(word, self.family.IF.Set())
		for docid in list(docids):
			ooset = self._rev_index[docid]
			if len(ooset) == 1:
				result.append(docid)
				self.unindex_doc(docid)
			else:
				ooset.remove(word)
		return result
	remove_word = removeWord

	def replaceWord(self, word, replacement):
		if word not in self._fwd_index or replacement in self._fwd_index:
			return None

		result = []
		docids = self._fwd_index.get(word)
		for docid in list(docids):
			result.append(docid)
			ooset = self._rev_index[docid]
			if word in ooset:
				ooset.remove(word)
			ooset.add(replacement)
		return result
	replace_word = replaceWord
