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

from hypatia.keyword import KeywordIndex

from .interfaces import ISearchKeywordIndex

@interface.implementer(ISearchKeywordIndex)
class SearchKeywordIndex(KeywordIndex):

	def get_words(self, docid):
		result = ()
		if docid in self._rev_index:
			result = tuple(self._rev_index[docid])
		return result

