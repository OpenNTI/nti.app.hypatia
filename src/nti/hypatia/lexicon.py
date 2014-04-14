#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
.. $Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from zope import interface

from hypatia.text.lexicon import Lexicon, CaseNormalizer, Splitter

from . import levenshtein
from . import interfaces as hypatia_interfaces

@interface.implementer(hypatia_interfaces.ISearchLexicon)
class SearchLexicon(Lexicon):

	def get_similiar_words(self, term, threshold=0.75, common_length=-1):
		if common_length > -1:
			prefix = term[:common_length]
			words = self._wids.keys(prefix, prefix + u'\uffff')
		else:
			words = self.words()
		for w in words:
			r = levenshtein.ratio(w, term)
			if r > threshold:
				yield (w, r)

	getSimiliarWords = get_similiar_words

def defaultLexicon():
	result = SearchLexicon(Splitter(), CaseNormalizer())
	return result
