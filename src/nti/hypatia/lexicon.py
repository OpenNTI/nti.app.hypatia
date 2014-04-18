#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
.. $Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from zope import interface

from hypatia.text import interfaces as text_interfaces
from hypatia.text.lexicon import Lexicon, CaseNormalizer, Splitter

from . import levenshtein
from . import interfaces as hypatia_interfaces

@interface.implementer(text_interfaces.IPipelineElement)
class StopWordRemover(object):
	"""
	deprecated pipeline. Stop word removal is handled in
	the nti.contentsearch package
	"""
	def stopwords(self):
		return ()

	def process(self, words):
		return words

@interface.implementer(hypatia_interfaces.ISearchLexicon)
class SearchLexicon(Lexicon):

	def get_similiar_words(self, term, threshold=0.75, common_length=-1):
		if common_length > -1:
			prefix = term[:common_length]
			words = self._wids.keys(prefix, prefix + u'\uffff')
		else:
			words = self.words()
		for word in words:
			ratio = levenshtein.ratio(word, term)
			if ratio > threshold:
				yield (word, ratio)

	getSimiliarWords = get_similiar_words

def defaultLexicon():
	result = SearchLexicon(Splitter(), CaseNormalizer(), StopWordRemover())
	return result
