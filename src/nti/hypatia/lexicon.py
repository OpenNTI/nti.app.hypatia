#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
hypatia lexicon

$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from zope import interface
from zope import component

from hypatia.text import interfaces as text_interfaces
from hypatia.text.lexicon import Lexicon, CaseNormalizer, Splitter

from nti.contentsearch import interfaces as search_interfaces

from .levenshtein import ratio
from . import interfaces as hypatia_interfaces

@interface.implementer(text_interfaces.IPipelineElement)
class StopWordRemover(object):

	def stopwords(self):
		util = component.queryUtility(search_interfaces.IStopWords)
		return util.stopwords() if util is not None else ()

	def process(self, lst):
		stopwords = self.stopwords()
		if stopwords:
			return [w for w in lst if w not in stopwords]
		return lst

@interface.implementer(hypatia_interfaces.ISearchLexicon)
class SearchLexicon(Lexicon):

	def get_similiar_words(self, term, threshold=0.75, common_length=-1):
		if common_length > -1:
			prefix = term[:common_length]
			words = self._wids.keys(prefix, prefix + u'\uffff')
		else:
			words = self.words()
		return [(w, ratio(w, term)) for w in words if ratio(w, term) > threshold]

	getSimiliarWords = get_similiar_words

def defaultLexicon():
	result = SearchLexicon(Splitter(), CaseNormalizer(), StopWordRemover())
	return result