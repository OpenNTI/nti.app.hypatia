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

def defaultLexicon():
	result = Lexicon(Splitter(), CaseNormalizer(), StopWordRemover())
	return result
