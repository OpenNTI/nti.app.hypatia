#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
hypatia catalog

$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import BTrees

from zope import interface

from hypatia.text import TextIndex
from hypatia.catalog import Catalog
from hypatia.keyword import KeywordIndex
from hypatia.text.cosineindex import CosineIndex

from nti.contentsearch import discriminators

from .lexicon import defaultLexicon
from .interfaces import ISearchCatalog

def create_catalog(lexicon=None):
	
	lexicon = defaultLexicon() if lexicon is None else lexicon

	result = Catalog(family=BTrees.family64)
	interface.alsoProvides(result, ISearchCatalog)

	index = CosineIndex(lexicon=lexicon, family=BTrees.family64)
	result['content'] = TextIndex(lexicon=lexicon,
								  index=index,
								  discriminator=discriminators.get_object_content,
								  family=BTrees.family64)

	index = CosineIndex(lexicon=lexicon, family=BTrees.family64)
	result['ngrams'] = TextIndex(lexicon=lexicon,
								 index=index,
								 discriminator=discriminators.get_object_ngrams,
								 family=BTrees.family64)

	index = CosineIndex(lexicon=lexicon, family=BTrees.family64)
	result['title'] = TextIndex(lexicon=lexicon,
								index=index,
								discriminator=discriminators.get_title_and_ngrams,
								family=BTrees.family64)

	index = CosineIndex(lexicon=lexicon, family=BTrees.family64)
	result['redactionExplanation'] = \
			TextIndex(lexicon=lexicon,
					  index=index,
					  discriminator=discriminators.get_redaction_explanation_and_ngrams,
					  family=BTrees.family64)

	index = CosineIndex(lexicon=lexicon, family=BTrees.family64)
	result['replacementContent'] = \
						TextIndex(lexicon=lexicon,
								  index=index,
								  discriminator=discriminators.get_replacement_content,
								  family=BTrees.family64)

	result['acl'] = KeywordIndex(discriminator=discriminators.get_acl,
								 family=BTrees.family64)


	return result
