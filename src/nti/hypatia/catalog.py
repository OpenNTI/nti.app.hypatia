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
from hypatia.text.cosineindex import CosineIndex

from nti.contentsearch import discriminators
from nti.contentsearch.constants import (content_, ngrams_, title_, tags_, keywords_,
										 redactionExplanation_, replacementContent_,
										 createdTime_, lastModified_)

from nti.zodb.containers import time_to_64bit_int

from .lexicon import defaultLexicon
from .interfaces import ISearchCatalog
from .keyword import SearchKeywordIndex
from .field import SearchTimeFieldIndex

def get_type(obj, default=None):
	type_ = discriminators.get_type(obj, default)
	result = (type_,) if type_ else default
	return result

def get_created_time(obj, default=None):
	value = discriminators.get_created_time(obj, default)
	result = time_to_64bit_int(value) if value is not None else None
	return result

def get_last_modified(obj, default=None):
	value = discriminators.get_last_modified(obj, default)
	result = time_to_64bit_int(value) if value is not None else None
	return result

def create_catalog(lexicon=None, ngram_lexicon=None):
	
	family64 = BTrees.family64
	lexicon = defaultLexicon() if lexicon is None else lexicon
	ngram_lexicon = lexicon if ngram_lexicon is None else ngram_lexicon

	result = Catalog(family=family64)
	interface.alsoProvides(result, ISearchCatalog)

	result['type'] = SearchKeywordIndex(discriminator=get_type,
								  		family=family64)

	index = CosineIndex(lexicon=lexicon, family=family64)
	result[content_] = TextIndex(lexicon=lexicon,
								 index=index,
								 discriminator=discriminators.get_object_content,
								 family=family64)

	index = CosineIndex(lexicon=ngram_lexicon, family=family64)
	result[ngrams_] = TextIndex(lexicon=ngram_lexicon,
								index=index,
								discriminator=discriminators.get_object_ngrams,
								family=family64)

	result[tags_] = SearchKeywordIndex(discriminator=discriminators.get_tags,
								 	   family=family64)

	result[keywords_] = SearchKeywordIndex(discriminator=discriminators.get_keywords,
									 	   family=family64)

	index = CosineIndex(lexicon=lexicon, family=family64)
	result[title_] = TextIndex(lexicon=lexicon,
							   index=index,
							   discriminator=discriminators.get_title_and_ngrams,
							   family=family64)

	index = CosineIndex(lexicon=lexicon, family=family64)
	result[redactionExplanation_] = \
			TextIndex(lexicon=lexicon,
					  index=index,
					  discriminator=discriminators.get_redaction_explanation_and_ngrams,
					  family=family64)

	index = CosineIndex(lexicon=lexicon, family=family64)
	result[replacementContent_] = \
						TextIndex(lexicon=lexicon,
								  index=index,
								  discriminator=discriminators.get_replacement_content,
								  family=family64)

	result[createdTime_] = SearchTimeFieldIndex(discriminator=get_created_time,
								 	  			family=family64)

	result[lastModified_] = SearchTimeFieldIndex(discriminator=get_last_modified,
								 	   			 family=family64)

	result['acl'] = SearchKeywordIndex(discriminator=discriminators.get_acl,
								 	   family=family64)


	return result


