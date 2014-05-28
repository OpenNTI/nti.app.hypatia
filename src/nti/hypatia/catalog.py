#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
hypatia catalog

.. $Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import BTrees

from zope import interface

from hypatia.text import TextIndex
from hypatia.catalog import Catalog
from hypatia.field import FieldIndex
from hypatia.catalog import CatalogQuery
from hypatia.text.cosineindex import CosineIndex

from nti.contentsearch import discriminators
from nti.contentsearch.constants import (content_, ngrams_, title_, tags_, keywords_,
										 redactionExplanation_, replacementContent_,
										 createdTime_, lastModified_, type_, acl_,
										 creator_)

from .lexicon import defaultLexicon
from .keyword import SearchKeywordIndex
from .field import SearchTimeFieldIndex

from .interfaces import ISearchCatalog
from .interfaces import ISearchCatalogQuery

@interface.implementer(ISearchCatalog)
class SearchCatalog(Catalog):
	family = BTrees.family64

def get_type(obj, default=None):
	result = discriminators.get_type(obj, default)
	result = (result,) if result and result is not default else default
	return result

def create_catalog(lexicon=None, ngram_lexicon=None, family=BTrees.family64):
	lexicon = defaultLexicon() if lexicon is None else lexicon
	ngram_lexicon = lexicon if ngram_lexicon is None else ngram_lexicon

	result = SearchCatalog(family=family)

	result[type_] = SearchKeywordIndex(discriminator=get_type, family=family)

	index = CosineIndex(lexicon=lexicon, family=family)
	result[content_] = TextIndex(lexicon=lexicon,
								 index=index,
								 discriminator=discriminators.get_object_content,
								 family=family)

	index = CosineIndex(lexicon=ngram_lexicon, family=family)
	result[ngrams_] = TextIndex(lexicon=ngram_lexicon,
								index=index,
								discriminator=discriminators.get_object_ngrams,
								family=family)

	result[tags_] = SearchKeywordIndex(discriminator=discriminators.get_tags,
								 	   family=family)

	result[keywords_] = SearchKeywordIndex(discriminator=discriminators.get_keywords,
									 	   family=family)

	index = CosineIndex(lexicon=lexicon, family=family)
	result[title_] = TextIndex(lexicon=lexicon,
							   index=index,
							   discriminator=discriminators.get_title_and_ngrams,
							   family=family)

	index = CosineIndex(lexicon=lexicon, family=family)
	result[redactionExplanation_] = \
			TextIndex(lexicon=lexicon,
					  index=index,
					  discriminator=discriminators.get_redaction_explanation_and_ngrams,
					  family=family)

	index = CosineIndex(lexicon=lexicon, family=family)
	result[replacementContent_] = \
						TextIndex(lexicon=lexicon,
								  index=index,
								  discriminator=discriminators.get_replacement_content,
								  family=family)

	result[createdTime_] = SearchTimeFieldIndex(
									discriminator=discriminators.get_created_time,
								 	family=family)

	result[lastModified_] = SearchTimeFieldIndex(
									discriminator=discriminators.get_last_modified,
								 	family=family)

	result[creator_] = FieldIndex(discriminator=discriminators.get_creator,
								  family=family)
	
	result[acl_] = SearchKeywordIndex(discriminator=discriminators.get_acl,
									  family=family)

	return result

@interface.implementer(ISearchCatalogQuery)
class SearchCatalogQuery(CatalogQuery):
	
	def search(self, **query):
		numdocs, result = super(SearchCatalogQuery, self).search(**query)
		return numdocs, result
	
	def query(self, queryobject, sort_index=None, limit=None, sort_type=None,
              reverse=False, names=None):
		numdocs, result = super(SearchCatalogQuery, self).query(queryobject=queryobject,
																sort_index=sort_index,
																limit=limit,
																sort_type=sort_type,
																reverse=reverse,
																names=names)
		return numdocs, result
