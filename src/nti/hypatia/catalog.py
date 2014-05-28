#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
hypatia catalog

.. $Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import sys
from zope import component
from zope import interface

from zope.catalog.interfaces import ICatalog as ZOPE_ICATALOG

import BTrees

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

from nti.dataserver import metadata_index

from nti.utils.property import Lazy

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


class _proxy(object):
	
	__slots__ = ('_seq',)
	
	def __init__(self, seq):
		self._seq = seq
		
	def items(self):
		for x in self._seq:
			yield x, 1.0

def to_proxy(source):
	source = _proxy(source) if not hasattr(source, "items") else source
	return source

def to_map(source):
	source = to_proxy(source)
	result = {x:y for x, y in source.items()}
	return result

@interface.implementer(ISearchCatalogQuery)
class SearchCatalogQuery(CatalogQuery):
	
	family = BTrees.family64

	def __init__(self, catalog, search_query, family=None):
		super(SearchCatalogQuery, self).__init__(catalog, family)
		self.search_query = search_query
		
	@Lazy
	def metadata(self):
		return component.getUtility(ZOPE_ICATALOG, name=metadata_index.CATALOG_NAME)

	def _query_index(self, index, dateRange, mapped):
		# query meta-data index
		startTime = dateRange.startTime or 0
		endTime = dateRange.endTime or sys.maxint
		docs = index.apply({'between': (startTime, endTime, True, True)})

		# intersect result documents with previous docs
		keys = self.family.IF.LFSet(mapped.iterkeys())
		intersected = self.family.IF.intersection(docs, keys)

		# return new result map (docid, score)
		mapped = {x:mapped[x] for x in intersected} if intersected else {}
		numdocs = len(mapped)
		return numdocs, mapped

	def _time_prune(self, numdocs, result):
		creationTime = self.search_query.creationTime
		modificationTime = self.search_query.modificationTime

		if creationTime is not None or modificationTime is not None:
			# if sort_index is used the sort order is lost
			result = to_map(result)
			if creationTime is not None:
				index = self.metadata[metadata_index.IX_CREATEDTIME]
				numdocs, result = self._query_index(index, creationTime, result)

			if modificationTime is not None:
				index = self.metadata[metadata_index.IX_LASTMODIFIED]
				numdocs, result = self._query_index(index, modificationTime, result)
		else:
			result = to_proxy(result)

		return numdocs, result

	def search(self, **query):
		numdocs, result = super(SearchCatalogQuery, self).search(**query)
		return self._time_prune(numdocs, result)
	
	def query(self, queryobject, sort_index=None, limit=None, sort_type=None,
              reverse=False, names=None):
		numdocs, result = super(SearchCatalogQuery, self).query(queryobject=queryobject,
																sort_index=sort_index,
																limit=limit,
																sort_type=sort_type,
																reverse=reverse,
																names=names)

		return self._time_prune(numdocs, result)
