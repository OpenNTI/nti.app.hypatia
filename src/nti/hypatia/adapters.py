#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
hypatia adapters

$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from zope import component
from zope import interface

from perfmetrics import metricmethod

from hypatia.catalog import CatalogQuery

from nti.chatserver import interfaces as chat_interfaces

from nti.contentprocessing import rank_words

from nti.contentsearch import constants
from nti.contentsearch import discriminators
from nti.contentsearch import search_results
from nti.contentsearch import interfaces as search_interfaces

from nti.dataserver import interfaces as nti_interfaces

from . import search_queue
from . import search_catalog
from . import interfaces as hypatia_interfaces

@component.adapter(nti_interfaces.IUser)
@interface.implementer(search_interfaces.IEntityIndexController)
class _HypatiaUserIndexController(object):

	__slots__ = ('entity',)

	def __init__(self, entity):
		self.entity = entity

	@property
	def username(self):
		return self.entity.username

	def verify_access(self, obj):
		result = chat_interfaces.IMessageInfo.providedBy(obj) or \
				 (nti_interfaces.IShareableModeledContent.providedBy(obj) and \
				  obj.isSharedDirectlyWith(self.entity))

		if not result:
			acl = discriminators.get_acl(obj, ())
			result = self.username in acl

		return result

	def get_object(self, uid):
		result = discriminators.query_object(uid)
		if result is None:
			logger.debug('Could not find object with id %r' % uid)
			try:
				search_queue().remove(uid)
			except TypeError:
				pass
		elif not self.verify_access(result):
			result = None
		return result

	def index_content(self, *args, **kwargs):
		pass
	
	def update_content(self, *args, **kwargs):
		pass

	def delete_content(self, *args, **kwargs):
		pass

	@metricmethod
	def do_search(self, query, creator_method=None):
		query = search_interfaces.ISearchQuery(query)
		creator_method = creator_method or search_results.empty_search_results
		results = creator_method(query)
		if query.is_empty:
			return results

		# parse catalog
		parser = component.getUtility(hypatia_interfaces.ISearchQueryParser,
									  name=query.language)
		parsed_query = parser.parse(query, self.entity)

		cq = CatalogQuery(search_catalog())
		_, sequence = cq.query(parsed_query)
		if not hasattr(sequence, "items"):
			sequence = {x:1.0 for x in sequence}

		# get docs from db
		for docid, score in sequence.items():
			obj = self.get_object(docid)
			if obj is not None:
				results.add(search_results.IndexHit(docid, score))

		return results

	def search(self, query):
		query = search_interfaces.ISearchQuery(query)
		return self.do_search(query)
		
	def suggest(self, query):
		query = search_interfaces.ISearchQuery(query)
		results = search_results.empty_suggest_results(query)
		if query.is_empty:
			return results

		threshold = query.threshold
		prefix = query.prefix or len(query.term)
		textfield = search_catalog()[constants.content_]  # lexicon is shared
		
		words = textfield.lexicon.get_similiar_words(term=query.term,
												  	 threshold=threshold,
												 	 common_length=prefix)
		results.add(map(lambda t: t[0], words))

		return results

	def suggest_and_search(self, query):
		query = search_interfaces.ISearchQuery(query)
		if ' ' in query.term or query.is_prefix_search or query.is_phrase_search:
			results = \
				self.do_search(query,
							   search_results.empty_suggest_and_search_results)
		else:
			result = self.suggest(query)
			suggestions = result.suggestions
			if suggestions:
				suggestions = rank_words(query.term, suggestions)
				query.term = suggestions[0]

			results = \
				self.do_search(query,
							   search_results.empty_suggest_and_search_results)

		return results

