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

from nti.contentprocessing import rank_words

from nti.contentsearch import common
from nti.contentsearch import constants
from nti.contentsearch import discriminators
from nti.contentsearch import search_results
from nti.contentsearch import interfaces as search_interfaces

from nti.dataserver import interfaces as nti_interfaces

from . import search_queue
from . import search_catalog
from . import interfaces as hypatia_interfaces
from . import get_usernames_of_dynamic_memberships

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
		result = obj.isSharedDirectlyWith(self.entity)
		if not result:
			acl = set(discriminators.get_acl(obj, ()))
			memberships = set(get_usernames_of_dynamic_memberships(self.entity))
			result = memberships.intersection(acl)
		result = result and not nti_interfaces.IDeletedObjectPlaceholder.providedBy(obj)
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

	def index_content(self, data):
		um = search_interfaces.IRepozeEntityIndexManager(self.entity, None)
		if data is not None and um is not None:
			um.index_content(data)

	def update_content(self, data):
		um = search_interfaces.IRepozeEntityIndexManager(self.entity, None)
		if data is not None and um is not None:
			um.update_content(data)

	def delete_content(self, data):
		um = search_interfaces.IRepozeEntityIndexManager(self.entity, None)
		if data is not None and um is not None:
			um.delete_content(data)

	@metricmethod
	def do_search(self, query, results):
		query = search_interfaces.ISearchQuery(query)
		if query.is_empty:
			return results

		searchOn = set(query.searchOn or ())
		if searchOn and not searchOn.intersection(common.get_ugd_indexable_types()):
			return results

		# parse catalog
		parser = component.getUtility(hypatia_interfaces.ISearchQueryParser,
									  name=query.language)
		parsed_query = parser.parse(query, self.entity)

		cq = CatalogQuery(search_catalog())
		_, sequence = cq.query(parsed_query)
		if not hasattr(sequence, "items"):
			class _proxy(object):
				def __init__(self, seq):
					self._seq = seq
				def items(self):
					for x in self._seq:
						yield x, 1.0
			sequence = _proxy(sequence)

		# get docs from db
		for docid, score in sequence.items():
			obj = self.get_object(docid)
			if obj is not None:
				results.add(obj, score)
		return results

	def search(self, query, store=None, **kwargs):
		query = search_interfaces.ISearchQuery(query)
		store = search_results.get_or_create_search_results(query, store)
		return self.do_search(query, store)
		
	def suggest(self, query, store=None, **kwargs):
		query = search_interfaces.ISearchQuery(query)
		results = search_results.get_or_create_suggest_results(query, store)
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

	def suggest_and_search(self, query, store=None, **kwargs):
		query = search_interfaces.ISearchQuery(query)
		store = search_results.get_or_create_suggest_and_search_results(query, store)
		if ' ' in query.term or query.IsPrefixSearch or query.IsPhraseSearch:
			results = self.do_search(query, store)
		else:
			suggest_results = self.suggest(query)
			suggestions = suggest_results.Suggestions
			if suggestions:
				suggestions = rank_words(query.term, suggestions)
				query.term = suggestions[0]
			results = self.do_search(query, store)
		return results
