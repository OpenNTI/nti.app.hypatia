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

from nti.contentsearch import discriminators
from nti.contentsearch import search_results
from nti.contentsearch import interfaces as search_interfaces

from nti.dataserver import interfaces as nti_interfaces

from . import subscribers
from . import search_catalog
from . import interfaces as hypatia_interfaces

@component.adapter(nti_interfaces.IEntity)
@interface.implementer(search_interfaces.IEntityIndexManager)
class _HypatiaEntityIndexManager(object):

	@property
	def entity(self):
		return self.__parent__

	@property
	def username(self):
		return self.entity.username

	def get_object(self, uid):
		result = discriminators.query_object(uid)
		if result is None:
			logger.debug('Could not find object with id %r' % uid)
		return result

	def index_content(self, data, *args, **kwargs):
		subscribers.queue_added(data)
	
	def update_content(self, data, *args, **kwargs):
		subscribers.queue_modified(data)

	def delete_content(self, data, *args, **kwargs):
		subscribers.queue_remove(data)

	@metricmethod
	def do_search(self, query, creator_method=None):
		query = search_interfaces.ISearchQuery(query)
		creator_method = creator_method or search_results.empty_search_results
		results = creator_method(query)
		if query.is_empty:
			return results

		# parse catalog
		parser = component.getUtility(hypatia_interfaces.ISearchQueryParser, name=query.language)
		parsed_query = parser.parse(query, self.entity)
		cq = CatalogQuery(search_catalog())
		_, sequence = cq.query(parsed_query)

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
		pass

	def suggest_and_search(self, query):
		pass
