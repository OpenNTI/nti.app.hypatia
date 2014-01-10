#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
hypatia query

$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from zope import interface

from hypatia.query import Any
from hypatia.query import Contains

from nti.contentsearch import content_utils
from nti.contentsearch import interfaces as search_interfaces
from nti.contentsearch.constants import (content_, ngrams_, title_, tags_, keywords_,
										 redactionExplanation_, replacementContent_)

from . import get_user
from . import search_catalog
from . import interfaces as hypatia_interfaces
from . import get_usernames_of_dynamic_memberships

def can_use_ngram_field(query):
	return content_utils.is_covered_by_ngram_computer(query.term, query.language)

@interface.implementer(hypatia_interfaces.ISearchQueryParser)
class _DefaultQueryParser(object):

	def parse(self, query, user=None):
		query = search_interfaces.ISearchQuery(query)
		term = query.term.lower()

		catalog = search_catalog()
		# type
		if query.searchOn:
			type_query = Any(catalog["type"], [x.lower() for x in query.searchOn])
		else:
			type_query = None

		# tags & keywords
		result = Any(catalog[tags_], [term]) | Any(catalog[keywords_], [term])

		fields = (title_, redactionExplanation_, replacementContent_)
		if 	query.is_prefix_search or query.is_phrase_search or \
			not can_use_ngram_field(query):
			fields += (content_,)
		else:
			fields += (ngrams_,)

		for field in fields:
			result = result | Contains(catalog[field], term)

		if type_query is not None:
			result = result & type_query

		user = get_user(user)
		if user:
			usernames = get_usernames_of_dynamic_memberships(user)
			result = result & Any(catalog["acl"], usernames)

		return result
