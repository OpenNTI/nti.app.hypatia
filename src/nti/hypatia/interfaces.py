#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
hypatia interfaces

$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

from zope import interface

from zc.catalogqueue.interfaces import ICatalogQueue

from hypatia import interfaces as hypatia_interfaces
from hypatia.text import interfaces as text_interfaces

from nti.contentsearch import interfaces as search_interfaces

DEFAULT_HEARTBEAT = 10
DEFAULT_QUEUE_LIMIT = 10

class IIndexReactor(interface.Interface):
    """
    marker interface for a reactor
    """

class ISearchCatalogQueue(ICatalogQueue):
    pass

class ISearchCatalog(hypatia_interfaces.ICatalog):
    pass

class ISearchLexicon(text_interfaces.ILexicon):

    def get_similiar_words(term, threshold=0.75, common_length=-1):
        """
        return a list of similar words based on the levenshtein distance
        """

class ISearchQueryParser(search_interfaces.ISearchQueryParser):

    def parse(query, user=None):
        pass
