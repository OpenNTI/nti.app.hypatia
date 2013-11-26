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
