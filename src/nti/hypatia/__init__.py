#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
hypatia module

$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from zope import component

from .  import interfaces

def queue():
    result = component.getUtility(interfaces.ISearchCatalogQueue)
    return result
