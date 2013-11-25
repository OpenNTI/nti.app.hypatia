#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
graphdb db utils

$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from zope import interface

from .. import interfaces as graph_interfaces

@interface.implementer(graph_interfaces.IUniqueAttributeAdapter)
class UniqueAttribute(object):

    __slots__ = ('key', 'value')

    def __init__(self, key, value):
        self.key = key
        self.value = value

    def __str__(self):
        return "(%s,%s)" % (self.key, self.value)

    def __repr__(self):
        return "%s(%s,%s)" % (self.__class__.__name__, self.key, self.value)

    def __eq__(self, other):
        try:
            return self is other or (self.key == other.key
                                     and self.value == other.value)
        except AttributeError:
            return NotImplemented

    def __hash__(self):
        xhash = 47
        xhash ^= hash(self.key)
        xhash ^= hash(self.value)
        return xhash
