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

from nti.contentsearch import interfaces as search_interfaces

from nti.dataserver import users
from nti.dataserver import interfaces as nti_interfaces

from . import interfaces

LOCK_NAME = u"nti/hypatia/lock"

def search_queue():
    result = component.getUtility(interfaces.ISearchCatalogQueue)
    return result

def search_catalog():
    result = component.getUtility(interfaces.ISearchCatalog)
    return result

def get_user(user):
    user = users.User.get_user(str(user)) \
           if not nti_interfaces.IUser.providedBy(user) and user else user
    return user

def get_usernames_of_dynamic_memberships(user):
    user  = get_user(user)
    dynamic_memberships = getattr(user, 'usernames_of_dynamic_memberships', ())
    usernames = (user.username,) + tuple(dynamic_memberships)
    result = [x.lower() for x in usernames]
    return result

def is_indexable(x):
    return  nti_interfaces.IModeledContent.providedBy(x) and \
            search_interfaces.ITypeResolver(x, None) is not None
