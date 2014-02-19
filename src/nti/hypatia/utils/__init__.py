#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
hypatia utils

$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import zope.intid
from zope import component

from nti.externalization.oids import to_external_oid

from nti.hypatia import is_indexable

def all_indexable_objects_iids(users=()):
	intids = component.getUtility(zope.intid.IIntIds)
	usernames = {getattr(user, 'username', user) for user in users or ()}
	for uid, obj in intids.items():
		try:
			creator = getattr(obj, 'creator', None)
			if 	is_indexable(obj) and \
				(not usernames or getattr(creator, 'username', creator) in usernames):
				yield uid
		except TypeError as e:
			oid = to_external_oid(obj)
			logger.error("Error getting creator for %s(%s,%s); %s",
						 type(obj), uid, oid, e)
