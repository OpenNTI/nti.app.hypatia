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
from zope.catalog.interfaces import ICatalog

from ZODB.POSException import POSKeyError

from nti.dataserver.metadata_index import CATALOG_NAME as METADATA_CATALOG_NAME

from nti.externalization.oids import to_external_oid

from nti.hypatia import is_indexable

def all_indexable_objects_iids(users=()):
	obj = None
	intids = component.getUtility(zope.intid.IIntIds)
	usernames = {getattr(user, 'username', user).lower() for user in users or ()}
	for uid in intids:
		try:
			obj = intids.getObject(uid)
			creator = getattr(obj, 'creator', None) or u''
			username = getattr(creator, 'username', creator).lower()
			if is_indexable(obj) and (not usernames or username in usernames):
				yield uid, obj
		except (POSKeyError, TypeError) as e:
			logger.error("Ignoring %s(%s); %s", type(obj), uid, e)


def all_cataloged_objects(users=()):
	obj = None
	intids = component.getUtility(zope.intid.IIntIds)
	catalog = component.getUtility(ICatalog, METADATA_CATALOG_NAME)
	usernames = {getattr(user, 'username', user).lower() for user in users or ()}
	if usernames:
		intids_created_by = catalog['creator'].apply({'any_of': usernames})
	else:
		intids_created_by = catalog['creator'].ids()

	for uid in intids_created_by:
		try:
			obj = intids.getObject(uid)
			getattr(obj, 'creator', None)
			yield uid, None
		except (POSKeyError, TypeError) as e:
			logger.error("Ignoring %s(%s); %s", type(obj), uid, e)
