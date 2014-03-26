# -*- coding: utf-8 -*-
"""
generation 4.

$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

generation = 4

import zope.intid

from zope import component
from zope.component.hooks import site, setHooks

from nti.contentsearch.constants import type_

from .. import utils
from .. import catalog as hypatia_catalog
from .. import interfaces as hypatia_interfaces

def do_evolve(context):
	setHooks()
	conn = context.connection
	root = conn.root()
	ds_folder = root['nti.dataserver']

	lsm = ds_folder.getSiteManager()
	intids = lsm.getUtility(zope.intid.IIntIds)

	with site(ds_folder):
		assert	component.getSiteManager() == ds_folder.getSiteManager(), \
				"Hooks not installed?"

		# unregister
		old_catalog = lsm.getUtility(provided=hypatia_interfaces.ISearchCatalog)
		intids.unregister(old_catalog)
		lsm.unregisterUtility(old_catalog, provided=hypatia_interfaces.ISearchCatalog)
		old_catalog.__parent__ = None

		# recreate
		new_catalog = hypatia_catalog.SearchCatalog()
		new_catalog.__parent__ = ds_folder
		new_catalog.__name__ = '++etc++hypatia++catalog'
		intids.register(new_catalog)
		lsm.registerUtility(new_catalog, provided=hypatia_interfaces.ISearchCatalog)

		# reset indices
		for k, v in old_catalog.items():
			new_catalog[k] = v

		# search anything that has not been indexed
		total = 0
		type_index = new_catalog[type_]
		search_queue = lsm.getUtility(provided=hypatia_interfaces.ISearchCatalogQueue)
		for iid, _ in utils.all_indexable_objects_iids():
			try:
				if not type_index.has_doc(iid):
					search_queue.add(iid)
					total += 1
			except TypeError:
				pass
		logger.info('Hypatia evolution gen 4 done; %s missing object(s) indexed', total)
		return total

def evolve(context):
	"""
	Evolve to generation 4 by recreating the catalog
	"""
	do_evolve(context)
