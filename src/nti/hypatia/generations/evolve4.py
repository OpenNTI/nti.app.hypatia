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

		logger.info('Hypatia evolution gen 4 done')

def evolve(context):
	"""
	Evolve to generation 4 by recreating the catalog
	"""
	do_evolve(context)
