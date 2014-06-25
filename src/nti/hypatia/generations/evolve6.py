#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
generation 6.

$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

generation = 6

from zope import component
from zope.component.hooks import site, setHooks

from nti.contentsearch.constants import (createdTime_, lastModified_)

from .. import interfaces as hypatia_interfaces

def do_evolve(context):
	setHooks()
	conn = context.connection
	root = conn.root()
	ds_folder = root['nti.dataserver']

	lsm = ds_folder.getSiteManager()

	with site(ds_folder):
		assert	component.getSiteManager() == ds_folder.getSiteManager(), \
				"Hooks not installed?"

		catalog = lsm.getUtility(provided=hypatia_interfaces.ISearchCatalog)
		if createdTime_ in catalog:
			del catalog[createdTime_]

		if lastModified_ in catalog:
			del catalog[lastModified_]

		logger.info('Hypatia evolution gen 6 done')

def evolve(context):
	"""
	Evolve to generation 6 by removing time indices
	"""
	do_evolve(context)
