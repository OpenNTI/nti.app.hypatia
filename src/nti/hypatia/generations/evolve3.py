#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
generation 3.

$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

generation = 3

import BTrees

from zope import component
from zope.component.hooks import site, setHooks

from hypatia.field import FieldIndex

from nti.contentsearch import discriminators
from nti.contentsearch.constants import creator_

from .. import utils
from .. import interfaces as hypatia_interfaces

def do_evolve(context):
	setHooks()
	conn = context.connection
	root = conn.root()
	ds_folder = root['nti.dataserver']

	lsm = ds_folder.getSiteManager()
	catalog  = lsm.getUtility(provided=hypatia_interfaces.ISearchCatalog)
		
	if creator_ in catalog:
		return 0

	total = 0
	with site(ds_folder):
		assert	component.getSiteManager() == ds_folder.getSiteManager(), \
				"Hooks not installed?"

		index = FieldIndex(discriminator=discriminators.get_creator,
						   family=BTrees.family64)
		catalog[creator_] = index

		logger.info('Hypatia evolution gen 3 started')

		for iid, obj in utils.all_indexable_objects_iids():
			index.index_doc(iid, obj)
			total += 1

		logger.info('Hypatia evolution gen 3 done; %s object(s) processed' % total)

	return total

def evolve(context):
	"""
	Evolve to generation 3 by indexing the creator field
	"""
	do_evolve(context)
