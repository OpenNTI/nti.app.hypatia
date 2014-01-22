# -*- coding: utf-8 -*-
"""
generation 2.

$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

generation = 2

import zope.intid
from zope import component
from zope.component.hooks import site, setHooks

from zc import intid as zc_intid

from .. import utils
from .. import subscribers

def do_evolve(context):
	setHooks()
	conn = context.connection
	root = conn.root()
	ds_folder = root['nti.dataserver']
	lsm = ds_folder.getSiteManager()

	ds_intid = lsm.getUtility(provided=zope.intid.IIntIds)
	component.provideUtility(ds_intid, zope.intid.IIntIds)
	component.provideUtility(ds_intid, zc_intid.IIntIds)

	logger.info('Hypatia evolution started')

	with site(ds_folder):
		assert	component.getSiteManager() == ds_folder.getSiteManager(), \
				"Hooks not installed?"

		count = 0
		users = ds_folder['users']
		for user in users.values():
			for obj in utils.get_user_indexable_objects(user):
				try:
					subscribers.add_2_queue(obj)
					count += 1
				except TypeError:  # ignore objects in queue
					pass

	logger.info('Evolution done!!! %s objects added to search queue' % count)

def evolve(context):
	"""
	Evolve to generation 2 by adding all objects to index queue
	"""
	do_evolve(context)
