# -*- coding: utf-8 -*-
"""
schema generation installation.

$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

generation = 6

from zope.generations.generations import SchemaManager

import zope.intid

from .. import queue as hypatia_queue
from .. import catalog as hypatia_catalog
from .. import lexicon as hypatia_lexicon
from .. import interfaces as hypatia_interfaces

from . import evolve2

class _HypatiaSearchSchemaManager(SchemaManager):
	"""
	A schema manager that we can register as a utility in ZCML.
	"""
	def __init__(self):
		super(_HypatiaSearchSchemaManager, self).__init__(
												generation=generation,
												minimum_generation=generation,
												package_name='nti.hypatia.generations')
def evolve(context):
	# ### from IPython.core.debugger import Tracer; Tracer()()
	install_queue(context)
	install_hypatia(context)
	evolve2.do_evolve(context, False)  # go to version 2

def install_hypatia(context):
	conn = context.connection
	root = conn.root()

	dataserver_folder = root['nti.dataserver']
	lsm = dataserver_folder.getSiteManager()
	intids = lsm.getUtility(zope.intid.IIntIds)

	lexicon = hypatia_lexicon.defaultLexicon()

	catalog = hypatia_catalog.create_catalog(lexicon)
	catalog.__parent__ = dataserver_folder
	catalog.__name__ = '++etc++hypatia++catalog'
	intids.register(catalog)
	lsm.registerUtility(catalog, provided=hypatia_interfaces.ISearchCatalog)

	return catalog

def install_queue(context):
	conn = context.connection
	root = conn.root()

	dataserver_folder = root['nti.dataserver']
	lsm = dataserver_folder.getSiteManager()
	intids = lsm.getUtility(zope.intid.IIntIds)

	queue = hypatia_queue.SearchCatalogQueue()
	queue.__parent__ = dataserver_folder
	queue.__name__ = '++etc++hypatia++catalogqueue'
	intids.register(queue)
	lsm.registerUtility(queue, provided=hypatia_interfaces.ISearchCatalogQueue)

	return queue
