#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
admin views.

.. $Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import six
import time
import simplejson

from zope import component

from pyramid.view import view_config
from pyramid import httpexceptions as hexc

from nti.contentsearch.constants import type_

from nti.dataserver import authorization as nauth
from nti.dataserver import interfaces as nti_interfaces

from nti.externalization.interfaces import LocatedExternalDict

from nti.utils.maps import CaseInsensitiveDict

from . import utils
from . import views
from . import reactor
from . import search_queue
from . import search_catalog
from . import interfaces as hypatia_interfaces

def _make_min_max_btree_range(search_term):
	min_inclusive = search_term  # start here
	max_exclusive = search_term[0:-1] + unichr(ord(search_term[-1]) + 1)
	return min_inclusive, max_exclusive

def username_search(search_term):
	min_inclusive, max_exclusive = _make_min_max_btree_range(search_term)
	dataserver = component.getUtility(nti_interfaces.IDataserver)
	_users = nti_interfaces.IShardLayout(dataserver).users_folder
	usernames = list(_users.iterkeys(min_inclusive, max_exclusive, excludemax=True))
	return usernames

def readInput(request):
	body = request.body
	result = CaseInsensitiveDict()
	if body:
		try:
			values = simplejson.loads(unicode(body, request.charset))
		except UnicodeError:
			values = simplejson.loads(unicode(body, 'iso-8859-1'))
		result.update(**values)
	return result

@view_config(route_name='objects.generic.traversal',
			 name='reindex_content',
			 renderer='rest',
			 request_method='POST',
			 context=views.HypatiaPathAdapter,
			 permission=nauth.ACT_MODERATE)
def reindex_content(request):
	values = readInput(request)
	usernames = values.get('usernames')
	queue_limit = values.get('limit', None)
	term = values.get('term', values.get('search', None))
	missing = values.get('onlyMissing', values.get('missing', None)) or u''
	missing = str(missing).lower() in ('1', 'true', 't', 'yes', 'y', 'on')
	if term:
		usernames = username_search(term)
	elif usernames and isinstance(usernames, six.string_types):
		usernames = usernames.split(',')
	else:
		usernames = ()  # ALL

	if queue_limit is not None:
		try:
			queue_limit = int(queue_limit)
			assert queue_limit > 0 or queue_limit == -1
		except (ValueError, AssertionError):
			raise hexc.HTTPUnprocessableEntity('invalid queue size')

	total = 0
	now = time.time()
	type_index = search_catalog()[type_] if missing else None

	generator = utils.all_cataloged_objects(usernames) \
			    if missing else utils.all_indexable_objects_iids(usernames)

	for iid, _ in generator:
		try:
			if not missing or not type_index.has_doc(iid):
				search_queue().add(iid)
				total += 1
		except TypeError:
			pass

	if queue_limit is not None:
		reactor.process_queue(queue_limit)
		
	elapsed = time.time() - now
	result = LocatedExternalDict()
	result['Elapsed'] = elapsed
	result['Total'] = total

	logger.info("%s object(s) processed in %s(s)", total, elapsed)
	return result

@view_config(route_name='objects.generic.traversal',
			 name='process_queue',
			 renderer='rest',
			 request_method='POST',
			 context=views.HypatiaPathAdapter,
			 permission=nauth.ACT_MODERATE)
def process_queue(request):
	values = readInput(request)
	limit = values.get('limit', hypatia_interfaces.DEFAULT_QUEUE_LIMIT)
	try:
		limit = int(limit)
		assert limit > 0 or limit == -1
	except (ValueError, AssertionError):
		raise hexc.HTTPUnprocessableEntity('invalid limit size')

	now = time.time()
	total = reactor.process_queue(limit)
	result = LocatedExternalDict()
	result['Elapsed'] = time.time() - now
	result['Total'] = total
	return result

@view_config(route_name='objects.generic.traversal',
			 name='empty_queue',
			 renderer='rest',
			 request_method='POST',
			 context=views.HypatiaPathAdapter,
			 permission=nauth.ACT_MODERATE)
def empty_queue(request):
	values = readInput(request)
	limit = values.get('limit', -1)
	try:
		limit = int(limit)
		assert limit > 0 or limit == -1
	except (ValueError, AssertionError):
		raise hexc.HTTPUnprocessableEntity('invalid limit size')

	catalog_queue = search_queue()
	catalog_queue.syncQueue()

	length = len(catalog_queue)
	limit = length if limit == -1 else min(length, limit)

	done = 0
	now = time.time()
	for queue in catalog_queue:
		for _, _ in queue.process(limit - done).iteritems():
			done += 1
	catalog_queue.changeLength(-done)
	catalog_queue.syncQueue()

	result = LocatedExternalDict()
	result['Elapsed'] = time.time() - now
	result['Total'] = done
	return result

@view_config(route_name='objects.generic.traversal',
			 name='queue_info',
			 renderer='rest',
			 request_method='GET',
			 context=views.HypatiaPathAdapter,
			 permission=nauth.ACT_MODERATE)
def queue_info(request):
	catalog_queue = search_queue()
	result = LocatedExternalDict()
	result['QueueLength'] = len(catalog_queue)
	result['EventQueueLength'] = catalog_queue.eventQueueLength()
	return result

@view_config(route_name='objects.generic.traversal',
			 name='sync_queue',
			 renderer='rest',
			 request_method='POST',
			 context=views.HypatiaPathAdapter,
			 permission=nauth.ACT_MODERATE)
def sync_queue(request):
	catalog_queue = search_queue()
	if catalog_queue.syncQueue():
		logger.info("Queue synched")
	return hexc.HTTPNoContent()
