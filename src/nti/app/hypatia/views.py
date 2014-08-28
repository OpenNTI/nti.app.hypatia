#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import six
import time
import simplejson

from zope import component
from zope import interface
from zope.container.contained import Contained
from zope.traversing.interfaces import IPathAdapter

from pyramid.view import view_config
from pyramid import httpexceptions as hexc

from nti.contentsearch.constants import type_

from nti.dataserver import authorization as nauth
from nti.dataserver.interfaces import IDataserver
from nti.dataserver.interfaces import IShardLayout

from nti.externalization.interfaces import LocatedExternalDict

from nti.hypatia import search_queue
from nti.hypatia import search_catalog
from nti.hypatia.reactor import process_queue
from nti.hypatia.utils import all_cataloged_objects
from nti.hypatia.interfaces import DEFAULT_QUEUE_LIMIT
from nti.hypatia.utils import all_indexable_objects_iids

from nti.utils.maps import CaseInsensitiveDict

@interface.implementer(IPathAdapter)
class HypatiaPathAdapter(Contained):

	__name__ = 'hypatia'

	def __init__(self, context, request):
		self.context = context
		self.request = request
		self.__parent__ = context

def _make_min_max_btree_range(search_term):
	min_inclusive = search_term  # start here
	max_exclusive = search_term[0:-1] + unichr(ord(search_term[-1]) + 1)
	return min_inclusive, max_exclusive

def is_true(s):
	return s and str(s).lower() in ('1', 'true', 't', 'yes', 'y', 'on')

def username_search(search_term):
	min_inclusive, max_exclusive = _make_min_max_btree_range(search_term)
	dataserver = component.getUtility(IDataserver)
	_users = IShardLayout(dataserver).users_folder
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
			 context=HypatiaPathAdapter,
			 permission=nauth.ACT_MODERATE)
def reindex_content_view(request):
	values = readInput(request)
	usernames = values.get('usernames')
	queue_limit = values.get('limit', None)
	term = values.get('term', values.get('search', None))
	
	# missing flag
	missing = values.get('onlyMissing') or values.get('missing') or u''
	missing = is_true(missing)
	
	# user search
	if term:
		usernames = username_search(term)
	elif usernames and isinstance(usernames, six.string_types):
		usernames = usernames.split(',')
	else:
		usernames = ()  # ALL

	# queue limit
	if queue_limit is not None:
		try:
			queue_limit = int(queue_limit)
			assert queue_limit > 0 or queue_limit == -1
		except (ValueError, AssertionError):
			raise hexc.HTTPUnprocessableEntity('invalid queue size')

	total = 0
	now = time.time()
	type_index = search_catalog()[type_] if missing else None

	generator = all_cataloged_objects(usernames) \
				if missing else all_indexable_objects_iids(usernames)

	queue = search_queue()
	for iid, _ in generator:
		try:
			if not missing or not type_index.has_doc(iid):
				queue.add(iid)
				total += 1
		except TypeError:
			pass

	if queue_limit is not None:
		process_queue(queue_limit)
		
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
			 context=HypatiaPathAdapter,
			 permission=nauth.ACT_MODERATE)
def process_queue_view(request):
	values = readInput(request)
	limit = values.get('limit', DEFAULT_QUEUE_LIMIT)
	try:
		limit = int(limit)
		assert limit > 0 or limit == -1
	except (ValueError, AssertionError):
		raise hexc.HTTPUnprocessableEntity('invalid limit size')

	now = time.time()
	total = process_queue(limit)
	result = LocatedExternalDict()
	result['Elapsed'] = time.time() - now
	result['Total'] = total
	return result

@view_config(route_name='objects.generic.traversal',
			 name='empty_queue',
			 renderer='rest',
			 request_method='POST',
			 context=HypatiaPathAdapter,
			 permission=nauth.ACT_MODERATE)
def empty_queue_view(request):
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
			 context=HypatiaPathAdapter,
			 permission=nauth.ACT_MODERATE)
def queue_info_view(request):
	catalog_queue = search_queue()
	result = LocatedExternalDict()
	result['QueueLength'] = len(catalog_queue)
	result['EventQueueLength'] = catalog_queue.eventQueueLength()
	return result

@view_config(route_name='objects.generic.traversal',
			 name='sync_queue',
			 renderer='rest',
			 request_method='POST',
			 context=HypatiaPathAdapter,
			 permission=nauth.ACT_MODERATE)
def sync_queue_view(request):
	catalog_queue = search_queue()
	if catalog_queue.syncQueue():
		logger.info("Queue synched")
	return hexc.HTTPNoContent()
