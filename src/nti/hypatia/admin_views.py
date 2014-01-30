#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
admin views.

$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import six
import time
import simplejson
import collections

from zope import component

from pyramid.view import view_config
from pyramid import httpexceptions as hexc

from nti.dataserver import users
from nti.dataserver import authorization as nauth
from nti.dataserver import interfaces as nti_interfaces

from nti.externalization.datastructures import LocatedExternalDict

from nti.utils.maps import CaseInsensitiveDict

from . import utils
from . import reactor
from . import LOCK_NAME
from . import subscribers
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

def _add_to_objects_2_queue(user):
	count = 0
	for obj in utils.get_user_indexable_objects(user):
		try:
			subscribers.add_2_queue(obj)
			count += 1
		except TypeError:  # ignore objects in queue
			pass
	return count

@view_config(route_name='objects.generic.traversal',
			 name='reindex_hypatia_content',
			 renderer='rest',
			 request_method='POST',
			 permission=nauth.ACT_MODERATE)
def reindex_hypatia_content(request):
	values = simplejson.loads(unicode(request.body, request.charset)) \
			 if request.body else {}
	values = CaseInsensitiveDict(**values)
	usernames = values.get('usernames')
	queue_limit = values.get('limit', None)
	term = values.get('term', values.get('search', None))
	if term:
		usernames = username_search(term)
	elif usernames and isinstance(usernames, six.string_types):
		usernames = usernames.split()
	else:
		dataserver = component.getUtility(nti_interfaces.IDataserver)
		_users = nti_interfaces.IShardLayout(dataserver).users_folder
		usernames = _users.keys()

	if queue_limit is not None:
		try:
			queue_limit = abs(int(queue_limit))
			assert queue_limit > 0
		except (ValueError, AssertionError):
			raise hexc.HTTPUnprocessableEntity('invalid queue size')

	now = time.time()
	counter = collections.defaultdict(int)
	for username in usernames or ():
		user = users.Entity.get_entity(username)
		if user is not None and nti_interfaces.IUser.providedBy(user):
			counter[username] = _add_to_objects_2_queue(user)

	if queue_limit is not None:
		reactor.process_queue(queue_limit)
		
	result = LocatedExternalDict()
	result['Items'] = dict(counter)
	result['Elapsed'] = time.time() - now
	result['Total'] = reduce(lambda x, y: x + y, counter.values(), 0)
	return result

@view_config(route_name='objects.generic.traversal',
			 name='process_hypatia_content',
			 renderer='rest',
			 request_method='POST',
			 permission=nauth.ACT_MODERATE)
def process_hypatia_content(request):
	values = simplejson.loads(unicode(request.body, request.charset)) \
			 if request.body else {}
	values = CaseInsensitiveDict(**values)
	queue_limit = values.get('limit', hypatia_interfaces.DEFAULT_QUEUE_LIMIT)
	try:
		queue_limit = abs(int(queue_limit))
		assert queue_limit > 0
	except (ValueError, AssertionError):
		raise hexc.HTTPUnprocessableEntity('invalid queue size')

	reactor.process_queue(queue_limit)

	return hexc.HTTPNoContent()

@view_config(route_name='objects.generic.traversal',
			 name='force_delete_hypatia_lock',
			 renderer='rest',
			 request_method='POST',
			 permission=nauth.ACT_MODERATE)
def force_delete_hypatia_lock(request):
	redis = component.getUtility(nti_interfaces.IRedisClient)
	redis.delete(LOCK_NAME)
	return hexc.HTTPNoContent()
