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

from zope import component

from pyramid.view import view_config
from pyramid import httpexceptions as hexc

from nti.dataserver import users
from nti.dataserver import authorization as nauth
from nti.dataserver import interfaces as nti_interfaces

from nti.externalization.datastructures import LocatedExternalDict

from nti.utils.maps import CaseInsensitiveDict

from .. import reactor
from .. import subscribers
from . import get_user_indexable_objects
from .. import interfaces as hypatia_interfaces

def _add_to_objects_2_queue(user):
	count = 0
	for obj in get_user_indexable_objects(user):
		try:
			subscribers.queue_added(obj)
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
	if usernames:
		if isinstance(usernames, six.string_types):
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

	counter = 0
	now = time.time()
	for username in usernames or ():
		user = users.Entity.get_entity(username)
		if user is not None and nti_interfaces.IUser.providedBy(user):
			counter += _add_to_objects_2_queue(user)

	if queue_limit is not None:
		reactor.process_queue(queue_limit)
		
	result = LocatedExternalDict()
	result['Items'] = counter
	result['Elapsed'] = time.time() - now
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
