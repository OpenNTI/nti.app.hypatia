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

from pyramid.view import view_config
from pyramid.security import authenticated_userid

from nti.dataserver import users
from nti.dataserver import authorization as nauth
from nti.dataserver import interfaces as nti_interfaces

from nti.externalization.datastructures import LocatedExternalDict

from nti.utils.maps import CaseInsensitiveDict

from .. import subscribers
from . import get_user_indexable_objects

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
	if usernames:
		if isinstance(usernames, six.string_types):
			usernames = usernames.split()
	else:
		username = values.get('username', authenticated_userid(request))
		usernames = (username,) if username else ()

	counter = 0
	now = time.time()
	for username in usernames or ():
		user = users.Entity.get_entity(username)
		if user is not None and nti_interfaces.IUser.providedBy(user):
			counter += _add_to_objects_2_queue(user)

	result = LocatedExternalDict()
	result['Items'] = counter
	result['Elapsed'] = time.time() - now
	return result
