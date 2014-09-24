#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import functools
import transaction

from zope import component

from pyramid.interfaces import INewRequest

from nti.contentsearch.constants import acl_

from nti.dataserver.interfaces import IDataserverTransactionRunner

from nti.hypatia import process_queue
from nti.hypatia import search_catalog

@component.adapter(INewRequest)
def requestIndexation(event):
	def _process_event():
		transaction_runner = \
			component.getUtility(IDataserverTransactionRunner)
		func = functools.partial(process_queue, limit=-1)
		transaction_runner(func)
		return True

	transaction.get().addAfterCommitHook(
					lambda success: success and _process_event())

import zope.intid

from zope.securitypolicy.interfaces import IPrincipalRoleMap

from nti.app.products.courseware.interfaces import ILegacyCommunityBasedCourseInstance

from nti.app.assessment.interfaces import IUsersCourseAssignmentHistory

from nti.contenttypes.courses.interfaces import RID_TA
from nti.contenttypes.courses.interfaces import RID_INSTRUCTOR
from nti.contenttypes.courses.interfaces import ICourseEnrollments
from nti.contenttypes.courses.interfaces import ICourseInstanceAvailableEvent

def get_course_rids(course):
	result = set()
	role_map = IPrincipalRoleMap(course, None)
	if role_map is not None:
		for role in (RID_INSTRUCTOR, RID_TA):
			settings = role_map.getPrincipalsForRole(role) or ()
			result.update(x[0].lower() for x in settings)
	return result

@component.adapter(ICourseInstanceAvailableEvent)
def on_course_instance_available(event):
	course = event.object
	
	## CS: Ignore legacy commmunity courses as these are
	## added to the global catalog during application start up
	## and they are no longer modifiable
	if ILegacyCommunityBasedCourseInstance.providedBy(course):
		return
	
	## CS: No roles return
	course_rids = get_course_rids(course)
	if not course_rids:
		return
	
	catalog  = search_catalog()
	acl_index = catalog[acl_]
	intids = component.getUtility(zope.intid.IIntIds)
	
	## CS: Get all the feedbacks items and force them
	## to reindex to make sure their ACL is updated
	enrollments = ICourseEnrollments(course)
	for record in enrollments.iter_enrollments():
		principal = record.Principal
		history = component.queryMultiAdapter((course, principal),
											  IUsersCourseAssignmentHistory)
		for item in history.values():
			if not item.has_feedback():
				continue
			for feedback in item.Feedback.values():
				uid = intids.queryIdI(feedback)
				if uid is None:
					continue

				# get expanded course acl
				creator = getattr(feedback.creator, 'username', None)
				expanded = set(course_rids)
				expanded.add(creator)
				expanded.discard(None)
				
				# feedback acl
				words = acl_index.get_words(uid)
				words = set(words or ())
				
				# if acl changed - reindex
				if expanded.difference(words):
					acl_index.reindex_doc(uid, feedback)
