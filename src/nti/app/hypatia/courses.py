#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from zope import component

from ZODB.POSException import POSKeyError

from nti.contentsearch.constants import acl_

import zope.intid

from zope.securitypolicy.interfaces import IPrincipalRoleMap

from nti.app.products.courseware.interfaces import ILegacyCommunityBasedCourseInstance

from nti.app.assessment.interfaces import IUsersCourseAssignmentHistory

from nti.contenttypes.courses.interfaces import RID_TA
from nti.contenttypes.courses.interfaces import RID_INSTRUCTOR
from nti.contenttypes.courses.interfaces import ICourseEnrollments
from nti.contenttypes.courses.interfaces import ICourseInstanceAvailableEvent

from nti.hypatia import search_catalog

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
	
	catalog = search_catalog()
	acl_index = catalog[acl_]
	intids = component.getUtility(zope.intid.IIntIds)
	
	## CS: Get all the feedbacks items and force them
	## to reindex to make sure their ACL is updated
	enrollments = ICourseEnrollments(course)
	for record in enrollments.iter_enrollments():
		try:
			principal = record.Principal
		except POSKeyError:
			continue
		history = component.queryMultiAdapter((course, principal),
											  IUsersCourseAssignmentHistory)
		if not history:
			continue
		for item in history.values():
			if not item.has_feedback():
				continue
			for feedback in item.Feedback.values():
				uid = intids.queryId(feedback)
				if uid is None:
					continue

				# get expanded course acl
				expanded = set(course_rids)
				creator = getattr(feedback.creator, 'username', None)
				if creator:
					expanded.add(creator.lower())
				
				# feedback acl
				words = acl_index.get_words(uid)
				words = set(words or ())
				
				# if acl changed - reindex
				if expanded.difference(words):
					acl_index.reindex_doc(uid, feedback)
