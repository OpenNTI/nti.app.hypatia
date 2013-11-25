#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

import os

from zope.configuration import xmlconfig

from nti.assessment import submission as asm_submission

from nti.contentlibrary.filesystem import DynamicFilesystemLibrary as FileLibrary

from nti.dataserver.users import User

from nti.externalization.externalization import toExternalObject

from nti.ntiids import ntiids

from nti import graphdb
from nti.graphdb import neo4j
from nti.graphdb import assessments
from nti.graphdb import relationships

from nti.graphdb.tests import ApplicationTestBase, WithSharedApplicationMockDSWithChanges

from nti.appserver.tests.test_application import TestApp

from nti.dataserver.tests import mock_dataserver

from hamcrest import (assert_that, is_not, none, has_length, greater_than_or_equal_to)

class TestAssessments(ApplicationTestBase):

	child_ntiid =  b'tag:nextthought.com,2011-10:MN-NAQ-MiladyCosmetology.naq.1'
	question_ntiid = child_ntiid

	@classmethod
	def setUpClass(cls):
		super(TestAssessments, cls).setUpClass()
		cls.db = neo4j.Neo4jDB(cls.DEFAULT_URI)
		cls.configuration_context = \
			xmlconfig.file("configure.zcml", graphdb, cls.configuration_context)
		
	@classmethod
	def _setup_library( cls, *args, **kwargs ):
		return FileLibrary( os.path.join( os.path.dirname(__file__), 'ExLibrary' ) )

	def _create_random_user(self):
		username = self._random_username()
		user = self._create_user(username)
		return user

	@WithSharedApplicationMockDSWithChanges
	def test_posting_assesses(self):
		with mock_dataserver.mock_db_trans(self.ds):
			user = self._create_random_user()
			username = user.username

		question = asm_submission.QuestionSubmission(
										questionId=self.child_ntiid, parts=('correct',))
		ext_obj = toExternalObject(question)
		ext_obj['ContainerId'] = 'tag:nextthought.com,2011-10:mathcounts-HTML-MN.2012.0'
		ext_obj.pop('Class')

		testapp = TestApp(self.app,
						  extra_environ=self._make_extra_environ(user=username))
		path = '/dataserver2/users/%s' % username
		res = testapp.post_json(path, ext_obj)
		oid = res.json_body[u'NTIID']
		with mock_dataserver.mock_db_trans(self.ds):
			assessments.process_assessed_question(self.db, oid)

			node = self.db.get_indexed_node("ID", self.child_ntiid)
			assert_that(node, is_not(none()))

			user = User.get_user(username)
			qa = ntiids.find_object_with_ntiid(oid)
			rels = self.db.match(start=user, end=qa,
								 rel_type=relationships.TakeAssessment())
			assert_that(rels, has_length(greater_than_or_equal_to(1)))
