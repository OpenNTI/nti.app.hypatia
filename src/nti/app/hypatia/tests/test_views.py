#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import is_
from hamcrest import has_entry
from hamcrest import has_length
from hamcrest import assert_that

import json

from nti.contentfragments.interfaces import IPlainTextContentFragment

from nti.dataserver.users import User
from nti.dataserver.contenttypes import Note

from nti.ntiids.ntiids import make_ntiid

from nti.hypatia import interfaces as hypatia_interfaces

from nti.appserver.tests.test_application import TestApp

import nti.dataserver.tests.mock_dataserver as mock_dataserver

from nti.app.hypatia.tests import HypatiaApplicationTestLayer

from nti.app.testing.application_webtest import ApplicationLayerTest
from nti.app.testing.decorators import WithSharedApplicationMockDSHandleChanges

class TestAdminViews(ApplicationLayerTest):

	layer = HypatiaApplicationTestLayer

	def _create_note(self, msg, owner, containerId=None, title=None):
		note = Note()
		if title:
			note.title = IPlainTextContentFragment(title)
		note.body = [unicode(msg)]
		note.creator = owner
		note.containerId = containerId or make_ntiid(nttype='bleach', specific='manga')
		return note

	@WithSharedApplicationMockDSHandleChanges(users=True, testapp=True)
	def test_process_queue(self):
		username = 'ichigo@bleach.com'
		with mock_dataserver.mock_db_trans(self.ds):
			ichigo = self._create_user(username=username)
			note = self._create_note(u'As Nodt Fear', ichigo.username)
			ichigo.addContainedObject(note)

		testapp = TestApp(self.app)
		testapp.post('/dataserver2/hypatia/process_queue',
					 extra_environ=self._make_extra_environ(),
					 status=200)

		with mock_dataserver.mock_db_trans(self.ds):
			user = User.get_user(username)
			rim = hypatia_interfaces.IHypatiaUserIndexController(user)
			hits = rim.search('fear')
			assert_that(hits, has_length(1))
			
		testapp.post('/dataserver2/hypatia/process_queue',
					 json.dumps({'limit': 'xyt'}),
					 extra_environ=self._make_extra_environ(),
					 status=422)

	@WithSharedApplicationMockDSHandleChanges(testapp=False, users=True)
	def test_reindex_content(self):
		with mock_dataserver.mock_db_trans(self.ds):
			for x in range(10):
				username = 'bankai%s' % x
				usr = self._create_user(username=username)
				note = self._create_note(u'Shikai %s' % x, usr)
				usr.addContainedObject(note)
				
		testapp = TestApp(self.app)
		testapp.post('/dataserver2/hypatia/process_queue',
					 extra_environ=self._make_extra_environ(),
					 status=200)

		result = testapp.post('/dataserver2/hypatia/reindex_content',
							  json.dumps({'limit': 100,
										  'accept':'application/vnd.nextthought.redaction'}),
							  extra_environ=self._make_extra_environ(),
							  status=200)
		result = result.json
		assert_that(result, has_entry('Total', is_(0)))
		
		result = testapp.post('/dataserver2/hypatia/@@reindex_content',
							  json.dumps({'limit': 100}),
							  extra_environ=self._make_extra_environ(),
							  status=200)
		result = result.json
		assert_that(result, has_entry('Total', is_(10)))

		result = testapp.post('/dataserver2/hypatia/reindex_content',
							  json.dumps({'term':'bank', 'limit': 100}),
							  extra_environ=self._make_extra_environ(),
							  status=200)
		result = result.json
		assert_that(result, has_entry('Total', is_(10)))

		result = testapp.post('/dataserver2/hypatia/reindex_content',
							  json.dumps({'onlyMissing':True, 'limit': 100}),
							  extra_environ=self._make_extra_environ(),
							  status=200)
		result = result.json
		assert_that(result, has_entry('Total', is_(0)))

		result = testapp.post('/dataserver2/hypatia/reindex_content',
							  json.dumps({'limit': 100, 'usernames':'bankai1,bankai2'}),
							  extra_environ=self._make_extra_environ(),
							  status=200)
		result = result.json
		assert_that(result, has_entry('Total', is_(2)))

		result = testapp.post('/dataserver2/hypatia/reindex_content',
							  json.dumps({'limit': 100, 'usernames':'foo,foo2'}),
							  extra_environ=self._make_extra_environ(),
							  status=200)
		result = result.json
		assert_that(result, has_entry('Total', is_(0)))

		result = testapp.post('/dataserver2/hypatia/reindex_content',
							  json.dumps({'limit': 'xyz', 'usernames':'bankai1,bankai2'}),
							  extra_environ=self._make_extra_environ(),
							  status=422)

	@WithSharedApplicationMockDSHandleChanges(testapp=False, users=True)
	def test_empty_queue(self):
		with mock_dataserver.mock_db_trans(self.ds):
			for x in range(10):
				usr = self._create_user(username='bankai%s' % x)
				note = self._create_note(u'Shikai %s' % x, usr.username)
				usr.addContainedObject(note)

		testapp = TestApp(self.app)
		result = testapp.post('/dataserver2/hypatia/empty_queue',
							  extra_environ=self._make_extra_environ(),
					 		  status=200)

		result = result.json
		assert_that(result, has_entry('Total', is_(10)))

		result = testapp.post('/dataserver2/hypatia/@@empty_queue',
							  json.dumps({'limit': 100}),
							  extra_environ=self._make_extra_environ(),
							  status=200)
		result = result.json
		assert_that(result, has_entry('Total', is_(0)))

	@WithSharedApplicationMockDSHandleChanges(testapp=False, users=True)
	def test_sync_queue(self):
		with mock_dataserver.mock_db_trans(self.ds):
			for x in range(10):
				usr = self._create_user(username='bankai%s' % x)
				note = self._create_note(u'Shikai %s' % x, usr.username)
				usr.addContainedObject(note)

		testapp = TestApp(self.app)
		testapp.post('/dataserver2/hypatia/sync_queue',
					 extra_environ=self._make_extra_environ(),
					 status=204)
		
	@WithSharedApplicationMockDSHandleChanges(testapp=False, users=True)
	def test_unindex_missing(self):
		testapp = TestApp(self.app)
		result = testapp.post('/dataserver2/hypatia/unindex_missing',
					  		  extra_environ=self._make_extra_environ(),
			 		 		  status=200)
		result = result.json
		assert_that(result, has_entry('Total', is_(0)))

