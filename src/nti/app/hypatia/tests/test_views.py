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

from nti.contentfragments.interfaces import IPlainTextContentFragment

from nti.dataserver.users import User
from nti.dataserver.contenttypes import Note

from nti.ntiids.ntiids import make_ntiid

from nti.hypatia.interfaces import IHypatiaUserIndexController

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

		testapp = self.testapp
		testapp.post_json('/dataserver2/hypatia/process_queue', status=200)

		with mock_dataserver.mock_db_trans(self.ds):
			user = User.get_user(username)
			rim = IHypatiaUserIndexController(user)
			hits = rim.search('fear')
			assert_that(hits, has_length(1))
			
		testapp.post_json('/dataserver2/hypatia/process_queue',
					 	  {'limit': 'xyt'},
					 	  status=422)

	@WithSharedApplicationMockDSHandleChanges(testapp=True, users=True)
	def test_reindex_content(self):
		with mock_dataserver.mock_db_trans(self.ds):
			for x in range(10):
				username = 'bankai%s' % x
				usr = self._create_user(username=username)
				note = self._create_note(u'Shikai %s' % x, usr)
				usr.addContainedObject(note)
				
		testapp = self.testapp
		testapp.post_json('/dataserver2/hypatia/process_queue', 
						   {'limit':-1},
						   status=200)

		result = testapp.post_json('/dataserver2/hypatia/reindex_content',
							  	   {'limit': 100,
									'accept':'application/vnd.nextthought.redaction'},
							 		status=200)
		assert_that(result.json_body, has_entry('Total', 0))
		
		result = testapp.post_json('/dataserver2/hypatia/@@reindex_content',
							 	   {'limit': 100},
							  	   status=200)
		assert_that(result.json_body, has_entry('Total', 10))

		result = testapp.post_json('/dataserver2/hypatia/@@reindex_content',
							 	   {'uncataloged': True},
							  	   status=200)
		assert_that(result.json_body, has_entry('Total', 10))
		
		result = testapp.post_json('/dataserver2/hypatia/reindex_content',
							  	   {'term':'bank', 'limit': 100},
							  	   status=200)
		assert_that(result.json_body, has_entry('Total', 10))

		result = testapp.post_json('/dataserver2/hypatia/reindex_content',
							  	   {'onlyMissing':True, 'limit': 100},
							 	   status=200)
		assert_that(result.json_body, has_entry('Total', is_(0)))

		result = testapp.post_json('/dataserver2/hypatia/reindex_content',
							  	   {'limit': 100, 'usernames':'bankai1,bankai2'},
							  	   status=200)
		assert_that(result.json_body, has_entry('Total', is_(2)))

		result = testapp.post_json('/dataserver2/hypatia/reindex_content',
							 	   {'limit': 100, 'usernames':'foo,foo2'},
							  	   status=200)
		assert_that(result.json_body, has_entry('Total', 0))

		result = testapp.post_json('/dataserver2/hypatia/reindex_content',
							  		{'limit': 'xyz', 'usernames':'bankai1,bankai2'},
							  		status=422)

	@WithSharedApplicationMockDSHandleChanges(testapp=True, users=True)
	def test_empty_queue(self):
		with mock_dataserver.mock_db_trans(self.ds):
			for x in range(10):
				usr = self._create_user(username='bankai%s' % x)
				note = self._create_note(u'Shikai %s' % x, usr.username)
				usr.addContainedObject(note)

		testapp = self.testapp
		result = testapp.post_json('/dataserver2/hypatia/empty_queue', 
								 	{'limit':-1},
								 	status=200)
		assert_that(result.json_body, has_entry('Total', is_(0)))

	@WithSharedApplicationMockDSHandleChanges(testapp=True, users=True)
	def test_sync_queue(self):
		with mock_dataserver.mock_db_trans(self.ds):
			for x in range(10):
				usr = self._create_user(username='bankai%s' % x)
				note = self._create_note(u'Shikai %s' % x, usr.username)
				usr.addContainedObject(note)

		testapp = self.testapp
		testapp.post_json('/dataserver2/hypatia/sync_queue', status=204)
		
	@WithSharedApplicationMockDSHandleChanges(testapp=True, users=True)
	def test_unindex_missing(self):
		testapp = self.testapp
		result = testapp.post_json('/dataserver2/hypatia/unindex_missing', status=200)
		assert_that(result.json_body, has_entry('TotalBroken', is_(0)))
		assert_that(result.json_body, has_entry('TotalMissing', is_(0)))
