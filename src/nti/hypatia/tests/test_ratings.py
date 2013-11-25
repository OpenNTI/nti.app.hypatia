#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from nti.dataserver import liking
from nti.dataserver.users import User

from nti.dataserver.contenttypes import Note

from nti.graphdb import neo4j
from nti.graphdb import ratings

from nti.ntiids.ntiids import make_ntiid

import nti.dataserver.tests.mock_dataserver as mock_dataserver

from nti.graphdb.tests import ConfiguringTestBase

from hamcrest import (assert_that, is_)

class TestRatings(ConfiguringTestBase):

	@classmethod
	def setUpClass(cls):
		super(ConfiguringTestBase, cls).setUpClass()
		cls.db = neo4j.Neo4jDB(cls.DEFAULT_URI)

	def _create_user(self, username='nt@nti.com', password='temp001'):
		usr = User.create_user(self.ds, username=username, password=password)
		return usr

	def _create_random_user(self):
		username = self._random_username()
		user = self._create_user(username)
		return user

	def _create_note(self, msg, creator, containerId=None):
		note = Note()
		note.body = [unicode(msg)]
		note.creator = creator
		note.containerId = containerId or make_ntiid(nttype='bleach', specific='manga')
		return note

	@mock_dataserver.WithMockDSTrans
	def test_install(self):
		conn = mock_dataserver.current_transaction
		user = self._create_random_user()
		note = self._create_note('test', user)
		if conn: conn.add(note)
		note = user.addContainedObject(note)
		liking.like_object(note, user.username)
		
		cnt_rels = ratings.install(self.db)
		assert_that(cnt_rels, is_(1))

