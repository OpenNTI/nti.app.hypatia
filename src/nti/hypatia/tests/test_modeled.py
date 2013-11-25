#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from nti.contentfragments.interfaces import IPlainTextContentFragment

from nti.dataserver.users import User

from nti.dataserver.contenttypes import Note

from nti.graphdb import neo4j
from nti.graphdb import modeled

from nti.ntiids.ntiids import make_ntiid

import nti.dataserver.tests.mock_dataserver as mock_dataserver

from nti.graphdb.tests import ConfiguringTestBase

from hamcrest import (assert_that, is_)

class TestModeled(ConfiguringTestBase):

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

	def _create_note(self, msg, creator, containerId=None, title=None, inReplyTo=None):
		note = Note()
		if title:
			note.title = IPlainTextContentFragment(title)
		note.body = [unicode(msg)]
		note.creator = creator
		note.inReplyTo = inReplyTo
		note.containerId = containerId or make_ntiid(nttype='bleach', specific='manga')
		return note

	@mock_dataserver.WithMockDSTrans
	def test_install(self):
		conn = mock_dataserver.current_transaction
		user = self._create_random_user()
		note = self._create_note('test', user)
		if conn: conn.add(note)
		note = user.addContainedObject(note)

		note2 = self._create_note('test2', user, inReplyTo=note)
		if conn: conn.add(note2)
		note2 = user.addContainedObject(note2)
		
		cnt_rels = modeled.install(self.db)
		assert_that(cnt_rels, is_(1))

