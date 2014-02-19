#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import is_
from hamcrest import assert_that

from nti.contentfragments.interfaces import IPlainTextContentFragment

from nti.dataserver.users import User
from nti.dataserver.contenttypes import Note

from nti.ntiids.ntiids import make_ntiid

from nti.hypatia import reactor
from nti.hypatia import subscribers

import nti.dataserver.tests.mock_dataserver as mock_dataserver
from nti.dataserver.tests.mock_dataserver import WithMockDSTrans

from nti.hypatia.tests import zanpakuto_commands
from nti.hypatia.tests import ConfiguringTestBase

class TestSubcribers(ConfiguringTestBase):

	def _create_user(self, username='nt@nti.com', password='temp001'):
		ds = mock_dataserver.current_mock_ds
		usr = User.create_user(ds, username=username, password=password)
		return usr

	def _create_note(self, msg, username, containerId=None, title=None, sharedWith=()):
		note = Note()
		if title:
			note.title = IPlainTextContentFragment(title)
		note.body = [unicode(msg)]
		note.creator = username
		note.containerId = containerId or make_ntiid(nttype='bleach', specific='manga')
		for u in sharedWith:
			note.addSharingTarget(u)
		return note

	def _add_notes(self, user=None):
		notes = []
		connection = mock_dataserver.current_transaction
		user = user or self._create_user()
		for x in zanpakuto_commands:
			note = self._create_note(x, user.username)
			if connection:
				connection.add(note)
			note = user.addContainedObject(note)
			notes.append(note)
		return notes, user

	@WithMockDSTrans
	def test_add_2_queue_not_stored(self):
		note = self._create_note("plastic", 'nt@nti.com')
		result = subscribers.add_2_queue(note)
		assert_that(result, is_(False))

	@WithMockDSTrans
	def test_queue_added(self):
		notes, _ = self._add_notes()
		result = subscribers.queue_added(notes[0])
		assert_that(result, is_(False))

	@WithMockDSTrans
	def test_queue_modified(self):
		notes, _ = self._add_notes()
		result = subscribers.queue_modified(notes[0])
		assert_that(result, is_(True))
		result = subscribers.queue_modified(notes[0])
		assert_that(result, is_(True))
		note = self._create_note("plastic", 'nt@nti.com')
		result = subscribers.queue_modified(note)
		assert_that(result, is_(False))

	@WithMockDSTrans
	def test_user_deleted(self):
		user1 = self._create_user("n1@nti.com")
		user2 = self._create_user("n2@nti.com")
		note = self._create_note("not-shared", 'n1@nti.com')
		user1.addContainedObject(note)

		note = self._create_note("shared", 'n2@nti.com', sharedWith=(user1,))
		user2.addContainedObject(note)

		reactor.process_queue()

		count = subscribers.delete_userdata("n1@nti.com")
		assert_that(count, is_(1))

