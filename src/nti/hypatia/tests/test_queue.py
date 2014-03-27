#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import is_
from hamcrest import none
from hamcrest import is_not
from hamcrest import has_length
from hamcrest import assert_that
from hamcrest import has_property

import unittest

from nti.contentfragments.interfaces import IPlainTextContentFragment

from nti.dataserver.users import User
from nti.dataserver.contenttypes import Note

from nti.ntiids.ntiids import make_ntiid

from nti.hypatia import search_queue
from nti.hypatia import queue as hypatia_queue

import nti.dataserver.tests.mock_dataserver as mock_dataserver
from nti.dataserver.tests.mock_dataserver import WithMockDSTrans

from nti.hypatia.tests import zanpakuto_commands
from nti.hypatia.tests import SharedConfiguringTestLayer

class TestQueue(unittest.TestCase):

	layer = SharedConfiguringTestLayer

	def _create_user(self, username='nt@nti.com', password='temp001'):
		ds = mock_dataserver.current_mock_ds
		usr = User.create_user(ds, username=username, password=password)
		return usr

	def _create_note(self, msg, username, containerId=None, title=None):
		note = Note()
		if title:
			note.title = IPlainTextContentFragment(title)
		note.body = [unicode(msg)]
		note.creator = username
		note.containerId = containerId or make_ntiid(nttype='bleach', specific='manga')
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

	def _index_notes(self, user=None):
		notes, user = self._add_notes(user=user)
		return user, notes

	def test_constructor_event_queue(self):
		queue = hypatia_queue.SearchCatalogEventQueue()
		assert_that(queue, has_length(0))

	def test_constructor_queue(self):
		queue = hypatia_queue.SearchCatalogQueue()
		assert_that(queue, has_length(0))
		assert_that(queue, has_property('buckets', is_(1009)))
		assert_that(queue.eventQueueLength(), is_(0))
		assert_that(queue[100], is_not(none()))

	@WithMockDSTrans
	def test_search_catalog_queue(self):
		user = self._create_user()
		self._add_notes(user)
		queue = search_queue()
		assert_that(queue, has_length(len(zanpakuto_commands)))
		assert_that(queue.eventQueueLength(), is_(len(zanpakuto_commands)))

