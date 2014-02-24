#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import has_length
from hamcrest import assert_that

import unittest

from nti.dataserver.users import User
from nti.dataserver.contenttypes import Note

from nti.ntiids.ntiids import make_ntiid

from nti.hypatia import search_queue
from nti.hypatia.generations import evolve2

from nti.hypatia.tests import zanpakuto_commands
from nti.hypatia.tests import SharedConfiguringTestLayer

import nti.dataserver.tests.mock_dataserver as mock_dataserver
from nti.dataserver.tests.mock_dataserver import WithMockDSTrans

class TestEvolve2(unittest.TestCase):

	layer = SharedConfiguringTestLayer

	def _create_user(self, username='nt@nti.com', password='temp001'):
		ds = mock_dataserver.current_mock_ds
		usr = User.create_user(ds, username=username, password=password)
		return usr

	def _create_note(self, msg, username, containerId=None, title=None):
		note = Note()
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

	@WithMockDSTrans
	def test_evolve2(self):
		self._add_notes()
		conn = mock_dataserver.current_transaction

		class _context(object): pass
		context = _context()
		context.connection = conn

		evolve2.do_evolve(context, False)

		queue = search_queue()
		assert_that(queue, has_length(len(zanpakuto_commands)))

