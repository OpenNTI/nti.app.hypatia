#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import is_
from hamcrest import is_not
from hamcrest import has_key
from hamcrest import assert_that
does_not = is_not

import unittest

from nti.contentsearch.constants import creator_

from nti.dataserver.users import User
from nti.dataserver.contenttypes import Note

from nti.ntiids.ntiids import make_ntiid

from nti.hypatia import reactor
from nti.hypatia import search_catalog
from nti.hypatia.generations import evolve3

from nti.hypatia.tests import zanpakuto_commands
from nti.hypatia.tests import SharedConfiguringTestLayer

import nti.dataserver.tests.mock_dataserver as mock_dataserver
from nti.dataserver.tests.mock_dataserver import WithMockDSTrans

class TestEvolve3(unittest.TestCase):

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
	def test_evolve3(self):
		self._add_notes()
		conn = mock_dataserver.current_transaction
		reactor.process_queue()

		class _context(object): pass
		context = _context()
		context.connection = conn

		total = evolve3.do_evolve(context)
		assert_that(total, is_(0))

		catalog = search_catalog()
		assert_that(catalog, has_key(creator_))
		del catalog[creator_]
		assert_that(catalog, does_not(has_key(creator_)))

		total = evolve3.do_evolve(context)
		assert_that(total, is_(len(zanpakuto_commands)))

		catalog = search_catalog()
		assert_that(catalog, has_key(creator_))
		index = catalog[creator_]
		assert_that(index.word_count(), is_(1))
		assert_that(index.indexed_count(), is_(len(zanpakuto_commands)))
