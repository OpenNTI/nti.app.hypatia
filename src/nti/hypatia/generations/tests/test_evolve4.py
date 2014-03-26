#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import is_
from hamcrest import is_not
from hamcrest import equal_to
from hamcrest import assert_that
does_not = is_not

import unittest

from nti.contentsearch.constants import type_

from nti.dataserver.users import User
from nti.dataserver.contenttypes import Note

from nti.ntiids.ntiids import make_ntiid

from nti.hypatia import reactor
from nti.hypatia import search_catalog
from nti.hypatia.generations import evolve4

from nti.hypatia.tests import zanpakuto_commands
from nti.hypatia.tests import SharedConfiguringTestLayer

import nti.dataserver.tests.mock_dataserver as mock_dataserver
from nti.dataserver.tests.mock_dataserver import WithMockDSTrans

class TestEvolve4(unittest.TestCase):

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
	def test_evolve4(self):
		self._add_notes()
		conn = mock_dataserver.current_transaction
		reactor.process_queue()

		old_catalog = search_catalog()
		id_old = id(old_catalog)
		index = old_catalog[type_]
		old_docs = index.num_docs

		class _context(object): pass
		context = _context()
		context.connection = conn

		evolve4.do_evolve(context)

		new_catalog = search_catalog()
		id_new = id(new_catalog)
		index = old_catalog[type_]
		new_docs = index.num_docs
		
		assert_that(id_new, is_not(equal_to(id_old)))
		assert_that(new_docs, is_(equal_to(old_docs)))
