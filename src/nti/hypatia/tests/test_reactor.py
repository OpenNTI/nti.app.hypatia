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

import time
import unittest
import functools
import threading

from nti.contentfragments.interfaces import IPlainTextContentFragment

from nti.dataserver.users import User
from nti.dataserver.contenttypes import Note

from nti.ntiids.ntiids import make_ntiid

from nti.hypatia import reactor
from nti.hypatia import interfaces as hypatia_interfaces

import nti.dataserver.tests.mock_dataserver as mock_dataserver
from nti.dataserver.tests.mock_dataserver import WithMockDSTrans

from nti.hypatia.tests import zanpakuto_commands
from nti.hypatia.tests import SharedConfiguringTestLayer

class TestReactor(unittest.TestCase):

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
		reactor.process_queue(-1)
		return user, notes

	@WithMockDSTrans
	def test_process_index_msgs(self):
		user = self._create_user()
		self._add_notes(user)

		processed = reactor.process_index_msgs("lock", 100, False)
		assert_that(processed, is_(len(zanpakuto_commands)))

		rim = hypatia_interfaces.IHypatiaUserIndexController(user)
		results = rim.search("shield")
		assert_that(results, has_length(1))

	@WithMockDSTrans
	def test_index_reactor_ctr(self):
		ir = reactor.IndexReactor(12, 35, 500)

		assert_that(ir, has_property('min_wait_time', is_(12)))
		assert_that(ir, has_property('max_wait_time', is_(35)))
		assert_that(ir, has_property('limit', is_(500)))

		ir.start()
		assert_that(ir, has_property('processor', is_not(none())))

	@WithMockDSTrans
	def test_index_reactor_run(self):
		ir = reactor.IndexReactor(1, 1)
		target = functools.partial(ir.run, time.sleep)
		thread = threading.Thread(target=target)
		thread.start()
		time.sleep(2)
		ir.halt()
		time.sleep(1.5)

		assert_that(thread.isAlive(), is_(False))
		assert_that(repr(ir), is_not(none()))
		assert_that(ir, has_property('pid', is_not(none())))
		assert_that(ir, has_property('processor', is_(none())))
