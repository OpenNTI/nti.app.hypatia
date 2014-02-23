#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import is_
from hamcrest import none
from hamcrest import has_length
from hamcrest import assert_that

import unittest

from nti.contentfragments.interfaces import IPlainTextContentFragment

from nti.contentsearch import discriminators

from nti.dataserver.users import User

from nti.dataserver.contenttypes import Note

from nti.hypatia import reactor
from nti.hypatia import interfaces as hypatia_interfaces

from nti.ntiids.ntiids import make_ntiid

from nti.dataserver.tests.mock_dataserver import WithMockDSTrans
import nti.dataserver.tests.mock_dataserver as mock_dataserver

from nti.hypatia.tests import SharedConfiguringTestLayer

class TestAdapters(unittest.TestCase):

	layer = SharedConfiguringTestLayer

	def _create_user(self, username='nt@nti.com', password='temp001'):
		usr = User.create_user(self.ds, username=username, password=password)
		return usr

	def _create_note(self, msg, creator, containerId=None, title=None, sharedWith=()):
		note = Note()
		if title:
			note.title = IPlainTextContentFragment(title)
		note.body = [unicode(msg)]
		note.creator = creator
		note.containerId = containerId or make_ntiid(nttype='bleach', specific='manga')
		for shared in sharedWith or ():
			note.addSharingTarget(shared)
		return note

	@WithMockDSTrans
	def test_add_search(self):
		user1 = self._create_user(username='user1@nti.com')
		user2 = self._create_user(username='user2@nti.com')
		user3 = self._create_user(username='user3@nti.com')
		conn = mock_dataserver.current_transaction
		note = self._create_note('Hitsugaya and Madarame performing Jinzen', user1,
								 title='communication',
								 sharedWith=(user2,))
		if conn: conn.add(note)
		note = user1.addContainedObject(note)

		reactor.process_queue()

		searcher = hypatia_interfaces.IHypatiaUserIndexController(user1)
		hits = searcher.search("Jinzen")
		assert_that(hits, has_length(1))
		assert_that(searcher.username, is_('user1@nti.com'))

		searcher = hypatia_interfaces.IHypatiaUserIndexController(user2)
		hits = searcher.search("communication")
		assert_that(hits, has_length(1))

		searcher = hypatia_interfaces.IHypatiaUserIndexController(user1)
		hits = searcher.search("Madarame")
		assert_that(hits, has_length(1))

		searcher = hypatia_interfaces.IHypatiaUserIndexController(user3)
		hits = searcher.search("Madarame")
		assert_that(hits, has_length(0))

		uid = discriminators.get_uid(note)
		obj = searcher.get_object(uid)
		assert_that(obj, is_(none()))

		hits = searcher.search("")
		assert_that(hits, has_length(0))

		hits = searcher.suggest("")
		assert_that(hits, has_length(0))

		searcher = hypatia_interfaces.IHypatiaUserIndexController(user1)
		hits = searcher.suggest_and_search("jinz")
		assert_that(hits, has_length(1))

		hits = searcher.suggest_and_search("performing Jinzen")
		assert_that(hits, has_length(1))

	@WithMockDSTrans
	def test_noops(self):
		user1 = self._create_user(username='user1@nti.com')
		note = self._create_note('Hakka No Togame', user1,
								 title='White mist sentence')
		conn = mock_dataserver.current_transaction
		if conn: conn.add(note)
		note = user1.addContainedObject(note)

		searcher = hypatia_interfaces.IHypatiaUserIndexController(user1)
		searcher.index_content(note)
		searcher.update_content(note)
		searcher.delete_content(note)

		obj = searcher.get_object(10)
		assert_that(obj, is_(none()))
