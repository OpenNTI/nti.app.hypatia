#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import has_length
from hamcrest import assert_that

from nti.contentfragments.interfaces import IPlainTextContentFragment

from nti.dataserver.users import User

from nti.dataserver.contenttypes import Note

from nti.hypatia import reactor
from nti.hypatia import interfaces as hypatia_interfaces

from nti.ntiids.ntiids import make_ntiid

import nti.dataserver.tests.mock_dataserver as mock_dataserver

from nti.hypatia.tests import ConfiguringTestBase

class TestAdapters(ConfiguringTestBase):

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

	@mock_dataserver.WithMockDSTrans
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

		searcher = hypatia_interfaces.IHypatiaEntityIndexManager(user1)
		hits = searcher.search("Jinzen")
		assert_that(hits, has_length(1))

		searcher = hypatia_interfaces.IHypatiaEntityIndexManager(user2)
		hits = searcher.search("communication")
		assert_that(hits, has_length(1))

		searcher = hypatia_interfaces.IHypatiaEntityIndexManager(user1)
		hits = searcher.search("Madarame")
		assert_that(hits, has_length(1))

		searcher = hypatia_interfaces.IHypatiaEntityIndexManager(user3)
		hits = searcher.search("Madarame")
		assert_that(hits, has_length(0))

if __name__ == '__main__':
	import unittest
	unittest.main()
