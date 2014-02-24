#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import is_
from hamcrest import has_length
from hamcrest import assert_that

import unittest

import BTrees

from hypatia import query
from hypatia.catalog import CatalogQuery

from nti.contentfragments.interfaces import IPlainTextContentFragment

from nti.dataserver.users import User

from nti.dataserver.contenttypes import Note

from nti.hypatia import reactor
from nti.hypatia import search_queue
from nti.hypatia import search_catalog

from nti.ntiids.ntiids import make_ntiid

import nti.dataserver.tests.mock_dataserver as mock_dataserver

from nti.hypatia.tests import SharedConfiguringTestLayer

class TestModeled(unittest.TestCase):

	layer = SharedConfiguringTestLayer

	def _create_user(self, username='nt@nti.com', password='temp001'):
		usr = User.create_user(self.ds, username=username, password=password)
		return usr

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
	def test_add_remove(self):
		conn = mock_dataserver.current_transaction
		user = self._create_user()
		note = self._create_note('test', user)
		if conn: conn.add(note)
		note = user.addContainedObject(note)

		queue = search_queue()
		assert_that(queue, has_length(1))

		user.deleteContainedObject(note.containerId, note.id)
		assert_that(queue, has_length(1))

	@mock_dataserver.WithMockDSTrans
	def test_process_search(self):
		conn = mock_dataserver.current_transaction
		user = self._create_user()
		note = self._create_note('the light of dangai joue finds its mark', user)
		if conn: conn.add(note)
		note = user.addContainedObject(note)

		queue = search_queue()
		assert_that(queue, has_length(1))

		reactor.process_queue()
		assert_that(queue, has_length(0))
		
		catalog = search_catalog()
		content = catalog['content']
		q = CatalogQuery(catalog, family=BTrees.family64)
		hits, seq = q.query(query.Contains(content, "light"))
		assert_that(hits, is_(1))
		assert_that(seq, has_length(1))

		hits, _ = q.search(content='"light of dangai"')
		assert_that(hits, is_(1))

		hits, _ = q.search(content='dangai*')
		assert_that(hits, is_(1))
		
		hits, _ = q.search(content='light AND mark')
		assert_that(hits, is_(1))
		
		hits, _ = q.search(content='light AND dark')
		assert_that(hits, is_(0))

