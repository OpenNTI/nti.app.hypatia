#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import has_length
from hamcrest import assert_that

import random
import unittest
import collections

from nti.dataserver.users import User
from nti.dataserver.contenttypes import Note
from nti.dataserver.contenttypes import Highlight

from nti.ntiids.ntiids import make_ntiid

from . import zanpakuto_commands
from .. import all_cataloged_objects
from .. import all_indexable_objects_iids

import nti.dataserver.tests.mock_dataserver as mock_dataserver
from nti.dataserver.tests.mock_dataserver import WithMockDSTrans

from nti.hypatia.tests import SharedConfiguringTestLayer

class TestUtils(unittest.TestCase):

	layer = SharedConfiguringTestLayer

	def _create_user(self, username='nt@nti.com', password='temp001'):
		ds = mock_dataserver.current_mock_ds
		usr = User.create_user(ds, username=username, password=password)
		return usr

	def _create_note(self, msg, owner, containerId=None, sharedWith=()):
		note = Note()
		note.creator = owner
		note.body = [unicode(msg)]
		note.containerId = containerId or make_ntiid(nttype='bleach', specific='manga')
		for s in sharedWith or ():
			note.addSharingTarget(s)
		mock_dataserver.current_transaction.add(note)
		note = owner.addContainedObject(note)
		return note

	def _create_highlight(self, msg, owner, sharedWith=()):
		highlight = Highlight()
		highlight.selectedText = unicode(msg)
		highlight.creator = owner.username
		highlight.containerId = make_ntiid(nttype='bleach', specific='manga')
		for s in sharedWith or ():
			highlight.addSharingTarget(s)
		mock_dataserver.current_transaction.add(highlight)
		highlight = owner.addContainedObject(highlight)
		return highlight

	def _create_notes(self, usr=None, sharedWith=()):
		notes = []
		usr = usr or self._create_user()
		for msg in zanpakuto_commands:
			note = self._create_note(msg, usr, sharedWith=sharedWith)
			notes.append(note)
		return notes, usr

	def _create_highlights(self, usr=None, sharedWith=()):
		result = []
		usr = usr or self._create_user()
		for msg in zanpakuto_commands:
			hi = self._create_highlight(msg, usr, sharedWith=sharedWith)
			result.append(hi)
		return result, usr

	@WithMockDSTrans
	def test_find_indexable_objects_notes(self):
		notes, user = self._create_notes()
		iids = list(all_indexable_objects_iids((user,)))
		assert_that(iids, has_length(len(notes)))

	@WithMockDSTrans
	def test_find_indexable_objects_highglights(self):
		highlights, user = self._create_highlights()

		iids = list(all_indexable_objects_iids((user,)))
		assert_that(iids, has_length(len(highlights)))

		iids = list(all_indexable_objects_iids(('xxxx',)))
		assert_that(iids, has_length(0))

	@WithMockDSTrans
	def test_10_notes(self):
		for x in range(10):
			usr = self._create_user(username='bankai%s' % x)
			self._create_note(u'Shikai %s' % x, usr)
		iids = list(all_indexable_objects_iids(('BANKAI1', 'bankai2')))
		assert_that(iids, has_length(2))

	@WithMockDSTrans
	def test_30_notes(self):
		users = []
		for x in xrange(5):
			usr = self._create_user(username='shinigami_%s' % x)
			users.append(usr)

		collector = collections.defaultdict(int)
		random.seed()
		for x in xrange(30):
			usr = random.choice(users)
			self._create_note(u'Shikai %s' % x, usr)
			collector[usr] += 1

		sample = random.sample(users, 3)
		accum = sum([collector[x] for x in sample])
		iids = list(all_cataloged_objects(sample))
		assert_that(iids, has_length(accum))

		iids = list(all_cataloged_objects())
		assert_that(iids, has_length(30))
