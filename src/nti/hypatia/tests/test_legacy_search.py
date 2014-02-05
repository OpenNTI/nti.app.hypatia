#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import has_length
from hamcrest import assert_that
from hamcrest import greater_than_or_equal_to

from hypatia.text import ParseError

from nti.contentsearch import interfaces as search_interfaces

from nti.contentfragments.interfaces import IPlainTextContentFragment

from nti.dataserver.users import User
from nti.dataserver.users import Community
from nti.dataserver.contenttypes import Note
from nti.dataserver.contenttypes import Redaction
from nti.dataserver.users import DynamicFriendsList

from nti.externalization.internalization import update_from_external_object

from nti.ntiids.ntiids import make_ntiid

from nti.hypatia import reactor
from nti.hypatia import interfaces as hypatia_interfaces

import nti.dataserver.tests.mock_dataserver as mock_dataserver
from nti.dataserver.tests.mock_dataserver import WithMockDSTrans

from nti.hypatia.tests import zanpakuto_commands
from nti.hypatia.tests import ApplicationTestBase
from nti.hypatia.tests import ConfiguringTestBase
from nti.hypatia.tests import WithSharedApplicationMockDSHandleChanges as WithSharedApplicationMockDS

class TestLegacySearch(ConfiguringTestBase):

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
		reactor.process_queue()
		return user, notes

	@WithMockDSTrans
	def test_query_notes(self):
		user, _ = self._index_notes()
		rim = hypatia_interfaces.IHypatiaUserIndexController(user)

		results = rim.search("shield")
		assert_that(results, has_length(1))

		try:
			rim.search("*")
			self.fail("glob search should not be allowed")
		except ParseError:
			pass

		try:
			rim.search("?")
			self.fail("glob search should not be allowed")
		except ParseError:
			pass

		hits = rim.search("ra*")
		assert_that(hits, has_length(3))

		hits = rim.search('"%s"' % zanpakuto_commands[0])
		assert_that(hits, has_length(1))

		query = search_interfaces.ISearchQuery("shield")
		query.searchOn = ("redaction",)
		results = rim.search(query)
		assert_that(results, has_length(0))

	@WithMockDSTrans
	def test_suggest(self):
		user, _, = self._index_notes()
		rim = hypatia_interfaces.IHypatiaUserIndexController(user)

		hits = rim.suggest("ra")
		assert_that(hits, has_length(greater_than_or_equal_to(4)))

	@WithMockDSTrans
	def test_create_redaction(self):
		username = 'kuchiki@bleach.com'
		user = self._create_user(username=username)
		redaction = Redaction()
		redaction.selectedText = u'Fear'
		update_from_external_object(redaction,
				{'replacementContent': u'Ichigo',
				 'redactionExplanation': u'Have overcome it everytime I have been on the verge of death'})
		redaction.creator = username
		redaction.containerId = make_ntiid(nttype='bleach', specific='manga')
		redaction = user.addContainedObject(redaction)

		# index
		reactor.process_queue()

		# search
		rim = hypatia_interfaces.IHypatiaUserIndexController(user)

		hits = rim.search("fear")
		assert_that(hits, has_length(1))

		hits = rim.search("death")
		assert_that(hits, has_length(1))

		hits = rim.search("ichigo")
		assert_that(hits, has_length(1))

	@WithMockDSTrans
	def test_note_phrase(self):
		username = 'kuchiki@bleach.com'
		user = self._create_user(username=username)
		msg = u"you'll be ready to rumble"
		note = Note()
		note.body = [unicode(msg)]
		note.creator = username
		note.containerId = make_ntiid(nttype='bleach', specific='manga')
		note = user.addContainedObject(note)

		# index
		reactor.process_queue()

		# search
		rim = hypatia_interfaces.IHypatiaUserIndexController(user)

		hits = rim.search('"you\'ll be ready"')
		assert_that(hits, has_length(1))

		hits = rim.search('"you will be ready"')
		assert_that(hits, has_length(0))

		hits = rim.search('"Ax+B"')
		assert_that(hits, has_length(0))

	@WithMockDSTrans
	def test_note_math_equation(self):
		username = 'ichigo@bleach.com'
		user = self._create_user(username=username)
		msg = u"ax+by = 100"
		note = Note()
		note.body = [unicode(msg)]
		note.creator = username
		note.containerId = make_ntiid(nttype='bleach', specific='manga')
		note = user.addContainedObject(note)

		# index
		reactor.process_queue()

		# search
		rim = hypatia_interfaces.IHypatiaUserIndexController(user)

		hits = rim.search('"ax+by"')
		assert_that(hits, has_length(1))

		hits = rim.search('"ax by"')
		assert_that(hits, has_length(1))

	@WithMockDSTrans
	def test_columbia_issue(self):
		username = 'ichigo@bleach.com'
		user = self._create_user(username=username)
		note = Note()
		note.body = [unicode('light a candle')]
		note.creator = username
		note.containerId = make_ntiid(nttype='bleach', specific='manga')
		note = user.addContainedObject(note)

		# index
		reactor.process_queue()

		# search
		rim = hypatia_interfaces.IHypatiaUserIndexController(user)

		hits = rim.search('"light a candle"')
		assert_that(hits, has_length(1))

		hits = rim.search("light a candle")
		assert_that(hits, has_length(1))

	@WithMockDSTrans
	def test_title_indexing(self):
		username = 'ichigo@bleach.com'
		user = self._create_user(username=username)
		note = self._create_note(u'The Asauchi breaks away to reveal Hollow Ichigo.', username,
								 title=u'Zangetsu Gone')
		note = user.addContainedObject(note)

		# index
		reactor.process_queue()

		# search
		rim = hypatia_interfaces.IHypatiaUserIndexController(user)

		hits = rim.search('Asauchi')
		assert_that(hits, has_length(1))

		hits = rim.search('Zangetsu')
		assert_that(hits, has_length(1))

	@WithMockDSTrans
	def test_note_share_comm(self):
		ds = mock_dataserver.current_mock_ds
		user_1 = User.create_user(ds, username='nti-1.com', password='temp001')
		user_2 = User.create_user(ds, username='nti-2.com', password='temp001')

		c = Community.create_community(ds, username='Bankai')
		for u in (user_1, user_2):
			u.record_dynamic_membership(c)
			u.follow(c)

		note = Note()
		note.body = [unicode('Hitsugaya and Madarame performing Jinzen')]
		note.creator = 'nti.com'
		note.containerId = make_ntiid(nttype='bleach', specific='manga')
		note.addSharingTarget(c)
		note = user_2.addContainedObject(note)

		# index
		reactor.process_queue()

		# search
		rim = hypatia_interfaces.IHypatiaUserIndexController(user_1)
		hits = rim.search('Jinzen')
		assert_that(hits, has_length(1))

		rim = hypatia_interfaces.IHypatiaUserIndexController(user_2)
		hits = rim.search('Madarame')
		assert_that(hits, has_length(1))

	@WithMockDSTrans
	def test_same_content_two_comm(self):
		ds = mock_dataserver.current_mock_ds
		user = User.create_user(ds, username='nti.com', password='temp001')

		note = Note()
		note.body = [unicode('Only a few atain both')]
		note.creator = 'nti.com'
		note.containerId = make_ntiid(nttype='bleach', specific='manga')

		comms = []
		for name in ('Bankai', 'Shikai'):
			c = Community.create_community(ds, username=name)
			user.record_dynamic_membership(c)
			user.follow(c)
			comms.append(c)
			note.addSharingTarget(c)
		note = user.addContainedObject(note)

		# index
		reactor.process_queue()

		rim = hypatia_interfaces.IHypatiaUserIndexController(user)
		hits = rim.search('atain')
		assert_that(hits, has_length(1))

	@WithMockDSTrans
	def test_note_share_dfl(self):
		ds = mock_dataserver.current_mock_ds
		ichigo = User.create_user(ds, username='ichigo@nti.com', password='temp001')
		aizen = User.create_user(ds, username='aizen@nti.com', password='temp001')
		gin = User.create_user(ds, username='gin@nti.com', password='temp001')

		bleach = DynamicFriendsList(username='bleach')
		bleach.creator = ichigo  # Creator must be set
		ichigo.addContainedObject(bleach)
		bleach.addFriend(aizen)
		bleach.addFriend(gin)

		note = Note()
		note.body = [u'Getsuga Tensho']
		note.creator = 'nti.com'
		note.containerId = make_ntiid(nttype='bleach', specific='manga')
		note.addSharingTarget(bleach)
		note = ichigo.addContainedObject(note)

		# index
		reactor.process_queue()

		rim = hypatia_interfaces.IHypatiaUserIndexController(gin)
		hits = rim.search('getsuga')
		assert_that(hits, has_length(1))

		rim = hypatia_interfaces.IHypatiaUserIndexController(aizen)
		hits = rim.search('tensho')
		assert_that(hits, has_length(1))

class TestAppLegacySearch(ApplicationTestBase):

	extra_environ_default_user = b'ichigo@bleach.com'

	@WithSharedApplicationMockDS(testapp=True, users=True)
	def test_blog_post(self):

		data = { 'Class': 'Post',
				 'title': 'Unohana',
				 'body': ["Begging her not to die Kenpachi screams out in rage as his opponent fades away"],
				 'tags': ['yachiru', 'haori'] }

		username = self.extra_environ_default_user
		testapp = self.testapp
		testapp.post_json('/dataserver2/users/%s/Blog' % username, data, status=201)

		with mock_dataserver.mock_db_trans(self.ds):
			# index
			reactor.process_queue()

			# search
			user = User.get_user(username)
			rim = hypatia_interfaces.IHypatiaUserIndexController(user)
			hits = rim.search('Kenpachi')
			assert_that(hits, has_length(1))

			hits = rim.search('Unohana'.upper())
			assert_that(hits, has_length(1))

			hits = rim.search('yachiru')
			assert_that(hits, has_length(1))

			hits = rim.search('yachiru'.upper())
			assert_that(hits, has_length(1))

