#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import is_
from hamcrest import has_length
from hamcrest import assert_that

import simplejson as json
from urllib import quote as UQ

from nti.dataserver.users import User
from nti.dataserver.users import Community
from nti.dataserver.contenttypes.forums.board import CommunityBoard
from nti.dataserver.contenttypes.forums.forum import CommunityForum

from nti.hypatia.interfaces import IHypatiaUserIndexController

import nti.dataserver.tests.mock_dataserver as mock_dataserver

from nti.app.testing.decorators import WithSharedApplicationMockDS
from nti.app.testing.application_webtest import ApplicationLayerTest

from nti.appserver.tests.test_application import TestApp as _TestApp

_FORUM_NAME = CommunityForum.__default_name__
_BOARD_NAME = CommunityBoard.__default_name__
POST_MIME_TYPE = 'application/vnd.nextthought.forums.post'

def _create_user(username='nt@nti.com', password='temp001'):
	ds = mock_dataserver.current_mock_ds
	usr = User.create_user(ds, username=username, password=password)
	return usr

class UserCommunityFixture(object):

	def __init__(self, test, community_name='TheCommunity'):
		self.community_name = community_name
		self.ds = test.ds
		self.test = test
		with mock_dataserver.mock_db_trans(self.ds):
			user = _create_user(username='original_user@foo')
			user2 = _create_user(username='user2@foo')
			user3 = _create_user(username='user3@foo')
			user_following_2 = _create_user(username='user_following_2@foo')
			user2_following_2 = _create_user(username='user2_following_2@foo')

			# make them share a community
			community = Community.get_community(community_name, self.ds) or \
						Community.create_community(username=community_name)

			user.record_dynamic_membership(community)
			user2.record_dynamic_membership(community)
			user3.record_dynamic_membership(community)
			user_following_2.record_dynamic_membership(community)
			user2_following_2.record_dynamic_membership(community)

			user2.follow(user)
			user_following_2.follow(user2)
			user2_following_2.follow(user2)

			self.user2_username = user2.username
			self.user_username = user.username
			self.user3_username = user3.username
			self.user2_follower_username = user_following_2.username
			self.user2_follower2_username = user2_following_2.username

		self.testapp = _TestApp(self.app, extra_environ=self._make_extra_environ(
															username=self.user_username))
		self.testapp2 = _TestApp(self.app, extra_environ=self._make_extra_environ(
															username=self.user2_username))
		self.testapp3 = _TestApp(self.app, extra_environ=self._make_extra_environ(
															username=self.user3_username))
		self.user2_followerapp = _TestApp(self.app, extra_environ=self._make_extra_environ(
															username=self.user2_follower_username))
		self.user2_follower2app = _TestApp(self.app, extra_environ=self._make_extra_environ(
															username=self.user2_follower2_username))

	def __getattr__(self, name):
		return getattr(self.test, name)

class TestAppLegacySearch(ApplicationLayerTest):

	default_entityname = 'TheCommunity'
	forum_ntiid = 'tag:nextthought.com,2011-10:TheCommunity-Forum:GeneralCommunity-Forum'
	forum_topic_ntiid_base = 'tag:nextthought.com,2011-10:TheCommunity-Topic:GeneralCommunity-Forum.'
	forum_url_relative_to_user = _BOARD_NAME + '/' + _FORUM_NAME

	forum_headline_class_type = 'Post'
	forum_headline_content_type = POST_MIME_TYPE
	forum_comment_unique = 'UNIQUETOCOMMENT'
	forum_headline_unique = 'UNIQUETOHEADLINE'

	def _create_post_data_for_POST(self):
		unique = self.forum_headline_unique
		data = { 'Class': self.forum_headline_class_type,
				 'MimeType': self.forum_headline_content_type,
				 'title': 'My New Blog',
				 'description': "This is a description of the thing I'm creating",
				 'body': ['My first thought. ' + unique] }

		return data

	def _create_comment_data_for_POST(self):
		unique = self.forum_comment_unique
		data = { 'Class': 'Post',
				 'title': 'A comment',
				 'body': ['This is a comment body ' + unique ] }
		return data

	def _POST_topic_entry(self, data=None, content_type=None, status_only=None, forum_url=None):
		testapp = self.testapp
		if data is None:
			data = self._create_post_data_for_POST()

		kwargs = {'status': 201}
		meth = testapp.post_json
		post_data = data
		if content_type:
			kwargs['headers'] = {b'Content-Type': str(content_type)}
			meth = testapp.post
			post_data = json.dumps(data)
		if status_only:
			kwargs['status'] = status_only

		res = meth(forum_url or self.forum_pretty_url, post_data, **kwargs)
		return res

	def _POST_and_publish_topic_entry(self, data=None, forum_url=None):
		if data is None:
			data = self._create_post_data_for_POST()
		res = self._POST_topic_entry(data=data, forum_url=forum_url)
		publish_url = self.require_link_href_with_rel(res.json_body, 'publish')
		res = self.testapp.post(publish_url)
		return res, data

	def _do_test_user_can_POST_new_forum_entry(self, data, content_type=None, status_only=None):
		res = self._POST_topic_entry(data, content_type=content_type, status_only=status_only)
		return res

	def setUp(self):
		super(TestAppLegacySearch, self).setUp()
		self.forum_pretty_url = UQ('/dataserver2/users/' + self.default_entityname + '/' +
									self.forum_url_relative_to_user)
		self.board_pretty_url = self.forum_pretty_url[:-(len(_FORUM_NAME) + 1)]

	@WithSharedApplicationMockDS(users=True, testapp=True)
	def test_user_can_POST_new_forum_entry_and_search_for_it(self):
		username = self.extra_environ_default_user
		with mock_dataserver.mock_db_trans(self.ds):
			community = Community.create_community(username=self.default_entityname)
			user = User.get_user(username)
			user.record_dynamic_membership(community)

		data = self._create_post_data_for_POST()
		self._do_test_user_can_POST_new_forum_entry(data)

		with mock_dataserver.mock_db_trans(self.ds):
			user = User.get_user(username)
			rim = IHypatiaUserIndexController(user)
			hits = rim.search(self.forum_headline_unique)
			assert_that(hits, has_length(1))

	@WithSharedApplicationMockDS(users=True, testapp=True)
	def test_creator_can_POST_new_comment_to_contents(self):
		username = self.extra_environ_default_user
		with mock_dataserver.mock_db_trans(self.ds):
			community = Community.create_community(username=self.default_entityname)
			user = User.get_user(username)
			user.record_dynamic_membership(community)

		# By posting to /contents, we can get better client-side cache behaviour
		testapp = self.testapp

		# Create the topic
		res = self._POST_topic_entry()

		# (Same user) comments on blog by POSTing a new post
		data = self._create_comment_data_for_POST()
		contents_url = self.require_link_href_with_rel(res.json_body, 'contents')
		res = testapp.post_json(contents_url, data, status=201)

		with mock_dataserver.mock_db_trans(self.ds):
			user = User.get_user(username)
			rim = IHypatiaUserIndexController(user)
			hits = rim.search(self.forum_comment_unique)
			assert_that(hits, has_length(1))

	@WithSharedApplicationMockDS(users=True, testapp=True)
	def test_creator_can_DELETE_comment_yielding_placeholders(self):
		testapp = self.testapp
		username = self.extra_environ_default_user
		with mock_dataserver.mock_db_trans(self.ds):
			community = Community.create_community(username=self.default_entityname)
			user = User.get_user(username)
			user.record_dynamic_membership(community)


		# Create the topic
		res = self._POST_topic_entry()
		entry_url = res.location

		data = self._create_comment_data_for_POST()
		res = testapp.post_json(entry_url, data, status=201)
		assert_that(res.status_int, is_(201))
		edit_url = self.require_link_href_with_rel(res.json_body, 'edit')

		res = testapp.delete(edit_url)
		assert_that(res.status_int, is_(204))

		with mock_dataserver.mock_db_trans(self.ds):
			user = User.get_user(username)
			rim = IHypatiaUserIndexController(user)
			hits = rim.search(self.forum_comment_unique)
			assert_that(hits, has_length(0))

	@WithSharedApplicationMockDS
	def test_community_user_can_search_for_published_topic(self):
		fixture = UserCommunityFixture(self)
		self.testapp = fixture.testapp

		self._POST_and_publish_topic_entry()

		with mock_dataserver.mock_db_trans(self.ds):
			user = User.get_user(fixture.user2_username)
			rim = IHypatiaUserIndexController(user)
			hits = rim.search(self.forum_headline_unique)
			assert_that(hits, has_length(1))

	@WithSharedApplicationMockDS
	def test_community_user_can_search_for_publish_unpublished_comments(self):
		fixture = UserCommunityFixture(self)
		self.testapp = fixture.testapp
		testapp2 = fixture.testapp2

		publish_res, _ = self._POST_and_publish_topic_entry()
		topic_url = publish_res.location

		# non-creator can comment
		comment_data = self._create_comment_data_for_POST()
		testapp2.post_json(topic_url, comment_data, status=201)

		with mock_dataserver.mock_db_trans(self.ds):
			user = User.get_user(fixture.user3_username)
			rim = IHypatiaUserIndexController(user)
			hits = rim.search(self.forum_comment_unique)
			assert_that(hits, has_length(1))

		self.testapp.post(self.require_link_href_with_rel(publish_res.json_body, 'unpublish'))

		with mock_dataserver.mock_db_trans(self.ds):
			user = User.get_user(fixture.user3_username)
			rim = IHypatiaUserIndexController(user)
			hits = rim.search(self.forum_comment_unique)
			assert_that(hits, has_length(0))

			user = User.get_user(fixture.user2_username)
			rim = IHypatiaUserIndexController(user)
			hits = rim.search(self.forum_comment_unique)
			assert_that(hits, has_length(1))

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
			# search
			user = User.get_user(username)
			rim = IHypatiaUserIndexController(user)
			hits = rim.search('Kenpachi')
			assert_that(hits, has_length(1))

			hits = rim.search('Unohana'.upper())
			assert_that(hits, has_length(1))

			hits = rim.search('yachiru')
			assert_that(hits, has_length(1))

			hits = rim.search('yachiru'.upper())
			assert_that(hits, has_length(1))
