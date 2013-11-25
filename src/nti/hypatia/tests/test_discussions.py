#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from nti.dataserver.users import User

from nti.dataserver.contenttypes.forums.topic import PersonalBlogEntry
from nti.dataserver.contenttypes.forums.post import PersonalBlogComment
from nti.dataserver.contenttypes.forums import interfaces as frm_interfaces

from nti.graphdb import neo4j
from nti.graphdb import discussions
from nti.graphdb import relationships
from nti.graphdb import interfaces as graph_interfaces

import nti.dataserver.tests.mock_dataserver as mock_dataserver

from nti.graphdb.tests import ConfiguringTestBase

from hamcrest import (assert_that, is_, is_not, none, has_entry, has_property, has_length)

class TestDiscussions(ConfiguringTestBase):

	@classmethod
	def setUpClass(cls):
		super(ConfiguringTestBase, cls).setUpClass()
		cls.db = neo4j.Neo4jDB(cls.DEFAULT_URI)

	def _create_user(self, username='nt@nti.com', password='temp001'):
		usr = User.create_user(self.ds, username=username, password=password)
		return usr

	def _create_random_user(self):
		username = self._random_username()
		user = self._create_user(username)
		return user

	@mock_dataserver.WithMockDSTrans
	def test_user_blog_node(self):
		user = self._create_random_user()
		blog = frm_interfaces.IPersonalBlog(user)
		entry = PersonalBlogEntry()
		entry.creator = user
		blog['bleach'] = entry
		entry.__parent__ = blog
		entry.lastModified = 42
		entry.createdTime = 24

		oid = discussions.to_external_ntiid_oid(entry)
		pk = graph_interfaces.IUniqueAttributeAdapter(entry)

		node, topic = discussions.modify_topic_node(self.db, oid, pk.key, pk.value)
		assert_that(node, is_not(none()))
		assert_that(topic, is_(entry))

		assert_that(node, has_property('properties', has_entry('type', 'Topic')))
		assert_that(node, has_property('properties', has_entry('ntiid', is_not(none()))))
		assert_that(node, has_property('properties', has_entry('author', user.username)))

		res = discussions.delete_topic_node(self.db, pk.key, pk.value)
		assert_that(res, is_(True))

		res = discussions.delete_topic_node(self.db, pk.key, pk.value)
		assert_that(res, is_(False))

	@mock_dataserver.WithMockDSTrans
	def test_user_blog_comment(self):
		user = self._create_random_user()
		blog = frm_interfaces.IPersonalBlog(user)
		entry = PersonalBlogEntry()
		entry.creator = user
		blog['bleach'] = entry
		entry.__parent__ = blog
		entry.createdTime = entry.lastModified = 42
		mock_dataserver.current_transaction.add(entry)

		comment = PersonalBlogComment()
		comment.creator = user
		entry['comment316072059'] = comment
		comment.__parent__ = entry
		comment.createdTime = comment.lastModified = 43
		mock_dataserver.current_transaction.add(comment)

		oid = discussions.to_external_ntiid_oid(comment)
		comment_rel_pk = discussions.get_comment_relationship_PK(comment)
				
		rel = discussions.add_comment_relationship(self.db, oid, comment_rel_pk)
		assert_that(rel, is_not(none()))
		assert_that(rel, has_property('properties', has_entry('created', '1969-12-31T18:00:43')))
		assert_that(rel, has_property('properties', has_entry('oid', is_not(none()))))

		node = self.db.get_indexed_node("username", user.username)
		assert_that(node, is_not(none()))

		pk = graph_interfaces.IUniqueAttributeAdapter(entry)
		node = self.db.get_indexed_node(pk.key, pk.value)
		assert_that(node, is_not(none()))

		rels = self.db.match(start=user, rel_type=relationships.CommentOn())
		assert_that(rels, has_length(1))

	@mock_dataserver.WithMockDSTrans
	def test_install(self):
		user = self._create_random_user()
		blog = frm_interfaces.IPersonalBlog(user)
		entry = PersonalBlogEntry()
		entry.creator = user
		blog['bleach'] = entry
		entry.__parent__ = blog
		entry.createdTime = entry.lastModified = 42
		mock_dataserver.current_transaction.add(entry)

		comment = PersonalBlogComment()
		comment.creator = user
		entry['comment316072059'] = comment
		comment.__parent__ = entry
		comment.createdTime = comment.lastModified = 43
		mock_dataserver.current_transaction.add(comment)

		rels = discussions.install(self.db)
		assert_that(rels, is_(2))

