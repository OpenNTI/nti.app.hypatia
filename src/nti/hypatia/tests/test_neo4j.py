#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from nti.dataserver.users import User
from nti.dataserver.users import FriendsList

from nti.graphdb import neo4j
from nti.graphdb import relationships

from nti.dataserver.tests.mock_dataserver import WithMockDSTrans

from nti.graphdb.tests import ConfiguringTestBase

from hamcrest import (assert_that, has_length, has_property, has_entry, is_not, is_, none)

class TestNeo4j(ConfiguringTestBase):

	@classmethod
	def setUpClass(cls):
		super(ConfiguringTestBase, cls).setUpClass()
		# cls.db = neo4j.Neo4jDB.create_db(cls.DEFAULT_URI)
		cls.db = neo4j.Neo4jDB(cls.DEFAULT_URI)

	def _create_user(self, username='nt@nti.com', password='temp001', **kwargs):
		usr = User.create_user(self.ds, username=username, password=password, **kwargs)
		return usr

	def _create_friendslist(self, owner, name="mycontacts", *friends):
		result = FriendsList(username=name)
		result.creator = owner
		for friend in friends:
			result.addFriend(friend)
		owner.addContainedObject(result)
		return result

	@WithMockDSTrans
	def test_node_funcs(self):
		username = self._random_username()
		user = self._create_user(username)
		node = self.db.create_node(user)
		assert_that(node, has_property('id', is_not(none())))
		assert_that(node, has_property('uri', is_not(none())))
		assert_that(node, has_property('properties',
									   has_entry('username', username)))

		res = self.db.get_node(node.id)
		assert_that(res, is_not(none()))

		res = self.db.get_node(user)
		assert_that(res, is_not(none()))

		user2 = self._create_user()
		res = self.db.get_node(user2)
		assert_that(res, is_(none()))

		props = dict(node.properties)
		props['language'] = 'latin'
		res = self.db.update_node(node, properties=props)

		node = self.db.get_node(node.id)
		assert_that(node, has_property('properties',
									   has_entry('language', 'latin')))

		res = self.db.delete_node(user)
		assert_that(res, is_(True))

		node = self.db.get_node(user)
		assert_that(node, is_(none()))

	@WithMockDSTrans
	def test_relationship_funcs(self):
		user1 = self._random_username()
		user1 = self._create_user(user1)
		node1 = self.db.create_node(user1)
		
		user2 = self._random_username()
		user2 = self._create_user(user2)
		node2 = self.db.create_node(user2)

		rel = self.db.create_relationship(user1, user2, relationships.FriendOf())
		assert_that(rel, is_not(none()))
		assert_that(rel, has_property('id', is_not(none())))
		assert_that(rel, has_property('uri', is_not(none())))
		assert_that(rel, has_property('end', is_not(none())))
		assert_that(rel, has_property('start', is_not(none())))
		assert_that(rel, has_property('type', is_not(none())))

		res = self.db.get_relationship(rel.id)
		assert_that(res, is_not(none()))
		assert_that(rel, has_property('id', is_not(none())))
		assert_that(rel, has_property('uri', is_not(none())))
		assert_that(rel, has_property('end', is_not(none())))
		assert_that(rel, has_property('start', is_not(none())))

		res = self.db.get_relationship(rel)
		assert_that(res, is_not(none()))

		res = self.db.match(start=user1, end=user2, rel_type=relationships.FriendOf())
		assert_that(res, has_length(1))

		rel_type = str(relationships.FriendOf())
		res = self.db.match(start=node1, end=node2, rel_type=rel_type)
		assert_that(res, has_length(1))

		res = self.db.match(start=user1, end=user2, rel_type="unknown")
		assert_that(res, has_length(0))

		self.db.delete_relationships(rel)
		res = self.db.get_relationship(rel)
		assert_that(res, is_(none()))

	@WithMockDSTrans
	def test_create_nodes(self):
		users = []
		for _ in range(4):
			user = self._random_username()
			users.append(self._create_user(user))

		nodes = self.db.create_nodes(*users)
		assert_that(nodes, has_length(len(users)))

		deleted = self.db.delete_nodes(*users)
		assert_that(deleted, is_(len(users)))
