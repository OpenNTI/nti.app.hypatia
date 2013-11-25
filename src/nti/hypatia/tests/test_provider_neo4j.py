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
from nti.graphdb import provider_neo4j as provider

from nti.dataserver.tests.mock_dataserver import WithMockDSTrans

from nti.graphdb.tests import ConfiguringTestBase

from hamcrest import (assert_that, has_length, is_)

class TestNeo4jProvider(ConfiguringTestBase):

    @classmethod
    def setUpClass(cls):
        super(ConfiguringTestBase, cls).setUpClass()
        # cls.db = neo4j.Neo4jDB.create_db(cls.DEFAULT_URI)
        cls.db = neo4j.Neo4jDB(cls.DEFAULT_URI)
        cls.provider = provider.Neo4jQueryProvider(cls.db)

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
    def test_node_friend_suggestion(self):
        users = []
        for _ in range(0, 4):
            user = self._random_username()
            user = self._create_user(user)
            users.append(user)

        self.db.create_relationship(users[0], users[1], relationships.FriendOf())
        self.db.create_relationship(users[1], users[2], relationships.FriendOf())
        self.db.create_relationship(users[2], users[3], relationships.FriendOf())
        self.db.create_relationship(users[0], users[3], relationships.FriendOf())

        results = self.provider.suggest_friends_to(users[0])
        assert_that(results, has_length(1))
        assert_that(results[0][0], is_(users[2].username))
        assert_that(results[0][1], is_(1))

