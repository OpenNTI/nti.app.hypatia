#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import has_length
from hamcrest import assert_that

from nti.dataserver.users import User
from nti.dataserver.users import Community
from nti.dataserver.contenttypes import Note
from nti.dataserver.contenttypes import Highlight
from nti.dataserver.contenttypes.forums.forum import CommunityForum
from nti.dataserver.contenttypes.forums.topic import PersonalBlogEntry
from nti.dataserver.contenttypes.forums.post import GeneralForumComment
from nti.dataserver.contenttypes.forums.post import PersonalBlogComment
from nti.dataserver.contenttypes.forums.topic import CommunityHeadlineTopic
from nti.dataserver.contenttypes.forums import interfaces as frm_interfaces

from nti.ntiids.ntiids import make_ntiid

from . import zanpakuto_commands
from .. import get_user_indexable_objects

import nti.dataserver.tests.mock_dataserver as mock_dataserver
from nti.dataserver.tests.mock_dataserver import WithMockDSTrans

from nti.hypatia.tests import ConfiguringTestBase

class TestUtils(ConfiguringTestBase):

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
        objects = list(get_user_indexable_objects(user))
        assert_that(objects, has_length(len(notes)))

    @WithMockDSTrans
    def test_find_indexable_objects_highglights(self):
        notes, user = self._create_highlights()
        objects = list(get_user_indexable_objects(user))
        assert_that(objects, has_length(len(notes)))

    @WithMockDSTrans
    def test_find_indexable_objects_personal(self):
        user = self._create_user()

        blog = frm_interfaces.IPersonalBlog(user)
        entry = PersonalBlogEntry()
        blog['bleach'] = entry
        for x, _ in enumerate(zanpakuto_commands):
            comment = PersonalBlogComment()
            entry[str(x)] = comment

        objects = list(get_user_indexable_objects(user))
        assert_that(objects, has_length(len(zanpakuto_commands) + 1))

    @WithMockDSTrans
    def test_find_indexable_objects_community(self):
        ds = mock_dataserver.current_mock_ds
        user = self._create_user()
        comm = Community.create_community(ds, username='Bankai')
        user.record_dynamic_membership(comm)
        user.follow(comm)

        board = frm_interfaces.IBoard(comm)
        forum = CommunityForum()
        forum.creator = user
        board['bleach'] = forum
        topic = CommunityHeadlineTopic()
        topic.creator = user
        forum['bankai'] = topic

        for x, _ in enumerate(zanpakuto_commands):
            comment = GeneralForumComment()
            comment.creator = user
            topic[str(x)] = comment

        objects = list(get_user_indexable_objects(user))
        assert_that(objects, has_length(len(zanpakuto_commands) + 1))

