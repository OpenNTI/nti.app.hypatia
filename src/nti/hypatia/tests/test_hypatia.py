#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import is_
from hamcrest import assert_that

from nti.dataserver.contenttypes import Note
from nti.dataserver.contenttypes import Canvas

from nti.hypatia import is_indexable

from nti.hypatia.tests import ConfiguringTestBase

class TestModeled(ConfiguringTestBase):

	def test_is_indexable(self):
		assert_that(is_indexable(Note()), is_(True))
		assert_that(is_indexable(Canvas()), is_(False))
		
