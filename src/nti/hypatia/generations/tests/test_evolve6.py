#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import is_not
from hamcrest import has_key
from hamcrest import assert_that
does_not = is_not

import unittest

from hypatia.field import FieldIndex

from nti.contentsearch.constants import (createdTime_, lastModified_)

from nti.hypatia import search_catalog
from nti.hypatia.generations import evolve6

from nti.hypatia.tests import SharedConfiguringTestLayer

import nti.dataserver.tests.mock_dataserver as mock_dataserver
from nti.dataserver.tests.mock_dataserver import WithMockDSTrans

def noop(*args):
	pass

class TestEvolve6(unittest.TestCase):

	layer = SharedConfiguringTestLayer

	@WithMockDSTrans
	def test_evolve6(self):
		conn = mock_dataserver.current_transaction

		catalog = search_catalog()
		catalog[createdTime_] = FieldIndex(noop)
		catalog[lastModified_] = FieldIndex(noop)

		assert_that(catalog, has_key(createdTime_))
		assert_that(catalog, has_key(createdTime_))

		class _context(object): pass
		context = _context()
		context.connection = conn

		evolve6.do_evolve(context)

		assert_that(catalog, does_not(has_key(createdTime_)))
		assert_that(catalog, does_not(has_key(createdTime_)))

