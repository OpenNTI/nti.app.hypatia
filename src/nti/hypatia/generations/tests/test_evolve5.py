#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import is_not
from hamcrest import equal_to
from hamcrest import assert_that

import unittest

from nti.hypatia import search_queue
from nti.hypatia.generations import evolve5

from nti.hypatia.tests import SharedConfiguringTestLayer

import nti.dataserver.tests.mock_dataserver as mock_dataserver
from nti.dataserver.tests.mock_dataserver import WithMockDSTrans

class TestEvolve5(unittest.TestCase):

	layer = SharedConfiguringTestLayer

	@WithMockDSTrans
	def test_evolve5(self):
		conn = mock_dataserver.current_transaction

		old_search_queue = search_queue()
		id_old = id(old_search_queue)

		class _context(object): pass
		context = _context()
		context.connection = conn

		evolve5.do_evolve(context)

		new_queue = search_queue()
		id_new = id(new_queue)
		
		assert_that(id_new, is_not(equal_to(id_old)))
