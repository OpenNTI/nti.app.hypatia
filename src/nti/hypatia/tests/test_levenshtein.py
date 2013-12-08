#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import is_
from hamcrest import close_to
from hamcrest import assert_that

import unittest

from nti.hypatia.levenshtein import ratio

class TestLevenshtein(unittest.TestCase):

	def test_ratio(self):
		r = ratio('ichigo', 'rukia')
		assert_that(r, is_(close_to(0.1818, 0.01)))
		
		r = ratio('ichigo', 'ichigo')
		assert_that(r, is_(1.0))

		r = ratio('ichigo', 'ichi')
		assert_that(r, is_(close_to(0.8, 0.01)))
