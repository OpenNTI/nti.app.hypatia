#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import is_
from hamcrest import has_length
from hamcrest import assert_that

import unittest

from nti.hypatia.lexicon import defaultLexicon

class TestLexicon(unittest.TestCase):

    def test_similar(self):
        lexicon = defaultLexicon()
        wids = lexicon.sourceToWordIds("shikai bankai nozarashi bleach")
        assert_that(wids, is_([1, 2, 3, 4]))

        words = [w for w, _ in lexicon.get_similiar_words("shi", threshold=0.499)]
        assert_that(words, is_(['nozarashi', 'shikai']))
        
        words = [w for w, _ in lexicon.get_similiar_words("blea", common_length=2)]
        assert_that(words, is_(['bleach']))
        
        words = list(lexicon.get_similiar_words("ichigo"))
        assert_that(words, has_length(0))
