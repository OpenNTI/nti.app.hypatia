#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
Natural Language Toolkit: Distance Metrics

Copyright (C) 2001-2013 NLTK Project
Author: Edward Loper <edloper@gmail.com>
		Steven Bird <stevenbird1@gmail.com>
		Tom Lippincott <tom@cs.columbia.edu>
URL: <http://nltk.org/>
For license information, see LICENSE.TXT

.. $Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import repoze.lru
	
"""
Distance Metrics.

Compute the distance between two items (usually strings).
As metrics, they must satisfy the following three requirements:

1. d(a, a) = 0
2. d(a, b) >= 0
3. d(a, c) <= d(a, b) + d(b, c)

"""
def _edit_dist_init(len1, len2):
	lev = []
	for i in range(len1):
		lev.append([0] * len2)  # initialize 2-D array to zero
	for i in range(len1):
		lev[i][0] = i		   # column 0: 0,1,2,3,4,...
	for j in range(len2):
		lev[0][j] = j		   # row 0: 0,1,2,3,4,...
	return lev

def _edit_dist_step(lev, i, j, s1, s2, transpositions=False):
	c1 = s1[i - 1]
	c2 = s2[j - 1]

	# skipping a character in s1
	a = lev[i - 1][j] + 1
	# skipping a character in s2
	b = lev[i][j - 1] + 1
	# substitution
	c = lev[i - 1][j - 1] + (c1 != c2)

	# transposition
	d = c + 1  # never picked by default
	if transpositions and i > 1 and j > 1:
		if s1[i - 2] == c2 and s2[j - 2] == c1:
			d = lev[i - 2][j - 2] + 1

	# pick the cheapest
	lev[i][j] = min(a, b, c, d)

def edit_distance(s1, s2, transpositions=False):
	"""
	Calculate the Levenshtein edit-distance between two strings.
	The edit distance is the number of characters that need to be
	substituted, inserted, or deleted, to transform s1 into s2.  For
	example, transforming "rain" to "shine" requires three steps,
	consisting of two substitutions and one insertion:
	"rain" -> "sain" -> "shin" -> "shine".  These operations could have
	been done in other orders, but at least three steps are needed.

	This also optionally allows transposition edits (e.g., "ab" -> "ba"),
	though this is disabled by default.

	:param s1, s2: The strings to be analysed
	:param transpositions: Whether to allow transposition edits
	:type s1: str
	:type s2: str
	:type transpositions: bool
	:rtype int
	"""
	# set up a 2-D array
	len1 = len(s1)
	len2 = len(s2)
	lev = _edit_dist_init(len1 + 1, len2 + 1)

	# iterate over the array
	for i in range(len1):
		for j in range(len2):
			_edit_dist_step(lev, i + 1, j + 1, s1, s2, transpositions=transpositions)
	return lev[len1][len2]

@repoze.lru.lru_cache(1000)
def nltk_ratio(s1, s2):
	d = edit_distance(s1, s2, True)
	lensum = len(s1) + len(s2)
	result = (lensum - d)/float(lensum)
	return result

try:
	from zopyx.txng3.ext.levenshtein import distance
	from zopyx.txng3.ext.levenshtein import ratio as zopy_ratio

	@repoze.lru.lru_cache(1000)
	def ratio(a, b):
		return zopy_ratio(a, b)
except ImportError:  # PyPy
	ratio = nltk_ratio
	distance = edit_distance
	
