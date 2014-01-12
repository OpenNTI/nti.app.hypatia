#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import is_
from hamcrest import is_not
from hamcrest import contains
from hamcrest import has_item
from hamcrest import has_length
from hamcrest import assert_that
does_not = is_not

import unittest

import BTrees
from BTrees.IFBTree import IFSet

from hypatia import query
from hypatia import RangeValue
from hypatia.exc import Unsortable
from hypatia.interfaces import NBEST
from hypatia.interfaces import STABLE
from hypatia.interfaces import OPTIMAL

from nti.hypatia.field import time_to_64bit_int
from nti.hypatia.field import SearchTimeFieldIndex

_marker = object()

def to_int(value):
	return time_to_64bit_int(value)

one_ival = to_int(1.0)
two_ival = to_int(2.0)

class TestSearchTimeFieldIndex(unittest.TestCase):

	def _makeOne(self):
		def discriminator(obj, default):
			if obj is _marker:
				return default
			if type(obj) == float:
				obj = to_int(obj)
			return obj

		family = BTrees.family64
		return SearchTimeFieldIndex(discriminator=discriminator, family=family)
	
	def _populateIndex(self, index):
		index.index_doc(5, 1.0)  # docid, obj
		index.index_doc(2, 2.0)
		index.index_doc(1, 3.0)
		index.index_doc(3, 4.0)
		index.index_doc(4, 5.0)
		index.index_doc(8, 6.0)
		index.index_doc(9, 7.0)
		index.index_doc(7, 8.0)
		index.index_doc(6, 9.0)
		index.index_doc(11, 10.0)
		index.index_doc(10, 11.0)
	
	def test_index_doc_existing_same_value(self):
		index = self._makeOne()
		index.index_doc(1, 1.0)
		index.index_doc(1, 1.0)
		assert_that(index.indexed_count(), is_(1))
		assert_that(index.word_count(), is_(1))
		assert_that(index._rev_index, contains(1))
		assert_that(index._fwd_index, contains(one_ival))
		assert_that(list(index._fwd_index[one_ival]), is_([1]))
		assert_that(list(index.unique_values()), is_([one_ival]))

	def test_index_doc_existing_new_value(self):
		index = self._makeOne()
		index.index_doc(1, 1.0)
		index.index_doc(1, 2.0)
		assert_that(index.indexed_count(), is_(1))
		assert_that(index.word_count(), is_(1))
		assert_that(index._rev_index, contains(1))
		assert_that(index._fwd_index, does_not(contains(one_ival)))
		assert_that(index._fwd_index, contains(two_ival))
		assert_that(list(index._fwd_index[two_ival]), is_([1]))
		assert_that(list(index.unique_values()), is_([two_ival]))

	def test_unindex_doc_no_residual_fwd_values(self):
		index = self._makeOne()
		index.index_doc(1, 1.0)
		index.unindex_doc(1)  # doesn't raise
		assert_that(index.indexed_count(), is_(0))
		assert_that(index.word_count(), is_(0))
		assert_that(index._rev_index, does_not(contains(1)))
		assert_that(index._fwd_index, does_not(contains(one_ival)))
		assert_that(list(index.unique_values()), has_length(0))

	def test_unindex_doc_w_residual_fwd_values(self):
		index = self._makeOne()
		index.index_doc(1, one_ival)
		index.index_doc(2, two_ival)
		index.unindex_doc(1)  # doesn't raise
		assert_that(index.indexed_count(), is_(1))
		assert_that(index.word_count(), is_(1))
		assert_that(index._rev_index, does_not(contains(1)))
		assert_that(index._rev_index, contains(2))
		assert_that(index._fwd_index, contains(two_ival))
		assert_that(list(index._fwd_index[two_ival]), is_([2]))
		assert_that(list(index.unique_values()), is_([two_ival]))

	def test_apply_two_tuple_miss(self):
		index = self._makeOne()
		assert_that(list(index.apply((one_ival, two_ival))), is_([]))

	def test_apply_two_tuple_hit(self):
		index = self._makeOne()
		index.index_doc(1, one_ival)
		assert_that(list(index.apply((one_ival, two_ival))), is_([1]))

	def test_sort_w_limit_lt_1(self):
		index = self._makeOne()
		try:
			index.sort([1, 2, 3], limit=0)
			self.fail()
		except ValueError:
			pass

	def test_sort_w_empty_index(self):
		index = self._makeOne()
		assert_that(
			list(index.sort([1, 2, 3], raise_unsortable=False)), is_([]))

	def test_sort_w_empty_docids(self):
		index = self._makeOne()
		index.index_doc(1, one_ival)
		assert_that(list(index.sort([])), is_([]))

	def test_sort_w_missing_docids(self):
		index = self._makeOne()
		index.index_doc(1, one_ival)
		gen = index.sort([1, 3])
		dids = []
		try:
			for did in gen:
				dids.append(did)
		except Unsortable as e:
			assert_that(list(e.docids), is_([3]))
		else:  # pragma: no cover
			self.fail('Unsortable not raised')
		assert_that(dids, is_([1]))

	def test_sort_w_missing_docids_raise_unsortable_False(self):
		index = self._makeOne()
		index.index_doc(1, one_ival)
		gen = index.sort([1, 3], raise_unsortable=False)
		assert_that(list(gen), is_([1]))

	def test_sort_force_nbest_w_missing_docids(self):
		index = self._makeOne()
		index.index_doc(1, one_ival)
		result = index.sort([2, 3], limit=10, sort_type=NBEST)
		dids = []
		try:  # pragma: no cover
			for did in result:
				dids.append(did)
		except Unsortable as e:
			assert_that(list(e.docids), is_([2, 3]))
		else:  # pragma: no cover
			self.fail('Unsortable not raised')
		assert_that(dids, is_([]))

	def test_sort_force_nbest_w_missing_docids_raise_unsortable_false(self):
		index = self._makeOne()
		index.index_doc(1, one_ival)
		result = index.sort([2, 3], limit=10, sort_type=NBEST,
							raise_unsortable=False)
		assert_that(list(result), is_([]))

	def test_sort_default_w_missing_docids(self):
		index = self._makeOne()
		index.index_doc(1, one_ival)
		result = index.sort([2, 3])
		dids = []
		try:  # pragma: no cover
			for did in result:
				dids.append(did)
		except Unsortable as e:
			assert_that(list(e.docids), is_([2, 3]))
		else:  # pragma: no cover
			self.fail('Unsortable not raised')
		assert_that(dids, is_([]))
		
	def test_sort_default_w_missing_docids_raise_unsortable_false(self):
		index = self._makeOne()
		index.index_doc(1, one_ival)
		assert_that(list(index.sort([2, 3], raise_unsortable=False)), is_([]))

	def test_sort_default_nolimit(self):
		index = self._makeOne()
		self._populateIndex(index)
		c1 = IFSet([1, 2, 3, 4, 5])
		result = index.sort(c1)
		assert_that(list(result), is_([5, 2, 1, 3, 4]))

	def test_sort_default_withlimit(self):
		index = self._makeOne()
		self._populateIndex(index)
		c1 = IFSet([1, 2, 3, 4, 5])
		result = index.sort(c1, limit=3)
		assert_that(list(result), is_([5, 2, 1]))

	def test_sort_optimal_means_None(self):
		index = self._makeOne()
		self._populateIndex(index)
		c1 = IFSet([1, 2, 3, 4, 5])
		result = index.sort(c1, sort_type=OPTIMAL)
		assert_that(list(result), is_([5, 2, 1, 3, 4]))

	def test_sort_stable_means_timsort(self):
		index = self._makeOne()
		self._populateIndex(index)
		c1 = IFSet([1, 2, 3, 4, 5])
		result = index.sort(c1, sort_type=STABLE)
		assert_that(list(result), is_([5, 2, 1, 3, 4]))

	def test_sort_nbest_missing_reverse_unlimited(self):
		index = self._makeOne()
		self._populateIndex(index)
		c1 = IFSet([1, 2, 3, 4, 5, 99])
		result = index.sort(c1, reverse=True, limit=10, sort_type=NBEST)
		dids = []
		try:
			for did in result:
				dids.append(did)
		except Unsortable as e:
			assert_that(list(e.docids), is_([99]))
		else:  # pragma: no cover
			self.fail('Unsortable not raised')
		assert_that(dids, is_([4, 3, 1, 2, 5]))

	def test_sort_nodocids(self):
		index = self._makeOne()
		self._populateIndex(index)
		c1 = IFSet()
		result = index.sort(c1)
		assert_that(list(result), is_([]))

	def test_reindex_doc_w_existing_docid_same_value(self):
		index = self._makeOne()
		index.index_doc(5, one_ival)
		assert_that(index.indexed_count(), is_(1))
		assert_that(index._rev_index[5], is_(one_ival))
		index.reindex_doc(5, one_ival)
		assert_that(index.indexed_count(), is_(1))
		assert_that(index._rev_index[5], is_(one_ival))

	def test_reindex_doc_w_existing_docid_different_value(self):
		index = self._makeOne()
		index.index_doc(5, one_ival)
		assert_that(index.indexed_count(), is_(1))
		index.reindex_doc(5, two_ival)
		assert_that(index.indexed_count(), is_(1))
		assert_that(index._rev_index[5], is_(two_ival))

	def test_search_single_range_querymember_or(self):
		index = self._makeOne()
		self._populateIndex(index)
		index.index_doc(50, 1.0)
		result = index.search([RangeValue(one_ival, one_ival)])
		result = sorted(list(result))
		assert_that(result, is_([5, 50]))

	def test_search_double_range_querymember_or(self):
		index = self._makeOne()
		self._populateIndex(index)
		index.index_doc(50, 1.0)
		result = index.search([RangeValue(one_ival, one_ival),
							   RangeValue(one_ival, two_ival)])
		result = sorted(list(result))
		assert_that(result, is_([2, 5, 50]))

	def test_search_double_range_querymember_and(self):
		index = self._makeOne()
		self._populateIndex(index)
		index.index_doc(50, 1.0)
		result = index.search([RangeValue(one_ival, one_ival),
							   RangeValue(one_ival, two_ival)], 'and')
		result = sorted(list(result))
		assert_that(result, is_([5, 50]))

	def test_search_single_int_querymember_or(self):
		index = self._makeOne()
		self._populateIndex(index)
		index.index_doc(50, 1.0)
		result = index.search([one_ival])
		result = sorted(list(result))
		assert_that(result, is_([5, 50]))

	def test_search_double_int_querymember_and(self):
		# this is a nonsensical query
		index = self._makeOne()
		self._populateIndex(index)
		index.index_doc(50, 1.0)
		result = index.search([one_ival, two_ival], 'and')
		result = sorted(list(result))
		assert_that(result, is_([]))

	def test_apply_dict_single_range(self):
		index = self._makeOne()
		self._populateIndex(index)
		index.index_doc(50, 1.0)
		result = index.apply({'query': RangeValue(one_ival, two_ival)})
		result = sorted(list(result))
		assert_that(result, is_([2, 5, 50]))

	def test_apply_dict_operator_or_with_ranges(self):
		index = self._makeOne()
		self._populateIndex(index)
		index.index_doc(50, 1.0)
		result = index.apply({'query':[	RangeValue(one_ival, one_ival),
										RangeValue(one_ival, two_ival)],
							  'operator':'or'})
		result = sorted(list(result))
		assert_that(result, is_([2, 5, 50]))

	def test_apply_dict_operator_and_with_ranges_and(self):
		index = self._makeOne()
		self._populateIndex(index)
		index.index_doc(50, 1.0)
		result = index.apply({'query':[	RangeValue(one_ival, one_ival),
										RangeValue(one_ival, two_ival)],
							  'operator':'and'})
		result = sorted(list(result))
		assert_that(result, is_([5, 50]))
		
	def test_apply_dict_operator_or_with_int_and_range_or(self):
		index = self._makeOne()
		self._populateIndex(index)
		index.index_doc(50, 1.0)
		result = index.apply({'query':[one_ival, RangeValue(one_ival, two_ival)],
							  'operator':'or'})
		result = sorted(list(result))
		assert_that(result, is_([2, 5, 50]))

	def test_apply_nondict_2tuple(self):
		index = self._makeOne()
		self._populateIndex(index)
		index.index_doc(50, 1.0)
		result = index.apply((one_ival, two_ival))
		result = sorted(list(result))
		assert_that(result, is_([2, 5, 50]))

	def test_apply_nondict_int(self):
		index = self._makeOne()
		self._populateIndex(index)
		index.index_doc(50, 1.0)
		result = index.apply(one_ival)
		result = sorted(list(result))
		assert_that(result, is_([5, 50]))

	def test_apply_list(self):
		index = self._makeOne()
		self._populateIndex(index)
		index.index_doc(50, 1.0)
		result = index.apply([one_ival, two_ival])
		result = sorted(list(result))
		assert_that(result, is_([2, 5, 50]))

	def test_applyEq(self):
		index = self._makeOne()
		self._populateIndex(index)
		index.index_doc(50, 1.0)
		result = index.applyEq(1.0)
		result = sorted(list(result))
		assert_that(result, is_([5, 50]))

	def test_applyNotEq(self):
		index = self._makeOne()
		self._populateIndex(index)
		result = index.applyNotEq(1.0)
		result = sorted(list(result))
		assert_that(result, is_([1, 2, 3, 4, 6, 7, 8, 9, 10, 11]))

	def test_applyGe(self):
		index = self._makeOne()
		self._populateIndex(index)
		index.index_doc(50, 1.0)
		result = index.applyGe(10.0)
		result = sorted(list(result))
		assert_that(result, is_([10, 11]))

	def test_applyGt(self):
		index = self._makeOne()
		self._populateIndex(index)
		index.index_doc(50, 1.0)
		result = index.applyGt(10.0)
		result = sorted(list(result))
		assert_that(result, is_([10]))

	def test_applyLe(self):
		index = self._makeOne()
		self._populateIndex(index)
		index.index_doc(50, 1.0)
		result = index.applyLe(2.0)
		result = sorted(list(result))
		assert_that(result, is_([2, 5, 50]))

	def test_applyLt(self):
		index = self._makeOne()
		self._populateIndex(index)
		index.index_doc(50, 1.0)
		result = index.applyLt(2.0)
		result = sorted(list(result))
		assert_that(result, is_([5, 50]))

	def test_applyAny(self):
		index = self._makeOne()
		self._populateIndex(index)
		index.index_doc(50, 1.0)
		result = index.applyAny([1.0, 2.0, 60.0])
		result = sorted(list(result))
		assert_that(result, is_([2, 5, 50]))

	def test_applyInRange_inclusive_inclusive(self):
		index = self._makeOne()
		self._populateIndex(index)
		result = index.applyInRange(3.0, 7.0)
		result = sorted(list(result))
		assert_that(result, is_([1, 3, 4, 8, 9]))

	def test_applyInRange_inclusive_exclusive(self):
		index = self._makeOne()
		self._populateIndex(index)
		result = index.applyInRange(3.0, 7.0, excludemax=True)
		result = sorted(list(result))
		assert_that(result, is_([1, 3, 4, 8]))

	def test_applyInRange_exclusive_inclusive(self):
		index = self._makeOne()
		self._populateIndex(index)
		result = index.applyInRange(3.0, 7.0, excludemin=True)
		result = sorted(list(result))
		assert_that(result, is_([3, 4, 8, 9]))

	def test_applyInRange_exclusive_exclusive(self):
		index = self._makeOne()
		self._populateIndex(index)
		result = index.applyInRange(3.0, 7.0, excludemin=True, excludemax=True)
		result = sorted(list(result))
		assert_that(result, is_([3, 4, 8]))

	def test_applyNotInRange(self):
		index = self._makeOne()
		self._populateIndex(index)
		result = index.applyNotInRange(3.0, 7.0)
		result = sorted(list(result))
		assert_that(result, is_([2, 5, 6, 7, 10, 11]))

	def test_not_indexed_count(self):
		index = self._makeOne()
		index.index_doc(1, 1.0)
		index.index_doc(2, _marker)
		assert_that(index.not_indexed_count(), is_(1))

	def test_index_doc_value_is_marker(self):
		index = self._makeOne()
		# this should never be raised
		index.unindex_doc = lambda *arg, **kw: 0 / 1
		index.index_doc(1, _marker)
		assert_that(index._not_indexed, contains(1))
		index.index_doc(1, _marker)
		assert_that(index._not_indexed, contains(1))

	def test_index_doc_then_missing_value(self):
		index = self._makeOne()
		self._populateIndex(index)
		assert_that(set([3]), is_(set(index.applyEq(4.0))))
		assert_that(index.docids(), has_item(3))
		index.index_doc(3, _marker)
		assert_that(set(), is_(set(index.applyEq(4.0))))
		assert_that(index.docids(), has_item(3))

	def test_index_doc_missing_value_then_with_value(self):
		index = self._makeOne()
		self._populateIndex(index)
		index.index_doc(3, _marker)
		assert_that(set(), is_(set(index.applyEq(4.0))))
		assert_that(index.docids(), has_item(3))
		index.index_doc(3, 42.0)
		assert_that(set([3]), is_(set(index.applyEq(42.0))))
		assert_that(index.docids(), has_item(3))

	def test_index_doc_missing_value_then_unindex(self):
		index = self._makeOne()
		self._populateIndex(index)
		index.index_doc(3, _marker)
		assert_that(set(), is_(set(index.applyEq(4.0))))
		assert_that(index.docids(), has_item(3))
		index.unindex_doc(3)
		assert_that(set(), is_(set(index.applyEq(4.0))))
		assert_that(index.docids(), does_not(has_item(3)))

	def test_eq(self):
		index = self._makeOne()
		result = index.eq(1.0)
		assert_that(result.__class__, query.Eq)
		assert_that(result._value, is_(one_ival))
		
	def test_noteq(self):
		index = self._makeOne()
		result = index.noteq(1.0)
		assert_that(result.__class__, query.NotEq)
		assert_that(result._value, is_(one_ival))

	def test_ge(self):
		index = self._makeOne()
		result = index.ge(1.0)
		assert_that(result.__class__, query.Ge)
		assert_that(result._value, is_(one_ival))

	def test_le(self):
		index = self._makeOne()
		result = index.le(1.0)
		assert_that(result.__class__, query.Le)
		assert_that(result._value, is_(one_ival))
		
	def test_gt(self):
		index = self._makeOne()
		result = index.gt(1.0)
		assert_that(result.__class__, query.Gt)
		assert_that(result._value, is_(one_ival))

	def test_lt(self):
		index = self._makeOne()
		result = index.lt(1.0)
		assert_that(result.__class__, query.Gt)
		assert_that(result._value, is_(one_ival))
		
	def test_docids(self):
		index = self._makeOne()
		self._populateIndex(index)
		assert_that(
			set(index.docids()),
			is_(set((1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))))

	def test_docids_with_indexed_and_not_indexed(self):
		index = self._makeOne()
		index.index_doc(1, 1.0)
		index.index_doc(2, _marker)
		assert_that(set([1, 2]), is_(set(index.docids())))
