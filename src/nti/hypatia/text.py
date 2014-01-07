#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from hypatia.text.okapiindex import OkapiIndex
from hypatia.text.cosineindex import CosineIndex

class SourceBaseIndexMixin(object):

	def reset(self):
		super(SourceBaseIndexMixin, self).reset()
		self._textsource = self.family.IO.BTree()

	def index_doc(self, docid, text):
		super(SourceBaseIndexMixin, self).index_doc(docid, text)
		self._textsource[docid] = text or u''

	def reindex_doc(self, docid, text):
		super(SourceBaseIndexMixin, self).reindex_doc(docid, text)
		self._textsource[docid] = text or u''

	def unindex_doc(self, docid):
		super(SourceBaseIndexMixin, self).unindex_doc(docid)
		if docid in self._textsource: 
			del self._textsource[docid]

	def text_source(self, docid):
		return self._textsource[docid]

class SourceCosineIndex(SourceBaseIndexMixin, CosineIndex):

	def __init__(self, *args, **kwargs):
		CosineIndex.__init__(self, *args, **kwargs)

class SourceOkapiIndex(SourceBaseIndexMixin, OkapiIndex):

	def __init__(self, *args, **kwargs):
		OkapiIndex.__init__(self, *args, **kwargs)
