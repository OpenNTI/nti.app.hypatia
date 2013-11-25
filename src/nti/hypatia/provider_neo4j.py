#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
Neo4J graphdb query provider

$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from zope import interface

from . import relationships
from . import interfaces as graph_interfaces

@interface.implementer(graph_interfaces.IGraphQueryProvider)
class Neo4jQueryProvider(object):

	def __init__(self, db):
		self.db = db
		
	def suggest_friends_to(self, user, max_depth=2, limit=None):
		node = self.db.get_node(user)
		if node is None:
			result = ()
		else:
			result = []
			# prepare query
			rel_type = str(relationships.FriendOf())
			query = ["START", "n=node(%s)" % node.id]
			query.append("MATCH (n)-[:%s*2..%s]-(x)" % (rel_type, max_depth))
			query.append("WHERE n <> x")
			query.append("AND NOT n-[:%s]->x" % rel_type)
			query.append("WITH x, n")
			query.append("MATCH pmfs=x-[?:%s]->mf<-[?:%s]-n" % (rel_type, rel_type))
			query.append("RETURN x.username as username, COUNT(DISTINCT pmfs) as mfs")
			query.append("ORDER BY mfs DESC, username")
			if limit:
				query.append("LIMIT %s" % limit)
			query.append(";")
			query = ' '.join(query)
			
			# execute query
			cyper_results = self.db.execute(query)
			for data in cyper_results:
				result.append((data.username, data.mfs))

		return result
