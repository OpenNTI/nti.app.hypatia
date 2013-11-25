#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
graphdb db factory

$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from . import neo4j
from . import interfaces as graph_interfaces

def create_database(url, username=None, password=None, dbtype=graph_interfaces.NEO4J):
	if dbtype.lower() == graph_interfaces.NEO4J:
		result = neo4j.Neo4jDB(url, username, password)
	else:
		raise Exception("Invalid database type")
	return result

