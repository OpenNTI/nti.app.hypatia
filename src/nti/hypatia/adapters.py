#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
graphdb adapters

$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import time
from datetime import datetime

from zope import component
from zope import interface

from nti.assessment import interfaces as asm_interfaces

from nti.dataserver import interfaces as nti_interfaces
from nti.dataserver.users import interfaces as user_interfaces
from nti.dataserver.contenttypes.forums import interfaces as frm_interfaces

from nti.externalization import externalization

from nti.utils.maps import CaseInsensitiveDict

from . import interfaces as graph_interfaces

def _to_isoformat(t):
	d = datetime.fromtimestamp(t)
	return unicode(d.isoformat())

#### labels

@interface.implementer(graph_interfaces.ILabelAdapter)
@component.adapter(interface.Interface)
def _GenericLabelAdpater(obj):
	return ()

@interface.implementer(graph_interfaces.ILabelAdapter)
@component.adapter(nti_interfaces.IEntity)
def _EntityLabelAdpater(entity):
	return (entity.__class__.__name__.lower(),)

@interface.implementer(graph_interfaces.ILabelAdapter)
@component.adapter(nti_interfaces.IDynamicSharingTargetFriendsList)
def _DFLLabelAdpater(obj):
	return ('dfl',)

@interface.implementer(graph_interfaces.ILabelAdapter)
@component.adapter(nti_interfaces.IModeledContent)
def _ModeledContentLabelAdpater(modeled):
	return (modeled.__class__.__name__.lower(),)

@component.adapter(nti_interfaces.INote)
def _NoteLabelAdpater(note):
	result = set()
	result.update(getattr(note, 'tags', ()))
	result.update(getattr(note, 'AutoTags', ()))
	return ('note',) + tuple([r.lower() for r in result])

@interface.implementer(graph_interfaces.ILabelAdapter)
def _CommentLabelAdpater(obj):
	result = ('comment',)
	return result

@interface.implementer(graph_interfaces.ILabelAdapter)
@component.adapter(frm_interfaces.ITopic)
def _TopicLabelAdpater(topic):
	result = set(topic.tags or ())
	tags = getattr(getattr(topic, 'headline', None), 'tags', ())
	result.update(tags or ())
	return ('topic',) + tuple([r.lower() for r in result])
	
@interface.implementer(graph_interfaces.ILabelAdapter)
@component.adapter(asm_interfaces.IQAssessedQuestionSet)
def _QuestionSetLabelAdpater(obj):
	result = ('questionset',)
	return result

@interface.implementer(graph_interfaces.ILabelAdapter)
@component.adapter(asm_interfaces.IQAssessedQuestion)
def _QuestionLabelAdpater(question):
	result = ('question',)
	return result

#### properties

@interface.implementer(graph_interfaces.IPropertyAdapter)
@component.adapter(interface.Interface)
def _GenericPropertyAdpater(obj):
	return CaseInsensitiveDict()

@interface.implementer(graph_interfaces.IPropertyAdapter)
@component.adapter(nti_interfaces.IEntity)
def _EntityPropertyAdpater(entity):
	result = CaseInsensitiveDict({"username":entity.username})
	names = user_interfaces.IFriendlyNamed(entity, None)
	alias = getattr(names, 'alias', None)
	name = getattr(names, 'realname', None)
	for key, value in (('alias', alias), ('name', name)):
		if value:
			result[key] = unicode(value)
	result['oid'] = externalization.to_external_ntiid_oid(entity)
	return result

@component.adapter(nti_interfaces.ICommunity)
def _CommunityPropertyAdpater(community):
	result = _EntityPropertyAdpater(community)
	result['type'] = u'Community'
	return result

@component.adapter(nti_interfaces.IUser)
def _UserPropertyAdpater(user):
	result = _EntityPropertyAdpater(user)
	result['type'] = u'User'
	return result

@component.adapter(nti_interfaces.IDynamicSharingTargetFriendsList)
def _DFLPropertyAdpater(dfl):
	result = _EntityPropertyAdpater(dfl)
	result['type'] = u'DFL'
	return result

@interface.implementer(graph_interfaces.IPropertyAdapter)
@component.adapter(nti_interfaces.IModeledContent)
def _ModeledContentPropertyAdpater(modeled):
	result = CaseInsensitiveDict({'type':modeled.__class__.__name__})
	result['creator'] = modeled.creator.username
	result['created'] = _to_isoformat(modeled.createdTime)
	result['oid'] = externalization.to_external_ntiid_oid(modeled)
	return result

@component.adapter(nti_interfaces.INote)
def _NotePropertyAdpater(note):
	result = _ModeledContentPropertyAdpater(note)
	result['title'] = unicode(note.title)
	return result

@interface.implementer(graph_interfaces.IPropertyAdapter)
@component.adapter(frm_interfaces.ITopic)
def _TopicPropertyAdpater(topic):
	result = CaseInsensitiveDict({'type':'Topic'})
	result['author'] = topic.creator.username
	result['title'] = unicode(topic.title)
	result['ntiid'] = topic.NTIID
	result['oid'] = externalization.to_external_ntiid_oid(topic)
	return result

@interface.implementer(graph_interfaces.IPropertyAdapter)
def _CommentPropertyAdpater(post):
	result = CaseInsensitiveDict({'type':'Comment'})
	result['author'] = post.creator.username
	result['oid'] = externalization.to_external_ntiid_oid(post)
	return result

@interface.implementer(graph_interfaces.IPropertyAdapter)
def _CommentRelationshipPropertyAdpater(_from, _post, _rel):
	result = CaseInsensitiveDict({'created': _to_isoformat(_post.createdTime)})
	result['oid'] = externalization.to_external_ntiid_oid(_post)
	result['topic'] = _post.__parent__.NTIID
	return result

@interface.implementer(graph_interfaces.IPropertyAdapter)
@component.adapter(asm_interfaces.IQAssessedQuestionSet)
def _QuestionSetPropertyAdpater(obj):
	result = CaseInsensitiveDict({'type':'QuestionSet'})
	result['id'] = obj.questionSetId
	return result
	
@interface.implementer(graph_interfaces.IPropertyAdapter)
@component.adapter(asm_interfaces.IQAssessedQuestion)
def _QuestionPropertyAdpater(obj):
	result = CaseInsensitiveDict({'type':'Question'})
	result['id'] = obj.questionId
	return result

def _question_stats(question):
	total = incorrect = correct = partial = 0
	for part in question.parts:
		total += 1
		if part.assessedValue <= 0.01:
			incorrect += 1
		elif part.assessedValue >= 0.99:
			correct += 1
		else:
			partial += 1
	return (total == correct, total == incorrect, partial > 0)

@interface.implementer(graph_interfaces.IUniqueAttributeAdapter)
def _CreatedTimePropertyAdpater(_from, _to, _rel):
	result = CaseInsensitiveDict({'created':_to_isoformat(time.time())})
	return result

@interface.implementer(graph_interfaces.IPropertyAdapter)
@component.adapter(nti_interfaces.IUser, asm_interfaces.IQAssessedQuestion,
				   graph_interfaces.ITakeAssessment)
def _AssessedQuestionRelationshipPropertyAdpater(_from, _question, _rel):
	result = CaseInsensitiveDict({'taker' : _from.username})
	result['created'] = _to_isoformat(_question.createdTime)
	result['oid'] = externalization.to_external_ntiid_oid(_question)
	is_correct, is_incorrect, partial = _question_stats(_question)
	result['correct'] = is_correct
	result['incorrect'] = is_incorrect
	result['partial'] = partial
	return result

@interface.implementer(graph_interfaces.IPropertyAdapter)
@component.adapter(nti_interfaces.IUser, asm_interfaces.IQAssessedQuestionSet,
				  graph_interfaces.ITakeAssessment)
def _AssessedQuestionSetRelationshipPropertyAdpater(_from, _qset, _rel):
	result = CaseInsensitiveDict({'taker' : _from.username})
	result['created'] = _to_isoformat(_qset.createdTime)
	result['oid'] = externalization.to_external_ntiid_oid(_qset)
	correct = incorrect = 0
	questions = _qset.questions
	for question in questions:
		is_correct, is_incorrect, _ = _question_stats(question)
		if is_correct:
			correct += 1
		elif is_incorrect:
			incorrect += 1
	result['correct'] = correct
	result['incorrect'] = incorrect
	return result

_LikeRelationshipPropertyAdpater = _CreatedTimePropertyAdpater
_RateRelationshipPropertyAdpater = _CreatedTimePropertyAdpater
_FollowRelationshipPropertyAdpater = _CreatedTimePropertyAdpater
_AuthorshipRelationshipPropertyAdpater = _CreatedTimePropertyAdpater

#### unique attribute

@interface.implementer(graph_interfaces.IUniqueAttributeAdapter)
@component.adapter(interface.Interface)
class _GenericUniqueAttributeAdpater(object):

	key = value = None

	def __init__(self, obj):
		pass

@interface.implementer(graph_interfaces.IUniqueAttributeAdapter)
class _OIDUniqueAttributeAdpater(object):

	key = "oid"

	def __init__(self, obj):
		self.obj = obj

	@property
	def value(self):
		result = externalization.to_external_ntiid_oid(self.obj)
		return result

@interface.implementer(graph_interfaces.IUniqueAttributeAdapter)
class _EndRelationshipUniqueAttributeAdpater(_OIDUniqueAttributeAdpater):

	def __init__(self, _from, _to, _rel):
		# a relationship is identified by the end object oid
		super(_EndRelationshipUniqueAttributeAdpater, self).__init__(_to)

@interface.implementer(graph_interfaces.IUniqueAttributeAdapter)
@component.adapter(nti_interfaces.IEntity)
class _EntityUniqueAttributeAdpater(object):

	key = "username"

	def __init__(self, obj):
		self.obj = obj

	@property
	def value(self):
		return self.obj.username

@interface.implementer(graph_interfaces.IUniqueAttributeAdapter)
@component.adapter(nti_interfaces.IModeledContent)
class _ModeledContentUniqueAttributeAdpater(_OIDUniqueAttributeAdpater):
	pass

@interface.implementer(graph_interfaces.IUniqueAttributeAdapter)
@component.adapter(frm_interfaces.ITopic)
class _TopicUniqueAttributeAdpater(object):

	key = "ntiid"

	def __init__(self, obj):
		self.obj = obj

	@property
	def value(self):
		return self.obj.NTIID

@interface.implementer(graph_interfaces.IUniqueAttributeAdapter)
class _CommentUniqueAttributeAdpater(_OIDUniqueAttributeAdpater):
	pass

@interface.implementer(graph_interfaces.IUniqueAttributeAdapter)
class _CommentRelationshipUniqueAttributeAdpater(object):

	def __init__(self, _from, _to, _rel):
		self._to = _to
		self._rel = _rel
		self._from = _from

	@property
	def key(self):
		return self._from.username

	@property
	def value(self):
		oid = externalization.to_external_ntiid_oid(self._to)
		result = '%s,%s' % (self._rel, oid)
		return result

@interface.implementer(graph_interfaces.IUniqueAttributeAdapter)
class _RelationshipUniqueAttributeAdpater(object):

	def __init__(self, _from, _to, _rel):
		self._to = _to
		self._rel = _rel
		self._from = _from

	@property
	def key(self):
		return self._from.username

	@property
	def value(self):
		result = '%s,%s' % (self._rel, self._to.username)
		return result

_FollowUniqueAttributeAdpater = _RelationshipUniqueAttributeAdpater
_FriendshipUniqueAttributeAdpater = _RelationshipUniqueAttributeAdpater
_TargetMembershipUniqueAttributeAdpater = _RelationshipUniqueAttributeAdpater

@interface.implementer(graph_interfaces.IUniqueAttributeAdapter)
@component.adapter(asm_interfaces.IQAssessedQuestionSet)
class _QuestionSetUniqueAttributeAdpater(object):

	key = "id"

	def __init__(self, obj):
		self.obj = obj

	@property
	def value(self):
		return self.obj.questionSetId

@interface.implementer(graph_interfaces.IUniqueAttributeAdapter)
@component.adapter(asm_interfaces.IQAssessedQuestion)
class _QuestionUniqueAttributeAdpater(object):

	key = "id"

	def __init__(self, obj):
		self.obj = obj

	@property
	def value(self):
		return self.obj.questionId

_AssessedRelationshipUniqueAttributeAdpater = _EndRelationshipUniqueAttributeAdpater

@interface.implementer(graph_interfaces.IUniqueAttributeAdapter)
@component.adapter(asm_interfaces.IQAssessedQuestion,
				   asm_interfaces.IQAssessedQuestionSet,
				   graph_interfaces.IMemberOf)
class _QuestionMembershipUniqueAttributeAdpater(object):

	def __init__(self, _from, _to, _rel):
		self._to = _to
		self._rel = _rel
		self._from = _from

	@property
	def key(self):
		return self._from.questionId

	@property
	def value(self):
		result = '%s,%s' % (self._rel, self._to.questionSetId)
		return result

@interface.implementer(graph_interfaces.IUniqueAttributeAdapter)
class _UserObjectUniqueAttributeAdpater(object):

	def __init__(self, _from, _to, _rel):
		self._to = _to
		self._rel = _rel
		self._from = _from

	@property
	def key(self):
		return self._from.username

	@property
	def value(self):
		oid = externalization.to_external_ntiid_oid(self._to)
		result = '%s,%s' % (self._rel, oid)
		return result

@component.adapter(nti_interfaces.IEntity,
				   nti_interfaces.ILikeable,
				   graph_interfaces.ILike)
class _LikeUniqueAttributeAdpater(_UserObjectUniqueAttributeAdpater):
	pass

@component.adapter(nti_interfaces.IEntity,
				   nti_interfaces.IRatable,
				   graph_interfaces.IRate)
class _RateUniqueAttributeAdpater(_UserObjectUniqueAttributeAdpater):
	pass

@component.adapter(nti_interfaces.IEntity,
				   nti_interfaces.IThreadable,
				   graph_interfaces.IReply)
class _InReplyToUniqueAttributeAdpater(_UserObjectUniqueAttributeAdpater):
	pass

_AuthorshipUniqueAttributeAdpater = _UserObjectUniqueAttributeAdpater
