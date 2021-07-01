//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 -2011 EMC Corp.
//
//	@filename:
//		CGroup.cpp
//
//	@doc:
//		Implementation of Memo groups; database agnostic
//---------------------------------------------------------------------------

#include "gpopt/search/CGroup.h"

#include "gpos/base.h"
#include "gpos/common/owner.h"
#include "gpos/error/CAutoTrace.h"
#include "gpos/task/CAutoSuspendAbort.h"
#include "gpos/task/CAutoTraceFlag.h"
#include "gpos/task/CWorker.h"

#include "gpopt/base/CDrvdProp.h"
#include "gpopt/base/CDrvdPropCtxtPlan.h"
#include "gpopt/base/CDrvdPropCtxtRelational.h"
#include "gpopt/base/COptimizationContext.h"
#include "gpopt/exception.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalCTEConsumer.h"
#include "gpopt/operators/CLogicalCTEProducer.h"
#include "gpopt/operators/CLogicalInnerJoin.h"
#include "gpopt/operators/COperator.h"
#include "gpopt/operators/CPhysicalMotionGather.h"
#include "gpopt/operators/CScalarSubquery.h"
#include "gpopt/search/CGroupProxy.h"
#include "gpopt/search/CJobGroup.h"
#include "naucrates/statistics/CStatistics.h"
#include "naucrates/traceflags/traceflags.h"

using namespace gpnaucrates;
using namespace gpopt;

FORCE_GENERATE_DBGSTR(CGroup);

#define GPOPT_OPTCTXT_HT_BUCKETS 100


//---------------------------------------------------------------------------
//	@function:
//		CGroup::SContextLink::SContextLink
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CGroup::SContextLink::SContextLink(gpos::owner<CCostContext *> pccParent,
								   ULONG child_index,
								   gpos::owner<COptimizationContext *> poc)
	: m_pccParent(std::move(pccParent)),
	  m_ulChildIndex(child_index),
	  m_poc(std::move(poc))
{
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::SContextLink::~SContextLink
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CGroup::SContextLink::~SContextLink()
{
	CRefCount::SafeRelease(m_pccParent);
	CRefCount::SafeRelease(m_poc);
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::SContextLink::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CGroup::SContextLink::HashValue(const SContextLink *pclink)
{
	ULONG ulHashPcc = 0;
	if (nullptr != pclink->m_pccParent)
	{
		ulHashPcc = CCostContext::HashValue(*pclink->m_pccParent);
	}

	ULONG ulHashPoc = 0;
	if (nullptr != pclink->m_poc)
	{
		ulHashPoc = COptimizationContext::HashValue(*pclink->m_poc);
	}

	return CombineHashes(pclink->m_ulChildIndex,
						 CombineHashes(ulHashPcc, ulHashPoc));
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::SContextLink::Equals
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
BOOL
CGroup::SContextLink::Equals(const SContextLink *pclink1,
							 const SContextLink *pclink2)
{
	BOOL fEqualChildIndexes =
		(pclink1->m_ulChildIndex == pclink2->m_ulChildIndex);
	BOOL fEqual = false;
	if (fEqualChildIndexes)
	{
		if (nullptr == pclink1->m_pccParent || nullptr == pclink2->m_pccParent)
		{
			fEqual = (nullptr == pclink1->m_pccParent &&
					  nullptr == pclink2->m_pccParent);
		}
		else
		{
			fEqual = (*pclink1->m_pccParent == *pclink2->m_pccParent);
		}
	}

	if (fEqual)
	{
		if (nullptr == pclink1->m_poc || nullptr == pclink2->m_poc)
		{
			return (nullptr == pclink1->m_poc && nullptr == pclink2->m_poc);
		}

		return COptimizationContext::Equals(*pclink1->m_poc, *pclink2->m_poc);
	}

	return fEqual;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::CGroup
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CGroup::CGroup(CMemoryPool *mp, BOOL fScalar)
	: m_mp(mp),
	  m_id(GPOPT_INVALID_GROUP_ID),
	  m_fScalar(fScalar),
	  m_pdrgpexprJoinKeysOuter(nullptr),
	  m_pdrgpexprJoinKeysInner(nullptr),
	  m_join_opfamilies(nullptr),
	  m_pdp(nullptr),
	  m_pstats(nullptr),
	  m_pexprScalarRep(nullptr),
	  m_pexprScalarRepIsExact(false),
	  m_pccDummy(nullptr),
	  m_pgroupDuplicate(nullptr),
	  m_plinkmap(nullptr),
	  m_pstatsmap(nullptr),
	  m_ulGExprs(0),
	  m_pcostmap(nullptr),
	  m_ulpOptCtxts(0),
	  m_estate(estUnexplored),
	  m_eolMax(EolLow),
	  m_fHasNewLogicalOperators(false),
	  m_ulCTEProducerId(gpos::ulong_max),
	  m_fCTEConsumer(false)
{
	GPOS_ASSERT(nullptr != mp);

	m_listGExprs.Init(GPOS_OFFSET(CGroupExpression, m_linkGroup));
	m_listDupGExprs.Init(GPOS_OFFSET(CGroupExpression, m_linkGroup));

	m_sht.Init(
		mp, GPOPT_OPTCTXT_HT_BUCKETS, GPOS_OFFSET(COptimizationContext, m_link),
		0, /*cKeyOffset (0 because we use COptimizationContext class as key)*/
		&(COptimizationContext::m_ocInvalid), COptimizationContext::HashValue,
		COptimizationContext::Equals);
	m_plinkmap = GPOS_NEW(mp) LinkMap(mp);
	m_pstatsmap = GPOS_NEW(mp) OptCtxtToIStatisticsMap(mp);
	m_pcostmap = GPOS_NEW(mp) ReqdPropPlanToCostMap(mp);
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::~CGroup
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CGroup::~CGroup()
{
	CRefCount::SafeRelease(m_pdrgpexprJoinKeysOuter);
	CRefCount::SafeRelease(m_pdrgpexprJoinKeysInner);
	CRefCount::SafeRelease(m_join_opfamilies);
	CRefCount::SafeRelease(m_pdp);
	CRefCount::SafeRelease(m_pexprScalarRep);
	CRefCount::SafeRelease(m_pccDummy);
	CRefCount::SafeRelease(m_pstats);
	m_plinkmap->Release();
	m_pstatsmap->Release();
	m_pcostmap->Release();

	// cleaning-up group expressions
	gpos::owner<CGroupExpression *> pgexpr = m_listGExprs.First();
	while (nullptr != pgexpr)
	{
		CGroupExpression *pgexprNext = m_listGExprs.Next(pgexpr);
		pgexpr->CleanupContexts();
		pgexpr->Release();

		pgexpr = pgexprNext;
	}

	// cleaning-up duplicate expressions
	pgexpr = m_listDupGExprs.First();
	while (nullptr != pgexpr)
	{
		CGroupExpression *pgexprNext = m_listDupGExprs.Next(pgexpr);
		pgexpr->CleanupContexts();
		pgexpr->Release();

		pgexpr = pgexprNext;
	}

	// cleanup optimization contexts
	CleanupContexts();
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::CleanupContexts
//
//	@doc:
//		 Destroy stored contexts in hash table
//
//---------------------------------------------------------------------------
void
CGroup::CleanupContexts()
{
	// need to suspend cancellation while cleaning up
	{
		CAutoSuspendAbort asa;

		gpos::owner<COptimizationContext *> poc = nullptr;
		ShtIter shtit(m_sht);

		while (nullptr != poc || shtit.Advance())
		{
			CRefCount::SafeRelease(poc);

			// iter's accessor scope
			{
				ShtAccIter shtitacc(shtit);
				if (nullptr != (poc = shtitacc.Value()))
				{
					shtitacc.Remove(poc);
				}
			}
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::UpdateBestCost
//
//	@doc:
//		 Update the group expression with best cost under the given
//		 optimization context
//
//---------------------------------------------------------------------------
void
CGroup::UpdateBestCost(gpos::pointer<COptimizationContext *> poc,
					   gpos::pointer<CCostContext *> pcc)
{
	GPOS_ASSERT(CCostContext::estCosted == pcc->Est());

	COptimizationContext *pocFound = nullptr;

	{
		// scope for accessor
		ShtAcc shta(Sht(), *poc);
		pocFound = shta.Find();
	}

	if (nullptr == pocFound)
	{
		// it should never happen, but instead of crashing, raise an exception
		GPOS_RAISE(CException::ExmaInvalid, CException::ExmiInvalid,
				   GPOS_WSZ_LIT(
					   "Updating cost for non-existing optimization context"));
	}

	// update best cost context
	gpos::pointer<CCostContext *> pccBest = pocFound->PccBest();
	if (GPOPT_INVALID_COST != pcc->Cost() &&
		(nullptr == pccBest || pcc->FBetterThan(pccBest)))
	{
		pocFound->SetBest(pcc);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::PocLookup
//
//	@doc:
//		Lookup a given context in contexts hash table
//
//---------------------------------------------------------------------------
COptimizationContext *
CGroup::PocLookup(CMemoryPool *mp, CReqdPropPlan *prpp,
				  ULONG ulSearchStageIndex)
{
	prpp->AddRef();
	gpos::owner<COptimizationContext *> poc = GPOS_NEW(mp) COptimizationContext(
		mp, this, prpp,
		GPOS_NEW(mp) CReqdPropRelational(GPOS_NEW(mp) CColRefSet(
			mp)),  // required relational props is not used when looking up contexts
		GPOS_NEW(mp) IStatisticsArray(
			mp),  // stats context is not used when looking up contexts
		ulSearchStageIndex);

	COptimizationContext *pocFound = nullptr;
	{
		ShtAcc shta(Sht(), *poc);
		pocFound = shta.Find();
	}
	poc->Release();

	return pocFound;
}



//---------------------------------------------------------------------------
//	@function:
//		CGroup::PocLookupBest
//
//	@doc:
//		Lookup the best context across all stages for the given required
//		properties
//
//---------------------------------------------------------------------------
COptimizationContext *
CGroup::PocLookupBest(CMemoryPool *mp, ULONG ulSearchStages,
					  CReqdPropPlan *prpp)
{
	COptimizationContext *pocBest = nullptr;
	CCostContext *pccBest = nullptr;
	for (ULONG ul = 0; ul < ulSearchStages; ul++)
	{
		COptimizationContext *pocCurrent = PocLookup(mp, prpp, ul);
		if (nullptr == pocCurrent)
		{
			continue;
		}

		CCostContext *pccCurrent = pocCurrent->PccBest();
		if (nullptr == pccBest ||
			(nullptr != pccCurrent && pccCurrent->FBetterThan(pccBest)))
		{
			pocBest = pocCurrent;
			pccBest = pccCurrent;
		}
	}

	return pocBest;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::Ppoc
//
//	@doc:
//		Lookup a context by id
//
//---------------------------------------------------------------------------
COptimizationContext *
CGroup::Ppoc(ULONG id) const
{
	COptimizationContext *poc = nullptr;
	ShtIter shtit(const_cast<CGroup *>(this)->m_sht);
	while (shtit.Advance())
	{
		{
			ShtAccIter shtitacc(shtit);
			poc = shtitacc.Value();

			if (poc->Id() == id)
			{
				return poc;
			}
		}
	}
	return nullptr;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::PocInsert
//
//	@doc:
//		Insert a given context into contexts hash table only if a matching
//		context does not already exist;
//		return either the inserted or the existing matching context
//
//---------------------------------------------------------------------------
COptimizationContext *
CGroup::PocInsert(COptimizationContext *poc)
{
	ShtAcc shta(Sht(), *poc);

	COptimizationContext *pocFound = shta.Find();
	if (nullptr == pocFound)
	{
		poc->SetId((ULONG) UlpIncOptCtxts());
		shta.Insert(poc);
		return poc;
	}

	return pocFound;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::PgexprBest
//
//	@doc:
//		Lookup best group expression under optimization context
//
//---------------------------------------------------------------------------
CGroupExpression *
CGroup::PgexprBest(gpos::pointer<COptimizationContext *> poc)
{
	ShtAcc shta(Sht(), *poc);
	gpos::pointer<COptimizationContext *> pocFound = shta.Find();
	if (nullptr != pocFound)
	{
		return pocFound->PgexprBest();
	}

	return nullptr;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::SetId
//
//	@doc:
//		Set group id;
//		separated from constructor to avoid synchronization issues
//
//---------------------------------------------------------------------------
void
CGroup::SetId(ULONG id)
{
	GPOS_ASSERT(GPOPT_INVALID_GROUP_ID == m_id &&
				"Overwriting previously assigned group id");

	m_id = id;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::InitProperties
//
//	@doc:
//		Initialize group's properties
//
//---------------------------------------------------------------------------
void
CGroup::InitProperties(CDrvdProp *pdp)
{
	GPOS_ASSERT(nullptr == m_pdp);
	GPOS_ASSERT(nullptr != pdp);
	GPOS_ASSERT_IMP(FScalar(), CDrvdProp::EptScalar == pdp->Ept());
	GPOS_ASSERT_IMP(!FScalar(), CDrvdProp::EptRelational == pdp->Ept());

	m_pdp = pdp;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::InitStats
//
//	@doc:
//		Initialize group's stats
//
//---------------------------------------------------------------------------
void
CGroup::InitStats(gpos::owner<IStatistics *> stats)
{
	GPOS_ASSERT(nullptr == m_pstats);
	GPOS_ASSERT(nullptr != stats);

	m_pstats = stats;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::SetState
//
//	@doc:
//		Set group state;
//
//---------------------------------------------------------------------------
void
CGroup::SetState(EState estNewState)
{
	GPOS_ASSERT(estNewState == (EState)(m_estate + 1));

	m_estate = estNewState;
}


void
CGroup::SetJoinKeys(gpos::pointer<CExpressionArray *> pdrgpexprOuter,
					gpos::pointer<CExpressionArray *> pdrgpexprInner,
					gpos::pointer<IMdIdArray *> join_opfamilies)
{
	GPOS_ASSERT(m_fScalar);
	GPOS_ASSERT(nullptr != pdrgpexprOuter);
	GPOS_ASSERT(nullptr != pdrgpexprInner);

	if (nullptr != m_pdrgpexprJoinKeysOuter)
	{
		GPOS_ASSERT(nullptr != m_pdrgpexprJoinKeysInner);

		// hash join keys have been already set, exit here
		return;
	}

	pdrgpexprOuter->AddRef();
	m_pdrgpexprJoinKeysOuter = pdrgpexprOuter;

	pdrgpexprInner->AddRef();
	m_pdrgpexprJoinKeysInner = pdrgpexprInner;

	if (GPOS_FTRACE(EopttraceConsiderOpfamiliesForDistribution))
	{
		GPOS_ASSERT(join_opfamilies != nullptr);
		join_opfamilies->AddRef();
	}
	else
	{
		GPOS_ASSERT(nullptr == join_opfamilies);
	}
	m_join_opfamilies = join_opfamilies;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::HashValue
//
//	@doc:
//		Hash function for group identification
//
//---------------------------------------------------------------------------
ULONG
CGroup::HashValue() const
{
	ULONG id = m_id;
	if (FDuplicateGroup() && 0 == m_ulGExprs)
	{
		// group has been merged into another group
		id = PgroupDuplicate()->Id();
	}

	return gpos::HashValue<ULONG>(&id);
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::Insert
//
//	@doc:
//		Insert group expression
//
//---------------------------------------------------------------------------
void
CGroup::Insert(CGroupExpression *pgexpr)
{
	m_listGExprs.Append(pgexpr);
	gpos::pointer<COperator *> pop = pgexpr->Pop();
	if (pop->FLogical())
	{
		m_fHasNewLogicalOperators = true;
		if (COperator::EopLogicalCTEConsumer == pop->Eopid())
		{
			m_fCTEConsumer = true;
		}

		if (COperator::EopLogicalCTEProducer == pop->Eopid())
		{
			GPOS_ASSERT(gpos::ulong_max == m_ulCTEProducerId);
			m_ulCTEProducerId =
				gpos::dyn_cast<CLogicalCTEProducer>(pop)->UlCTEId();
			;
		}
	}

	if (pgexpr->Eol() > m_eolMax)
	{
		m_eolMax = pgexpr->Eol();
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::MoveDuplicateGExpr
//
//	@doc:
//		Move duplicate group expression to duplicates list
//
//---------------------------------------------------------------------------
void
CGroup::MoveDuplicateGExpr(CGroupExpression *pgexpr)
{
	m_listGExprs.Remove(pgexpr);
	m_ulGExprs--;

	m_listDupGExprs.Append(pgexpr);
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::PgexprAnyCTEConsumer
//
//	@doc:
//		Retrieve the group expression containing a CTE Consumer operator
//
//---------------------------------------------------------------------------
CGroupExpression *
CGroup::PgexprAnyCTEConsumer()
{
	BOOL fFoundCTEConsumer = false;
	// get first logical group expression
	CGroupExpression *pgexprCurrent = nullptr;
	{
		CGroupProxy gp(this);
		pgexprCurrent = gp.PgexprNextLogical(nullptr /*pgexpr*/);
		fFoundCTEConsumer =
			(COperator::EopLogicalCTEConsumer == pgexprCurrent->Pop()->Eopid());
	}

	while (nullptr != pgexprCurrent && !fFoundCTEConsumer)
	{
		GPOS_CHECK_ABORT;
		// move to next logical group expression
		{
			CGroupProxy gp(this);
			pgexprCurrent = gp.PgexprNextLogical(pgexprCurrent);
		}

		if (nullptr != pgexprCurrent)
		{
			gpos::pointer<COperator *> popCurrent = pgexprCurrent->Pop();
			fFoundCTEConsumer =
				(COperator::EopLogicalCTEConsumer == popCurrent->Eopid());
		}
	}

	if (fFoundCTEConsumer)
	{
		return pgexprCurrent;
	}

	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::PgexprFirst
//
//	@doc:
//		Retrieve first expression in group
//
//---------------------------------------------------------------------------
CGroupExpression *
CGroup::PgexprFirst()
{
	return m_listGExprs.First();
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::PgexprNext
//
//	@doc:
//		Retrieve next expression in group
//
//---------------------------------------------------------------------------
CGroupExpression *
CGroup::PgexprNext(gpos::pointer<CGroupExpression *> pgexpr)
{
	return m_listGExprs.Next(pgexpr);
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::FMatchGroups
//
//	@doc:
//		Determine whether two arrays of groups are equivalent
//
//---------------------------------------------------------------------------
BOOL
CGroup::FMatchGroups(gpos::pointer<CGroupArray *> pdrgpgroupFst,
					 gpos::pointer<CGroupArray *> pdrgpgroupSnd)
{
	ULONG arity = pdrgpgroupFst->Size();
	GPOS_ASSERT(pdrgpgroupSnd->Size() == arity);

	for (ULONG i = 0; i < arity; i++)
	{
		gpos::pointer<CGroup *> pgroupFst = (*pdrgpgroupFst)[i];
		gpos::pointer<CGroup *> pgroupSnd = (*pdrgpgroupSnd)[i];
		if (pgroupFst != pgroupSnd && !FDuplicateGroups(pgroupFst, pgroupSnd))
		{
			return false;
		}
	}

	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::FMatchNonScalarGroups
//
//	@doc:
//		 Matching of pairs of arrays of groups while skipping scalar groups
//
//---------------------------------------------------------------------------
BOOL
CGroup::FMatchNonScalarGroups(gpos::pointer<CGroupArray *> pdrgpgroupFst,
							  gpos::pointer<CGroupArray *> pdrgpgroupSnd)
{
	GPOS_ASSERT(nullptr != pdrgpgroupFst);
	GPOS_ASSERT(nullptr != pdrgpgroupSnd);

	if (pdrgpgroupFst->Size() != pdrgpgroupSnd->Size())
	{
		return false;
	}

	ULONG arity = pdrgpgroupFst->Size();
	GPOS_ASSERT(pdrgpgroupSnd->Size() == arity);

	for (ULONG i = 0; i < arity; i++)
	{
		gpos::pointer<CGroup *> pgroupFst = (*pdrgpgroupFst)[i];
		gpos::pointer<CGroup *> pgroupSnd = (*pdrgpgroupSnd)[i];
		if (pgroupFst->FScalar())
		{
			// skip scalar groups
			continue;
		}

		if (pgroupFst != pgroupSnd && !FDuplicateGroups(pgroupFst, pgroupSnd))
		{
			return false;
		}
	}

	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::FDuplicateGroups
//
//	@doc:
//		Determine whether two groups are equivalent
//
//---------------------------------------------------------------------------
BOOL
CGroup::FDuplicateGroups(gpos::pointer<CGroup *> pgroupFst,
						 gpos::pointer<CGroup *> pgroupSnd)
{
	GPOS_ASSERT(nullptr != pgroupFst);
	GPOS_ASSERT(nullptr != pgroupSnd);

	gpos::pointer<CGroup *> pgroupFstDup = pgroupFst->PgroupDuplicate();
	gpos::pointer<CGroup *> pgroupSndDup = pgroupSnd->PgroupDuplicate();

	return (pgroupFst == pgroupSnd) ||	// pointer equality
		   (pgroupFst ==
			pgroupSndDup) ||  // first group is duplicate of second group
		   (pgroupSnd ==
			pgroupFstDup) ||  // second group is duplicate of first group
		   // both groups have the same duplicate group
		   (nullptr != pgroupFstDup && nullptr != pgroupSndDup &&
			pgroupFstDup == pgroupSndDup);
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::FInitStats
//
//	@doc:
//		Attempt initializing stats with the given stat object
//
//---------------------------------------------------------------------------
BOOL
CGroup::FInitStats(gpos::owner<IStatistics *> stats)
{
	GPOS_ASSERT(nullptr != stats);

	CGroupProxy gp(this);
	if (nullptr == Pstats())
	{
		gp.InitStats(std::move(stats));
		return true;
	}

	// mark group as having no new logical operators to disallow
	// resetting stats until a new logical operator is inserted
	ResetHasNewLogicalOperators();

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::AppendStats
//
//	@doc:
//		Append given stats to group stats
//
//---------------------------------------------------------------------------
void
CGroup::AppendStats(CMemoryPool *mp, gpos::pointer<IStatistics *> stats)
{
	GPOS_ASSERT(nullptr != stats);
	GPOS_ASSERT(nullptr != Pstats());

	gpos::owner<IStatistics *> stats_copy = Pstats()->CopyStats(mp);
	stats_copy->AppendStats(mp, stats);

	gpos::owner<IStatistics *> current_stats = nullptr;
	{
		CGroupProxy gp(this);
		current_stats = m_pstats;
		m_pstats = nullptr;
		gp.InitStats(std::move(stats_copy));
	}

	current_stats->Release();
	current_stats = nullptr;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::AddDuplicateGrp
//
//	@doc:
//		Add duplicate group
//
//---------------------------------------------------------------------------
void
CGroup::AddDuplicateGrp(CGroup *pgroup)
{
	GPOS_ASSERT(nullptr != pgroup);

	// add link following monotonic ordering of group IDs
	CGroup *pgroupSrc = this;
	CGroup *pgroupDest = pgroup;
	if (pgroupSrc->Id() > pgroupDest->Id())
	{
		std::swap(pgroupSrc, pgroupDest);
	}

	// keep looping until we add link
	while (pgroupSrc->m_pgroupDuplicate != pgroupDest)
	{
		if (nullptr == pgroupSrc->m_pgroupDuplicate)
		{
			pgroupSrc->m_pgroupDuplicate = pgroupDest;
		}
		else
		{
			pgroupSrc = pgroupSrc->m_pgroupDuplicate;
			if (pgroupSrc->Id() > pgroupDest->Id())
			{
				std::swap(pgroupSrc, pgroupDest);
			}
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::ResolveDuplicateMaster
//
//	@doc:
//		Resolve master duplicate group
//
//---------------------------------------------------------------------------
void
CGroup::ResolveDuplicateMaster()
{
	if (!FDuplicateGroup())
	{
		return;
	}
	gpos::pointer<CGroup *> pgroupTarget = m_pgroupDuplicate;
	while (nullptr != pgroupTarget->m_pgroupDuplicate)
	{
		GPOS_ASSERT(pgroupTarget->Id() < pgroupTarget->m_pgroupDuplicate->Id());
		pgroupTarget = pgroupTarget->m_pgroupDuplicate;
	}

	// update reference to target group
	m_pgroupDuplicate = pgroupTarget;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::MergeGroup
//
//	@doc:
//		Merge group with its duplicate - not thread-safe
//
//---------------------------------------------------------------------------
void
CGroup::MergeGroup()
{
	if (!FDuplicateGroup())
	{
		return;
	}
	GPOS_ASSERT(FExplored());
	GPOS_ASSERT(!FImplemented());

	// resolve target group
	ResolveDuplicateMaster();
	gpos::pointer<CGroup *> pgroupTarget = m_pgroupDuplicate;

	// move group expressions from this group to target
	while (!m_listGExprs.IsEmpty())
	{
		CGroupExpression *pgexpr = m_listGExprs.RemoveHead();
		m_ulGExprs--;

		pgexpr->Reset(pgroupTarget, pgroupTarget->m_ulGExprs++);
		pgroupTarget->Insert(pgexpr);

		GPOS_CHECK_ABORT;
	}

	GPOS_ASSERT(0 == m_ulGExprs);
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::CreateScalarExpression
//
//	@doc:
//		Materialize a scalar expression for stat derivation only if
//		this is a scalar group
//
//---------------------------------------------------------------------------
void
CGroup::CreateScalarExpression()
{
	GPOS_ASSERT(FScalar());
	GPOS_ASSERT(nullptr == m_pexprScalarRep);

	CGroupExpression *pgexprFirst = nullptr;
	{
		CGroupProxy gp(this);
		pgexprFirst = gp.PgexprFirst();
	}
	GPOS_ASSERT(nullptr != pgexprFirst);
	COperator *pop = pgexprFirst->Pop();

	if (CUtils::FSubquery(pop))
	{
		if (COperator::EopScalarSubquery == pop->Eopid())
		{
			gpos::pointer<CScalarSubquery *> subquery_pop =
				gpos::dyn_cast<CScalarSubquery>(pop);
			const CColRef *subquery_colref = subquery_pop->Pcr();

			// replace the scalar subquery with a NULL value of the same type
			m_pexprScalarRep = CUtils::PexprScalarConstNull(
				m_mp, subquery_colref->RetrieveType(),
				subquery_colref->TypeModifier());
		}
		else
		{
			// for subqueries that are predicates, make a "true" constant
			m_pexprScalarRep = CUtils::PexprScalarConstBool(
				m_mp, true /* make a "true" constant*/);
		}

		m_pexprScalarRepIsExact = false;
		return;
	}

	m_pexprScalarRepIsExact = true;

	gpos::owner<CExpressionArray *> pdrgpexpr =
		GPOS_NEW(m_mp) CExpressionArray(m_mp);
	const ULONG arity = pgexprFirst->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		gpos::pointer<CGroup *> pgroupChild = (*pgexprFirst)[ul];
		GPOS_ASSERT(pgroupChild->FScalar());

		gpos::owner<CExpression *> pexprChild = pgroupChild->PexprScalarRep();
		pexprChild->AddRef();
		pdrgpexpr->Append(pexprChild);

		if (!pgroupChild->FScalarRepIsExact())
		{
			m_pexprScalarRepIsExact = false;
		}
	}

	pop->AddRef();
	m_pexprScalarRep =
		GPOS_NEW(m_mp) CExpression(m_mp, pop, std::move(pdrgpexpr));
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::CreateDummyCostContext
//
//	@doc:
//		Create a dummy cost context attached to the first group expression,
//		used for plan enumeration for scalar groups
//
//
//---------------------------------------------------------------------------
void
CGroup::CreateDummyCostContext()
{
	GPOS_ASSERT(FScalar());
	GPOS_ASSERT(nullptr == m_pccDummy);

	CGroupExpression *pgexprFirst = nullptr;
	{
		CGroupProxy gp(this);
		pgexprFirst = gp.PgexprFirst();
	}
	GPOS_ASSERT(nullptr != pgexprFirst);

	gpos::owner<COptimizationContext *> poc =
		GPOS_NEW(m_mp) COptimizationContext(
			m_mp, this, CReqdPropPlan::PrppEmpty(m_mp),
			GPOS_NEW(m_mp) CReqdPropRelational(GPOS_NEW(m_mp) CColRefSet(m_mp)),
			GPOS_NEW(m_mp) IStatisticsArray(m_mp),
			0  // ulSearchStageIndex
		);

	pgexprFirst->AddRef();
	m_pccDummy = GPOS_NEW(m_mp)
		CCostContext(m_mp, std::move(poc), 0 /*ulOptReq*/, pgexprFirst);
	m_pccDummy->SetState(CCostContext::estCosting);
	m_pccDummy->SetCost(CCost(0.0));
	m_pccDummy->SetState(CCostContext::estCosted);

#ifdef GPOS_DEBUG
	CGroupExpression *pgexprNext = nullptr;
	{
		CGroupProxy gp(this);
		pgexprNext = gp.PgexprNext(pgexprFirst);
	}
	GPOS_ASSERT(nullptr == pgexprNext &&
				"scalar group can only have one group expression");
#endif	// GPOS_DEBUG
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::RecursiveBuildTreeMap
//
//	@doc:
//		Find all cost contexts of current group expression that carry valid
//		implementation of the given optimization context,
//		for all such cost contexts, introduce a link from parent cost context
//		to child cost context and then process child groups recursively
//
//
//---------------------------------------------------------------------------
void
CGroup::RecursiveBuildTreeMap(
	CMemoryPool *mp, COptimizationContext *poc,
	gpos::pointer<CCostContext *> pccParent,
	gpos::pointer<CGroupExpression *> pgexprCurrent, ULONG child_index,
	CTreeMap<CCostContext, CExpression, CDrvdPropCtxtPlan,
			 CCostContext::HashValue, CCostContext::Equals> *ptmap)
{
	GPOS_ASSERT(pgexprCurrent->Pop()->FPhysical());
	GPOS_ASSERT(nullptr != ptmap);
	GPOS_ASSERT_IMP(nullptr != pccParent,
					child_index < pccParent->Pgexpr()->Arity());

	gpos::owner<CCostContextArray *> pdrgpcc =
		pgexprCurrent->PdrgpccLookupAll(mp, poc);
	const ULONG ulCCSize = pdrgpcc->Size();

	if (0 == ulCCSize)
	{
		// current group expression has no valid implementations of optimization context
		pdrgpcc->Release();
		return;
	}

	// iterate over all valid implementations of given optimization context
	for (ULONG ulCC = 0; ulCC < ulCCSize; ulCC++)
	{
		GPOS_CHECK_ABORT;

		CCostContext *pccCurrent = (*pdrgpcc)[ulCC];
		if (nullptr != pccParent)
		{
			// link parent cost context to child cost context
			ptmap->Insert(pccParent, child_index, pccCurrent);
		}

		gpos::pointer<COptimizationContextArray *> pdrgpoc =
			pccCurrent->Pdrgpoc();
		if (nullptr != pdrgpoc)
		{
			// process children recursively
			const ULONG arity = pgexprCurrent->Arity();
			for (ULONG ul = 0; ul < arity; ul++)
			{
				GPOS_CHECK_ABORT;

				gpos::pointer<CGroup *> pgroupChild = (*pgexprCurrent)[ul];
				COptimizationContext *pocChild = nullptr;
				if (!pgroupChild->FScalar())
				{
					pocChild = (*pdrgpoc)[ul];
					GPOS_ASSERT(nullptr != pocChild);
				}
				pgroupChild->BuildTreeMap(mp, pocChild, pccCurrent, ul, ptmap);
			}
		}
	}

	pdrgpcc->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::BuildTreeMap
//
//	@doc:
//		Given a parent cost context and an optimization context,
//		link parent cost context to all cost contexts in current group
//		that carry valid implementation of the given optimization context
//
//
//---------------------------------------------------------------------------
void
CGroup::BuildTreeMap(
	CMemoryPool *mp,
	COptimizationContext *poc,	// NULL if we are in a Scalar group
	CCostContext *pccParent,	// NULL if we are in the Root group
	ULONG
		child_index,  // index used for treating group as child of parent context
	CTreeMap<CCostContext, CExpression, CDrvdPropCtxtPlan,
			 CCostContext::HashValue, CCostContext::Equals>
		*ptmap	// map structure
)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(nullptr != ptmap);
	GPOS_ASSERT_IMP(nullptr == poc, FScalar());

#ifdef GPOS_DEBUG
	gpos::pointer<CGroupExpression *> pgexprParent = nullptr;
#endif	// GPOS_DEBUG
	if (nullptr != pccParent)
	{
		pccParent->AddRef();
#ifdef GPOS_DEBUG
		pgexprParent = pccParent->Pgexpr();
#endif	// GPOS_DEBUG
	}
	if (nullptr != poc)
	{
		poc->AddRef();
	}

	// check if link has been processed before,
	// this is crucial to eliminate unnecessary recursive calls
	SContextLink *pclink =
		GPOS_NEW(m_mp) SContextLink(pccParent, child_index, poc);
	if (m_plinkmap->Find(pclink))
	{
		// link is already processed
		GPOS_DELETE(pclink);
		return;
	}

	// start with first non-logical group expression
	CGroupExpression *pgexprCurrent = nullptr;
	{
		CGroupProxy gp(this);
		pgexprCurrent = gp.PgexprSkipLogical(nullptr /*pgexpr*/);
	}

	while (nullptr != pgexprCurrent)
	{
		gpos::pointer<COperator *> pop = pgexprCurrent->Pop();
		if (pop->FPhysical())
		{
			// create links recursively
			RecursiveBuildTreeMap(mp, poc, pccParent, pgexprCurrent,
								  child_index, ptmap);
		}
		else
		{
			GPOS_ASSERT(pop->FScalar());
			GPOS_ASSERT(nullptr == poc);
			GPOS_ASSERT(child_index < pgexprParent->Arity());

			// this is a scalar group, link parent cost context to group's dummy context
			ptmap->Insert(pccParent, child_index, PccDummy());

			// recursively link group's dummy context to child contexts
			const ULONG arity = pgexprCurrent->Arity();
			for (ULONG ul = 0; ul < arity; ul++)
			{
				gpos::pointer<CGroup *> pgroupChild = (*pgexprCurrent)[ul];
				GPOS_ASSERT(pgroupChild->FScalar());

				pgroupChild->BuildTreeMap(mp, nullptr /*poc*/, PccDummy(), ul,
										  ptmap);
			}
		}

		// move to next non-logical group expression
		{
			CGroupProxy gp(this);
			pgexprCurrent = gp.PgexprSkipLogical(pgexprCurrent);
		}
		GPOS_ASSERT_IMP(
			FScalar(), nullptr == pgexprCurrent &&
						   "a scalar group can only have one group expression");

		GPOS_CHECK_ABORT;
	}

	// remember processed links to avoid re-processing them later
	BOOL fInserted GPOS_ASSERTS_ONLY =
		m_plinkmap->Insert(pclink, GPOS_NEW(m_mp) BOOL(true));
	GPOS_ASSERT(fInserted);
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::FStatsDerivable
//
//	@doc:
//		Returns true if stats can be derived on this group
//
//---------------------------------------------------------------------------
BOOL
CGroup::FStatsDerivable(CMemoryPool *mp)
{
	GPOS_CHECK_STACK_SIZE;

	if (nullptr != m_pstats)
	{
		return true;
	}

	CGroupExpression *pgexprBest = nullptr;
	CLogical::EStatPromise espBest = CLogical::EspNone;

	CGroupExpression *pgexprCurrent = nullptr;
	// get first logical group expression
	{
		CGroupProxy gp(this);
		pgexprCurrent = gp.PgexprNextLogical(nullptr /*pgexpr*/);
	}

	while (nullptr != pgexprCurrent)
	{
		CExpressionHandle exprhdl(mp);
		exprhdl.Attach(pgexprCurrent);
		gpos::owner<CDrvdPropCtxtRelational *> pdpctxtrel =
			GPOS_NEW(mp) CDrvdPropCtxtRelational(mp);
		exprhdl.DeriveProps(pdpctxtrel);
		pdpctxtrel->Release();

		gpos::pointer<CLogical *> popLogical =
			gpos::dyn_cast<CLogical>(pgexprCurrent->Pop());
		CLogical::EStatPromise esp = popLogical->Esp(exprhdl);

		if (esp > espBest)
		{
			pgexprBest = pgexprCurrent;
			espBest = esp;
		}

		// move to next logical group expression
		{
			CGroupProxy gp(this);
			pgexprCurrent = gp.PgexprNextLogical(pgexprCurrent);
		}

		GPOS_CHECK_ABORT;
	}

	if (nullptr == pgexprBest)
	{
		return false;
	}

	BOOL fStatsDerivable = true;
	const ULONG arity = pgexprBest->Arity();
	for (ULONG ul = 0; fStatsDerivable && ul < arity; ul++)
	{
		gpos::pointer<CGroup *> pgroupChild = (*pgexprBest)[ul];
		fStatsDerivable =
			(pgroupChild->FScalar() || pgroupChild->FStatsDerivable(mp));
	}

	return fStatsDerivable;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::FBetterPromise
//
//	@doc:
//		Return true if first promise is better than second promise
//
//---------------------------------------------------------------------------
BOOL
CGroup::FBetterPromise(CMemoryPool *mp, CLogical::EStatPromise espFst,
					   gpos::pointer<CGroupExpression *> pgexprFst,
					   CLogical::EStatPromise espSnd,
					   gpos::pointer<CGroupExpression *> pgexprSnd)
{
	// if there is a tie and both group expressions are inner join, we prioritize
	// the inner join having less predicates
	return espFst > espSnd ||
		   (espFst == espSnd &&
			CLogicalInnerJoin::FFewerConj(mp, pgexprFst, pgexprSnd));
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::EspDerive
//
//	@doc:
//		Derive statistics recursively on group expression
//
//---------------------------------------------------------------------------
CLogical::EStatPromise
CGroup::EspDerive(CMemoryPool *pmpLocal, CMemoryPool *pmpGlobal,
				  gpos::pointer<CGroupExpression *> pgexpr,
				  gpos::pointer<CReqdPropRelational *> prprel,
				  IStatisticsArray *stats_ctxt, BOOL fDeriveChildStats)
{
	GPOS_ASSERT(pgexpr->Pop()->FLogical());

	CExpressionHandle exprhdl(pmpGlobal);
	exprhdl.Attach(pgexpr);
	gpos::owner<CDrvdPropCtxtRelational *> pdpctxtrel =
		GPOS_NEW(pmpGlobal) CDrvdPropCtxtRelational(pmpGlobal);
	exprhdl.DeriveProps(pdpctxtrel);
	pdpctxtrel->Release();

	// compute stat promise for gexpr
	gpos::pointer<CLogical *> popLogical =
		gpos::dyn_cast<CLogical>(pgexpr->Pop());
	CLogical::EStatPromise esp = popLogical->Esp(exprhdl);

	// override promise if outer child references columns of inner children
	if (2 < exprhdl.Arity() && !exprhdl.FScalarChild(0 /*child_index*/) &&
		exprhdl.HasOuterRefs(0 /*child_index*/) && !exprhdl.HasOuterRefs())
	{
		// stat derivation always starts by outer child,
		// any outer references in outer child cannot be resolved for stats derivation purposes
		esp = CLogical::EspLow;
	}

	if (fDeriveChildStats && esp > CLogical::EspNone)
	{
		// we only aim here at triggering stat derivation on child groups recursively,
		// there is no need to compute stats for group expression's root operator
		gpos::owner<IStatistics *> stats = pgexpr->PstatsRecursiveDerive(
			pmpLocal, pmpGlobal, prprel, stats_ctxt,
			false /*fComputeRootStats*/);
		CRefCount::SafeRelease(stats);
	}

	return esp;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::PstatsRecursiveDerive
//
//	@doc:
//		Derive statistics recursively on group
//
//---------------------------------------------------------------------------
gpos::pointer<IStatistics *>
CGroup::PstatsRecursiveDerive(CMemoryPool *pmpLocal, CMemoryPool *pmpGlobal,
							  CReqdPropRelational *prprel,
							  IStatisticsArray *stats_ctxt)
{
	GPOS_CHECK_STACK_SIZE;

	GPOS_ASSERT(!FImplemented());
	GPOS_ASSERT(nullptr != stats_ctxt);
	GPOS_CHECK_ABORT;

	// create empty stats if a scalar group
	if (FScalar())
	{
		return PstatsInitEmpty(pmpGlobal);
	}

	gpos::owner<IStatistics *> stats = nullptr;
	// if this is a duplicate group, return stats from the duplicate
	if (FDuplicateGroup())
	{
		// call stat derivation on the duplicate group
		stats = PgroupDuplicate()->PstatsRecursiveDerive(pmpLocal, pmpGlobal,
														 prprel, stats_ctxt);
	}
	GPOS_ASSERT(0 < m_ulGExprs);

	prprel->AddRef();
	gpos::owner<CReqdPropRelational *> prprelInput = prprel;

	if (nullptr == stats)
	{
		stats = Pstats();
	}
	// if group has derived stats, check if requirements are covered
	// by what's already derived

	if (nullptr != stats)
	{
		prprelInput->Release();
		gpos::owner<CReqdPropRelational *> prprelExisting =
			stats->GetReqdRelationalProps(pmpGlobal);
		prprelInput = prprel->PrprelDifference(pmpGlobal, prprelExisting);
		prprelExisting->Release();
		if (prprelInput->IsEmpty())
		{
			// required stat columns are already covered by existing stats
			prprelInput->Release();
			return stats;
		}
	}

	// required stat columns are not covered by existing stats, we need to
	// derive the missing ones

	// find the best group expression to derive stats on
	gpos::pointer<CGroupExpression *> pgexprBest =
		PgexprBestPromise(pmpLocal, pmpGlobal, prprelInput, stats_ctxt);

	if (nullptr == pgexprBest)
	{
		GPOS_RAISE(
			gpopt::ExmaGPOPT, gpopt::ExmiNoPlanFound,
			GPOS_WSZ_LIT(
				"Could not choose a group expression for statistics derivation"));
	}

	// derive stats on group expression and copy them to group
	stats = pgexprBest->PstatsRecursiveDerive(pmpLocal, pmpGlobal, prprelInput,
											  stats_ctxt);
	if (!FInitStats(stats))
	{
		// a group stat object already exists, we append derived stats to that object
		AppendStats(pmpGlobal, stats);
		stats->Release();
	}
	GPOS_ASSERT(nullptr != Pstats());

	prprelInput->Release();

	return Pstats();
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::PgexprBestPromise
//
//	@doc:
//		Find group expression with best stats promise and the
//		same children as given expression
//
//---------------------------------------------------------------------------
CGroupExpression *
CGroup::PgexprBestPromise(CMemoryPool *mp,
						  gpos::pointer<CGroupExpression *> pgexprToMatch)
{
	GPOS_ASSERT(nullptr != pgexprToMatch);

	gpos::owner<CReqdPropRelational *> prprel =
		GPOS_NEW(mp) CReqdPropRelational(GPOS_NEW(mp) CColRefSet(mp));
	gpos::owner<IStatisticsArray *> stats_ctxt =
		GPOS_NEW(mp) IStatisticsArray(mp);

	CLogical::EStatPromise espBest = CLogical::EspNone;
	CGroupExpression *pgexprCurrent = nullptr;
	CGroupExpression *pgexprBest = nullptr;
	// get first logical group expression
	{
		CGroupProxy gp(this);
		pgexprCurrent = gp.PgexprNextLogical(nullptr /*pgexpr*/);
	}

	while (nullptr != pgexprCurrent)
	{
		CLogical::EStatPromise espCurrent =
			EspDerive(mp, mp, pgexprCurrent, prprel, stats_ctxt,
					  false /*fDeriveChildStats*/);

		if (pgexprCurrent->FMatchNonScalarChildren(pgexprToMatch) &&
			FBetterPromise(mp, espCurrent, pgexprCurrent, espBest, pgexprBest))
		{
			pgexprBest = pgexprCurrent;
			espBest = espCurrent;
		}

		// move to next logical group expression
		{
			CGroupProxy gp(this);
			pgexprCurrent = gp.PgexprNextLogical(pgexprCurrent);
		}

		GPOS_CHECK_ABORT;
	}

	prprel->Release();
	stats_ctxt->Release();

	return pgexprBest;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::PgexprBestPromise
//
//	@doc:
//		Find the group expression having the best stats promise for this group
//
//---------------------------------------------------------------------------
CGroupExpression *
CGroup::PgexprBestPromise(CMemoryPool *pmpLocal, CMemoryPool *pmpGlobal,
						  gpos::pointer<CReqdPropRelational *> prprelInput,
						  IStatisticsArray *stats_ctxt)
{
	CGroupExpression *pgexprBest = nullptr;
	CLogical::EStatPromise espBest = CLogical::EspNone;
	CGroupExpression *pgexprCurrent = nullptr;
	// get first logical group expression
	{
		CGroupProxy gp(this);
		pgexprCurrent = gp.PgexprNextLogical(nullptr /*pgexpr*/);
	}

	while (nullptr != pgexprCurrent)
	{
		if (!pgexprCurrent->ContainsCircularDependencies())
		{
			CLogical::EStatPromise espCurrent =
				EspDerive(pmpLocal, pmpGlobal, pgexprCurrent, prprelInput,
						  stats_ctxt, true /*fDeriveChildStats*/);

			if (FBetterPromise(pmpLocal, espCurrent, pgexprCurrent, espBest,
							   pgexprBest))
			{
				pgexprBest = pgexprCurrent;
				espBest = espCurrent;
			}
		}

		// move to next logical group expression
		{
			CGroupProxy gp(this);
			pgexprCurrent = gp.PgexprNextLogical(pgexprCurrent);
		}

		GPOS_CHECK_ABORT;
	}

	return pgexprBest;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::PstatsInitEmpty
//
//	@doc:
//		Initialize and return empty stats for this group
//
//---------------------------------------------------------------------------
IStatistics *
CGroup::PstatsInitEmpty(CMemoryPool *pmpGlobal)
{
	gpos::owner<CStatistics *> stats = CStatistics::MakeEmptyStats(pmpGlobal);
	if (!FInitStats(stats))
	{
		stats->Release();
	}

	return Pstats();
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::OsPrintGrpOptCtxts
//
//	@doc:
//		Print group optimization contexts
//
//---------------------------------------------------------------------------
IOstream &
CGroup::OsPrintGrpOptCtxts(IOstream &os, const CHAR *szPrefix) const
{
	if (!FScalar() && !FDuplicateGroup() &&
		GPOS_FTRACE(EopttracePrintOptimizationContext))
	{
		os << szPrefix << "Grp OptCtxts:" << std::endl;

		ULONG num_opt_contexts = m_sht.Size();

		for (ULONG ul = 0; ul < num_opt_contexts; ul++)
		{
			gpos::pointer<COptimizationContext *> poc = Ppoc(ul);

			if (nullptr != poc)
			{
				os << szPrefix;
				(void) poc->OsPrintWithPrefix(os, szPrefix);
			}

			GPOS_CHECK_ABORT;
		}
	}

	return os;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::OsPrintGrpScalarProps
//
//	@doc:
//		Print scalar group properties
//
//---------------------------------------------------------------------------
IOstream &
CGroup::OsPrintGrpScalarProps(IOstream &os, const CHAR *szPrefix) const
{
	GPOS_ASSERT(FScalar());

	if (nullptr != PexprScalarRep())
	{
		os << szPrefix << "Scalar Expression:";
		if (!FScalarRepIsExact())
		{
			os << " (subqueries replaced with true or NULL):";
		}
		os << std::endl << szPrefix << *PexprScalarRep() << std::endl;
	}

	GPOS_CHECK_ABORT;

	if (nullptr != m_pdrgpexprJoinKeysOuter)
	{
		os << szPrefix << "Outer Join Keys: " << std::endl;

		const ULONG size = m_pdrgpexprJoinKeysOuter->Size();
		for (ULONG ul = 0; ul < size; ul++)
		{
			os << szPrefix << *(*m_pdrgpexprJoinKeysOuter)[ul] << std::endl;
		}
	}

	GPOS_CHECK_ABORT;

	if (nullptr != m_pdrgpexprJoinKeysInner)
	{
		os << szPrefix << "Inner Join Keys: " << std::endl;

		const ULONG size = m_pdrgpexprJoinKeysInner->Size();
		for (ULONG ul = 0; ul < size; ul++)
		{
			os << szPrefix << *(*m_pdrgpexprJoinKeysInner)[ul] << std::endl;
		}
	}

	GPOS_CHECK_ABORT;

	if (nullptr != m_join_opfamilies)
	{
		os << szPrefix << "Inner Join Opfamilies: " << std::endl;

		const ULONG size = m_join_opfamilies->Size();
		for (ULONG ul = 0; ul < size; ul++)
		{
			os << szPrefix;
			(*m_join_opfamilies)[ul]->OsPrint(os);
			os << std::endl;
		}
	}

	return os;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::OsPrintGrpProps
//
//	@doc:
//		Print group properties and stats
//
//---------------------------------------------------------------------------
IOstream &
CGroup::OsPrintGrpProps(IOstream &os, const CHAR *szPrefix) const
{
	if (!FDuplicateGroup() && GPOS_FTRACE(EopttracePrintGroupProperties))
	{
		os << szPrefix << "Grp Props:" << std::endl
		   << szPrefix << szPrefix << *m_pdp << std::endl;
		if (!FScalar() && nullptr != m_pstats)
		{
			os << szPrefix << "Grp Stats:" << std::endl
			   << szPrefix << szPrefix << *m_pstats;
		}

		if (FScalar())
		{
			(void) OsPrintGrpScalarProps(os, szPrefix);
		}

		GPOS_CHECK_ABORT;
	}

	return os;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::ResetGroupState
//
//	@doc:
//		Reset group state;
//		resetting state is not thread-safe
//
//---------------------------------------------------------------------------
void
CGroup::ResetGroupState()
{
	// reset group expression states
	CGroupExpression *pgexpr = m_listGExprs.First();
	while (nullptr != pgexpr)
	{
		pgexpr->ResetState();
		pgexpr = m_listGExprs.Next(pgexpr);

		GPOS_CHECK_ABORT;
	}

	// reset group state
	{
		CGroupProxy gp(this);
		m_estate = estUnexplored;
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::Pstats
//
//	@doc:
//		Group stats accessor
//
//---------------------------------------------------------------------------
gpos::pointer<IStatistics *>
CGroup::Pstats() const
{
	if (nullptr != m_pstats)
	{
		return m_pstats;
	}

	if (FDuplicateGroup())
	{
		return PgroupDuplicate()->Pstats();
	}

	return nullptr;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::ResetStats
//
//	@doc:
//		Reset computed stats;
//
//
//---------------------------------------------------------------------------
void
CGroup::ResetStats()
{
	GPOS_ASSERT(!FScalar());

	gpos::owner<IStatistics *> stats = nullptr;
	{
		CGroupProxy gp(this);
		stats = m_pstats;
		m_pstats = nullptr;
	}
	CRefCount::SafeRelease(stats);
	stats = nullptr;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::ResetLinkMap
//
//	@doc:
//		Reset link map for plan enumeration;
//		this operation is not thread safe
//
//
//---------------------------------------------------------------------------
void
CGroup::ResetLinkMap()
{
	GPOS_ASSERT(nullptr != m_plinkmap);

	m_plinkmap->Release();
	m_plinkmap = GPOS_NEW(m_mp) LinkMap(m_mp);
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::FResetStats
//
//	@doc:
//		Check if we need to reset group stats before deriving statistics;
//		this function reset group stats in the following two cases:
//		(1) current group has newly-added logical operators, this can happen
//		during multi-stage search, where stage_{i+1} may append new logical
//		operators to a group created at stage_{i}
//		(2) a child group, reachable from current group at any depth, has new
//		logical group expressions
//
//---------------------------------------------------------------------------
BOOL
CGroup::FResetStats()
{
	GPOS_CHECK_STACK_SIZE;

	if (nullptr == Pstats())
	{
		// end recursion early if group stats have been already reset
		return true;
	}

	BOOL fResetStats = false;
	if (FHasNewLogicalOperators())
	{
		fResetStats = true;
	}

	// get first logical group expression
	CGroupExpression *pgexprCurrent = nullptr;
	{
		CGroupProxy gp(this);
		pgexprCurrent = gp.PgexprNextLogical(nullptr /*pgexpr*/);
	}

	// recursively process child groups reachable from current group
	while (nullptr != pgexprCurrent)
	{
		const ULONG arity = pgexprCurrent->Arity();
		for (ULONG ul = 0; ul < arity; ul++)
		{
			GPOS_CHECK_ABORT;

			gpos::pointer<CGroup *> pgroupChild = (*pgexprCurrent)[ul];
			if (!pgroupChild->FScalar() && pgroupChild->FResetStats())
			{
				fResetStats = true;
				// we cannot break here since we must visit all child groups
			}
		}

		// move to next logical group expression
		{
			CGroupProxy gp(this);
			pgexprCurrent = gp.PgexprNextLogical(pgexprCurrent);
		}

		GPOS_CHECK_ABORT;
	}

	if (fResetStats)
	{
		ResetStats();
	}

	return fResetStats;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::ResetGroupJobQueues
//
//	@doc:
//		Reset group job queues;
//
//---------------------------------------------------------------------------
void
CGroup::ResetGroupJobQueues()
{
	CGroupProxy gp(this);
	m_jqExploration.Reset();
	m_jqImplementation.Reset();
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::PstatsCompute
//
//	@doc:
//		Compute stats during costing
//
//---------------------------------------------------------------------------
gpos::pointer<IStatistics *>
CGroup::PstatsCompute(COptimizationContext *poc, CExpressionHandle &exprhdl,
					  gpos::pointer<CGroupExpression *> pgexpr)
{
	GPOS_ASSERT(nullptr != poc);
	GPOS_ASSERT(nullptr != pgexpr);
	GPOS_ASSERT(this == pgexpr->Pgroup());

	gpos::owner<IStatistics *> stats = m_pstatsmap->Find(poc);
	if (nullptr != stats)
	{
		return stats;
	}

	stats = gpos::dyn_cast<CLogical>(pgexpr->Pop())
				->PstatsDerive(m_mp, exprhdl, poc->Pdrgpstat());
	GPOS_ASSERT(nullptr != stats);

	// add computed stats to local map
	poc->AddRef();
	BOOL fSuccess GPOS_ASSERTS_ONLY = m_pstatsmap->Insert(poc, stats);
	GPOS_ASSERT(fSuccess);

	return stats;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::OsPrint
//
//	@doc:
//		Print function;
//		printing is not thread-safe
//
//---------------------------------------------------------------------------
IOstream &
CGroup::OsPrint(IOstream &os) const
{
	const CHAR *szPrefix = "  ";
	os << std::endl << "Group " << m_id << " (";
	if (!FScalar())
	{
		os << "#GExprs: " << m_listGExprs.Size();

		if (0 < m_listDupGExprs.Size())
		{
			os << ", #Duplicate GExprs: " << m_listDupGExprs.Size();
		}
		if (FDuplicateGroup())
		{
			os << ", Duplicate Group: " << m_pgroupDuplicate->Id();
		}
	}
	os << "):" << std::endl;

	CGroupExpression *pgexpr = m_listGExprs.First();
	while (nullptr != pgexpr)
	{
		(void) pgexpr->OsPrintWithPrefix(os, szPrefix);
		pgexpr = m_listGExprs.Next(pgexpr);

		GPOS_CHECK_ABORT;
	}

	(void) OsPrintGrpProps(os, szPrefix);
	(void) OsPrintGrpOptCtxts(os, szPrefix);

	return os;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroup::CostLowerBound
//
//	@doc:
//		Compute a cost lower bound on plans, rooted by a group expression
//		in current group, and satisfying the given required properties
//
//---------------------------------------------------------------------------
CCost
CGroup::CostLowerBound(CMemoryPool *mp, CReqdPropPlan *prppInput)
{
	GPOS_ASSERT(nullptr != prppInput);
	GPOS_ASSERT(!FScalar());

	CCost *pcostLowerBound = m_pcostmap->Find(prppInput);
	if (nullptr != pcostLowerBound)
	{
		return *pcostLowerBound;
	}

	CCost costLowerBound = GPOPT_INFINITE_COST;

	// start with first non-logical group expression
	CGroupExpression *pgexprCurrent = nullptr;
	{
		CGroupProxy gp(this);
		pgexprCurrent = gp.PgexprSkipLogical(nullptr /*pgexpr*/);
	}

	while (nullptr != pgexprCurrent)
	{
		// considering an enforcer introduces a deadlock here since its child is
		// the same group that contains it,
		// since an enforcer must reside on top of another operator from the same
		// group, it cannot produce a better cost lower-bound and can be skipped here

		if (!CUtils::FEnforcer(pgexprCurrent->Pop()))
		{
			CCost costLowerBoundGExpr = pgexprCurrent->CostLowerBound(
				mp, prppInput, nullptr /*pccChild*/,
				gpos::ulong_max /*child_index*/);
			if (costLowerBoundGExpr < costLowerBound)
			{
				costLowerBound = costLowerBoundGExpr;
			}
		}

		// move to next non-logical group expression
		{
			CGroupProxy gp(this);
			pgexprCurrent = gp.PgexprSkipLogical(pgexprCurrent);
		}
	}


	prppInput->AddRef();
	BOOL fSuccess GPOS_ASSERTS_ONLY =
		m_pcostmap->Insert(prppInput, GPOS_NEW(mp) CCost(costLowerBound.Get()));
	GPOS_ASSERT(fSuccess);

	return costLowerBound;
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CGroup::DbgPrint
//
//	@doc:
//		Print driving function for use in interactive debugging;
//		always prints to stderr;
//
//---------------------------------------------------------------------------
void
CGroup::DbgPrintWithProperties()
{
	CAutoTraceFlag atf(EopttracePrintGroupProperties, true);
	CAutoTrace at(m_mp);
	(void) this->OsPrint(at.Os());
}

#endif

// EOF
