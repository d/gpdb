//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		COptimizationContext.h
//
//	@doc:
//		Optimization context object stores properties required to hold
//		on the plan generated by the optimizer
//---------------------------------------------------------------------------
#ifndef GPOPT_COptimizationContext_H
#define GPOPT_COptimizationContext_H

#include "gpos/base.h"
#include "gpos/common/DbgPrintMixin.h"
#include "gpos/common/owner.h"
#include "gpos/task/CAutoTraceFlag.h"

#include "gpopt/base/CReqdPropPlan.h"
#include "gpopt/base/CReqdPropRelational.h"
#include "gpopt/search/CJobQueue.h"
#include "naucrates/statistics/IStatistics.h"

#define GPOPT_INVALID_OPTCTXT_ID gpos::ulong_max

namespace gpopt
{
using namespace gpos;

// forward declarations
class CGroup;
class CGroupExpression;
class CCostContext;
class COptimizationContext;
class CDrvdPropPlan;

// optimization context pointer definition
typedef COptimizationContext *OPTCTXT_PTR;

// array of optimization contexts
typedef CDynamicPtrArray<COptimizationContext, CleanupRelease>
	COptimizationContextArray;

//---------------------------------------------------------------------------
//	@class:
//		COptimizationContext
//
//	@doc:
//		Optimization context
//
//---------------------------------------------------------------------------
class COptimizationContext : public CRefCount,
							 public DbgPrintMixin<COptimizationContext>
{
public:
	// states of optimization context
	enum EState
	{
		estUnoptimized,	 // initial state

		estOptimizing,	// ongoing optimization
		estOptimized,	// done optimization

		estSentinel
	};

private:
	// memory pool
	CMemoryPool *m_mp{nullptr};

	// private copy ctor
	COptimizationContext(const COptimizationContext &);

	// unique id within owner group, used for debugging
	ULONG m_id{GPOPT_INVALID_OPTCTXT_ID};

	// back pointer to owner group, used for debugging
	CGroup *m_pgroup{nullptr};

	// required plan properties
	CReqdPropPlan *m_prpp{nullptr};

	// required relational properties -- used for stats computation during costing
	CReqdPropRelational *m_prprel{nullptr};

	// stats of previously optimized expressions
	IStatisticsArray *m_pdrgpstatCtxt{nullptr};

	// index of search stage where context is generated
	ULONG m_ulSearchStageIndex{0};

	// best cost context under the optimization context
	CCostContext *m_pccBest{nullptr};

	// optimization context state
	EState m_estate{estUnoptimized};

	// is there a multi-stage Agg plan satisfying required properties
	BOOL m_fHasMultiStageAggPlan{false};

	// context's optimization job queue
	CJobQueue m_jqOptimization;

	// internal matching function
	BOOL FMatchSortColumns(
		gpos::pointer<const COptimizationContext *> poc) const;

	// private dummy ctor; used for creating invalid context
	COptimizationContext() = default;

	// check if Agg node should be optimized for the given context
	static BOOL FOptimizeAgg(CMemoryPool *mp,
							 gpos::pointer<CGroupExpression *> pgexprParent,
							 gpos::pointer<CGroupExpression *> pgexprAgg,
							 gpos::pointer<COptimizationContext *> poc,
							 ULONG ulSearchStages);

	// check if Sort node should be optimized for the given context
	static BOOL FOptimizeSort(CMemoryPool *mp,
							  gpos::pointer<CGroupExpression *> pgexprParent,
							  gpos::pointer<CGroupExpression *> pgexprSort,
							  gpos::pointer<COptimizationContext *> poc,
							  ULONG ulSearchStages);

	// check if Motion node should be optimized for the given context
	static BOOL FOptimizeMotion(CMemoryPool *mp,
								gpos::pointer<CGroupExpression *> pgexprParent,
								gpos::pointer<CGroupExpression *> pgexprMotion,
								gpos::pointer<COptimizationContext *> poc,
								ULONG ulSearchStages);

	// check if NL join node should be optimized for the given context
	static BOOL FOptimizeNLJoin(CMemoryPool *mp,
								gpos::pointer<CGroupExpression *> pgexprParent,
								gpos::pointer<CGroupExpression *> pgexprMotion,
								gpos::pointer<COptimizationContext *> poc,
								ULONG ulSearchStages);

public:
	// ctor
	COptimizationContext(
		CMemoryPool *mp, CGroup *pgroup, CReqdPropPlan *prpp,
		CReqdPropRelational *
			prprel,	 // required relational props -- used during stats derivation
		IStatisticsArray
			*stats_ctxt,  // stats of previously optimized expressions
		ULONG ulSearchStageIndex)
		: m_mp(mp),
		  m_pgroup(pgroup),
		  m_prpp(prpp),
		  m_prprel(prprel),
		  m_pdrgpstatCtxt(stats_ctxt),
		  m_ulSearchStageIndex(ulSearchStageIndex)
	{
		GPOS_ASSERT(nullptr != pgroup);
		GPOS_ASSERT(nullptr != m_prpp);
		GPOS_ASSERT(nullptr != m_prprel);
		GPOS_ASSERT(nullptr != m_pdrgpstatCtxt);
	}

	// dtor
	~COptimizationContext() override;

	// best group expression accessor
	CGroupExpression *PgexprBest() const;

	// match optimization contexts
	BOOL Matches(gpos::pointer<const COptimizationContext *> poc) const;

	// get id
	ULONG
	Id() const
	{
		return m_id;
	}

	// group accessor
	gpos::pointer<CGroup *>
	Pgroup() const
	{
		return m_pgroup;
	}

	// required plan properties accessor
	gpos::pointer<CReqdPropPlan *>
	Prpp() const
	{
		return m_prpp;
	}

	// required relatoinal properties accessor
	gpos::pointer<CReqdPropRelational *>
	GetReqdRelationalProps() const
	{
		return m_prprel;
	}

	// stats of previously optimized expressions
	IStatisticsArray *
	Pdrgpstat() const
	{
		return m_pdrgpstatCtxt;
	}

	// search stage index accessor
	ULONG
	UlSearchStageIndex() const
	{
		return m_ulSearchStageIndex;
	}

	// best cost context accessor
	gpos::pointer<CCostContext *>
	PccBest() const
	{
		return m_pccBest;
	}

	// optimization job queue accessor
	CJobQueue *
	PjqOptimization()
	{
		return &m_jqOptimization;
	}

	// state accessor
	EState
	Est() const
	{
		return m_estate;
	}

	// is there a multi-stage Agg plan satisfying required properties
	BOOL
	FHasMultiStageAggPlan() const
	{
		return m_fHasMultiStageAggPlan;
	}

	// set optimization context id
	void
	SetId(ULONG id)
	{
		GPOS_ASSERT(m_id == GPOPT_INVALID_OPTCTXT_ID);

		m_id = id;
	}

	// set optimization context state
	void
	SetState(EState estNewState)
	{
		GPOS_ASSERT(estNewState == (EState)(m_estate + 1));

		m_estate = estNewState;
	}

	// set best cost context
	void SetBest(CCostContext *pcc);

	// comparison operator for hashtables
	BOOL
	operator==(const COptimizationContext &oc) const
	{
		return oc.Matches(this);
	}

	// debug print
	IOstream &OsPrint(IOstream &os) const;
	IOstream &OsPrintWithPrefix(IOstream &os, const CHAR *szPrefix) const;

	// check equality of optimization contexts
	static BOOL
	Equals(const COptimizationContext &ocLeft,
		   const COptimizationContext &ocRight)
	{
		return ocLeft == ocRight;
	}

	// hash function for optimization context
	static ULONG
	HashValue(const COptimizationContext &oc)
	{
		GPOS_ASSERT(nullptr != oc.Prpp());

		return oc.Prpp()->HashValue();
	}

	// equality function for cost contexts hash table
	static BOOL
	Equals(const OPTCTXT_PTR &pocLeft, const OPTCTXT_PTR &pocRight)
	{
		if (pocLeft == m_pocInvalid || pocRight == m_pocInvalid)
		{
			return pocLeft == m_pocInvalid && pocRight == m_pocInvalid;
		}

		return *pocLeft == *pocRight;
	}

	// hash function for cost contexts hash table
	static ULONG
	HashValue(const OPTCTXT_PTR &poc)
	{
		GPOS_ASSERT(m_pocInvalid != poc);

		return HashValue(*poc);
	}

	// hash function used for computing stats during costing
	static ULONG
	UlHashForStats(gpos::pointer<const COptimizationContext *> poc)
	{
		GPOS_ASSERT(m_pocInvalid != poc);

		return HashValue(*poc);
	}

	// equality function used for computing stats during costing
	static BOOL FEqualForStats(
		gpos::pointer<const COptimizationContext *> pocLeft,
		gpos::pointer<const COptimizationContext *> pocRight);

	// return true if given group expression should be optimized under given context
	static BOOL FOptimize(CMemoryPool *mp,
						  gpos::pointer<CGroupExpression *> pgexprParent,
						  gpos::pointer<CGroupExpression *> pgexprChild,
						  gpos::pointer<COptimizationContext *> pocChild,
						  ULONG ulSearchStages);

	// compare array of contexts based on context ids
	static BOOL FEqualContextIds(COptimizationContextArray *pdrgpocFst,
								 COptimizationContextArray *pdrgpocSnd);

	// compute required properties to CTE producer based on plan properties of CTE consumer
	static CReqdPropPlan *PrppCTEProducer(CMemoryPool *mp,
										  COptimizationContext *poc,
										  ULONG ulSearchStages);

	// link for optimization context hash table in CGroup
	SLink m_link;

	// invalid optimization context, needed for hash table iteration
	static const COptimizationContext m_ocInvalid;

	// invalid optimization context pointer, needed for cost contexts hash table iteration
	static const OPTCTXT_PTR m_pocInvalid;

};	// class COptimizationContext
}  // namespace gpopt


#endif	// !GPOPT_COptimizationContext_H

// EOF
