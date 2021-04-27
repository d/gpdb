//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CScalarProjectList.cpp
//
//	@doc:
//		Implementation of scalar projection list operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CScalarProjectList.h"

#include "gpos/base.h"
#include "gpos/common/owner.h"
#include "gpos/memory/CAutoMemoryPool.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CScalarWindowFunc.h"
#include "gpopt/xforms/CXformUtils.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CScalarProjectList::CScalarProjectList
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CScalarProjectList::CScalarProjectList(CMemoryPool *mp) : CScalar(mp)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarProjectList::Matches
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarProjectList::Matches(gpos::pointer<COperator *> pop) const
{
	return (pop->Eopid() == Eopid());
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarProjectList::FInputOrderSensitive
//
//	@doc:
//		Prjection lists are insensitive to order; no dependencies between
//		elements;
//
//---------------------------------------------------------------------------
BOOL
CScalarProjectList::FInputOrderSensitive() const
{
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarProjectList::UlDistinctAggs
//
//	@doc:
//		Return number of Distinct Aggs in the given project list
//
//---------------------------------------------------------------------------
ULONG
CScalarProjectList::UlDistinctAggs(CExpressionHandle &exprhdl)
{
	// We make do with an inexact representative expression returned by exprhdl.PexprScalarRep(),
	// knowing that at this time, aggregate functions are accurately contained in it. What's not
	// exact are subqueries. This is better than just returning 0 for project lists with subqueries.
	gpos::pointer<CExpression *> pexprPrjList = exprhdl.PexprScalarRep();

	GPOS_ASSERT(nullptr != pexprPrjList);
	GPOS_ASSERT(COperator::EopScalarProjectList ==
				pexprPrjList->Pop()->Eopid());

	ULONG ulDistinctAggs = 0;
	const ULONG arity = pexprPrjList->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		gpos::pointer<CExpression *> pexprPrjEl = (*pexprPrjList)[ul];
		gpos::pointer<CExpression *> pexprChild = (*pexprPrjEl)[0];
		COperator::EOperatorId eopidChild = pexprChild->Pop()->Eopid();

		if (COperator::EopScalarAggFunc == eopidChild)
		{
			gpos::pointer<CScalarAggFunc *> popScAggFunc =
				gpos::dyn_cast<CScalarAggFunc>(pexprChild->Pop());
			if (popScAggFunc->IsDistinct())
			{
				ulDistinctAggs++;
			}
		}
		else if (COperator::EopScalarWindowFunc == eopidChild)
		{
			gpos::pointer<CScalarWindowFunc *> popScWinFunc =
				gpos::dyn_cast<CScalarWindowFunc>(pexprChild->Pop());
			if (popScWinFunc->IsDistinct() && popScWinFunc->FAgg())
			{
				ulDistinctAggs++;
			}
		}
	}

	return ulDistinctAggs;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarProjectList::FHasMultipleDistinctAggs
//
//	@doc:
//		Check if given project list has multiple distinct aggregates, for example:
//			select count(distinct a), sum(distinct b) from T;
//
//			select count(distinct a) over(), sum(distinct b) over() from T;
//
//---------------------------------------------------------------------------
BOOL
CScalarProjectList::FHasMultipleDistinctAggs(CExpressionHandle &exprhdl)
{
	// We make do with an inexact representative expression returned by exprhdl.PexprScalarRep(),
	// knowing that at this time, aggregate functions are accurately contained in it. What's not
	// exact are subqueries. This is better than just returning false for project lists with subqueries.
	gpos::pointer<CExpression *> pexprPrjList = exprhdl.PexprScalarRep();

	GPOS_ASSERT(COperator::EopScalarProjectList ==
				pexprPrjList->Pop()->Eopid());
	if (0 == UlDistinctAggs(exprhdl))
	{
		return false;
	}

	CAutoMemoryPool amp;
	gpos::owner<ExprToExprArrayMap *> phmexprdrgpexpr = nullptr;
	ULONG ulDifferentDQAs = 0;
	CXformUtils::MapPrjElemsWithDistinctAggs(
		amp.Pmp(), pexprPrjList, &phmexprdrgpexpr, &ulDifferentDQAs);
	phmexprdrgpexpr->Release();

	return (1 < ulDifferentDQAs);
}


// EOF
