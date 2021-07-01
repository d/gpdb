//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CLogicalUnary.cpp
//
//	@doc:
//		Implementation of logical unary operators
//---------------------------------------------------------------------------

#include "gpopt/operators/CLogicalUnary.h"

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/xforms/CXformUtils.h"
#include "naucrates/statistics/CProjectStatsProcessor.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnary::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CLogicalUnary::Matches(COperator *pop) const
{
	return (pop->Eopid() == Eopid());
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnary::Esp
//
//	@doc:
//		Promise level for stat derivation
//
//---------------------------------------------------------------------------
CLogical::EStatPromise
CLogicalUnary::Esp(CExpressionHandle &exprhdl) const
{
	// low promise for stat derivation if scalar predicate has subqueries, or logical
	// expression has outer-refs or is part of an Apply expression
	if (exprhdl.DeriveHasSubquery(1) || exprhdl.HasOuterRefs() ||
		(nullptr != exprhdl.Pgexpr() &&
		 CXformUtils::FGenerateApply(exprhdl.Pgexpr()->ExfidOrigin())))
	{
		return EspLow;
	}

	return EspHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnary::PstatsDeriveProject
//
//	@doc:
//		Derive statistics for projection operators
//
//---------------------------------------------------------------------------
gpos::Ref<IStatistics>
CLogicalUnary::PstatsDeriveProject(CMemoryPool *mp, CExpressionHandle &exprhdl,
								   UlongToIDatumMap *phmuldatum) const
{
	GPOS_ASSERT(Esp(exprhdl) > EspNone);
	IStatistics *child_stats = exprhdl.Pstats(0);
	CReqdPropRelational *prprel =
		gpos::dyn_cast<CReqdPropRelational>(exprhdl.Prp());
	CColRefSet *pcrs = prprel->PcrsStat();
	gpos::Ref<ULongPtrArray> colids = GPOS_NEW(mp) ULongPtrArray(mp);
	pcrs->ExtractColIds(mp, colids.get());

	gpos::Ref<IStatistics> stats = CProjectStatsProcessor::CalcProjStats(
		mp, dynamic_cast<CStatistics *>(child_stats), colids.get(), phmuldatum);

	// clean up
	;

	return stats;
}

// EOF
