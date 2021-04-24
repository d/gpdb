//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates..
//
//	@filename:
//		CLogicalMaxOneRow.cpp
//
//	@doc:
//		Implementation of logical MaxOneRow operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CLogicalMaxOneRow.h"

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/xforms/CXformUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalMaxOneRow::Esp
//
//	@doc:
//		Promise level for stat derivation
//
//---------------------------------------------------------------------------
CLogical::EStatPromise
CLogicalMaxOneRow::Esp(CExpressionHandle &exprhdl) const
{
	// low promise for stat derivation if logical expression has outer-refs
	// or is part of an Apply expression
	if (exprhdl.HasOuterRefs() ||
		(nullptr != exprhdl.Pgexpr() &&
		 CXformUtils::FGenerateApply(exprhdl.Pgexpr()->ExfidOrigin())))
	{
		return EspLow;
	}

	return EspHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalMaxOneRow::PcrsStat
//
//	@doc:
//		Promise level for stat derivation
//
//---------------------------------------------------------------------------
gpos::owner<CColRefSet *>
CLogicalMaxOneRow::PcrsStat(CMemoryPool *mp, CExpressionHandle &exprhdl,
							gpos::pointer<CColRefSet *> pcrsInput,
							ULONG child_index) const
{
	GPOS_ASSERT(0 == child_index);

	gpos::owner<CColRefSet *> pcrs = GPOS_NEW(mp) CColRefSet(mp);
	pcrs->Union(pcrsInput);

	// intersect with the output columns of relational child
	pcrs->Intersection(exprhdl.DeriveOutputColumns(child_index));

	return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalMaxOneRow::PxfsCandidates
//
//	@doc:
//		Compute candidate xforms
//
//---------------------------------------------------------------------------
gpos::owner<CXformSet *>
CLogicalMaxOneRow::PxfsCandidates(CMemoryPool *mp) const
{
	gpos::owner<CXformSet *> xform_set = GPOS_NEW(mp) CXformSet(mp);
	(void) xform_set->ExchangeSet(CXform::ExfMaxOneRow2Assert);
	return xform_set;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalMaxOneRow::PstatsDerive
//
//	@doc:
//		Derive statistics
//
//---------------------------------------------------------------------------
gpos::owner<IStatistics *>
CLogicalMaxOneRow::PstatsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl,
								gpos::pointer<IStatisticsArray *>  // stats_ctxt
) const
{
	// no more than one row can be produced by operator, scale down input statistics accordingly
	gpos::pointer<IStatistics *> stats = exprhdl.Pstats(0);
	return stats->ScaleStats(mp, CDouble(1.0 / stats->Rows()));
}


// EOF
