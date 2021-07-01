//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalDifferenceAll.cpp
//
//	@doc:
//		Implementation of DifferenceAll operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CLogicalDifferenceAll.h"

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/base/CKeyCollection.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "naucrates/statistics/CStatsPredUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDifferenceAll::CLogicalDifferenceAll
//
//	@doc:
//		Ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalDifferenceAll::CLogicalDifferenceAll(CMemoryPool *mp)
	: CLogicalSetOp(mp)
{
	m_fPattern = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDifferenceAll::CLogicalDifferenceAll
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalDifferenceAll::CLogicalDifferenceAll(
	CMemoryPool *mp, gpos::Ref<CColRefArray> pdrgpcrOutput,
	gpos::Ref<CColRef2dArray> pdrgpdrgpcrInput)
	: CLogicalSetOp(mp, std::move(pdrgpcrOutput), std::move(pdrgpdrgpcrInput))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDifferenceAll::~CLogicalDifferenceAll
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalDifferenceAll::~CLogicalDifferenceAll() = default;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDifferenceAll::DeriveMaxCard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalDifferenceAll::DeriveMaxCard(CMemoryPool *,	 // mp
									 CExpressionHandle &exprhdl) const
{
	// contradictions produce no rows
	if (exprhdl.DerivePropertyConstraint()->FContradiction())
	{
		return CMaxCard(0 /*ull*/);
	}

	return exprhdl.DeriveMaxCard(0);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDifferenceAll::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
gpos::Ref<COperator>
CLogicalDifferenceAll::PopCopyWithRemappedColumns(
	CMemoryPool *mp, UlongToColRefMap *colref_mapping, BOOL must_exist)
{
	gpos::Ref<CColRefArray> pdrgpcrOutput = CUtils::PdrgpcrRemap(
		mp, m_pdrgpcrOutput.get(), colref_mapping, must_exist);
	gpos::Ref<CColRef2dArray> pdrgpdrgpcrInput = CUtils::PdrgpdrgpcrRemap(
		mp, m_pdrgpdrgpcrInput.get(), colref_mapping, must_exist);

	return GPOS_NEW(mp) CLogicalDifferenceAll(mp, std::move(pdrgpcrOutput),
											  std::move(pdrgpdrgpcrInput));
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDifferenceAll::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
gpos::Ref<CKeyCollection>
CLogicalDifferenceAll::DeriveKeyCollection(CMemoryPool *,		// mp,
										   CExpressionHandle &	//exprhdl
) const
{
	// TODO: Add keys on columns contributing to the setop from the outer child
	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDifferenceAll::PstatsDerive
//
//	@doc:
//		Derive statistics
//
//---------------------------------------------------------------------------
gpos::Ref<IStatistics>
CLogicalDifferenceAll::PstatsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl,
									IStatisticsArray *	// not used
) const
{
	GPOS_ASSERT(Esp(exprhdl) > EspNone);

	// difference all is transformed into a LASJ,
	// we follow the same route to compute statistics
	gpos::Ref<CColRefSetArray> output_colrefsets =
		GPOS_NEW(mp) CColRefSetArray(mp);
	const ULONG size = m_pdrgpdrgpcrInput->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		gpos::Ref<CColRefSet> pcrs =
			GPOS_NEW(mp) CColRefSet(mp, (*m_pdrgpdrgpcrInput)[ul].get());
		output_colrefsets->Append(pcrs);
	}

	IStatistics *outer_stats = exprhdl.Pstats(0);
	IStatistics *inner_side_stats = exprhdl.Pstats(1);

	// construct the scalar condition for the LASJ
	gpos::Ref<CExpression> pexprScCond =
		CUtils::PexprConjINDFCond(mp, m_pdrgpdrgpcrInput.get());

	// compute the statistics for LASJ
	CColRefSet *outer_refs = exprhdl.DeriveOuterReferences();
	gpos::Ref<CStatsPredJoinArray> join_preds_stats =
		CStatsPredUtils::ExtractJoinStatsFromExpr(
			mp, exprhdl, pexprScCond.get(), output_colrefsets.get(), outer_refs,
			true  // is an LASJ
		);
	gpos::Ref<IStatistics> LASJ_stats = outer_stats->CalcLASJoinStats(
		mp, inner_side_stats, join_preds_stats.get(),
		true /* DoIgnoreLASJHistComputation*/
	);

	// clean up
	;
	;
	;

	return LASJ_stats;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDifferenceAll::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
gpos::Ref<CXformSet>
CLogicalDifferenceAll::PxfsCandidates(CMemoryPool *mp) const
{
	gpos::Ref<CXformSet> xform_set = GPOS_NEW(mp) CXformSet(mp);
	(void) xform_set->ExchangeSet(CXform::ExfDifferenceAll2LeftAntiSemiJoin);
	return xform_set;
}

// EOF
