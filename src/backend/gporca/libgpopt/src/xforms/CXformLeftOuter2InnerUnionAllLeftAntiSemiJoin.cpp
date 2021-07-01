//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin.cpp
//
//	@doc:
//		Transform
//      LOJ
//        |--Small
//        +--Big
//
// 		to
//
//      UnionAll
//      |---CTEConsumer(A)
//      +---Project_{append nulls)
//          +---LASJ_(key(Small))
//                   |---CTEConsumer(B)
//                   +---Gb(keys(Small))
//                        +---CTEConsumer(A)
//
//		where B is the CTE that produces Small
//		and A is the CTE that produces InnerJoin(Big, CTEConsumer(B)).
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin.h"

#include "gpos/common/owner.h"
#include "gpos/memory/CAutoMemoryPool.h"

#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CLogicalCTEAnchor.h"
#include "gpopt/operators/CLogicalGbAgg.h"
#include "gpopt/operators/CLogicalInnerJoin.h"
#include "gpopt/operators/CLogicalLeftAntiSemiJoin.h"
#include "gpopt/operators/CLogicalLeftOuterJoin.h"
#include "gpopt/operators/CLogicalUnionAll.h"
#include "gpopt/operators/CPatternTree.h"
#include "gpopt/operators/CScalarProjectList.h"
#include "gpopt/xforms/CXformUtils.h"
#include "naucrates/traceflags/traceflags.h"

using namespace gpopt;

// if ratio of the cardinalities outer/inner is below this value, we apply the xform
const DOUBLE
	CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::m_dOuterInnerRatioThreshold =
		0.001;

//---------------------------------------------------------------------------
//	@function:
//		CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::
	CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin(CMemoryPool *mp)
	:  // pattern
	  CXformExploration(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalLeftOuterJoin(mp),
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternTree(mp)),  // left child
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternTree(mp)),  // right child
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternTree(mp))  // predicate
		  ))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle.
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::Exfp(
	CExpressionHandle &exprhdl) const
{
	gpos::pointer<CColRefSet *> pcrsInner =
		exprhdl.DeriveOutputColumns(1 /*child_index*/);
	gpos::pointer<CExpression *> pexprScalar =
		exprhdl.PexprScalarExactChild(2 /*child_index*/);
	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();

	if (nullptr == pexprScalar ||
		!CPredicateUtils::FSimpleEqualityUsingCols(mp, pexprScalar, pcrsInner))
	{
		return ExfpNone;
	}

	if (GPOS_FTRACE(
			gpos::
				EopttraceApplyLeftOuter2InnerUnionAllLeftAntiSemiJoinDisregardingStats) ||
		nullptr == exprhdl.Pgexpr())
	{
		return CXform::ExfpHigh;
	}

	// check if stats are derivable on child groups
	const ULONG arity = exprhdl.Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		gpos::pointer<CGroup *> pgroupChild = (*exprhdl.Pgexpr())[ul];
		if (!pgroupChild->FScalar() && !pgroupChild->FStatsDerivable(mp))
		{
			// stats must be derivable on every child
			return CXform::ExfpNone;
		}
	}

	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::FCheckStats
//
//	@doc:
//		Check the stats ratio to decide whether to apply the xform or not.
//
//---------------------------------------------------------------------------
BOOL
CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::FApplyXformUsingStatsInfo(
	gpos::pointer<const IStatistics *> outer_stats,
	gpos::pointer<const IStatistics *> inner_side_stats)
{
	if (GPOS_FTRACE(
			gpos::
				EopttraceApplyLeftOuter2InnerUnionAllLeftAntiSemiJoinDisregardingStats))
	{
		return true;
	}

	if (nullptr == outer_stats || nullptr == inner_side_stats)
	{
		return false;
	}

	DOUBLE num_rows_outer = outer_stats->Rows().Get();
	DOUBLE dRowsInner = inner_side_stats->Rows().Get();
	GPOS_ASSERT(0 < dRowsInner);

	return num_rows_outer / dRowsInner <= m_dOuterInnerRatioThreshold;
}


// Apply the transformation, e.g.
//
// clang-format off
// Input:
//  +--CLogicalLeftOuterJoin
//     |--CLogicalGet "items", Columns: ["i_item_sk" (95)]
//     |--CLogicalGet "store_sales", Columns: ["ss_item_sk" (124)]
//     +--CScalarCmp (=)
//        |--CScalarIdent "i_item_sk"
//        +--CScalarIdent "ss_item_sk"
//  Output:
//  Alternatives:
//  0:
//  +--CLogicalCTEAnchor (2)
//     +--CLogicalCTEAnchor (3)
//        +--CLogicalUnionAll ["i_item_sk" (95), "ss_item_sk" (124)]
//           |--CLogicalCTEConsumer (3), Columns: ["i_item_sk" (95), "ss_item_sk" (124)]
//           +--CLogicalProject
//              |--CLogicalLeftAntiSemiJoin
//              |  |--CLogicalCTEConsumer (2), Columns: ["i_item_sk" (342)]
//              |  |--CLogicalGbAgg( GetGlobalMemoryPool ) Grp Cols: ["i_item_sk" (343)][Global], Minimal Grp Cols: [], Generates Duplicates :[ 0 ]
//              |  |  |--CLogicalCTEConsumer (3), Columns: ["i_item_sk" (343), "ss_item_sk" (344)]
//              |  |  +--CScalarProjectList
//              |  +--CScalarBoolOp (EboolopNot)
//              |     +--CScalarIsDistinctFrom (=)
//              |        |--CScalarIdent "i_item_sk" (342)
//              |        +--CScalarIdent "i_item_sk" (343)
//              +--CScalarProjectList
//                 +--CScalarProjectElement "ss_item_sk" (466)
//                    +--CScalarConst (null)
//
//  +--CLogicalCTEProducer (2), Columns: ["i_item_sk" (190)]
//     +--CLogicalGet "items", Columns: ["i_item_sk" (190)]
//
//  +--CLogicalCTEProducer (3), Columns: ["i_item_sk" (247), "ss_item_sk" (248)]
//      +--CLogicalInnerJoin
//         |--CLogicalCTEConsumer (0), Columns: ["ss_item_sk" (248)]
//         |--CLogicalCTEConsumer (2), Columns: ["i_item_sk" (247)]
//         +--CScalarCmp (=)
//            |--CScalarIdent "i_item_sk" (247)
//            +--CScalarIdent "ss_item_sk" (248)
//
// clang-format on
void
CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::Transform(
	gpos::pointer<CXformContext *> pxfctxt,
	gpos::pointer<CXformResult *> pxfres,
	gpos::pointer<CExpression *> pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();
	// extract components
	gpos::pointer<CExpression *> pexprOuter = (*pexpr)[0];
	CExpression *pexprInner = (*pexpr)[1];
	CExpression *pexprScalar = (*pexpr)[2];

	if (!FValidInnerExpr(pexprInner))
	{
		return;
	}

	if (!FApplyXformUsingStatsInfo(pexprOuter->Pstats(), pexprInner->Pstats()))
	{
		return;
	}

	const ULONG ulCTEOuterId = COptCtxt::PoctxtFromTLS()->Pcteinfo()->next_id();
	gpos::pointer<CColRefSet *> outer_refs = pexprOuter->DeriveOutputColumns();
	gpos::owner<CColRefArray *> pdrgpcrOuter = outer_refs->Pdrgpcr(mp);
	(void) CXformUtils::PexprAddCTEProducer(mp, ulCTEOuterId, pdrgpcrOuter,
											pexprOuter);

	// invert the order of the branches of the original join, so that the small one becomes
	// inner
	pexprInner->AddRef();
	pexprScalar->AddRef();
	gpos::owner<CExpression *> pexprInnerJoin = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalInnerJoin(mp), pexprInner,
		CXformUtils::PexprCTEConsumer(mp, ulCTEOuterId, pdrgpcrOuter),
		pexprScalar);

	gpos::pointer<CColRefSet *> pcrsJoinOutput = pexpr->DeriveOutputColumns();
	gpos::owner<CColRefArray *> pdrgpcrJoinOutput = pcrsJoinOutput->Pdrgpcr(mp);
	const ULONG ulCTEJoinId = COptCtxt::PoctxtFromTLS()->Pcteinfo()->next_id();
	(void) CXformUtils::PexprAddCTEProducer(mp, ulCTEJoinId, pdrgpcrJoinOutput,
											pexprInnerJoin);

	gpos::pointer<CColRefSet *> pcrsScalar = pexprScalar->DeriveUsedColumns();
	gpos::pointer<CColRefSet *> pcrsInner = pexprInner->DeriveOutputColumns();

	gpos::owner<CColRefArray *> pdrgpcrProjectOutput = nullptr;
	gpos::owner<CExpression *> pexprProjectAppendNulls =
		PexprProjectOverLeftAntiSemiJoin(
			mp, pdrgpcrOuter, pcrsScalar, pcrsInner, pdrgpcrJoinOutput,
			ulCTEJoinId, ulCTEOuterId, &pdrgpcrProjectOutput);
	GPOS_ASSERT(nullptr != pdrgpcrProjectOutput);

	gpos::owner<CColRef2dArray *> pdrgpdrgpcrUnionInput =
		GPOS_NEW(mp) CColRef2dArray(mp);
	pdrgpcrJoinOutput->AddRef();
	pdrgpdrgpcrUnionInput->Append(pdrgpcrJoinOutput);
	pdrgpdrgpcrUnionInput->Append(std::move(pdrgpcrProjectOutput));
	pdrgpcrJoinOutput->AddRef();

	gpos::owner<CExpression *> pexprUnionAll = GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CLogicalUnionAll(mp, pdrgpcrJoinOutput,
									  std::move(pdrgpdrgpcrUnionInput)),
		CXformUtils::PexprCTEConsumer(mp, ulCTEJoinId, pdrgpcrJoinOutput),
		std::move(pexprProjectAppendNulls));
	gpos::owner<CExpression *> pexprJoinAnchor = GPOS_NEW(mp)
		CExpression(mp, GPOS_NEW(mp) CLogicalCTEAnchor(mp, ulCTEJoinId),
					std::move(pexprUnionAll));
	gpos::owner<CExpression *> pexprOuterAnchor = GPOS_NEW(mp)
		CExpression(mp, GPOS_NEW(mp) CLogicalCTEAnchor(mp, ulCTEOuterId),
					std::move(pexprJoinAnchor));
	pexprInnerJoin->Release();

	pxfres->Add(std::move(pexprOuterAnchor));
}

//---------------------------------------------------------------------------
//	@function:
//		CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::FValidInnerExpr
//
//	@doc:
//		Check if the inner expression is of a type which should be considered
//		by this xform.
//
//---------------------------------------------------------------------------
BOOL
CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::FValidInnerExpr(
	gpos::pointer<CExpression *> pexprInner)
{
	GPOS_ASSERT(nullptr != pexprInner);

	// set of inner operator ids that should not be considered because they usually
	// generate a relatively small number of tuples
	COperator::EOperatorId rgeopids[] = {
		COperator::EopLogicalConstTableGet,
		COperator::EopLogicalGbAgg,
		COperator::EopLogicalLimit,
	};

	const COperator::EOperatorId op_id = pexprInner->Pop()->Eopid();
	for (ULONG ul = 0; ul < GPOS_ARRAY_SIZE(rgeopids); ++ul)
	{
		if (rgeopids[ul] == op_id)
		{
			return false;
		}
	}

	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::PexprLeftAntiSemiJoinWithInnerGroupBy
//
//	@doc:
//		Construct a left anti semi join with the CTE consumer (ulCTEJoinId) as outer
//		and a group by as inner.
//
//---------------------------------------------------------------------------
gpos::owner<CExpression *>
CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::
	PexprLeftAntiSemiJoinWithInnerGroupBy(
		CMemoryPool *mp, gpos::pointer<CColRefArray *> pdrgpcrOuter,
		gpos::owner<CColRefArray *> pdrgpcrOuterCopy,
		gpos::pointer<CColRefSet *> pcrsScalar,
		gpos::pointer<CColRefSet *> pcrsInner,
		gpos::pointer<CColRefArray *> pdrgpcrJoinOutput, ULONG ulCTEJoinId,
		ULONG ulCTEOuterId)
{
	// compute the original outer keys and their correspondent keys on the two branches
	// of the LASJ
	gpos::owner<CColRefSet *> pcrsOuterKeys = GPOS_NEW(mp) CColRefSet(mp);
	pcrsOuterKeys->Include(pcrsScalar);
	pcrsOuterKeys->Difference(pcrsInner);
	gpos::owner<CColRefArray *> pdrgpcrOuterKeys = pcrsOuterKeys->Pdrgpcr(mp);

	gpos::owner<CColRefArray *> pdrgpcrConsumer2Output =
		CUtils::PdrgpcrCopy(mp, pdrgpcrJoinOutput);
	gpos::owner<ULongPtrArray *> pdrgpulIndexesOfOuterInGby =
		pdrgpcrJoinOutput->IndexesOfSubsequence(pdrgpcrOuterKeys);

	GPOS_ASSERT(nullptr != pdrgpulIndexesOfOuterInGby);
	gpos::owner<CColRefArray *> pdrgpcrGbyKeys =
		CXformUtils::PdrgpcrReorderedSubsequence(mp, pdrgpcrConsumer2Output,
												 pdrgpulIndexesOfOuterInGby);

	gpos::owner<CExpression *> pexprGby = GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp)
			CLogicalGbAgg(mp, pdrgpcrGbyKeys, COperator::EgbaggtypeGlobal),
		CXformUtils::PexprCTEConsumer(mp, ulCTEJoinId,
									  std::move(pdrgpcrConsumer2Output)),
		GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp)));

	gpos::owner<ULongPtrArray *> pdrgpulIndexesOfOuterKeys =
		pdrgpcrOuter->IndexesOfSubsequence(pdrgpcrOuterKeys);
	GPOS_ASSERT(nullptr != pdrgpulIndexesOfOuterKeys);
	gpos::owner<CColRefArray *> pdrgpcrKeysInOuterCopy =
		CXformUtils::PdrgpcrReorderedSubsequence(mp, pdrgpcrOuterCopy,
												 pdrgpulIndexesOfOuterKeys);

	gpos::owner<CColRef2dArray *> pdrgpdrgpcrLASJInput =
		GPOS_NEW(mp) CColRef2dArray(mp);
	pdrgpdrgpcrLASJInput->Append(std::move(pdrgpcrKeysInOuterCopy));
	pdrgpcrGbyKeys->AddRef();
	pdrgpdrgpcrLASJInput->Append(pdrgpcrGbyKeys);

	pcrsOuterKeys->Release();
	pdrgpcrOuterKeys->Release();

	gpos::owner<CExpression *> pexprLeftAntiSemi = GPOS_NEW(mp)
		CExpression(mp, GPOS_NEW(mp) CLogicalLeftAntiSemiJoin(mp),
					CXformUtils::PexprCTEConsumer(mp, ulCTEOuterId,
												  std::move(pdrgpcrOuterCopy)),
					std::move(pexprGby),
					CUtils::PexprConjINDFCond(mp, pdrgpdrgpcrLASJInput));

	pdrgpdrgpcrLASJInput->Release();
	pdrgpulIndexesOfOuterInGby->Release();
	pdrgpulIndexesOfOuterKeys->Release();

	return pexprLeftAntiSemi;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::PexprProjectOverLeftAntiSemiJoin
//
//	@doc:
//		Return a project over a left anti semi join that appends nulls for all
//		columns in the original inner child.
//
//---------------------------------------------------------------------------
gpos::owner<CExpression *>
CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::PexprProjectOverLeftAntiSemiJoin(
	CMemoryPool *mp, gpos::pointer<CColRefArray *> pdrgpcrOuter,
	gpos::pointer<CColRefSet *> pcrsScalar,
	gpos::pointer<CColRefSet *> pcrsInner,
	gpos::pointer<CColRefArray *> pdrgpcrJoinOutput, ULONG ulCTEJoinId,
	ULONG ulCTEOuterId, gpos::owner<CColRefArray *> *ppdrgpcrProjectOutput)
{
	GPOS_ASSERT(nullptr != pdrgpcrOuter);
	GPOS_ASSERT(nullptr != pcrsScalar);
	GPOS_ASSERT(nullptr != pcrsInner);
	GPOS_ASSERT(nullptr != pdrgpcrJoinOutput);

	// make a copy of outer for the second CTE consumer (outer of LASJ)
	gpos::owner<CColRefArray *> pdrgpcrOuterCopy =
		CUtils::PdrgpcrCopy(mp, pdrgpcrOuter);

	gpos::owner<CExpression *> pexprLeftAntiSemi =
		PexprLeftAntiSemiJoinWithInnerGroupBy(
			mp, pdrgpcrOuter, pdrgpcrOuterCopy, pcrsScalar, pcrsInner,
			pdrgpcrJoinOutput, ulCTEJoinId, ulCTEOuterId);

	gpos::owner<ULongPtrArray *> pdrgpulIndexesOfOuter =
		pdrgpcrJoinOutput->IndexesOfSubsequence(pdrgpcrOuter);
	GPOS_ASSERT(nullptr != pdrgpulIndexesOfOuter);

	gpos::owner<UlongToColRefMap *> colref_mapping =
		GPOS_NEW(mp) UlongToColRefMap(mp);
	const ULONG ulOuterCopyLength = pdrgpcrOuterCopy->Size();

	for (ULONG ul = 0; ul < ulOuterCopyLength; ++ul)
	{
		ULONG ulOrigIndex = *(*pdrgpulIndexesOfOuter)[ul];
		CColRef *pcrOriginal = (*pdrgpcrJoinOutput)[ulOrigIndex];
		BOOL fInserted GPOS_ASSERTS_ONLY = colref_mapping->Insert(
			GPOS_NEW(mp) ULONG(pcrOriginal->Id()), (*pdrgpcrOuterCopy)[ul]);
		GPOS_ASSERT(fInserted);
	}

	gpos::owner<CColRefArray *> pdrgpcrInner = pcrsInner->Pdrgpcr(mp);
	gpos::owner<CExpression *> pexprProject = CUtils::PexprLogicalProjectNulls(
		mp, pdrgpcrInner, std::move(pexprLeftAntiSemi), colref_mapping);

	// compute the output array in the order needed by the union-all above the projection
	*ppdrgpcrProjectOutput = CUtils::PdrgpcrRemap(
		mp, pdrgpcrJoinOutput, colref_mapping, true /*must_exist*/);

	pdrgpcrInner->Release();
	colref_mapping->Release();
	pdrgpulIndexesOfOuter->Release();

	return pexprProject;
}

BOOL
CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin::IsApplyOnce()
{
	return true;
}
// EOF
