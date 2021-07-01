//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformExpandNAryJoinDP.cpp
//
//	@doc:
//		Implementation of n-ary join expansion using dynamic programming
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformExpandNAryJoinDP.h"

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/engine/CHint.h"
#include "gpopt/operators/CLogicalNAryJoin.h"
#include "gpopt/operators/CNormalizer.h"
#include "gpopt/operators/CPatternMultiLeaf.h"
#include "gpopt/operators/CPatternMultiTree.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/xforms/CJoinOrderDP.h"
#include "gpopt/xforms/CXformUtils.h"



using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformExpandNAryJoinDP::CXformExpandNAryJoinDP
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformExpandNAryJoinDP::CXformExpandNAryJoinDP(CMemoryPool *mp)
	: CXformExploration(
		  // pattern
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalNAryJoin(mp),
			  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternMultiLeaf(mp)),
			  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternTree(mp))))
{
}


//---------------------------------------------------------------------------
//	@function:
//		CXformExpandNAryJoinDP::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformExpandNAryJoinDP::Exfp(CExpressionHandle &exprhdl) const
{
	gpos::pointer<COptimizerConfig *> optimizer_config =
		COptCtxt::PoctxtFromTLS()->GetOptimizerConfig();
	gpos::pointer<const CHint *> phint = optimizer_config->GetHint();

	const ULONG arity = exprhdl.Arity();

	// since the last child of the join operator is a scalar child
	// defining the join predicate, ignore it.
	const ULONG ulRelChild = arity - 1;

	if (ulRelChild > phint->UlJoinOrderDPLimit())
	{
		return CXform::ExfpNone;
	}

	return CXformUtils::ExfpExpandJoinOrder(exprhdl, this);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformExpandNAryJoinDP::Transform
//
//	@doc:
//		Actual transformation of n-ary join to cluster of inner joins using
//		dynamic programming
//
//---------------------------------------------------------------------------
void
CXformExpandNAryJoinDP::Transform(gpos::pointer<CXformContext *> pxfctxt,
								  gpos::pointer<CXformResult *> pxfres,
								  gpos::pointer<CExpression *> pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(nullptr != pxfres);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	const ULONG arity = pexpr->Arity();
	GPOS_ASSERT(arity >= 3);

	gpos::owner<CExpressionArray *> pdrgpexpr =
		GPOS_NEW(mp) CExpressionArray(mp);
	for (ULONG ul = 0; ul < arity - 1; ul++)
	{
		gpos::owner<CExpression *> pexprChild = (*pexpr)[ul];
		pexprChild->AddRef();
		pdrgpexpr->Append(pexprChild);
	}

	gpos::pointer<CExpression *> pexprScalar = (*pexpr)[arity - 1];
	gpos::owner<CExpressionArray *> pdrgpexprPreds =
		CPredicateUtils::PdrgpexprConjuncts(mp, pexprScalar);

	// create join order using dynamic programming
	CJoinOrderDP jodp(mp, pdrgpexpr, pdrgpexprPreds);
	gpos::owner<CExpression *> pexprResult = jodp.PexprExpand();

	if (nullptr != pexprResult)
	{
		// normalize resulting expression
		gpos::owner<CExpression *> pexprNormalized =
			CNormalizer::PexprNormalize(mp, pexprResult);
		pexprResult->Release();
		pxfres->Add(pexprNormalized);

		const ULONG UlTopKJoinOrders = jodp.PdrgpexprTopK()->Size();
		for (ULONG ul = 0; ul < UlTopKJoinOrders; ul++)
		{
			CExpression *pexprJoinOrder = (*jodp.PdrgpexprTopK())[ul];
			if (pexprJoinOrder != pexprResult)
			{
				// We should consider normalizing this expression before inserting it, as we do for pexprResult
				pexprJoinOrder->AddRef();
				pxfres->Add(pexprJoinOrder);
			}
		}
	}
}

// EOF
