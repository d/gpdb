//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformPushGbWithHavingBelowJoin.cpp
//
//	@doc:
//		Implementation of pushing group by below join transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformPushGbWithHavingBelowJoin.h"

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/operators/CLogicalGbAgg.h"
#include "gpopt/operators/CLogicalInnerJoin.h"
#include "gpopt/operators/CLogicalSelect.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/xforms/CXformUtils.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformPushGbWithHavingBelowJoin::CXformPushGbWithHavingBelowJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformPushGbWithHavingBelowJoin::CXformPushGbWithHavingBelowJoin(
	CMemoryPool *mp)
	:  // pattern
	  CXformExploration(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalSelect(mp),
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalGbAgg(mp),
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CLogicalInnerJoin(mp),
				  GPOS_NEW(mp) CExpression(
					  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // join outer child
				  GPOS_NEW(mp) CExpression(
					  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // join inner child
				  GPOS_NEW(mp) CExpression(
					  mp, GPOS_NEW(mp) CPatternTree(mp))  // join predicate
				  ),
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternTree(mp))  // scalar project list
			  ),
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))  // Having clause
		  ))
{
}


//---------------------------------------------------------------------------
//	@function:
//		CXformPushGbWithHavingBelowJoin::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//		we only push down global aggregates
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformPushGbWithHavingBelowJoin::Exfp(CExpressionHandle &  // exprhdl
) const
{
	return CXform::ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformPushGbWithHavingBelowJoin::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformPushGbWithHavingBelowJoin::Transform(
	gpos::pointer<CXformContext *> pxfctxt,
	gpos::pointer<CXformResult *> pxfres,
	gpos::pointer<CExpression *> pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	gpos::pointer<CExpression *> pexprGb = (*pexpr)[0];
	gpos::pointer<CLogicalGbAgg *> popGbAgg =
		gpos::dyn_cast<CLogicalGbAgg>(pexprGb->Pop());
	if (!popGbAgg->FGlobal())
	{
		// xform only applies to global aggs
		return;
	}

	gpos::owner<CExpression *> pexprResult =
		CXformUtils::PexprPushGbBelowJoin(mp, pexpr);

	if (nullptr != pexprResult)
	{
		// add alternative to results
		pxfres->Add(std::move(pexprResult));
	}
}


// EOF
