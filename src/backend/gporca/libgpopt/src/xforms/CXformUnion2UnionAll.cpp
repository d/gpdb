//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformUnion2UnionAll.cpp
//
//	@doc:
//		Implementation of the transformation that takes a logical union and
//		coverts it into an aggregate over a logical union all
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformUnion2UnionAll.h"

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/operators/CLogicalGbAgg.h"
#include "gpopt/operators/CLogicalUnion.h"
#include "gpopt/operators/CLogicalUnionAll.h"
#include "gpopt/operators/CPatternMultiLeaf.h"
#include "gpopt/operators/CScalarProjectList.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CXformUnion2UnionAll::CXformUnion2UnionAll
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformUnion2UnionAll::CXformUnion2UnionAll(CMemoryPool *mp)
	:  // pattern
	  CXformExploration(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalUnion(mp),
		  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternMultiLeaf(mp))))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUnion2UnionAll::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformUnion2UnionAll::Transform(gpos::pointer<CXformContext *> pxfctxt,
								gpos::pointer<CXformResult *> pxfres,
								gpos::pointer<CExpression *> pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	// extract components
	gpos::pointer<CLogicalUnion *> popUnion =
		gpos::dyn_cast<CLogicalUnion>(pexpr->Pop());
	CColRefArray *pdrgpcrOutput = popUnion->PdrgpcrOutput();
	CColRef2dArray *pdrgpdrgpcrInput = popUnion->PdrgpdrgpcrInput();

	gpos::owner<CExpressionArray *> pdrgpexpr =
		GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG arity = pexpr->Arity();

	for (ULONG ul = 0; ul < arity; ul++)
	{
		gpos::owner<CExpression *> pexprChild = (*pexpr)[ul];
		pexprChild->AddRef();
		pdrgpexpr->Append(pexprChild);
	}

	pdrgpcrOutput->AddRef();
	pdrgpdrgpcrInput->AddRef();

	// assemble new logical operator
	gpos::owner<CExpression *> pexprUnionAll = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalUnionAll(mp, pdrgpcrOutput, pdrgpdrgpcrInput),
		std::move(pdrgpexpr));

	pdrgpcrOutput->AddRef();

	gpos::owner<CExpression *> pexprProjList =
		GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp),
								 GPOS_NEW(mp) CExpressionArray(mp));

	gpos::owner<CExpression *> pexprAgg = GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CLogicalGbAgg(mp, pdrgpcrOutput,
								   COperator::EgbaggtypeGlobal /*egbaggtype*/),
		std::move(pexprUnionAll), std::move(pexprProjList));

	// add alternative to results
	pxfres->Add(std::move(pexprAgg));
}

// EOF
