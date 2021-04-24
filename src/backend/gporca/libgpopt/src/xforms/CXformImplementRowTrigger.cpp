//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformImplementRowTrigger.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformImplementRowTrigger.h"

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/operators/CLogicalRowTrigger.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPhysicalRowTrigger.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementRowTrigger::CXformImplementRowTrigger
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformImplementRowTrigger::CXformImplementRowTrigger(CMemoryPool *mp)
	: CXformImplementation(
		  // pattern
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalRowTrigger(mp),
			  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformImplementRowTrigger::Exfp
//
//	@doc:
//		Compute promise of xform
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformImplementRowTrigger::Exfp(CExpressionHandle &	 // exprhdl
) const
{
	return CXform::ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementRowTrigger::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformImplementRowTrigger::Transform(gpos::pointer<CXformContext *> pxfctxt,
									 gpos::pointer<CXformResult *> pxfres,
									 gpos::pointer<CExpression *> pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	gpos::pointer<CLogicalRowTrigger *> popRowTrigger =
		gpos::dyn_cast<CLogicalRowTrigger>(pexpr->Pop());
	CMemoryPool *mp = pxfctxt->Pmp();

	// extract components for alternative
	gpos::owner<IMDId *> rel_mdid = popRowTrigger->GetRelMdId();
	rel_mdid->AddRef();

	INT type = popRowTrigger->GetType();

	CColRefArray *pdrgpcrOld = popRowTrigger->PdrgpcrOld();
	if (nullptr != pdrgpcrOld)
	{
		pdrgpcrOld->AddRef();
	}

	CColRefArray *pdrgpcrNew = popRowTrigger->PdrgpcrNew();
	if (nullptr != pdrgpcrNew)
	{
		pdrgpcrNew->AddRef();
	}

	// child of RowTrigger operator
	gpos::owner<CExpression *> pexprChild = (*pexpr)[0];
	pexprChild->AddRef();

	// create physical RowTrigger
	gpos::owner<CExpression *> pexprAlt = GPOS_NEW(mp)
		CExpression(mp,
					GPOS_NEW(mp) CPhysicalRowTrigger(
						mp, std::move(rel_mdid), type, pdrgpcrOld, pdrgpcrNew),
					std::move(pexprChild));
	// add alternative to transformation result
	pxfres->Add(std::move(pexprAlt));
}

// EOF
