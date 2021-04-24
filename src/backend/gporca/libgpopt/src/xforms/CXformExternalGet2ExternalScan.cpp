//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 VMware, Inc. or its affiliates.
//
//	@filename:
//		CXformExternalGet2ExternalScan.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformExternalGet2ExternalScan.h"

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CLogicalExternalGet.h"
#include "gpopt/operators/CPhysicalExternalScan.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformExternalGet2ExternalScan::CXformExternalGet2ExternalScan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformExternalGet2ExternalScan::CXformExternalGet2ExternalScan(CMemoryPool *mp)
	: CXformImplementation(
		  // pattern
		  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalExternalGet(mp)))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformExternalGet2ExternalScan::Exfp
//
//	@doc:
//		Compute promise of xform
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformExternalGet2ExternalScan::Exfp(CExpressionHandle &  //exprhdl
) const
{
	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformExternalGet2ExternalScan::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformExternalGet2ExternalScan::Transform(
	gpos::pointer<CXformContext *> pxfctxt,
	gpos::pointer<CXformResult *> pxfres,
	gpos::pointer<CExpression *> pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	gpos::pointer<CLogicalExternalGet *> popGet =
		gpos::dyn_cast<CLogicalExternalGet>(pexpr->Pop());
	CMemoryPool *mp = pxfctxt->Pmp();

	// extract components for alternative
	CName *pname = GPOS_NEW(mp) CName(mp, popGet->Name());

	gpos::owner<CTableDescriptor *> ptabdesc = popGet->Ptabdesc();
	ptabdesc->AddRef();

	CColRefArray *pdrgpcrOutput = popGet->PdrgpcrOutput();
	GPOS_ASSERT(nullptr != pdrgpcrOutput);

	pdrgpcrOutput->AddRef();

	// create alternative expression
	gpos::owner<CExpression *> pexprAlt = GPOS_NEW(mp)
		CExpression(mp, GPOS_NEW(mp) CPhysicalExternalScan(
							mp, pname, std::move(ptabdesc), pdrgpcrOutput));

	// add alternative to transformation result
	pxfres->Add(std::move(pexprAlt));
}

// EOF
