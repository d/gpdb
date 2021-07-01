//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformInsert2DML.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformInsert2DML.h"

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CLogicalInsert.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/xforms/CXformUtils.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformInsert2DML::CXformInsert2DML
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformInsert2DML::CXformInsert2DML(CMemoryPool *mp)
	: CXformExploration(
		  // pattern
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalInsert(mp),
			  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformInsert2DML::Exfp
//
//	@doc:
//		Compute promise of xform
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformInsert2DML::Exfp(CExpressionHandle &	// exprhdl
) const
{
	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformInsert2DML::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformInsert2DML::Transform(gpos::pointer<CXformContext *> pxfctxt,
							gpos::pointer<CXformResult *> pxfres,
							gpos::pointer<CExpression *> pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	gpos::pointer<CLogicalInsert *> popInsert =
		gpos::dyn_cast<CLogicalInsert>(pexpr->Pop());
	CMemoryPool *mp = pxfctxt->Pmp();

	// extract components for alternative

	gpos::owner<CTableDescriptor *> ptabdesc = popInsert->Ptabdesc();
	ptabdesc->AddRef();

	gpos::owner<CColRefArray *> pdrgpcrSource = popInsert->PdrgpcrSource();
	pdrgpcrSource->AddRef();

	// child of insert operator
	gpos::owner<CExpression *> pexprChild = (*pexpr)[0];
	pexprChild->AddRef();

	// create logical DML
	gpos::owner<CExpression *> pexprAlt =
		CXformUtils::PexprLogicalDMLOverProject(
			mp, std::move(pexprChild), CLogicalDML::EdmlInsert,
			std::move(ptabdesc), std::move(pdrgpcrSource),
			nullptr,  //pcrCtid
			nullptr	  //pcrSegmentId
		);

	// add alternative to transformation result
	pxfres->Add(std::move(pexprAlt));
}

// EOF
