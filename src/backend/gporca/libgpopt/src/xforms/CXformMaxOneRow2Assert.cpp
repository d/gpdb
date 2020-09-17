//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CXformMaxOneRow2Assert.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/operators/CLogicalMaxOneRow.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/xforms/CXformMaxOneRow2Assert.h"
#include "gpopt/xforms/CXformUtils.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformMaxOneRow2Assert::CXformMaxOneRow2Assert
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformMaxOneRow2Assert::CXformMaxOneRow2Assert(CMemoryPool *mp)
	: CXformExploration(
		  // pattern
		  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalMaxOneRow(mp),
								   GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformMaxOneRow2Assert::Exfp
//
//	@doc:
//		Compute promise of xform
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformMaxOneRow2Assert::Exfp(CExpressionHandle &  // exprhdl
) const
{
	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformMaxOneRow2Assert::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformMaxOneRow2Assert::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
								  CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	// create Logical Assert that checks if more than one rows are generated by child
	CExpression *pexprAlt = CXformUtils::PexprAssertOneRow(mp, (*pexpr)[0]);

	// add alternative to transformation result
	pxfres->Add(pexprAlt);
}

// EOF
