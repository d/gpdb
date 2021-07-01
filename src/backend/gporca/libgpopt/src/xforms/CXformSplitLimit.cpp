//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformSplitLimit.cpp
//
//	@doc:
//		Implementation of the splitting of limit
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformSplitLimit.h"

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalLimit.h"
#include "gpopt/operators/CPatternLeaf.h"

using namespace gpmd;
using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformSplitLimit::CXformSplitLimit
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformSplitLimit::CXformSplitLimit(CMemoryPool *mp)
	: CXformExploration(
		  // pattern
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalLimit(mp),
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // relational child
			  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(
											   mp)),  // scalar child for offset
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp)
						  CPatternLeaf(mp))	 // scalar child for number of rows
			  ))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformSplitLimit::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformSplitLimit::Exfp(CExpressionHandle &exprhdl) const
{
	if (0 < exprhdl.DeriveOuterReferences()->Size())
	{
		return CXform::ExfpNone;
	}

	gpos::pointer<CLogicalLimit *> popLimit =
		gpos::dyn_cast<CLogicalLimit>(exprhdl.Pop());
	if (!popLimit->FGlobal() || !popLimit->FHasCount())
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformSplitLimit::Transform
//
//	@doc:
//		Actual transformation to expand a global limit into a pair of
//		local and global limit
//
//---------------------------------------------------------------------------
void
CXformSplitLimit::Transform(gpos::pointer<CXformContext *> pxfctxt,
							gpos::pointer<CXformResult *> pxfres,
							gpos::pointer<CExpression *> pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(nullptr != pxfres);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();
	// extract components
	gpos::pointer<CLogicalLimit *> popLimit =
		gpos::dyn_cast<CLogicalLimit>(pexpr->Pop());
	CExpression *pexprRelational = (*pexpr)[0];
	CExpression *pexprScalarStart = (*pexpr)[1];
	CExpression *pexprScalarRows = (*pexpr)[2];
	COrderSpec *pos = popLimit->Pos();

	// TODO: , Feb 20, 2012, we currently only split limit with offset 0.
	if (!CUtils::FHasZeroOffset(pexpr) ||
		0 < pexprRelational->DeriveOuterReferences()->Size())
	{
		return;
	}

	// addref all components
	pexprRelational->AddRef();

	// assemble local limit operator
	gpos::owner<CExpression *> pexprLimitLocal =
		PexprLimit(mp, pexprRelational, pexprScalarStart, pexprScalarRows, pos,
				   false,  // fGlobal
				   popLimit->FHasCount(), popLimit->IsTopLimitUnderDMLorCTAS());

	// assemble global limit operator
	gpos::owner<CExpression *> pexprLimitGlobal = PexprLimit(
		mp, std::move(pexprLimitLocal), pexprScalarStart, pexprScalarRows, pos,
		true,  // fGlobal
		popLimit->FHasCount(), popLimit->IsTopLimitUnderDMLorCTAS());

	pxfres->Add(std::move(pexprLimitGlobal));
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSplitLimit::PexprLimit
//
//	@doc:
//		Generate a limit operator
//
//---------------------------------------------------------------------------
gpos::owner<CExpression *>
CXformSplitLimit::PexprLimit(CMemoryPool *mp,
							 gpos::owner<CExpression *> pexprRelational,
							 CExpression *pexprScalarStart,
							 CExpression *pexprScalarRows, COrderSpec *pos,
							 BOOL fGlobal, BOOL fHasCount,
							 BOOL fTopLimitUnderDML)
{
	pexprScalarStart->AddRef();
	pexprScalarRows->AddRef();
	pos->AddRef();

	// assemble global limit operator
	gpos::owner<CExpression *> pexprLimit = GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp)
			CLogicalLimit(mp, pos, fGlobal, fHasCount, fTopLimitUnderDML),
		std::move(pexprRelational), pexprScalarStart, pexprScalarRows);

	return pexprLimit;
}

// EOF
