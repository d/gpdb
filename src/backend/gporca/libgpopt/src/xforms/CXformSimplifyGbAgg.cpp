//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformSimplifyGbAgg.cpp
//
//	@doc:
//		Implementation of simplifying an aggregate expression by finding
//		the minimal grouping columns based on functional dependencies
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformSimplifyGbAgg.h"

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/base/CKeyCollection.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CLogicalGbAgg.h"
#include "gpopt/operators/COperator.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPredicateUtils.h"

using namespace gpmd;
using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformSimplifyGbAgg::CXformSimplifyGbAgg
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformSimplifyGbAgg::CXformSimplifyGbAgg(CMemoryPool *mp)
	: CXformExploration(
		  // pattern
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalGbAgg(mp),
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // relational child
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternTree(mp))  // scalar project list
			  ))
{
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSimplifyGbAgg::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//		aggregate must have grouping columns
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformSimplifyGbAgg::Exfp(CExpressionHandle &exprhdl) const
{
	gpos::pointer<CLogicalGbAgg *> popAgg =
		gpos::dyn_cast<CLogicalGbAgg>(exprhdl.Pop());

	GPOS_ASSERT(COperator::EgbaggtypeGlobal == popAgg->Egbaggtype());

	if (0 == popAgg->Pdrgpcr()->Size() || nullptr != popAgg->PdrgpcrMinimal())
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSimplifyGbAgg::FDropGbAgg
//
//	@doc:
//		Return true if GbAgg operator can be dropped because grouping
//		columns include a key
//
//---------------------------------------------------------------------------
BOOL
CXformSimplifyGbAgg::FDropGbAgg(CMemoryPool *mp,
								gpos::pointer<CExpression *> pexpr,
								gpos::pointer<CXformResult *> pxfres)
{
	gpos::pointer<CLogicalGbAgg *> popAgg =
		gpos::dyn_cast<CLogicalGbAgg>(pexpr->Pop());
	CExpression *pexprRelational = (*pexpr)[0];
	gpos::pointer<CExpression *> pexprProjectList = (*pexpr)[1];

	if (0 < pexprProjectList->Arity())
	{
		// GbAgg cannot be dropped if Agg functions are computed
		return false;
	}

	gpos::pointer<CKeyCollection *> pkc =
		pexprRelational->DeriveKeyCollection();
	if (nullptr == pkc)
	{
		// relational child does not have key
		return false;
	}

	const ULONG ulKeys = pkc->Keys();
	BOOL fDrop = false;
	for (ULONG ul = 0; !fDrop && ul < ulKeys; ul++)
	{
		gpos::owner<CColRefArray *> pdrgpcrKey = pkc->PdrgpcrKey(mp, ul);
		gpos::owner<CColRefSet *> pcrs =
			GPOS_NEW(mp) CColRefSet(mp, pdrgpcrKey);
		pdrgpcrKey->Release();

		gpos::owner<CColRefSet *> pcrsGrpCols = GPOS_NEW(mp) CColRefSet(mp);
		pcrsGrpCols->Include(popAgg->Pdrgpcr());
		BOOL fGrpColsHasKey = pcrsGrpCols->ContainsAll(pcrs);

		pcrs->Release();
		pcrsGrpCols->Release();
		if (fGrpColsHasKey)
		{
			// Gb operator can be dropped
			pexprRelational->AddRef();
			gpos::owner<CExpression *> pexprResult = CUtils::PexprLogicalSelect(
				mp, pexprRelational,
				CPredicateUtils::PexprConjunction(mp, nullptr));
			pxfres->Add(pexprResult);
			fDrop = true;
		}
	}

	return fDrop;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSimplifyGbAgg::Transform
//
//	@doc:
//		Actual transformation to simplify a aggregate expression
//
//---------------------------------------------------------------------------
void
CXformSimplifyGbAgg::Transform(gpos::pointer<CXformContext *> pxfctxt,
							   gpos::pointer<CXformResult *> pxfres,
							   gpos::pointer<CExpression *> pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(nullptr != pxfres);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	if (FDropGbAgg(mp, pexpr, pxfres))
	{
		// grouping columns could be dropped, GbAgg is transformed to a Select
		return;
	}

	// extract components
	gpos::pointer<CLogicalGbAgg *> popAgg =
		gpos::dyn_cast<CLogicalGbAgg>(pexpr->Pop());
	CExpression *pexprRelational = (*pexpr)[0];
	CExpression *pexprProjectList = (*pexpr)[1];

	CColRefArray *colref_array = popAgg->Pdrgpcr();
	gpos::owner<CColRefSet *> pcrsGrpCols = GPOS_NEW(mp) CColRefSet(mp);
	pcrsGrpCols->Include(colref_array);

	gpos::owner<CColRefSet *> pcrsCovered =
		GPOS_NEW(mp) CColRefSet(mp);  // set of grouping columns covered by FD's
	gpos::owner<CColRefSet *> pcrsMinimal = GPOS_NEW(mp)
		CColRefSet(mp);	 // a set of minimal grouping columns based on FD's
	gpos::pointer<CFunctionalDependencyArray *> pdrgpfd =
		pexpr->DeriveFunctionalDependencies();

	// collect grouping columns FD's
	const ULONG size = (pdrgpfd == nullptr) ? 0 : pdrgpfd->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		gpos::pointer<CFunctionalDependency *> pfd = (*pdrgpfd)[ul];
		if (pfd->FIncluded(pcrsGrpCols))
		{
			pcrsCovered->Include(pfd->PcrsDetermined());
			pcrsCovered->Include(pfd->PcrsKey());
			pcrsMinimal->Include(pfd->PcrsKey());
		}
	}
	BOOL fCovered = pcrsCovered->Equals(pcrsGrpCols);
	pcrsGrpCols->Release();
	pcrsCovered->Release();

	if (!fCovered)
	{
		// the union of RHS of collected FD's does not cover all grouping columns
		pcrsMinimal->Release();
		return;
	}

	// create a new Agg with minimal grouping columns
	colref_array->AddRef();

	gpos::owner<CLogicalGbAgg *> popAggNew = GPOS_NEW(mp) CLogicalGbAgg(
		mp, colref_array, pcrsMinimal->Pdrgpcr(mp), popAgg->Egbaggtype());
	pcrsMinimal->Release();
	GPOS_ASSERT(!popAgg->Matches(popAggNew) &&
				"Simplified aggregate matches original aggregate");

	pexprRelational->AddRef();
	pexprProjectList->AddRef();
	gpos::owner<CExpression *> pexprResult = GPOS_NEW(mp) CExpression(
		mp, std::move(popAggNew), pexprRelational, pexprProjectList);
	pxfres->Add(std::move(pexprResult));
}


// EOF
