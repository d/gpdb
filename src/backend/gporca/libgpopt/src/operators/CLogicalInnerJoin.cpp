//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CLogicalInnerJoin.cpp
//
//	@doc:
//		Implementation of inner join operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CLogicalInnerJoin.h"

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPredicateUtils.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CLogicalInnerJoin::CLogicalInnerJoin
//
//	@doc:
//		ctor
//		Note: 04/09/2009 - ; so far inner join doesn't have any specific
//			members, hence, no need for a separate pattern ctor
//
//---------------------------------------------------------------------------
CLogicalInnerJoin::CLogicalInnerJoin(CMemoryPool *mp) : CLogicalJoin(mp)
{
	GPOS_ASSERT(nullptr != mp);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalInnerJoin::DeriveMaxCard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalInnerJoin::DeriveMaxCard(CMemoryPool *,	 // mp
								 CExpressionHandle &exprhdl) const
{
	return CLogical::Maxcard(exprhdl, 2 /*ulScalarIndex*/, MaxcardDef(exprhdl));
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalInnerJoin::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
gpos::owner<CXformSet *>
CLogicalInnerJoin::PxfsCandidates(CMemoryPool *mp) const
{
	gpos::owner<CXformSet *> xform_set = GPOS_NEW(mp) CXformSet(mp);

	(void) xform_set->ExchangeSet(CXform::ExfInnerJoin2NLJoin);
	(void) xform_set->ExchangeSet(CXform::ExfInnerJoin2HashJoin);
	(void) xform_set->ExchangeSet(CXform::ExfSubqJoin2Apply);
	(void) xform_set->ExchangeSet(CXform::ExfJoin2BitmapIndexGetApply);
	(void) xform_set->ExchangeSet(CXform::ExfJoin2IndexGetApply);

	(void) xform_set->ExchangeSet(CXform::ExfJoinCommutativity);
	(void) xform_set->ExchangeSet(CXform::ExfJoinAssociativity);
	(void) xform_set->ExchangeSet(CXform::ExfInnerJoinSemiJoinSwap);
	(void) xform_set->ExchangeSet(CXform::ExfInnerJoinAntiSemiJoinSwap);
	(void) xform_set->ExchangeSet(CXform::ExfInnerJoinAntiSemiJoinNotInSwap);

	return xform_set;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalInnerJoin::FFewerConj
//
//	@doc:
//		Compare two innerJoin group expressions, test whether the first one
//		has less join predicates than the second one. This is used to
//		prioritize innerJoin with less predicates for stats derivation
//
//---------------------------------------------------------------------------
BOOL
CLogicalInnerJoin::FFewerConj(CMemoryPool *mp,
							  gpos::pointer<CGroupExpression *> pgexprFst,
							  gpos::pointer<CGroupExpression *> pgexprSnd)
{
	if (nullptr == pgexprFst || nullptr == pgexprSnd)
	{
		return false;
	}

	if (COperator::EopLogicalInnerJoin != pgexprFst->Pop()->Eopid() ||
		COperator::EopLogicalInnerJoin != pgexprSnd->Pop()->Eopid())
	{
		return false;
	}

	// third child must be the group for join conditions
	gpos::pointer<CGroup *> pgroupScalarFst = (*pgexprFst)[2];
	gpos::pointer<CGroup *> pgroupScalarSnd = (*pgexprSnd)[2];
	GPOS_ASSERT(pgroupScalarFst->FScalar());
	GPOS_ASSERT(pgroupScalarSnd->FScalar());

	gpos::owner<CExpressionArray *> pdrgpexprConjFst =
		CPredicateUtils::PdrgpexprConjuncts(mp,
											pgroupScalarFst->PexprScalarRep());
	gpos::owner<CExpressionArray *> pdrgpexprConjSnd =
		CPredicateUtils::PdrgpexprConjuncts(mp,
											pgroupScalarSnd->PexprScalarRep());

	ULONG ulConjFst = pdrgpexprConjFst->Size();
	ULONG ulConjSnd = pdrgpexprConjSnd->Size();

	pdrgpexprConjFst->Release();
	pdrgpexprConjSnd->Release();

	return ulConjFst < ulConjSnd;
}

// EOF
