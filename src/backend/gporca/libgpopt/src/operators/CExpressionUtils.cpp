//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CExpressionUtils.cpp
//
//	@doc:
//		Utility routines for transforming expressions
//
//	@owner:
//		,
//
//	@test:
//
//---------------------------------------------------------------------------

#include "gpopt/operators/CExpressionUtils.h"

#include "gpos/common/owner.h"

#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CColRefTable.h"
#include "gpopt/base/CConstraintInterval.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/exception.h"
#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/CScalarSubqueryExists.h"
#include "gpopt/operators/CScalarSubqueryNotExists.h"
#include "naucrates/md/IMDScalarOp.h"
#include "naucrates/md/IMDType.h"
#include "naucrates/statistics/CStatistics.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CExpressionUtils::UnnestChild
//
//	@doc:
//		Unnest a given expression's child and append unnested nodes to
//		the given expression array
//
//---------------------------------------------------------------------------
void
CExpressionUtils::UnnestChild(
	CMemoryPool *mp,
	CExpression *pexpr,			 // parent node
	ULONG child_index,			 // child index
	BOOL fAnd,					 // is expression an AND node?
	BOOL fOr,					 // is expression an OR node?
	BOOL fHasNegatedChild,		 // does expression have NOT child nodes?
	CExpressionArray *pdrgpexpr	 // array to append results to
)
{
	GPOS_ASSERT(nullptr != mp);
	GPOS_ASSERT(nullptr != pexpr);
	GPOS_ASSERT(child_index < pexpr->Arity());
	GPOS_ASSERT(nullptr != pdrgpexpr);

	CExpression *pexprChild = (*pexpr)[child_index];

	if ((fAnd && CPredicateUtils::FAnd(pexprChild)) ||
		(fOr && CPredicateUtils::FOr(pexprChild)))
	{
		// two cascaded AND nodes or two cascaded OR nodes, recursively
		// pull-up children
		AppendChildren(mp, pexprChild, pdrgpexpr);

		return;
	}

	if (fHasNegatedChild && CPredicateUtils::FNot(pexprChild) &&
		CPredicateUtils::FNot((*pexprChild)[0]))
	{
		// two cascaded Not nodes cancel each other
		CExpression *pexprNot = (*pexprChild)[0];
		pexprChild = (*pexprNot)[0];
	}
	gpos::Ref<CExpression> pexprUnnestedChild = PexprUnnest(mp, pexprChild);
	pdrgpexpr->Append(std::move(pexprUnnestedChild));
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionUtils::AppendChildren
//
//	@doc:
//		Append the unnested children of given expression to given array
//
//---------------------------------------------------------------------------
void
CExpressionUtils::AppendChildren(CMemoryPool *mp, CExpression *pexpr,
								 CExpressionArray *pdrgpexpr)
{
	GPOS_ASSERT(nullptr != mp);
	GPOS_ASSERT(nullptr != pexpr);
	GPOS_ASSERT(nullptr != pdrgpexpr);

	gpos::Ref<CExpressionArray> pdrgpexprChildren =
		PdrgpexprUnnestChildren(mp, pexpr);
	CUtils::AddRefAppend(pdrgpexpr, pdrgpexprChildren.get());
	;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionUtils::PdrgpexprUnnestChildren
//
//	@doc:
//		Return an array of expression's children after unnesting nested
//		AND/OR/NOT subtrees
//
//---------------------------------------------------------------------------
gpos::Ref<CExpressionArray>
CExpressionUtils::PdrgpexprUnnestChildren(CMemoryPool *mp, CExpression *pexpr)
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(nullptr != mp);
	GPOS_ASSERT(nullptr != pexpr);

	// compute flags for cases where we may have nested predicates
	BOOL fAnd = CPredicateUtils::FAnd(pexpr);
	BOOL fOr = CPredicateUtils::FOr(pexpr);
	BOOL fHasNegatedChild = CPredicateUtils::FHasNegatedChild(pexpr);

	gpos::Ref<CExpressionArray> pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG arity = pexpr->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		UnnestChild(mp, pexpr, ul, fAnd, fOr, fHasNegatedChild,
					pdrgpexpr.get());
	}

	return pdrgpexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionUtils::PexprUnnest
//
//	@doc:
//		Unnest AND/OR/NOT predicates
//
//---------------------------------------------------------------------------
gpos::Ref<CExpression>
CExpressionUtils::PexprUnnest(CMemoryPool *mp, CExpression *pexpr)
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(nullptr != mp);
	GPOS_ASSERT(nullptr != pexpr);

	if (CPredicateUtils::FNot(pexpr))
	{
		CExpression *pexprChild = (*pexpr)[0];
		gpos::Ref<CExpression> pexprPushedNot =
			PexprPushNotOneLevel(mp, pexprChild);

		COperator *pop = pexprPushedNot->Pop();
		gpos::Ref<CExpressionArray> pdrgpexpr =
			PdrgpexprUnnestChildren(mp, pexprPushedNot.get());
		;

		// clean up
		;

		return GPOS_NEW(mp) CExpression(mp, pop, std::move(pdrgpexpr));
	}

	COperator *pop = pexpr->Pop();
	gpos::Ref<CExpressionArray> pdrgpexpr = PdrgpexprUnnestChildren(mp, pexpr);
	;

	return GPOS_NEW(mp) CExpression(mp, pop, std::move(pdrgpexpr));
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionUtils::PexprPushNotOneLevel
//
//	@doc:
// 		Push not expression one level down the given expression. For example:
// 		1. AND of expressions into an OR a negation of these expression
// 		2. OR of expressions into an AND a negation of these expression
// 		3. EXISTS into NOT EXISTS and vice versa
//      4. Else, return NOT of given expression
//---------------------------------------------------------------------------
gpos::Ref<CExpression>
CExpressionUtils::PexprPushNotOneLevel(CMemoryPool *mp, CExpression *pexpr)
{
	GPOS_ASSERT(nullptr != pexpr);

	BOOL fAnd = CPredicateUtils::FAnd(pexpr);
	BOOL fOr = CPredicateUtils::FOr(pexpr);

	if (fAnd || fOr)
	{
		gpos::Ref<COperator> popNew = nullptr;

		if (fOr)
		{
			popNew = GPOS_NEW(mp) CScalarBoolOp(mp, CScalarBoolOp::EboolopAnd);
		}
		else
		{
			popNew = GPOS_NEW(mp) CScalarBoolOp(mp, CScalarBoolOp::EboolopOr);
		}

		gpos::Ref<CExpressionArray> pdrgpexpr =
			GPOS_NEW(mp) CExpressionArray(mp);
		const ULONG arity = pexpr->Arity();
		for (ULONG ul = 0; ul < arity; ul++)
		{
			gpos::Ref<CExpression> pexprChild = (*pexpr)[ul];
			;
			pdrgpexpr->Append(CUtils::PexprNegate(mp, pexprChild));
		}

		return GPOS_NEW(mp)
			CExpression(mp, std::move(popNew), std::move(pdrgpexpr));
	}

	const COperator *pop = pexpr->Pop();
	if (COperator::EopScalarSubqueryExists == pop->Eopid())
	{
		;
		return GPOS_NEW(mp) CExpression(
			mp, GPOS_NEW(mp) CScalarSubqueryNotExists(mp), pexpr->PdrgPexpr());
	}

	if (COperator::EopScalarSubqueryNotExists == pop->Eopid())
	{
		;
		return GPOS_NEW(mp) CExpression(
			mp, GPOS_NEW(mp) CScalarSubqueryExists(mp), pexpr->PdrgPexpr());
	}

	// TODO: , Feb 4 2015, we currently only handling EXISTS/NOT EXISTS/AND/OR
	;
	return GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CScalarBoolOp(mp, CScalarBoolOp::EboolopNot), pexpr);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionUtils::PexprDedupChildren
//
//	@doc:
//		Remove duplicate AND/OR children
//
//---------------------------------------------------------------------------
gpos::Ref<CExpression>
CExpressionUtils::PexprDedupChildren(CMemoryPool *mp, CExpression *pexpr)
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(nullptr != mp);
	GPOS_ASSERT(nullptr != pexpr);

	// recursively process children
	const ULONG arity = pexpr->Arity();
	gpos::Ref<CExpressionArray> pdrgpexprChildren =
		GPOS_NEW(mp) CExpressionArray(mp);
	for (ULONG ul = 0; ul < arity; ul++)
	{
		gpos::Ref<CExpression> pexprChild =
			PexprDedupChildren(mp, (*pexpr)[ul]);
		pdrgpexprChildren->Append(pexprChild);
	}

	if (CPredicateUtils::FAnd(pexpr) || CPredicateUtils::FOr(pexpr))
	{
		gpos::Ref<CExpressionArray> pdrgpexprNewChildren =
			CUtils::PdrgpexprDedup(mp, pdrgpexprChildren.get());

		;
		pdrgpexprChildren = pdrgpexprNewChildren;

		// Check if we end with one child, return that child
		if (1 == pdrgpexprChildren->Size())
		{
			gpos::Ref<CExpression> pexprChild = (*pdrgpexprChildren)[0];
			;
			;

			return pexprChild;
		}
	}


	gpos::Ref<COperator> pop = pexpr->Pop();
	;
	return GPOS_NEW(mp)
		CExpression(mp, std::move(pop), std::move(pdrgpexprChildren));
}

// EOF
