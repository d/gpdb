//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformSubqJoin2Apply.cpp
//
//	@doc:
//		Implementation of Inner Join to Apply transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformSubqJoin2Apply.h"

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/operators/CLogicalInnerJoin.h"
#include "gpopt/operators/CLogicalNAryJoin.h"
#include "gpopt/operators/CNormalizer.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/CScalarProjectElement.h"
#include "gpopt/xforms/CSubqueryHandler.h"
#include "gpopt/xforms/CXformUtils.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformSubqJoin2Apply::CXformSubqJoin2Apply
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformSubqJoin2Apply::CXformSubqJoin2Apply(CMemoryPool *mp)
	:  // pattern
	  CXformSubqueryUnnest(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalInnerJoin(mp),
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // relational child
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // relational child
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternTree(mp))  // predicate tree
		  ))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformSubqJoin2Apply::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
// 		if subqueries exist in the scalar predicate, we must have an
// 		equivalent logical Apply expression created during exploration;
// 		no need for generating a Join expression here
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformSubqJoin2Apply::Exfp(CExpressionHandle &exprhdl) const
{
	if (exprhdl.DeriveHasSubquery(exprhdl.Arity() - 1))
	{
		return CXform::ExfpHigh;
	}

	return CXform::ExfpNone;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformSubqJoin2Apply::CollectSubqueries
//
//	@doc:
//		Collect subqueries that exclusively use columns from one join child
//
//---------------------------------------------------------------------------
void
CXformSubqJoin2Apply::CollectSubqueries(
	CMemoryPool *mp, CExpression *pexpr, CColRefSetArray *pdrgpcrs,
	CExpressionArrays
		*pdrgpdrgpexprSubqs	 // array-of-arrays indexed on join child index.
	//  i^{th} entry is an array corresponding to subqueries collected for join child #i
)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(nullptr != pexpr);
	GPOS_ASSERT(nullptr != pdrgpcrs);
	GPOS_ASSERT(nullptr != pdrgpdrgpexprSubqs);

	gpos::pointer<COperator *> pop = pexpr->Pop();
	if (CUtils::FSubquery(pop))
	{
		// extract outer references below subquery
		gpos::owner<CColRefSet *> outer_refs = GPOS_NEW(mp)
			CColRefSet(mp, *((*pexpr)[0]->DeriveOuterReferences()));

		// add columns used by subquery
		outer_refs->Union(pexpr->DeriveUsedColumns());

		ULONG child_index = gpos::ulong_max;
		const ULONG size = pdrgpcrs->Size();
		for (ULONG ul = 0; ul < size; ul++)
		{
			gpos::pointer<CColRefSet *> pcrsOutput = (*pdrgpcrs)[ul];
			if (pcrsOutput->ContainsAll(outer_refs))
			{
				// outer columns all come from the same join child, break here
				child_index = ul;
				break;
			}
		}

		if (gpos::ulong_max != child_index)
		{
			pexpr->AddRef();
			(*pdrgpdrgpexprSubqs)[child_index]->Append(pexpr);
		}

		outer_refs->Release();
		return;
	}

	// recursively process children
	const ULONG arity = pexpr->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprChild = (*pexpr)[ul];
		CollectSubqueries(mp, pexprChild, pdrgpcrs, pdrgpdrgpexprSubqs);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSubqJoin2Apply::PexprReplaceSubqueries
//
//	@doc:
//		Replace subqueries with scalar identifiers based on given map
//
//---------------------------------------------------------------------------
gpos::owner<CExpression *>
CXformSubqJoin2Apply::PexprReplaceSubqueries(
	CMemoryPool *mp, gpos::pointer<CExpression *> pexprScalar,
	ExprToColRefMap *phmexprcr)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(nullptr != pexprScalar);
	GPOS_ASSERT(nullptr != phmexprcr);

	CColRef *colref = phmexprcr->Find(pexprScalar);
	if (nullptr != colref)
	{
		// look-up succeeded on root operator, we return here
		return CUtils::PexprScalarIdent(mp, colref);
	}

	// recursively process children
	const ULONG arity = pexprScalar->Arity();
	gpos::owner<CExpressionArray *> pdrgpexprChildren =
		GPOS_NEW(mp) CExpressionArray(mp);
	for (ULONG ul = 0; ul < arity; ul++)
	{
		gpos::owner<CExpression *> pexprChild =
			PexprReplaceSubqueries(mp, (*pexprScalar)[ul], phmexprcr);
		pdrgpexprChildren->Append(pexprChild);
	}

	gpos::owner<COperator *> pop = pexprScalar->Pop();
	pop->AddRef();

	return GPOS_NEW(mp)
		CExpression(mp, std::move(pop), std::move(pdrgpexprChildren));
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSubqJoin2Apply::PexprSubqueryPushdown
//
//	@doc:
//		Push down subquery below join
//
//---------------------------------------------------------------------------
gpos::owner<CExpression *>
CXformSubqJoin2Apply::PexprSubqueryPushDown(CMemoryPool *mp,
											gpos::pointer<CExpression *> pexpr,
											BOOL fEnforceCorrelatedApply)
{
	GPOS_ASSERT(nullptr != pexpr);
	GPOS_ASSERT(COperator::EopLogicalSelect == pexpr->Pop()->Eopid());

	gpos::pointer<CExpression *> pexprJoin = (*pexpr)[0];
	const ULONG arity = pexprJoin->Arity();
	CExpression *pexprScalar = (*pexpr)[1];
	CExpression *join_pred_expr = (*pexprJoin)[arity - 1];
	gpos::pointer<CLogicalNAryJoin *> naryLOJOp =
		CLogicalNAryJoin::PopConvertNAryLOJ(pexprJoin->Pop());

	// collect output columns of all logical children
	gpos::owner<CColRefSetArray *> pdrgpcrs = GPOS_NEW(mp) CColRefSetArray(mp);
	gpos::owner<CExpressionArrays *> pdrgpdrgpexprSubqs =
		GPOS_NEW(mp) CExpressionArrays(mp);
	for (ULONG ul = 0; ul < arity - 1; ul++)
	{
		gpos::pointer<CExpression *> pexprChild = (*pexprJoin)[ul];
		gpos::owner<CColRefSet *> pcrsOutput = nullptr;

		if ((nullptr == naryLOJOp || naryLOJOp->IsInnerJoinChild(ul)))
		{
			// inner join child
			pcrsOutput = pexprChild->DeriveOutputColumns();
			pcrsOutput->AddRef();
		}
		else
		{
			// use an empty set for right children of LOJs, because we don't want to
			// push any subqueries down to those children (note that non-correlated
			// subqueries will be pushed to the leftmost child, which is never the
			// right child of an LOJ)
			pcrsOutput = GPOS_NEW(mp) CColRefSet(mp);
		}
		pdrgpcrs->Append(pcrsOutput);

		pdrgpdrgpexprSubqs->Append(GPOS_NEW(mp) CExpressionArray(mp));
	}

	// collect subqueries that exclusively use columns from each join child
	CollectSubqueries(mp, pexprScalar, pdrgpcrs, pdrgpdrgpexprSubqs);

	// create new join children by pushing subqueries to Project nodes on top
	// of corresponding join children
	gpos::owner<CExpressionArray *> pdrgpexprNewChildren =
		GPOS_NEW(mp) CExpressionArray(mp);
	gpos::owner<ExprToColRefMap *> phmexprcr = GPOS_NEW(mp) ExprToColRefMap(mp);
	for (ULONG ulChild = 0; ulChild < arity - 1; ulChild++)
	{
		gpos::owner<CExpression *> pexprChild = (*pexprJoin)[ulChild];
		pexprChild->AddRef();
		gpos::owner<CExpression *> pexprNewChild = pexprChild;

		gpos::pointer<CExpressionArray *> pdrgpexprSubqs =
			(*pdrgpdrgpexprSubqs)[ulChild];
		const ULONG ulSubqs = pdrgpexprSubqs->Size();
		if (0 < ulSubqs)
		{
			// join child has pushable subqueries
			pexprNewChild =
				CUtils::PexprAddProjection(mp, pexprChild, pdrgpexprSubqs);
			gpos::pointer<CExpression *> pexprPrjList = (*pexprNewChild)[1];

			// add pushed subqueries to map
			for (ULONG ulSubq = 0; ulSubq < ulSubqs; ulSubq++)
			{
				gpos::owner<CExpression *> pexprSubq =
					(*pdrgpexprSubqs)[ulSubq];
				pexprSubq->AddRef();
				CColRef *colref = gpos::dyn_cast<CScalarProjectElement>(
									  (*pexprPrjList)[ulSubq]->Pop())
									  ->Pcr();
#ifdef GPOS_DEBUG
				BOOL fInserted =
#endif	// GPOS_DEBUG
					phmexprcr->Insert(pexprSubq, colref);
				GPOS_ASSERT(fInserted);
			}

			// unnest subqueries in newly created child
			gpos::owner<CExpression *> pexprUnnested =
				PexprSubqueryUnnest(mp, pexprNewChild, fEnforceCorrelatedApply);
			if (nullptr != pexprUnnested)
			{
				pexprNewChild->Release();
				pexprNewChild = pexprUnnested;
			}
		}

		pdrgpexprNewChildren->Append(pexprNewChild);
	}

	join_pred_expr->AddRef();
	pdrgpexprNewChildren->Append(join_pred_expr);

	// replace subqueries in the original scalar expression with
	// scalar identifiers based on constructed map
	gpos::owner<CExpression *> pexprNewScalar =
		PexprReplaceSubqueries(mp, pexprScalar, phmexprcr);

	phmexprcr->Release();
	pdrgpcrs->Release();
	pdrgpdrgpexprSubqs->Release();

	// build the new join expression
	gpos::owner<COperator *> pop = pexprJoin->Pop();
	pop->AddRef();
	gpos::owner<CExpression *> pexprNewJoin =
		GPOS_NEW(mp) CExpression(mp, pop, std::move(pdrgpexprNewChildren));

	// return a new Select expression
	pop = pexpr->Pop();
	pop->AddRef();
	return GPOS_NEW(mp) CExpression(mp, pop, std::move(pexprNewJoin),
									std::move(pexprNewScalar));
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSubqJoin2Apply::Transform
//
//	@doc:
//		Helper of transformation function
//
//---------------------------------------------------------------------------
void
CXformSubqJoin2Apply::Transform(gpos::pointer<CXformContext *> pxfctxt,
								gpos::pointer<CXformResult *> pxfres,
								gpos::pointer<CExpression *> pexpr,
								BOOL fEnforceCorrelatedApply) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();
	gpos::owner<CExpression *> pexprSelect =
		CXformUtils::PexprSeparateSubqueryPreds(mp, pexpr);

	if (nullptr == pexprSelect)
	{
		// separating predicates failed, probably because the subquery was in the LOJ parts
		return;
	}

	// attempt pushing subqueries to join children,
	// this optimization may not always succeed since unnested subqueries below joins
	// could hide columns needed to evaluate join condition
	gpos::owner<CExpression *> pexprSubqsPushedDown =
		PexprSubqueryPushDown(mp, pexprSelect, fEnforceCorrelatedApply);

	// check if join columns in join condition are still accessible after subquery pushdown
	gpos::pointer<CExpression *> pexprJoin = (*pexprSubqsPushedDown)[0];
	gpos::pointer<CExpression *> pexprJoinCondition =
		(*pexprJoin)[pexprJoin->Arity() - 1];
	gpos::pointer<CColRefSet *> pcrsUsed =
		pexprJoinCondition->DeriveUsedColumns();
	gpos::pointer<CColRefSet *> pcrsJoinOutput =
		pexprJoin->DeriveOutputColumns();
	if (!pcrsJoinOutput->ContainsAll(pcrsUsed))
	{
		// discard expression after subquery push down
		pexprSubqsPushedDown->Release();
		pexprSelect->AddRef();
		pexprSubqsPushedDown = pexprSelect;
	}

	pexprSelect->Release();

	gpos::owner<CExpression *> pexprResult = nullptr;
	BOOL fHasSubquery = (*pexprSubqsPushedDown)[1]->DeriveHasSubquery();
	if (fHasSubquery)
	{
		// unnest subqueries remaining in the top Select expression
		pexprResult = PexprSubqueryUnnest(mp, pexprSubqsPushedDown,
										  fEnforceCorrelatedApply);
		pexprSubqsPushedDown->Release();
	}
	else
	{
		pexprResult = pexprSubqsPushedDown;
	}

	if (nullptr == pexprResult)
	{
		// unnesting failed, return here
		return;
	}

	// normalize resulting expression and add it to xform results container
	gpos::owner<CExpression *> pexprNormalized =
		CNormalizer::PexprNormalize(mp, pexprResult);
	pexprResult->Release();
	pxfres->Add(std::move(pexprNormalized));
}


// EOF
