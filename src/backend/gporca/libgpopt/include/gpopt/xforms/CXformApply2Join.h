//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformApply2Join.h
//
//	@doc:
//		Base class for transforming Apply to Join
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformApply2Join_H
#define GPOPT_CXformApply2Join_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/operators/CLogicalInnerCorrelatedApply.h"
#include "gpopt/operators/CLogicalLeftAntiSemiCorrelatedApply.h"
#include "gpopt/operators/CLogicalLeftOuterCorrelatedApply.h"
#include "gpopt/operators/CLogicalLeftSemiCorrelatedApply.h"
#include "gpopt/operators/CLogicalLeftSemiCorrelatedApplyIn.h"
#include "gpopt/operators/CNormalizer.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPatternTree.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/xforms/CDecorrelator.h"
#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformApply2Join
//
//	@doc:
//		Transform Apply into Join by decorrelating the inner side
//
//---------------------------------------------------------------------------
template <class TApply, class TJoin>
class CXformApply2Join : public CXformExploration
{
private:
	// check if we can create a correlated apply expression from the given expression
	static BOOL
	FCanCreateCorrelatedApply(CMemoryPool *, CExpression *pexprApply)
	{
		GPOS_ASSERT(nullptr != pexprApply);

		COperator::EOperatorId op_id = pexprApply->Pop()->Eopid();

		// consider only Inner/Outer/Left (Anti) Semi Apply here,
		// correlated left anti semi apply (with ALL/NOT-IN semantics) can only be generated by SubqueryHandler
		return COperator::EopLogicalInnerApply == op_id ||
			   COperator::EopLogicalLeftOuterApply == op_id ||
			   COperator::EopLogicalLeftSemiApply == op_id ||
			   COperator::EopLogicalLeftSemiApplyIn == op_id ||
			   COperator::EopLogicalLeftAntiSemiApply == op_id;
	}

	// create correlated apply expression
	static void
	CreateCorrelatedApply(CMemoryPool *mp, CExpression *pexprApply,
						  CXformResult *pxfres)
	{
		if (!FCanCreateCorrelatedApply(mp, pexprApply))
		{
			return;
		}

		CExpression *pexprInner = (*pexprApply)[1];
		CExpression *pexprOuter = (*pexprApply)[0];
		CExpression *pexprScalar = (*pexprApply)[2];

		;
		;
		;
		gpos::Ref<CExpression> pexprResult = nullptr;

		TApply *popApply = gpos::dyn_cast<TApply>(pexprApply->Pop());
		CColRefArray *colref_array = popApply->PdrgPcrInner();
		GPOS_ASSERT(nullptr != colref_array);
		GPOS_ASSERT(1 == colref_array->Size());

		;
		COperator::EOperatorId eopidSubq = popApply->EopidOriginSubq();
		COperator::EOperatorId op_id = pexprApply->Pop()->Eopid();
		switch (op_id)
		{
			case COperator::EopLogicalInnerApply:
				pexprResult =
					CUtils::PexprLogicalApply<CLogicalInnerCorrelatedApply>(
						mp, pexprOuter, pexprInner, colref_array, eopidSubq,
						pexprScalar);
				break;

			case COperator::EopLogicalLeftOuterApply:
				pexprResult =
					CUtils::PexprLogicalApply<CLogicalLeftOuterCorrelatedApply>(
						mp, pexprOuter, pexprInner, colref_array, eopidSubq,
						pexprScalar);
				break;

			case COperator::EopLogicalLeftSemiApply:
				pexprResult =
					CUtils::PexprLogicalApply<CLogicalLeftSemiCorrelatedApply>(
						mp, pexprOuter, pexprInner, colref_array, eopidSubq,
						pexprScalar);
				break;

			case COperator::EopLogicalLeftSemiApplyIn:
				pexprResult = CUtils::PexprLogicalCorrelatedQuantifiedApply<
					CLogicalLeftSemiCorrelatedApplyIn>(mp, pexprOuter,
													   pexprInner, colref_array,
													   eopidSubq, pexprScalar);
				break;

			case COperator::EopLogicalLeftAntiSemiApply:
				pexprResult = CUtils::PexprLogicalApply<
					CLogicalLeftAntiSemiCorrelatedApply>(
					mp, pexprOuter, pexprInner, colref_array, eopidSubq,
					pexprScalar);
				break;

			default:
				GPOS_ASSERT(!"Unexpected Apply operator");
				return;
		}

		pxfres->Add(std::move(pexprResult));
	}

protected:
	// helper function to attempt decorrelating Apply's inner child
	static BOOL
	FDecorrelate(CMemoryPool *mp, CExpression *pexprApply,
				 gpos::Ref<CExpression> *ppexprInner,
				 gpos::Ref<CExpressionArray> *ppdrgpexpr)
	{
		GPOS_ASSERT(nullptr != pexprApply);
		GPOS_ASSERT(nullptr != ppexprInner);
		GPOS_ASSERT(nullptr != ppdrgpexpr);

		*ppdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);

		gpos::Ref<CExpression> pexprPredicateOrig = (*pexprApply)[2];

		// add original predicate to array
		;
		(*ppdrgpexpr)->Append(pexprPredicateOrig);

		// since properties of inner child have been copied from
		// groups that may had subqueries that were decorrelated later, we reset
		// properties here to allow re-computing them during decorrelation
		(*pexprApply)[1]->ResetDerivedProperties();

		// decorrelate inner child
		if (!CDecorrelator::FProcess(
				mp, (*pexprApply)[1], false /*fEqualityOnly*/, ppexprInner,
				*ppdrgpexpr, (*pexprApply)[0]->DeriveOutputColumns()))
		{
			// decorrelation filed
			;
			return false;
		}

		// check for valid semi join correlations
		if ((COperator::EopLogicalLeftSemiJoin == pexprApply->Pop()->Eopid() ||
			 COperator::EopLogicalLeftAntiSemiJoin ==
				 pexprApply->Pop()->Eopid()) &&
			!CPredicateUtils::FValidSemiJoinCorrelations(mp, (*pexprApply)[0],
														 (ppexprInner->get()),
														 (ppdrgpexpr->get())))
		{
			;

			return false;
		}

		return true;
	}

	// helper function to decorrelate apply expression and insert alternative into results container
	static void
	Decorrelate(CXformContext *pxfctxt, CXformResult *pxfres,
				CExpression *pexprApply)
	{
		GPOS_ASSERT(CUtils::HasOuterRefs((*pexprApply)[1]) &&
					"Apply's inner child must have outer references");

		if (CUtils::FHasSubqueryOrApply((*pexprApply)[1]))
		{
			// Subquery/Apply must be unnested before reaching here
			return;
		}

		CMemoryPool *mp = pxfctxt->Pmp();
		gpos::Ref<CExpressionArray> pdrgpexpr = nullptr;
		gpos::Ref<CExpression> pexprInner = nullptr;
		if (!FDecorrelate(mp, pexprApply, &pexprInner, &pdrgpexpr))
		{
			// decorrelation failed, create correlated apply expression if possible
			CreateCorrelatedApply(mp, pexprApply, pxfres);

			return;
		}

		// build substitute
		GPOS_ASSERT(nullptr != pexprInner);
		;
		gpos::Ref<CExpression> pexprOuter = (*pexprApply)[0];
		gpos::Ref<CExpression> pexprPredicate =
			CPredicateUtils::PexprConjunction(mp, std::move(pdrgpexpr));

		gpos::Ref<CExpression> pexprResult = GPOS_NEW(mp)
			CExpression(mp,
						GPOS_NEW(mp) TJoin(mp),	 // join operator
						std::move(pexprOuter), std::move(pexprInner),
						std::move(pexprPredicate));
		gpos::Ref<CExpression> pexprNormalized =
			CNormalizer::PexprNormalize(mp, pexprResult.get());
		;

		// add alternative to results
		pxfres->Add(std::move(pexprNormalized));
	}

	// helper function to create a join expression from an apply expression and insert alternative into results container
	static void
	CreateJoinAlternative(CXformContext *pxfctxt, CXformResult *pxfres,
						  CExpression *pexprApply)
	{
#ifdef GPOS_DEBUG
		CExpressionHandle exprhdl(pxfctxt->Pmp());
		exprhdl.Attach(pexprApply);
		GPOS_ASSERT_IMP(
			CUtils::HasOuterRefs((*pexprApply)[1]),
			!exprhdl.DeriveOuterReferences(1)->ContainsAll(
				exprhdl.DeriveOutputColumns(0)) &&
				"Apply's inner child can only use external columns");
#endif	// GPOS_DEBUG

		CMemoryPool *mp = pxfctxt->Pmp();
		CExpression *pexprOuter = (*pexprApply)[0];
		CExpression *pexprInner = (*pexprApply)[1];
		CExpression *pexprPred = (*pexprApply)[2];
		;
		;
		;
		gpos::Ref<CExpression> pexprResult =
			GPOS_NEW(mp) CExpression(mp,
									 GPOS_NEW(mp) TJoin(mp),  // join operator
									 pexprOuter, pexprInner, pexprPred);

		// add alternative to results
		pxfres->Add(std::move(pexprResult));
	}

public:
	CXformApply2Join(const CXformApply2Join &) = delete;

	// ctor for deep pattern
	explicit CXformApply2Join<TApply, TJoin>(CMemoryPool *mp, BOOL)
		:  // pattern
		  CXformExploration(GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) TApply(mp),
			  GPOS_NEW(mp)
				  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // left child
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternTree(mp)),  // right child
			  GPOS_NEW(mp)
				  CExpression(mp, GPOS_NEW(mp) CPatternTree(mp))  // predicate
			  ))
	{
	}

	// ctor for shallow pattern
	explicit CXformApply2Join<TApply, TJoin>(CMemoryPool *mp)
		:  // pattern
		  CXformExploration(GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) TApply(mp),
			  GPOS_NEW(mp)
				  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // left child
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // right child
			  GPOS_NEW(mp)
				  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))  // predicate
			  ))
	{
	}

	// ctor for passed pattern
	CXformApply2Join<TApply, TJoin>(CMemoryPool *,	// mp
									gpos::Ref<CExpression> pexprPattern)
		: CXformExploration(std::move(pexprPattern))
	{
	}

	// dtor
	~CXformApply2Join<TApply, TJoin>() override = default;

	// is transformation an Apply decorrelation (Apply To Join) xform?
	BOOL
	FApplyDecorrelating() const override
	{
		return true;
	}

};	// class CXformApply2Join

}  // namespace gpopt

#endif	// !GPOPT_CXformApply2Join_H

// EOF
