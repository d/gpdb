//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformSubqueryUnnest.cpp
//
//	@doc:
//		Implementation of subquery unnesting base class
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformSubqueryUnnest.h"

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalGbAgg.h"
#include "gpopt/operators/CLogicalSequenceProject.h"
#include "gpopt/operators/CNormalizer.h"
#include "gpopt/operators/COperator.h"
#include "gpopt/xforms/CSubqueryHandler.h"


using namespace gpmd;
using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformSubqueryUnnest::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//		scalar child must have subquery
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformSubqueryUnnest::Exfp(CExpressionHandle &exprhdl) const
{
	if (exprhdl.DeriveHasSubquery(1))
	{
		return CXform::ExfpHigh;
	}

	return CXform::ExfpNone;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSubqueryUnnest::PexprSubqueryUnnest
//
//	@doc:
//		Helper for unnesting subquery under a given context
//
//---------------------------------------------------------------------------
gpos::owner<CExpression *>
CXformSubqueryUnnest::PexprSubqueryUnnest(CMemoryPool *mp,
										  gpos::pointer<CExpression *> pexpr,
										  BOOL fEnforceCorrelatedApply)
{
	GPOS_ASSERT(nullptr != pexpr);

	if (GPOS_FTRACE(EopttraceEnforceCorrelatedExecution) &&
		!fEnforceCorrelatedApply)
	{
		// if correlated execution is enforced, we cannot generate an expression
		// that does not use correlated Apply
		return nullptr;
	}

	// extract components
	CExpression *pexprOuter = (*pexpr)[0];
	CExpression *pexprScalar = (*pexpr)[1];

	// we add-ref the logical child since the resulting expression must re-use it
	pexprOuter->AddRef();

	gpos::owner<CExpression *> pexprNewOuter = nullptr;
	gpos::owner<CExpression *> pexprResidualScalar = nullptr;

	CSubqueryHandler::ESubqueryCtxt esqctxt = CSubqueryHandler::EsqctxtFilter;

	// calling the handler removes subqueries and sets new logical and scalar expressions
	CSubqueryHandler sh(mp, fEnforceCorrelatedApply);
	if (!sh.FProcess(pexprOuter, pexprScalar, esqctxt, &pexprNewOuter,
					 &pexprResidualScalar))
	{
		CRefCount::SafeRelease(pexprNewOuter);
		CRefCount::SafeRelease(pexprResidualScalar);

		return nullptr;
	}

	// create a new alternative using the new logical and scalar expressions
	gpos::owner<CExpression *> pexprResult = nullptr;
	if (COperator::EopScalarProjectList == pexprScalar->Pop()->Eopid())
	{
		gpos::pointer<CLogicalSequenceProject *> popSeqPrj = nullptr;
		gpos::pointer<CLogicalGbAgg *> popGbAgg = nullptr;
		COperator::EOperatorId op_id = pexpr->Pop()->Eopid();

		switch (op_id)
		{
			case COperator::EopLogicalProject:
				pexprResult = CUtils::PexprLogicalProject(
					mp, pexprNewOuter, pexprResidualScalar,
					false /*fNewComputedCol*/);
				break;

			case COperator::EopLogicalGbAgg:
				popGbAgg = gpos::dyn_cast<CLogicalGbAgg>(pexpr->Pop());
				popGbAgg->Pdrgpcr()->AddRef();
				pexprResult = CUtils::PexprLogicalGbAgg(
					mp, popGbAgg->Pdrgpcr(), pexprNewOuter, pexprResidualScalar,
					popGbAgg->Egbaggtype());
				break;

			case COperator::EopLogicalSequenceProject:
				popSeqPrj =
					gpos::dyn_cast<CLogicalSequenceProject>(pexpr->Pop());
				popSeqPrj->Pds()->AddRef();
				popSeqPrj->Pdrgpos()->AddRef();
				popSeqPrj->Pdrgpwf()->AddRef();
				pexprResult = CUtils::PexprLogicalSequenceProject(
					mp, popSeqPrj->Pds(), popSeqPrj->Pdrgpos(),
					popSeqPrj->Pdrgpwf(), pexprNewOuter, pexprResidualScalar);
				break;

			default:
				GPOS_ASSERT(!"Unnesting subqueries for an invalid operator");
				break;
		}
	}
	else
	{
		pexprResult =
			CUtils::PexprLogicalSelect(mp, pexprNewOuter, pexprResidualScalar);
	}

	// normalize resulting expression
	gpos::owner<CExpression *> pexprNormalized =
		CNormalizer::PexprNormalize(mp, pexprResult);
	pexprResult->Release();

	// pull up projections
	gpos::owner<CExpression *> pexprPullUpProjections =
		CNormalizer::PexprPullUpProjections(mp, pexprNormalized);
	pexprNormalized->Release();

	return pexprPullUpProjections;
}

void
CXformSubqueryUnnest::Transform(gpos::pointer<CXformContext *> pxfctxt,
								gpos::pointer<CXformResult *> pxfres,
								gpos::pointer<CExpression *> pexpr,
								BOOL fEnforceCorrelatedApply) const
{
	CMemoryPool *pmp = pxfctxt->Pmp();

	gpos::owner<CExpression *> pexprAvoidCorrelatedApply =
		PexprSubqueryUnnest(pmp, pexpr, fEnforceCorrelatedApply);
	if (nullptr != pexprAvoidCorrelatedApply)
	{
		// add alternative to results
		pxfres->Add(std::move(pexprAvoidCorrelatedApply));
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CXformSubqueryUnnest::Transform
//
//	@doc:
//		Actual transformation;
//		assumes unary operator, e.g., Select/Project, where the scalar
//		child contains subquery
//
//---------------------------------------------------------------------------
void
CXformSubqueryUnnest::Transform(gpos::pointer<CXformContext *> pxfctxt,
								gpos::pointer<CXformResult *> pxfres,
								gpos::pointer<CExpression *> pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	Transform(pxfctxt, pxfres, pexpr, false /*fEnforceCorrelatedApply*/);
	Transform(pxfctxt, pxfres, pexpr, true /*fEnforceCorrelatedApply*/);
}

// EOF
