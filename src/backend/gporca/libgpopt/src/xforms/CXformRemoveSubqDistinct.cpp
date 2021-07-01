//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 VMware, Inc. or its affiliates.
//
//	@filename:
//		CXformRemoveSubqDistinct.cpp
//
//	@doc:
//		Implementation of the transform that removes distinct clause from subquery
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformRemoveSubqDistinct.h"

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/operators/CLogicalSelect.h"
#include "gpopt/operators/COperator.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/search/CGroupProxy.h"
#include "gpopt/xforms/CXformUtils.h"

using namespace gpopt;

CXformRemoveSubqDistinct::CXformRemoveSubqDistinct(CMemoryPool *mp)
	:  // pattern
	  CXformExploration(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalSelect(mp),
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // relational child
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternTree(mp))  // predicate tree
		  ))
{
}

CXform::EXformPromise
CXformRemoveSubqDistinct::Exfp(CExpressionHandle &exprhdl) const
{
	// consider this transformation only if subqueries exist
	if (!exprhdl.DeriveHasSubquery(1))
	{
		return CXform::ExfpNone;
	}

	CGroupProxy gp((*exprhdl.Pgexpr())[1]);
	gpos::pointer<CGroupExpression *> pexprScalar = gp.PgexprFirst();
	gpos::pointer<COperator *> pop = pexprScalar->Pop();
	if (CUtils::FQuantifiedSubquery(pop) || CUtils::FExistentialSubquery(pop))
	{
		return CXform::ExfpHigh;
	}

	return CXform::ExfpNone;
}

// For quantified subqueries (IN / NOT IN), the following transformation will be applied:
// CLogicalSelect
// |--CLogicalGet "foo"
// +--CScalarSubqueryAny(=)["c" (9)]
//    |--CLogicalGbAgg( Global ) Grp Cols: ["c" (9)]
//    |  |--CLogicalGet "bar"
//    |  +--CScalarProjectList
//    +--CScalarIdent "a" (0)
//
// will produce
//
// CLogicalSelect
// |--CLogicalGet "foo"
// +--CScalarSubqueryAny(=)["c" (9)]
//    |--CLogicalGet "bar"
//    +--CScalarIdent "a" (0)
//
// For existential subqueries (EXISTS / NOT EXISTS), the following transformation will be applied:
// CLogicalSelect
// |--CLogicalGet "foo"
// +--CScalarSubqueryExists
//    +--CLogicalGbAgg( Global ) Grp Cols: ["c" (9)]
//       |--CLogicalGet "bar"
//       +--CScalarProjectList
//
// will produce
//
// CLogicalSelect
// |--CLogicalGet "foo"
// +--CScalarSubqueryExists
//    +--CLogicalGet "bar"
//
void
CXformRemoveSubqDistinct::Transform(gpos::pointer<CXformContext *> pxfctxt,
									gpos::pointer<CXformResult *> pxfres,
									gpos::pointer<CExpression *> pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(nullptr != pxfres);
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();
	gpos::pointer<CExpression *> pexprScalar = (*pexpr)[1];
	gpos::pointer<CExpression *> pexprGbAgg = (*pexprScalar)[0];

	if (COperator::EopLogicalGbAgg == pexprGbAgg->Pop()->Eopid())
	{
		gpos::pointer<CExpression *> pexprGbAggProjectList = (*pexprGbAgg)[1];
		// only consider removing distinct when there is no aggregation functions
		if (0 == pexprGbAggProjectList->Arity())
		{
			gpos::owner<CExpression *> pexprNewScalar = nullptr;
			gpos::owner<CExpression *> pexprRelChild = (*pexprGbAgg)[0];
			pexprRelChild->AddRef();

			gpos::owner<COperator *> pop = pexprScalar->Pop();
			pop->AddRef();
			if (CUtils::FExistentialSubquery(pop))
			{
				// EXIST/NOT EXIST scalar subquery
				pexprNewScalar =
					GPOS_NEW(mp) CExpression(mp, pop, pexprRelChild);
			}
			else
			{
				// IN/NOT IN scalar subquery
				gpos::owner<CExpression *> pexprScalarIdent = (*pexprScalar)[1];
				pexprScalarIdent->AddRef();
				pexprNewScalar = GPOS_NEW(mp)
					CExpression(mp, pop, pexprRelChild, pexprScalarIdent);
			}

			pexpr->Pop()->AddRef();	 // logical select operator
			(*pexpr)[0]->AddRef();	 // relational child of logical select

			// new logical select expression
			gpos::owner<CExpression *> ppexprNew = GPOS_NEW(mp) CExpression(
				mp, pexpr->Pop(), (*pexpr)[0], std::move(pexprNewScalar));
			pxfres->Add(std::move(ppexprNew));
		}
	}
}

// EOF
