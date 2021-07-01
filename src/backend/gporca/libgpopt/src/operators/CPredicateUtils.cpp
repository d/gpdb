//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright 2011 EMC Corp.
//
//	@filename:
//		CPredicateUtils.cpp
//
//	@doc:
//		Implementation of predicate normalization
//---------------------------------------------------------------------------

#include "gpopt/operators/CPredicateUtils.h"

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/base/CCastUtils.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CColRefTable.h"
#include "gpopt/base/CConstraintDisjunction.h"
#include "gpopt/base/CConstraintInterval.h"
#include "gpopt/base/CFunctionProp.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/exception.h"
#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/mdcache/CMDAccessorUtils.h"
#include "gpopt/operators/CLogicalSetOp.h"
#include "gpopt/operators/CNormalizer.h"
#include "gpopt/operators/CPhysicalJoin.h"
#include "gpopt/operators/CScalarCmp.h"
#include "gpopt/operators/CScalarFunc.h"
#include "gpopt/operators/CScalarIdent.h"
#include "naucrates/dxl/gpdb_types.h"
#include "naucrates/md/CMDArrayCoerceCastGPDB.h"
#include "naucrates/md/CMDIdGPDB.h"
#include "naucrates/md/CMDTypeBoolGPDB.h"
#include "naucrates/md/CMDTypeGenericGPDB.h"
#include "naucrates/md/CMDTypeInt2GPDB.h"
#include "naucrates/md/CMDTypeInt4GPDB.h"
#include "naucrates/md/CMDTypeInt8GPDB.h"
#include "naucrates/md/IMDCast.h"
#include "naucrates/md/IMDIndex.h"
#include "naucrates/md/IMDScalarOp.h"
#include "naucrates/md/IMDType.h"

using namespace gpopt;
using namespace gpmd;

// check if the expression is a negated boolean scalar identifier
BOOL
CPredicateUtils::FNegatedBooleanScalarIdent(CExpression *pexprPred)
{
	GPOS_ASSERT(nullptr != pexprPred);

	if (CPredicateUtils::FNot(pexprPred))
	{
		return (FNot(pexprPred) && FBooleanScalarIdent((*pexprPred)[0]));
	}

	return false;
}

// check if the expression is a boolean scalar identifier
BOOL
CPredicateUtils::FBooleanScalarIdent(CExpression *pexprPred)
{
	GPOS_ASSERT(nullptr != pexprPred);

	if (COperator::EopScalarIdent == pexprPred->Pop()->Eopid())
	{
		CScalarIdent *popScIdent =
			gpos::dyn_cast<CScalarIdent>(pexprPred->Pop());
		if (IMDType::EtiBool ==
			popScIdent->Pcr()->RetrieveType()->GetDatumType())
		{
			return true;
		}
	}

	return false;
}

// is the given expression an equality comparison
BOOL
CPredicateUtils::IsEqualityOp(CExpression *pexpr)
{
	return FComparison(pexpr, IMDType::EcmptEq);
}

// is the given expression a comparison
BOOL
CPredicateUtils::FComparison(CExpression *pexpr)
{
	GPOS_ASSERT(nullptr != pexpr);

	return COperator::EopScalarCmp == pexpr->Pop()->Eopid();
}

// is the given expression a comparison of the given type
BOOL
CPredicateUtils::FComparison(CExpression *pexpr, IMDType::ECmpType cmp_type)
{
	GPOS_ASSERT(nullptr != pexpr);

	COperator *pop = pexpr->Pop();

	if (COperator::EopScalarCmp != pop->Eopid())
	{
		return false;
	}

	CScalarCmp *popScCmp = gpos::dyn_cast<CScalarCmp>(pop);
	GPOS_ASSERT(nullptr != popScCmp);

	return cmp_type == popScCmp->ParseCmpType();
}

// Is the given expression a comparison over the given column. A comparison
// can only be between the given column and an expression involving only
// the allowed columns. If the allowed columns set is NULL, then we only want
// constant comparisons.
BOOL
CPredicateUtils::FComparison(
	CExpression *pexpr, CColRef *colref,
	CColRefSet
		*pcrsAllowedRefs  // other column references allowed in the comparison
)
{
	GPOS_ASSERT(nullptr != pexpr);

	if (COperator::EopScalarCmp != pexpr->Pop()->Eopid())
	{
		return false;
	}

	CExpression *pexprLeft = (*pexpr)[0];
	CExpression *pexprRight = (*pexpr)[1];

	if (CUtils::FScalarIdent(pexprLeft, colref) ||
		CScalarIdent::FCastedScId(pexprLeft, colref) ||
		CScalarIdent::FAllowedFuncScId(pexprLeft, colref))
	{
		return FValidRefsOnly(pexprRight, pcrsAllowedRefs);
	}

	if (CUtils::FScalarIdent(pexprRight, colref) ||
		CScalarIdent::FCastedScId(pexprRight, colref) ||
		CScalarIdent::FAllowedFuncScId(pexprRight, colref))
	{
		return FValidRefsOnly(pexprLeft, pcrsAllowedRefs);
	}

	return false;
}

// Is the given expression a range comparison only between the given column and
// an expression involving only the allowed columns. If the allowed columns set
// is NULL, then we only want constant comparisons.
// Also, the comparison type must be one of: LT, GT, LEq, GEq, Eq
// NEq is allowed only when requested by the caller
BOOL
CPredicateUtils::FRangeComparison(
	CExpression *pexpr, CColRef *colref,
	CColRefSet
		*pcrsAllowedRefs,  // other column references allowed in the comparison
	BOOL allowNotEqualPreds)
{
	if (!FComparison(pexpr, colref, pcrsAllowedRefs))
	{
		return false;
	}
	IMDType::ECmpType cmp_type =
		gpos::dyn_cast<CScalarCmp>(pexpr->Pop())->ParseCmpType();
	return (IMDType::EcmptOther != cmp_type &&
			(allowNotEqualPreds || IMDType::EcmptNEq != cmp_type));
}

BOOL
CPredicateUtils::FIdentCompareOuterRefExprIgnoreCast(
	CExpression *pexpr,
	CColRefSet
		*pcrsOuterRefs,	 // other column references allowed in the comparison
	CColRef **localColRef)
{
	GPOS_ASSERT(nullptr != pexpr);

	if (COperator::EopScalarCmp != pexpr->Pop()->Eopid() ||
		nullptr == pcrsOuterRefs)
	{
		return false;
	}

	CExpression *pexprLeft = (*pexpr)[0];
	CExpression *pexprRight = (*pexpr)[1];

	BOOL leftIsACol = CUtils::FScalarIdentIgnoreCast(pexprLeft);
	BOOL rightIsACol = CUtils::FScalarIdentIgnoreCast(pexprRight);
	CColRefSet *pcrsUsedLeft = pexprLeft->DeriveUsedColumns();
	CColRefSet *pcrsUsedRight = pexprRight->DeriveUsedColumns();

	// allow any expressions of the form
	//  col = expr(outer refs)
	//  expr(outer refs) = col
	BOOL colOpOuterrefExpr =
		(leftIsACol && !pcrsOuterRefs->FIntersects(pcrsUsedLeft) &&
		 pcrsOuterRefs->ContainsAll(pcrsUsedRight));
	BOOL outerRefExprOpCol =
		(rightIsACol && !pcrsOuterRefs->FIntersects(pcrsUsedRight) &&
		 pcrsOuterRefs->ContainsAll(pcrsUsedLeft));

	if (nullptr != localColRef)
	{
		if (colOpOuterrefExpr)
		{
			GPOS_ASSERT(pcrsUsedLeft->Size() == 1);
			*localColRef = pcrsUsedLeft->PcrFirst();
		}
		else if (outerRefExprOpCol)
		{
			GPOS_ASSERT(pcrsUsedRight->Size() == 1);
			*localColRef = pcrsUsedRight->PcrFirst();
		}
		else
		{
			// return value with be false, initialize the variable to be nice
			*localColRef = nullptr;
		}
	}

	return colOpOuterrefExpr || outerRefExprOpCol;
}

// Check whether the given expression contains references to only the given
// columns. If pcrsAllowedRefs is NULL, then check whether the expression has
// no column references and no volatile functions
BOOL
CPredicateUtils::FValidRefsOnly(CExpression *pexprScalar,
								CColRefSet *pcrsAllowedRefs)
{
	if (nullptr != pcrsAllowedRefs)
	{
		return pcrsAllowedRefs->ContainsAll(pexprScalar->DeriveUsedColumns());
	}

	return CUtils::FVarFreeExpr(pexprScalar) &&
		   IMDFunction::EfsVolatile !=
			   pexprScalar->DeriveScalarFunctionProperties()->Efs();
}



// is the given expression a conjunction of equality comparisons
BOOL
CPredicateUtils::FConjunctionOfEqComparisons(CMemoryPool *mp,
											 CExpression *pexpr)
{
	GPOS_ASSERT(nullptr != pexpr);

	if (IsEqualityOp(pexpr))
	{
		return true;
	}

	gpos::Ref<CExpressionArray> pdrgpexpr = PdrgpexprConjuncts(mp, pexpr);
	const ULONG ulConjuncts = pdrgpexpr->Size();

	for (ULONG ul = 0; ul < ulConjuncts; ul++)
	{
		if (!IsEqualityOp((*pexpr)[ul]))
		{
			;
			return false;
		}
	}

	;
	return true;
}

// does the given expression have any NOT children?
BOOL
CPredicateUtils::FHasNegatedChild(CExpression *pexpr)
{
	GPOS_ASSERT(nullptr != pexpr);

	const ULONG arity = pexpr->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		if (FNot((*pexpr)[ul]))
		{
			return true;
		}
	}

	return false;
}

// recursively collect conjuncts
void
CPredicateUtils::CollectConjuncts(CExpression *pexpr,
								  CExpressionArray *pdrgpexpr)
{
	GPOS_CHECK_STACK_SIZE;

	if (FAnd(pexpr))
	{
		const ULONG arity = pexpr->Arity();
		for (ULONG ul = 0; ul < arity; ul++)
		{
			CollectConjuncts((*pexpr)[ul], pdrgpexpr);
		}
	}
	else
	{
		;
		pdrgpexpr->Append(pexpr);
	}
}

// recursively collect disjuncts
void
CPredicateUtils::CollectDisjuncts(CExpression *pexpr,
								  CExpressionArray *pdrgpexpr)
{
	GPOS_CHECK_STACK_SIZE;

	if (FOr(pexpr))
	{
		const ULONG arity = pexpr->Arity();
		for (ULONG ul = 0; ul < arity; ul++)
		{
			CollectDisjuncts((*pexpr)[ul], pdrgpexpr);
		}
	}
	else
	{
		;
		pdrgpexpr->Append(pexpr);
	}
}

// extract conjuncts from a predicate
gpos::Ref<CExpressionArray>
CPredicateUtils::PdrgpexprConjuncts(CMemoryPool *mp, CExpression *pexpr)
{
	gpos::Ref<CExpressionArray> pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	CollectConjuncts(pexpr, pdrgpexpr.get());

	return pdrgpexpr;
}

// extract disjuncts from a predicate
gpos::Ref<CExpressionArray>
CPredicateUtils::PdrgpexprDisjuncts(CMemoryPool *mp, CExpression *pexpr)
{
	gpos::Ref<CExpressionArray> pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	CollectDisjuncts(pexpr, pdrgpexpr.get());

	return pdrgpexpr;
}

// This function expects an array of disjuncts (children of OR operator),
// the function expands disjuncts in the given array by converting
// ArrayComparison to AND/OR tree and deduplicating resulting disjuncts
gpos::Ref<CExpressionArray>
CPredicateUtils::PdrgpexprExpandDisjuncts(CMemoryPool *mp,
										  CExpressionArray *pdrgpexprDisjuncts)
{
	GPOS_ASSERT(nullptr != pdrgpexprDisjuncts);

	gpos::Ref<CExpressionArray> pdrgpexprExpanded =
		GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG size = pdrgpexprDisjuncts->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		CExpression *pexpr = (*pdrgpexprDisjuncts)[ul];
		if (COperator::EopScalarArrayCmp == pexpr->Pop()->Eopid())
		{
			gpos::Ref<CExpression> pexprExpanded =
				CScalarArrayCmp::PexprExpand(mp, pexpr);
			if (FOr(pexprExpanded.get()))
			{
				gpos::Ref<CExpressionArray> pdrgpexprArrayCmpDisjuncts =
					PdrgpexprDisjuncts(mp, pexprExpanded.get());
				CUtils::AddRefAppend(pdrgpexprExpanded.get(),
									 pdrgpexprArrayCmpDisjuncts.get());
				;
				;
			}
			else
			{
				pdrgpexprExpanded->Append(pexprExpanded);
			}

			continue;
		}

		if (FAnd(pexpr))
		{
			gpos::Ref<CExpressionArray> pdrgpexprConjuncts =
				PdrgpexprConjuncts(mp, pexpr);
			gpos::Ref<CExpressionArray> pdrgpexprExpandedConjuncts =
				PdrgpexprExpandConjuncts(mp, pdrgpexprConjuncts.get());
			;
			pdrgpexprExpanded->Append(
				PexprConjunction(mp, pdrgpexprExpandedConjuncts));

			continue;
		}

		;
		pdrgpexprExpanded->Append(pexpr);
	}

	gpos::Ref<CExpressionArray> pdrgpexprResult =
		CUtils::PdrgpexprDedup(mp, pdrgpexprExpanded.get());
	;

	return pdrgpexprResult;
}

// This function expects an array of conjuncts (children of AND operator),
// the function expands conjuncts in the given array by converting
// ArrayComparison to AND/OR tree and deduplicating resulting conjuncts
gpos::Ref<CExpressionArray>
CPredicateUtils::PdrgpexprExpandConjuncts(CMemoryPool *mp,
										  CExpressionArray *pdrgpexprConjuncts)
{
	GPOS_ASSERT(nullptr != pdrgpexprConjuncts);

	gpos::Ref<CExpressionArray> pdrgpexprExpanded =
		GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG size = pdrgpexprConjuncts->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		CExpression *pexpr = (*pdrgpexprConjuncts)[ul];
		if (COperator::EopScalarArrayCmp == pexpr->Pop()->Eopid())
		{
			gpos::Ref<CExpression> pexprExpanded =
				CScalarArrayCmp::PexprExpand(mp, pexpr);
			if (FAnd(pexprExpanded.get()))
			{
				gpos::Ref<CExpressionArray> pdrgpexprArrayCmpConjuncts =
					PdrgpexprConjuncts(mp, pexprExpanded.get());
				CUtils::AddRefAppend(pdrgpexprExpanded.get(),
									 pdrgpexprArrayCmpConjuncts.get());
				;
				;
			}
			else
			{
				pdrgpexprExpanded->Append(pexprExpanded);
			}

			continue;
		}

		if (FOr(pexpr))
		{
			gpos::Ref<CExpressionArray> pdrgpexprDisjuncts =
				PdrgpexprDisjuncts(mp, pexpr);
			gpos::Ref<CExpressionArray> pdrgpexprExpandedDisjuncts =
				PdrgpexprExpandDisjuncts(mp, pdrgpexprDisjuncts.get());
			;
			pdrgpexprExpanded->Append(
				PexprDisjunction(mp, pdrgpexprExpandedDisjuncts));

			continue;
		}

		;
		pdrgpexprExpanded->Append(pexpr);
	}

	gpos::Ref<CExpressionArray> pdrgpexprResult =
		CUtils::PdrgpexprDedup(mp, pdrgpexprExpanded.get());
	;

	return pdrgpexprResult;
}

// check if a conjunct/disjunct can be skipped
BOOL
CPredicateUtils::FSkippable(CExpression *pexpr, BOOL fConjunction)
{
	return ((fConjunction && CUtils::FScalarConstTrue(pexpr)) ||
			(!fConjunction && CUtils::FScalarConstFalse(pexpr)));
}

// check if a conjunction/disjunction can be reduced to a constant
// True/False based on the given conjunct/disjunct
BOOL
CPredicateUtils::FReducible(CExpression *pexpr, BOOL fConjunction)
{
	return ((fConjunction && CUtils::FScalarConstFalse(pexpr)) ||
			(!fConjunction && CUtils::FScalarConstTrue(pexpr)));
}

// reverse the given operator type, for example > => <, <= => >=
IMDType::ECmpType
CPredicateUtils::EcmptReverse(IMDType::ECmpType cmp_type)
{
	GPOS_ASSERT(IMDType::EcmptOther > cmp_type);

	IMDType::ECmpType rgrgecmpt[][2] = {{IMDType::EcmptEq, IMDType::EcmptEq},
										{IMDType::EcmptG, IMDType::EcmptL},
										{IMDType::EcmptGEq, IMDType::EcmptLEq},
										{IMDType::EcmptNEq, IMDType::EcmptNEq}};

	const ULONG size = GPOS_ARRAY_SIZE(rgrgecmpt);

	for (ULONG ul = 0; ul < size; ul++)
	{
		IMDType::ECmpType *pecmpt = rgrgecmpt[ul];

		if (pecmpt[0] == cmp_type)
		{
			return pecmpt[1];
		}

		if (pecmpt[1] == cmp_type)
		{
			return pecmpt[0];
		}
	}

	GPOS_ASSERT(!"Comparison does not have a reverse");

	return IMDType::EcmptOther;
}

// is the condition a LIKE predicate
BOOL
CPredicateUtils::FLikePredicate(IMDId *mdid)
{
	GPOS_ASSERT(nullptr != mdid);

	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDScalarOp *md_scalar_op = md_accessor->RetrieveScOp(mdid);

	const CWStringConst *str_opname = md_scalar_op->Mdname().GetMDName();

	// comparison semantics for statistics purposes is looser
	// than regular comparison
	CWStringConst pstrLike(GPOS_WSZ_LIT("~~"));
	if (!str_opname->Equals(&pstrLike))
	{
		return false;
	}

	return true;
}

// is the condition a LIKE predicate
BOOL
CPredicateUtils::FLikePredicate(CExpression *pexpr)
{
	COperator *pop = pexpr->Pop();
	if (COperator::EopScalarCmp != pop->Eopid())
	{
		return false;
	}

	CScalarCmp *popScCmp = gpos::dyn_cast<CScalarCmp>(pop);
	IMDId *mdid = popScCmp->MdIdOp();

	return FLikePredicate(mdid);
}

// extract the components of a LIKE predicate
void
CPredicateUtils::ExtractLikePredComponents(CExpression *pexprPred,
										   CExpression **ppexprScIdent,
										   CExpression **ppexprConst)
{
	GPOS_ASSERT(nullptr != pexprPred);
	GPOS_ASSERT(2 == pexprPred->Arity());
	GPOS_ASSERT(FLikePredicate(pexprPred));

	CExpression *pexprLeft = (*pexprPred)[0];
	CExpression *pexprRight = (*pexprPred)[1];

	*ppexprScIdent = nullptr;
	*ppexprConst = nullptr;

	CExpression *pexprLeftNoCast = pexprLeft;
	CExpression *pexprRightNoCast = pexprRight;

	if (COperator::EopScalarCast == pexprLeft->Pop()->Eopid())
	{
		pexprLeftNoCast = (*pexprLeft)[0];
	}

	if (COperator::EopScalarCast == pexprRight->Pop()->Eopid())
	{
		pexprRightNoCast = (*pexprRight)[0];
	}

	if (COperator::EopScalarIdent == pexprLeftNoCast->Pop()->Eopid())
	{
		*ppexprScIdent = pexprLeftNoCast;
	}
	else if (COperator::EopScalarIdent == pexprRightNoCast->Pop()->Eopid())
	{
		*ppexprScIdent = pexprRightNoCast;
	}

	if (COperator::EopScalarConst == pexprLeftNoCast->Pop()->Eopid())
	{
		*ppexprConst = pexprLeftNoCast;
	}
	else if (COperator::EopScalarConst == pexprRightNoCast->Pop()->Eopid())
	{
		*ppexprConst = pexprRightNoCast;
	}
}

// extract components in a comparison expression on the given key
void
CPredicateUtils::ExtractComponents(CExpression *pexprScCmp, CColRef *pcrKey,
								   CExpression **ppexprKey,
								   CExpression **ppexprOther,
								   IMDType::ECmpType *pecmpt)
{
	GPOS_ASSERT(nullptr != pexprScCmp);
	GPOS_ASSERT(nullptr != pcrKey);
	GPOS_ASSERT(FComparison(pexprScCmp));

	*ppexprKey = nullptr;
	*ppexprOther = nullptr;

	CExpression *pexprLeft = (*pexprScCmp)[0];
	CExpression *pexprRight = (*pexprScCmp)[1];

	IMDType::ECmpType cmp_type =
		gpos::dyn_cast<CScalarCmp>(pexprScCmp->Pop())->ParseCmpType();

	if (CUtils::FScalarIdent(pexprLeft, pcrKey) ||
		CScalarIdent::FCastedScId(pexprLeft, pcrKey) ||
		CScalarIdent::FAllowedFuncScId(pexprLeft, pcrKey))
	{
		*ppexprKey = pexprLeft;
		*ppexprOther = pexprRight;
		*pecmpt = cmp_type;
	}
	else if (CUtils::FScalarIdent(pexprRight, pcrKey) ||
			 CScalarIdent::FCastedScId(pexprRight, pcrKey) ||
			 CScalarIdent::FAllowedFuncScId(pexprRight, pcrKey))
	{
		*ppexprKey = pexprRight;
		*ppexprOther = pexprLeft;
		*pecmpt = EcmptReverse(cmp_type);
	}
	GPOS_ASSERT(nullptr != *ppexprKey && nullptr != *ppexprOther);
}

// Expression is a comparison with a simple identifer on at least one side
BOOL
CPredicateUtils::FIdentCompare(CExpression *pexpr, IMDType::ECmpType pecmpt,
							   CColRef *colref)
{
	GPOS_ASSERT(nullptr != pexpr);
	GPOS_ASSERT(nullptr != colref);

	if (!FComparison(pexpr, pecmpt))
	{
		return false;
	}

	CExpression *pexprLeft = (*pexpr)[0];
	CExpression *pexprRight = (*pexpr)[1];

	if (CUtils::FScalarIdent(pexprLeft, colref) ||
		CCastUtils::FBinaryCoercibleCastedScId(pexprLeft, colref))
	{
		return true;
	}
	else if (CUtils::FScalarIdent(pexprRight, colref) ||
			 CCastUtils::FBinaryCoercibleCastedScId(pexprRight, colref))
	{
		return true;
	}

	return false;
}

// create conjunction/disjunction from array of components; Takes ownership over the given array of expressions
gpos::Ref<CExpression>
CPredicateUtils::PexprConjDisj(CMemoryPool *mp,
							   gpos::Ref<CExpressionArray> pdrgpexpr,
							   BOOL fConjunction)
{
	CScalarBoolOp::EBoolOperator eboolop = CScalarBoolOp::EboolopAnd;
	if (!fConjunction)
	{
		eboolop = CScalarBoolOp::EboolopOr;
	}

	gpos::Ref<CExpressionArray> pdrgpexprFinal =
		GPOS_NEW(mp) CExpressionArray(mp);
	ULONG size = 0;
	if (nullptr != pdrgpexpr)
	{
		size = pdrgpexpr->Size();
	}

	for (ULONG ul = 0; ul < size; ul++)
	{
		CExpression *pexpr = (*pdrgpexpr)[ul];

		if (FSkippable(pexpr, fConjunction))
		{
			// skip current conjunct/disjunct
			continue;
		}

		if (FReducible(pexpr, fConjunction))
		{
			// a False (True) conjunct (disjunct) yields the whole conjunction (disjunction) False (True)
			;
			;

			return CUtils::PexprScalarConstBool(mp, !fConjunction /*fValue*/);
		}

		// add conjunct/disjunct to result array
		;
		pdrgpexprFinal->Append(pexpr);
	};

	// assemble result
	gpos::Ref<CExpression> pexprResult = nullptr;
	if (nullptr != pdrgpexprFinal && (0 < pdrgpexprFinal->Size()))
	{
		if (1 == pdrgpexprFinal->Size())
		{
			pexprResult = (*pdrgpexprFinal)[0];
			;
			;

			return pexprResult;
		}

		return CUtils::PexprScalarBoolOp(mp, eboolop,
										 std::move(pdrgpexprFinal));
	}

	pexprResult = CUtils::PexprScalarConstBool(mp, fConjunction /*fValue*/);
	;

	return pexprResult;
}

// create conjunction from array of components;
gpos::Ref<CExpression>
CPredicateUtils::PexprConjunction(CMemoryPool *mp,
								  gpos::Ref<CExpressionArray> pdrgpexpr)
{
	return PexprConjDisj(mp, std::move(pdrgpexpr), true /*fConjunction*/);
}

// create disjunction from array of components;
gpos::Ref<CExpression>
CPredicateUtils::PexprDisjunction(CMemoryPool *mp,
								  gpos::Ref<CExpressionArray> pdrgpexpr)
{
	return PexprConjDisj(mp, std::move(pdrgpexpr), false /*fConjunction*/);
}

// create a conjunction/disjunction of two components; Does *not* take ownership over given expressions
gpos::Ref<CExpression>
CPredicateUtils::PexprConjDisj(CMemoryPool *mp, CExpression *pexprOne,
							   CExpression *pexprTwo, BOOL fConjunction)
{
	GPOS_ASSERT(nullptr != pexprOne);
	GPOS_ASSERT(nullptr != pexprTwo);

	if (pexprOne == pexprTwo)
	{
		;
		return pexprOne;
	}

	gpos::Ref<CExpressionArray> pdrgpexprOne = nullptr;
	gpos::Ref<CExpressionArray> pdrgpexprTwo = nullptr;

	if (fConjunction)
	{
		pdrgpexprOne = PdrgpexprConjuncts(mp, pexprOne);
		pdrgpexprTwo = PdrgpexprConjuncts(mp, pexprTwo);
	}
	else
	{
		pdrgpexprOne = PdrgpexprDisjuncts(mp, pexprOne);
		pdrgpexprTwo = PdrgpexprDisjuncts(mp, pexprTwo);
	}

	gpos::Ref<CExpressionArray> pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	CUtils::AddRefAppend(pdrgpexpr.get(), pdrgpexprOne.get());
	CUtils::AddRefAppend(pdrgpexpr.get(), pdrgpexprTwo.get());

	;
	;

	return PexprConjDisj(mp, std::move(pdrgpexpr), fConjunction);
}

// create a conjunction of two components;
gpos::Ref<CExpression>
CPredicateUtils::PexprConjunction(CMemoryPool *mp, CExpression *pexprOne,
								  CExpression *pexprTwo)
{
	return PexprConjDisj(mp, pexprOne, pexprTwo, true /*fConjunction*/);
}

// create a disjunction of two components;
gpos::Ref<CExpression>
CPredicateUtils::PexprDisjunction(CMemoryPool *mp, CExpression *pexprOne,
								  CExpression *pexprTwo)
{
	return PexprConjDisj(mp, pexprOne, pexprTwo, false /*fConjunction*/);
}

// extract equality predicates over scalar identifiers
gpos::Ref<CExpressionArray>
CPredicateUtils::PdrgpexprPlainEqualities(CMemoryPool *mp,
										  CExpressionArray *pdrgpexpr)
{
	gpos::Ref<CExpressionArray> pdrgpexprEqualities =
		GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG arity = pdrgpexpr->Size();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprCurr = (*pdrgpexpr)[ul];

		if (FPlainEquality(pexprCurr))
		{
			;
			pdrgpexprEqualities->Append(pexprCurr);
		}
	}
	return pdrgpexprEqualities;
}

// is an expression an equality over scalar identifiers
BOOL
CPredicateUtils::FPlainEquality(CExpression *pexpr)
{
	if (IsEqualityOp(pexpr))
	{
		CExpression *pexprLeft = (*pexpr)[0];
		CExpression *pexprRight = (*pexpr)[1];

		// check if the scalar condition is over scalar idents
		if (COperator::EopScalarIdent == pexprLeft->Pop()->Eopid() &&
			COperator::EopScalarIdent == pexprRight->Pop()->Eopid())
		{
			return true;
		}
	}
	return false;
}

// is an expression a self comparison on some column
BOOL
CPredicateUtils::FSelfComparison(CExpression *pexpr, IMDType::ECmpType *pecmpt)
{
	GPOS_ASSERT(nullptr != pexpr);
	GPOS_ASSERT(nullptr != pecmpt);

	*pecmpt = IMDType::EcmptOther;
	COperator *pop = pexpr->Pop();
	if (CUtils::FScalarCmp(pexpr))
	{
		*pecmpt = gpos::dyn_cast<CScalarCmp>(pop)->ParseCmpType();
		COperator *popLeft = (*pexpr)[0]->Pop();
		COperator *popRight = (*pexpr)[1]->Pop();

		// return true if comparison is over the same column, and that column
		// is not nullable
		if (COperator::EopScalarIdent != popLeft->Eopid() ||
			COperator::EopScalarIdent != popRight->Eopid() ||
			gpos::dyn_cast<CScalarIdent>(popLeft)->Pcr() !=
				gpos::dyn_cast<CScalarIdent>(popRight)->Pcr())
		{
			return false;
		}

		CColRef *colref =
			const_cast<CColRef *>(gpos::dyn_cast<CScalarIdent>(popLeft)->Pcr());

		return CColRef::EcrtTable == colref->Ecrt() &&
			   !CColRefTable::PcrConvert(colref)->IsNullable();
	}

	return false;
}

// eliminate self comparison and replace it with True or False if possible
gpos::Ref<CExpression>
CPredicateUtils::PexprEliminateSelfComparison(CMemoryPool *mp,
											  CExpression *pexpr)
{
	GPOS_ASSERT(pexpr->Pop()->FScalar());

	;
	gpos::Ref<CExpression> pexprNew = pexpr;
	IMDType::ECmpType cmp_type = IMDType::EcmptOther;
	if (FSelfComparison(pexpr, &cmp_type))
	{
		switch (cmp_type)
		{
			case IMDType::EcmptEq:
			case IMDType::EcmptLEq:
			case IMDType::EcmptGEq:;
				pexprNew = CUtils::PexprScalarConstBool(mp, true /*value*/);
				break;

			case IMDType::EcmptNEq:
			case IMDType::EcmptL:
			case IMDType::EcmptG:
			case IMDType::EcmptIDF:;
				pexprNew = CUtils::PexprScalarConstBool(mp, false /*value*/);
				break;

			default:
				break;
		}
	}

	return pexprNew;
}

// is the given expression in the form (col1 Is NOT DISTINCT FROM col2)
BOOL
CPredicateUtils::FINDFScalarIdents(CExpression *pexpr)
{
	if (!FNot(pexpr))
	{
		return false;
	}

	CExpression *pexprChild = (*pexpr)[0];
	if (COperator::EopScalarIsDistinctFrom != pexprChild->Pop()->Eopid())
	{
		return false;
	}

	CExpression *pexprOuter = (*pexprChild)[0];
	CExpression *pexprInner = (*pexprChild)[1];

	return (COperator::EopScalarIdent == pexprOuter->Pop()->Eopid() &&
			COperator::EopScalarIdent == pexprInner->Pop()->Eopid());
}

// is the given expression in the form (col1 Is DISTINCT FROM col2)
BOOL
CPredicateUtils::FIDFScalarIdents(CExpression *pexpr)
{
	if (COperator::EopScalarIsDistinctFrom != pexpr->Pop()->Eopid())
	{
		return false;
	}

	CExpression *pexprOuter = (*pexpr)[0];
	CExpression *pexprInner = (*pexpr)[1];

	return (COperator::EopScalarIdent == pexprOuter->Pop()->Eopid() &&
			COperator::EopScalarIdent == pexprInner->Pop()->Eopid());
}

// is the given expression in the form 'expr IS DISTINCT FROM false)'
BOOL
CPredicateUtils::FIDFFalse(CExpression *pexpr)
{
	if (COperator::EopScalarIsDistinctFrom != pexpr->Pop()->Eopid())
	{
		return false;
	}

	return CUtils::FScalarConstFalse((*pexpr)[0]) ||
		   CUtils::FScalarConstFalse((*pexpr)[1]);
}

// is the given expression in the form (expr IS DISTINCT FROM expr)
BOOL
CPredicateUtils::FIDF(CExpression *pexpr)
{
	return (COperator::EopScalarIsDistinctFrom == pexpr->Pop()->Eopid());
}

// is the given expression in the form (expr Is NOT DISTINCT FROM expr)
BOOL
CPredicateUtils::FINDF(CExpression *pexpr)
{
	return (FNot(pexpr) && FIDF((*pexpr)[0]));
}

// generate a conjunction of INDF expressions between corresponding columns in the given arrays
gpos::Ref<CExpression>
CPredicateUtils::PexprINDFConjunction(CMemoryPool *mp,
									  CColRefArray *pdrgpcrFirst,
									  CColRefArray *pdrgpcrSecond)
{
	GPOS_ASSERT(nullptr != pdrgpcrFirst);
	GPOS_ASSERT(nullptr != pdrgpcrSecond);
	GPOS_ASSERT(pdrgpcrFirst->Size() == pdrgpcrSecond->Size());
	GPOS_ASSERT(0 < pdrgpcrFirst->Size());

	const ULONG num_cols = pdrgpcrFirst->Size();
	gpos::Ref<CExpressionArray> pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	for (ULONG ul = 0; ul < num_cols; ul++)
	{
		pdrgpexpr->Append(
			CUtils::PexprINDF(mp, (*pdrgpcrFirst)[ul], (*pdrgpcrSecond)[ul]));
	}

	return PexprConjunction(mp, std::move(pdrgpexpr));
}

// is the given expression a comparison between a scalar ident and a constant
BOOL
CPredicateUtils::FCompareIdentToConst(CExpression *pexpr)
{
	COperator *pop = pexpr->Pop();

	if (COperator::EopScalarCmp != pop->Eopid())
	{
		return false;
	}

	CExpression *pexprLeft = (*pexpr)[0];
	CExpression *pexprRight = (*pexpr)[1];

	// left side must be scalar ident

	if (!(CUtils::FScalarIdent(pexprLeft) ||
		  CCastUtils::FBinaryCoercibleCastedScId(pexprLeft)))
	{
		return false;
	}

	// right side must be a constant
	if (!(CUtils::FScalarConst(pexprRight) ||
		  CCastUtils::FBinaryCoercibleCastedConst(pexprRight)))
	{
		return false;
	}

	return true;
}

// is the given expression of the form (col IS DISTINCT FROM const)
BOOL
CPredicateUtils::FIdentIDFConst(CExpression *pexpr)
{
	COperator *pop = pexpr->Pop();

	if (COperator::EopScalarIsDistinctFrom != pop->Eopid())
	{
		return false;
	}

	CExpression *pexprLeft = (*pexpr)[0];
	CExpression *pexprRight = (*pexpr)[1];

	// left side must be scalar ident
	if (COperator::EopScalarIdent != pexprLeft->Pop()->Eopid())
	{
		return false;
	}

	// right side must be a constant
	if (COperator::EopScalarConst != pexprRight->Pop()->Eopid())
	{
		return false;
	}

	return true;
}

// is the given expression of the form (col = col)
BOOL
CPredicateUtils::FEqIdentsOfSameType(CExpression *pexpr)
{
	if (!CPredicateUtils::IsEqualityOp(pexpr))
	{
		return false;
	}
	CExpression *pexprLeft = (*pexpr)[0];
	CExpression *pexprRight = (*pexpr)[1];

	// left side must be scalar ident
	if (COperator::EopScalarIdent != pexprLeft->Pop()->Eopid())
	{
		return false;
	}

	// right side must be a scalar ident
	if (COperator::EopScalarIdent != pexprRight->Pop()->Eopid())
	{
		return false;
	}

	CScalarIdent *left_ident = gpos::dyn_cast<CScalarIdent>(pexprLeft->Pop());
	CScalarIdent *right_ident = gpos::dyn_cast<CScalarIdent>(pexprRight->Pop());
	if (!left_ident->MdidType()->Equals(right_ident->MdidType()))
	{
		return false;
	}

	return true;
}


// is the given expression is of the form (col IS DISTINCT FROM const)
// ignoring cast on either sides
BOOL
CPredicateUtils::FIdentIDFConstIgnoreCast(CExpression *pexpr)
{
	return FIdentCompareConstIgnoreCast(pexpr,
										COperator::EopScalarIsDistinctFrom);
}

// is the given expression of the form (col cmp constant) ignoring casting on either sides
BOOL
CPredicateUtils::FIdentCompareConstIgnoreCast(CExpression *pexpr,
											  COperator::EOperatorId op_id)
{
	COperator *pop = pexpr->Pop();

	if (op_id != pop->Eopid())
	{
		return false;
	}

	CExpression *pexprLeft = (*pexpr)[0];
	CExpression *pexprRight = (*pexpr)[1];

	// col <op> const
	if (COperator::EopScalarIdent == pexprLeft->Pop()->Eopid() &&
		COperator::EopScalarConst == pexprRight->Pop()->Eopid())
	{
		return true;
	}

	// cast(col) <op> const
	if (CScalarIdent::FCastedScId(pexprLeft) &&
		COperator::EopScalarConst == pexprRight->Pop()->Eopid())
	{
		return true;
	}

	// col <op> cast(constant)
	if (COperator::EopScalarIdent == pexprLeft->Pop()->Eopid() &&
		CScalarConst::FCastedConst(pexprRight))
	{
		return true;
	}

	// cast(col) <op> cast(constant)
	if (CScalarIdent::FCastedScId(pexprLeft) &&
		CScalarConst::FCastedConst(pexprRight))
	{
		return true;
	}

	return false;
}

// is the given expression a comparison between a const and a const
BOOL
CPredicateUtils::FCompareConstToConstIgnoreCast(CExpression *pexpr)
{
	COperator *pop = pexpr->Pop();

	if (COperator::EopScalarCmp != pop->Eopid())
	{
		return false;
	}

	CExpression *pexprLeft = (*pexpr)[0];
	CExpression *pexprRight = (*pexpr)[1];

	// left side must be scalar const

	if (!(CUtils::FScalarConst(pexprLeft) ||
		  CCastUtils::FBinaryCoercibleCastedConst(pexprLeft)))
	{
		return false;
	}

	// right side must be a constant
	if (!(CUtils::FScalarConst(pexprRight) ||
		  CCastUtils::FBinaryCoercibleCastedConst(pexprRight)))
	{
		return false;
	}

	return true;
}

// is the given expression an array comparison between scalar ident
// and a const array or a constant
BOOL
CPredicateUtils::FArrayCompareIdentToConstIgnoreCast(CExpression *pexpr)
{
	if (!CUtils::FScalarArrayCmp(pexpr))
	{
		return false;
	}

	CExpression *pexprLeft = (*pexpr)[0];
	CExpression *pexprRight = (*pexpr)[1];

	if (CUtils::FScalarIdentIgnoreCast(pexprLeft))
	{
		if ((CUtils::FScalarArray(pexprRight) ||
			 CUtils::FScalarArrayCoerce(pexprRight)))
		{
			CExpression *pexprArray = CUtils::PexprScalarArrayChild(pexpr);
			return CUtils::FScalarConstArray(pexprArray);
		}
		return CUtils::FScalarConst(pexprRight) ||
			   CScalarConst::FCastedConst(pexprRight);
	}

	return false;
}

// is the given expression of the form NOT (col IS DISTINCT FROM const) ignoring cast on either sides
BOOL
CPredicateUtils::FIdentINDFConstIgnoreCast(CExpression *pexpr)
{
	if (!FNot(pexpr))
	{
		return false;
	}

	return FIdentCompareConstIgnoreCast((*pexpr)[0],
										COperator::EopScalarIsDistinctFrom);
}

// is the given expression a comparison between a scalar ident under a scalar cast and a constant array
// +--CScalarArrayCmp Any (=)
// |--CScalarCast
// |  +--CScalarIdent
// +--CScalarConstArray:
BOOL
CPredicateUtils::FCompareCastIdentToConstArray(CExpression *pexpr)
{
	GPOS_ASSERT(nullptr != pexpr);

	if (CUtils::FScalarArrayCmp(pexpr) &&
		(CCastUtils::FBinaryCoercibleCast((*pexpr)[0]) &&
		 CUtils::FScalarIdent((*(*pexpr)[0])[0])))
	{
		CExpression *pexprArray = CUtils::PexprScalarArrayChild(pexpr);
		return CUtils::FScalarConstArray(pexprArray);
	}

	return false;
}

// is the given expression a comparison between a scalar ident and an array with constants or ScalarIdents
BOOL
CPredicateUtils::FCompareScalarIdentToConstAndScalarIdentArray(
	CExpression *pexpr)
{
	GPOS_ASSERT(nullptr != pexpr);

	if (!CUtils::FScalarArrayCmp(pexpr) || !CUtils::FScalarIdent((*pexpr)[0]) ||
		!CUtils::FScalarArray((*pexpr)[1]))
	{
		return false;
	}

	CExpression *pexprArray = CUtils::PexprScalarArrayChild(pexpr);
	return CUtils::FScalarConstAndScalarIdentArray(pexprArray);
}

// is the given expression a comparison between a scalar ident and a constant array
BOOL
CPredicateUtils::FCompareIdentToConstArray(CExpression *pexpr)
{
	GPOS_ASSERT(nullptr != pexpr);

	if (!CUtils::FScalarArrayCmp(pexpr) || !CUtils::FScalarIdent((*pexpr)[0]) ||
		!CUtils::FScalarArray((*pexpr)[1]))
	{
		return false;
	}

	CExpression *pexprArray = CUtils::PexprScalarArrayChild(pexpr);
	return CUtils::FScalarConstArray(pexprArray);
}

gpos::Ref<CExpression>
CPredicateUtils::ValidatePartPruningExpr(CMemoryPool *mp, CExpression *expr,
										 CColRef *pcrPartKey,
										 CColRefSet *pcrsAllowedRefs,
										 BOOL allow_not_equals_preds)
{
	GPOS_ASSERT(nullptr != expr);

	if (expr->DeriveScalarFunctionProperties()->NeedsSingletonExecution() ||
		COperator::EopScalarCmp != expr->Pop()->Eopid())
	{
		return nullptr;
	}

	CScalarCmp *sc_cmp = gpos::dyn_cast<CScalarCmp>(expr->Pop());

	if (!allow_not_equals_preds && sc_cmp->ParseCmpType() == IMDType::EcmptNEq)
	{
		return nullptr;
	}

	CExpression *expr_left = (*expr)[0];
	CExpression *expr_right = (*expr)[1];

	if ((CUtils::FScalarIdent(expr_left, pcrPartKey) ||
		 CCastUtils::FBinaryCoercibleCastedScId(expr_left, pcrPartKey)) &&
		FValidRefsOnly(expr_right, pcrsAllowedRefs))
	{
		;
		return expr;
	}

	if ((CUtils::FScalarIdent(expr_right, pcrPartKey) ||
		 CCastUtils::FBinaryCoercibleCastedScId(expr_right, pcrPartKey)) &&
		FValidRefsOnly(expr_left, pcrsAllowedRefs))
	{
		gpos::Ref<COperator> commuted_op = sc_cmp->PopCommutedOp(mp);
		;
		;
		gpos::Ref<CExpression> swapped_expr = GPOS_NEW(mp)
			CExpression(mp, std::move(commuted_op), expr_right, expr_left);
		return swapped_expr;
	}

	return nullptr;
}

// Find a predicate that can be used for partition pruning with the given
// part key in the array of expressions if one exists. Relevant predicates
// are those that compare the partition key to expressions involving only
// the allowed columns. If the allowed columns set is NULL, then we only want
// constant comparisons.
gpos::Ref<CExpression>
CPredicateUtils::PexprPartPruningPredicate(
	CMemoryPool *mp, const CExpressionArray *pdrgpexpr, CColRef *pcrPartKey,
	CExpression *pexprCol,	// predicate on pcrPartKey obtained from pcnstr
	CColRefSet *pcrsAllowedRefs, BOOL allow_not_equals_preds)
{
	gpos::Ref<CExpressionArray> pdrgpexprResult =
		GPOS_NEW(mp) CExpressionArray(mp);

	// Assert that pexprCol is an expr on pcrPartKey only and no other colref
	GPOS_ASSERT(pexprCol == nullptr || CUtils::FScalarConstTrue(pexprCol) ||
				(pexprCol->DeriveUsedColumns()->Size() == 1 &&
				 pexprCol->DeriveUsedColumns()->PcrFirst() == pcrPartKey));

	for (ULONG ul = 0; ul < pdrgpexpr->Size(); ul++)
	{
		CExpression *pexpr = (*pdrgpexpr)[ul];

		if (nullptr != pcrsAllowedRefs)
		{
			gpos::Ref<CExpression> canonical_expr = ValidatePartPruningExpr(
				mp, pexpr, pcrPartKey, pcrsAllowedRefs, allow_not_equals_preds);

			if (nullptr != canonical_expr)
			{
				pdrgpexprResult->Append(canonical_expr);
			}

			// pexprCol contains a predicate only on partKey, which is useless for
			// dynamic partition selection, so ignore it here
			pexprCol = nullptr;
		}
		else
		{
			// This may look like dead code, but it is actually used in
			// CLogicalSelect::PexprPredPart().
			// GPDB_12_MERGE_FIXME: Remove this & CLogicalSelect::PexprPredPart()

			// (NULL == pcrsAllowedRefs) implies static partition elimination, since
			// the expressions we select can only contain the partition key
			// If EopttraceAllowGeneralPredicatesforDPE is set, allow a larger set
			// of partition predicates for DPE as well (see note above).

			if (FBoolPredicateOnColumn(pexpr, pcrPartKey) ||
				FNullCheckOnColumn(pexpr, pcrPartKey) ||
				IsDisjunctionOfRangeComparison(mp, pexpr, pcrPartKey,
											   pcrsAllowedRefs,
											   allow_not_equals_preds) ||
				(FRangeComparison(pexpr, pcrPartKey, pcrsAllowedRefs,
								  allow_not_equals_preds) &&
				 !pexpr->DeriveScalarFunctionProperties()
					  ->NeedsSingletonExecution()))
			{
				;
				pdrgpexprResult->Append(pexpr);
			}
		}
	}

	// Remove any redundant "IS NOT NULL" filter on the partition key that was derived
	// from contraints
	if (pexprCol != nullptr &&
		CPredicateUtils::FNotNullCheckOnColumn(pexprCol, pcrPartKey) &&
		(pdrgpexprResult->Size() > 0 &&
		 ExprsContainsOnlyStrictComparisons(pdrgpexprResult.get())))
	{
#ifdef GPOS_DEBUG
		gpos::Ref<CColRefSet> pcrsUsed =
			CUtils::PcrsExtractColumns(mp, pdrgpexprResult.get());
		GPOS_ASSERT_IMP(pdrgpexprResult->Size() > 0,
						pcrsUsed->FMember(pcrPartKey));
		;
#endif
		// pexprCol is a redundent "IS NOT NULL" expr. Ignore it
		pexprCol = nullptr;
	}

	// Finally, remove duplicate expressions
	gpos::Ref<CExpressionArray> pdrgpexprResultNew =
		PdrgpexprAppendConjunctsDedup(mp, pdrgpexprResult.get(), pexprCol);
	;
	pdrgpexprResult = pdrgpexprResultNew;

	if (0 == pdrgpexprResult->Size())
	{
		;
		return nullptr;
	}

	return PexprConjunction(mp, std::move(pdrgpexprResult));
}

// append the conjuncts from the given expression to the given array, removing
// any duplicates, and return the resulting array
gpos::Ref<CExpressionArray>
CPredicateUtils::PdrgpexprAppendConjunctsDedup(CMemoryPool *mp,
											   CExpressionArray *pdrgpexpr,
											   CExpression *pexpr)
{
	GPOS_ASSERT(nullptr != pdrgpexpr);

	if (nullptr == pexpr)
	{
		;
		return pdrgpexpr;
	}

	gpos::Ref<CExpressionArray> pdrgpexprConjuncts =
		PdrgpexprConjuncts(mp, pexpr);
	CUtils::AddRefAppend(pdrgpexprConjuncts.get(), pdrgpexpr);

	gpos::Ref<CExpressionArray> pdrgpexprNew =
		CUtils::PdrgpexprDedup(mp, pdrgpexprConjuncts.get());
	;
	return pdrgpexprNew;
}

// check if the given expression is a boolean expression on the
// given column, e.g. if its of the form "ScalarIdent(colref)" or "Not(ScalarIdent(colref))"
BOOL
CPredicateUtils::FBoolPredicateOnColumn(CExpression *pexpr, CColRef *colref)
{
	BOOL fBoolean =
		(IMDType::EtiBool == colref->RetrieveType()->GetDatumType());

	if (fBoolean &&
		(CUtils::FScalarIdent(pexpr, colref) ||
		 (FNot(pexpr) && CUtils::FScalarIdent((*pexpr)[0], colref))))
	{
		return true;
	}

	return false;
}

// check if the given expression is a null check on the given column
// i.e. "is null" or "is not null"
BOOL
CPredicateUtils::FNullCheckOnColumn(CExpression *pexpr, CColRef *colref)
{
	GPOS_ASSERT(nullptr != pexpr);
	GPOS_ASSERT(nullptr != colref);

	CExpression *pexprIsNull = pexpr;
	if (FNot(pexpr))
	{
		pexprIsNull = (*pexpr)[0];
	}

	if (CUtils::FScalarNullTest(pexprIsNull))
	{
		CExpression *pexprChild = (*pexprIsNull)[0];
		return (CUtils::FScalarIdent(pexprChild, colref) ||
				CCastUtils::FBinaryCoercibleCastedScId(pexprChild, colref));
	}

	return false;
}

// check if the given expression of the form "col is not null"
BOOL
CPredicateUtils::FNotNullCheckOnColumn(CExpression *pexpr, CColRef *colref)
{
	GPOS_ASSERT(nullptr != pexpr);
	GPOS_ASSERT(nullptr != colref);

	if (0 == pexpr->Arity())
		return false;

	return (FNullCheckOnColumn(pexpr, colref) && FNot(pexpr));
}


// check if the given expression is a scalar array cmp expression on the
// given column
BOOL
CPredicateUtils::FScArrayCmpOnColumn(CExpression *pexpr, CColRef *colref,
									 BOOL fConstOnly)
{
	GPOS_ASSERT(nullptr != pexpr);

	if (COperator::EopScalarArrayCmp != pexpr->Pop()->Eopid())
	{
		return false;
	}

	CExpression *pexprLeft = (*pexpr)[0];
	CExpression *pexprRight = (*pexpr)[1];

	if (!CUtils::FScalarIdent(pexprLeft, colref) ||
		!CUtils::FScalarArray(pexprRight))
	{
		return false;
	}

	const ULONG ulArrayElems = pexprRight->Arity();

	BOOL fSupported = true;
	for (ULONG ul = 0; ul < ulArrayElems && fSupported; ul++)
	{
		CExpression *pexprArrayElem = (*pexprRight)[ul];
		if (fConstOnly && !CUtils::FScalarConst(pexprArrayElem))
		{
			fSupported = false;
		}
	}

	return fSupported;
}

// check if the given expression is a disjunction of scalar cmp expression
// on the given column
BOOL
CPredicateUtils::IsDisjunctionOfRangeComparison(CMemoryPool *mp,
												CExpression *pexpr,
												CColRef *colref,
												CColRefSet *pcrsAllowedRefs,
												BOOL allowNotEqualPreds)
{
	if (!FOr(pexpr))
	{
		return false;
	}

	gpos::Ref<CExpressionArray> pdrgpexprDisjuncts =
		PdrgpexprDisjuncts(mp, pexpr);
	const ULONG ulDisjuncts = pdrgpexprDisjuncts->Size();
	for (ULONG ulDisj = 0; ulDisj < ulDisjuncts; ulDisj++)
	{
		CExpression *pexprDisj = (*pdrgpexprDisjuncts)[ulDisj].get();
		if (!FRangeComparison(pexprDisj, colref, pcrsAllowedRefs,
							  allowNotEqualPreds))
		{
			;
			return false;
		}
	}

	;
	return true;
}

// extract interesting expressions involving the partitioning keys;
// the function Add-Refs the returned copy if not null. Relevant predicates
// are those that compare the partition keys to expressions involving only
// the allowed columns. If the allowed columns set is NULL, then we only want
// constant filters.
gpos::Ref<CExpression>
CPredicateUtils::PexprExtractPredicatesOnPartKeys(
	CMemoryPool *mp, CExpression *pexprScalar,
	CColRef2dArray *pdrgpdrgpcrPartKeys, CColRefSet *pcrsAllowedRefs,
	BOOL fUseConstraints, const IMDRelation *pmdrel)
{
	GPOS_ASSERT(nullptr != pdrgpdrgpcrPartKeys);
	if (GPOS_FTRACE(EopttraceDisablePartSelection))
	{
		return nullptr;
	}

	gpos::Ref<CExpressionArray> pdrgpexprConjuncts =
		PdrgpexprConjuncts(mp, pexprScalar);
	gpos::Ref<CColRefSetArray> pdrgpcrsChild = nullptr;
	gpos::Ref<CConstraint> pcnstr = nullptr;
	if (pexprScalar->DeriveHasScalarArrayCmp() &&
		!GPOS_FTRACE(EopttraceArrayConstraints))
	{
		// if we have any Array Comparisons, we expand them into conjunctions/disjunctions
		// of comparison predicates and then reconstruct scalar expression. This is because the
		// DXL translator for partitions would not previously handle array statements
		gpos::Ref<CExpressionArray> pdrgpexprExpandedConjuncts =
			PdrgpexprExpandConjuncts(mp, pdrgpexprConjuncts.get());
		;
		gpos::Ref<CExpression> pexprExpandedScalar =
			PexprConjunction(mp, pdrgpexprExpandedConjuncts);

		// this will no longer contain array statements
		pdrgpexprConjuncts = PdrgpexprConjuncts(mp, pexprExpandedScalar.get());

		pcnstr = CConstraint::PcnstrFromScalarExpr(
			mp, pexprExpandedScalar.get(), &pdrgpcrsChild);
		;
	}
	else
	{
		// skip this step when
		// 1. there are any Array comparisons
		// 2. previously, we expanded array expressions. However, there is now code to handle array
		// constraints in the DXL translator and therefore, it is unnecessary work to expand arrays
		// into disjunctions
		pcnstr =
			CConstraint::PcnstrFromScalarExpr(mp, pexprScalar, &pdrgpcrsChild);
	};


	// check if expanded scalar leads to a contradiction in computed constraint
	BOOL fContradiction = (nullptr != pcnstr && pcnstr->FContradiction());
	if (fContradiction)
	{
		;
		;

		return nullptr;
	}

	const ULONG ulLevels = pdrgpdrgpcrPartKeys->Size();
	gpos::Ref<CExpressionArray> pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	for (ULONG ul = 0; ul < ulLevels; ul++)
	{
		CColRef *colref = CUtils::PcrExtractPartKey(pdrgpdrgpcrPartKeys, ul);
		// extract part type for this level
		BOOL isKnownToBeListPartitioned = false;
		if (pmdrel != nullptr)
		{
			CHAR szPartType = pmdrel->PartTypeAtLevel(ul);
			// we want to allow NEq predicates for partition selection only for
			// list partitioned tables. We avoid this for ranged partitioned tables
			// since NEq predicates can hardly eliminate any range partitions
			isKnownToBeListPartitioned =
				(IMDRelation::ErelpartitionList == szPartType);
		}

		gpos::Ref<CExpression> pexprCol =
			PexprPredicateCol(mp, pcnstr.get(), colref, fUseConstraints);

		// look for a filter on the part key
		gpos::Ref<CExpression> pexprCmp = PexprPartPruningPredicate(
			mp, pdrgpexprConjuncts.get(), colref, pexprCol, pcrsAllowedRefs,
			isKnownToBeListPartitioned /* allowNotEqualPreds */);
		;
		GPOS_ASSERT_IMP(
			nullptr != pexprCmp &&
				COperator::EopScalarCmp == pexprCmp->Pop()->Eopid(),
			IMDType::EcmptOther !=
				gpos::dyn_cast<CScalarCmp>(pexprCmp->Pop())->ParseCmpType());

		if (nullptr != pexprCmp && !CUtils::FScalarConstTrue(pexprCmp.get()))
		{
			// include comparison predicate if it is non-trivial
			;
			pdrgpexpr->Append(pexprCmp);
		};
	}

	;
	;

	if (0 == pdrgpexpr->Size())
	{
		;
		return nullptr;
	}

	return PexprConjunction(mp, std::move(pdrgpexpr));
}

// extract the constraint on the given column and return the corresponding scalar expression
CExpression *
CPredicateUtils::PexprPredicateCol(CMemoryPool *mp, CConstraint *pcnstr,
								   CColRef *colref, BOOL fUseConstraints)
{
	if (nullptr == pcnstr || !fUseConstraints)
	{
		return nullptr;
	}

	CExpression *pexprCol = nullptr;
	gpos::Ref<CConstraint> pcnstrCol = pcnstr->Pcnstr(mp, colref);
	if (nullptr != pcnstrCol && !pcnstrCol->IsConstraintUnbounded())
	{
		pexprCol = pcnstrCol->PexprScalar(mp);
		;
	}

	;

	return pexprCol;
}

// checks if comparison is between two columns, or a column and a const
BOOL
CPredicateUtils::FCompareColToConstOrCol(CExpression *pexprScalar)
{
	if (!CUtils::FScalarCmp(pexprScalar))
	{
		return false;
	}

	CExpression *pexprLeft = (*pexprScalar)[0];
	CExpression *pexprRight = (*pexprScalar)[1];

	BOOL fColLeft = CUtils::FScalarIdent(pexprLeft);
	BOOL fColRight = CUtils::FScalarIdent(pexprRight);
	BOOL fConstLeft = CUtils::FScalarConst(pexprLeft);
	BOOL fConstRight = CUtils::FScalarConst(pexprRight);

	return (fColLeft && fColRight) || (fColLeft && fConstRight) ||
		   (fConstLeft && fColRight);
}

// checks if the given constraint specifies a constant column
BOOL
CPredicateUtils::FConstColumn(CConstraint *pcnstr, const CColRef *
#ifdef GPOS_DEBUG
													   colref
#endif	// GPOS_DEBUG
)
{
	if (nullptr == pcnstr || CConstraint::EctInterval != pcnstr->Ect())
	{
		// no constraint on column or constraint is not an interval
		return false;
	}

	GPOS_ASSERT(pcnstr->FConstraint(colref));

	CConstraintInterval *pcnstrInterval =
		dynamic_cast<CConstraintInterval *>(pcnstr);
	CRangeArray *pdrgprng = pcnstrInterval->Pdrgprng();
	if (1 < pdrgprng->Size())
	{
		return false;
	}

	if (0 == pdrgprng->Size())
	{
		return pcnstrInterval->FIncludesNull();
	}

	GPOS_ASSERT(1 == pdrgprng->Size());

	const CRange *prng = (*pdrgprng)[0].get();

	return prng->FPoint() && !pcnstrInterval->FIncludesNull();
}

// checks if the given constraint specifies a set of constants for a column
BOOL
CPredicateUtils::FColumnDisjunctionOfConst(CConstraint *pcnstr,
										   const CColRef *colref)
{
	if (FConstColumn(pcnstr, colref))
	{
		return true;
	}

	if (nullptr == pcnstr || CConstraint::EctInterval != pcnstr->Ect())
	{
		// no constraint on column or constraint is not an interval
		return false;
	}

	GPOS_ASSERT(pcnstr->FConstraint(colref));

	GPOS_ASSERT(CConstraint::EctInterval == pcnstr->Ect());

	CConstraintInterval *pcnstrInterval =
		dynamic_cast<CConstraintInterval *>(pcnstr);
	return FColumnDisjunctionOfConst(pcnstrInterval, colref);
}

// checks if the given constraint specifies a set of constants for a column
BOOL
CPredicateUtils::FColumnDisjunctionOfConst(CConstraintInterval *pcnstrInterval,
										   const CColRef *
#ifdef GPOS_DEBUG
											   colref
#endif
)
{
	GPOS_ASSERT(pcnstrInterval->FConstraint(colref));

	CRangeArray *pdrgprng = pcnstrInterval->Pdrgprng();

	if (0 == pdrgprng->Size())
	{
		return pcnstrInterval->FIncludesNull();
	}

	GPOS_ASSERT(0 < pdrgprng->Size());

	const ULONG ulRanges = pdrgprng->Size();

	for (ULONG ul = 0; ul < ulRanges; ul++)
	{
		CRange *prng = (*pdrgprng)[ul].get();
		if (!prng->FPoint())
		{
			return false;
		}
	}

	return true;
}

// helper to create index lookup comparison predicate with index key on left side
gpos::Ref<CExpression>
CPredicateUtils::PexprIndexLookupKeyOnLeft(CMemoryPool *mp,
										   CMDAccessor *md_accessor,
										   CExpression *pexprScalar,
										   const IMDIndex *pmdindex,
										   CColRefArray *pdrgpcrIndex,
										   CColRefSet *outer_refs)
{
	GPOS_ASSERT(nullptr != pexprScalar);

	CExpression *pexprLeft = (*pexprScalar)[0];
	CExpression *pexprRight = (*pexprScalar)[1];

	gpos::Ref<CColRefSet> pcrsIndex = GPOS_NEW(mp) CColRefSet(mp, pdrgpcrIndex);

	if ((CUtils::FScalarIdent(pexprLeft) &&
		 pcrsIndex->FMember(
			 gpos::dyn_cast<CScalarIdent>(pexprLeft->Pop())->Pcr())) ||
		(CCastUtils::FBinaryCoercibleCastedScId(pexprLeft) &&
		 pcrsIndex->FMember(
			 gpos::dyn_cast<CScalarIdent>((*pexprLeft)[0]->Pop())->Pcr())))
	{
		// left expression is a scalar identifier or casted scalar identifier on an index key
		CColRefSet *pcrsUsedRight = pexprRight->DeriveUsedColumns();
		BOOL fSuccess = true;

		if (0 < pcrsUsedRight->Size())
		{
			if (!pcrsUsedRight->IsDisjoint(pcrsIndex.get()))
			{
				// right argument uses index key, cannot use predicate for index lookup
				fSuccess = false;
			}
			else if (nullptr != outer_refs)
			{
				gpos::Ref<CColRefSet> pcrsOuterRefsRight =
					GPOS_NEW(mp) CColRefSet(mp, *pcrsUsedRight);
				pcrsOuterRefsRight->Difference(pcrsIndex.get());
				fSuccess = outer_refs->ContainsAll(pcrsOuterRefsRight.get());
				;
			}
		}

		fSuccess =
			(fSuccess && FCompatibleIndexPredicate(pexprScalar, pmdindex,
												   pdrgpcrIndex, md_accessor));

		if (fSuccess)
		{
			;
			;
			return pexprScalar;
		}
	}

	;
	return nullptr;
}

// helper to create index lookup comparison predicate with index key on right side
gpos::Ref<CExpression>
CPredicateUtils::PexprIndexLookupKeyOnRight(CMemoryPool *mp,
											CMDAccessor *md_accessor,
											CExpression *pexprScalar,
											const IMDIndex *pmdindex,
											CColRefArray *pdrgpcrIndex,
											CColRefSet *outer_refs)
{
	GPOS_ASSERT(nullptr != pexprScalar);

	CExpression *pexprLeft = (*pexprScalar)[0];
	CExpression *pexprRight = (*pexprScalar)[1];
	if (CUtils::FScalarCmp(pexprScalar))
	{
		CScalarCmp *popScCmp = gpos::dyn_cast<CScalarCmp>(pexprScalar->Pop());
		gpos::Ref<CScalarCmp> popScCmpCommute = popScCmp->PopCommutedOp(mp);

		if (popScCmpCommute)
		{
			// build new comparison after switching arguments and using commutative comparison operator
			;
			;
			gpos::Ref<CExpression> pexprCommuted = GPOS_NEW(mp) CExpression(
				mp, std::move(popScCmpCommute), pexprRight, pexprLeft);
			gpos::Ref<CExpression> pexprIndexCond =
				PexprIndexLookupKeyOnLeft(mp, md_accessor, pexprCommuted.get(),
										  pmdindex, pdrgpcrIndex, outer_refs);
			;

			return pexprIndexCond;
		}
	}

	return nullptr;
}

// Check if given expression is a valid index lookup predicate, and
// return modified (as needed) expression to be used for index lookup,
// a scalar expression is a valid index lookup predicate if it is in one
// the two forms:
//	[index-key CMP expr]
//	[expr CMP index-key]
// where expr is a scalar expression that is free of index keys and
// may have outer references (in the case of index nested loops)
gpos::Ref<CExpression>
CPredicateUtils::PexprIndexLookup(CMemoryPool *mp, CMDAccessor *md_accessor,
								  CExpression *pexprScalar,
								  const IMDIndex *pmdindex,
								  CColRefArray *pdrgpcrIndex,
								  CColRefSet *outer_refs,
								  BOOL allowArrayCmpForBTreeIndexes)
{
	GPOS_ASSERT(nullptr != pexprScalar);
	GPOS_ASSERT(nullptr != pdrgpcrIndex);

	IMDType::ECmpType cmptype = IMDType::EcmptOther;

	if (CUtils::FScalarCmp(pexprScalar))
	{
		cmptype =
			gpos::dyn_cast<CScalarCmp>(pexprScalar->Pop())->ParseCmpType();
	}
	else if (CUtils::FScalarArrayCmp(pexprScalar) &&
			 (IMDIndex::EmdindBitmap == pmdindex->IndexType() ||
			  (allowArrayCmpForBTreeIndexes &&
			   IMDIndex::EmdindBtree == pmdindex->IndexType())))
	{
		// array cmps are always allowed on bitmap indexes and when requested on btree indexes
		cmptype = CUtils::ParseCmpType(
			gpos::dyn_cast<CScalarArrayCmp>(pexprScalar->Pop())->MdIdOp());
	}

	BOOL gin_or_gist_or_brin = (pmdindex->IndexType() == IMDIndex::EmdindGist ||
								pmdindex->IndexType() == IMDIndex::EmdindGin ||
								pmdindex->IndexType() == IMDIndex::EmdindBrin);

	if (cmptype == IMDType::EcmptNEq || cmptype == IMDType::EcmptIDF ||
		(cmptype == IMDType::EcmptOther &&
		 !gin_or_gist_or_brin) ||  // only GIN/GiST/BRIN indexes with a comparison type other are ok
		(gin_or_gist_or_brin &&
		 pexprScalar->Arity() <
			 2))  // we do not support unary index expressions for GIN/GiST/BRIN indexes
	{
		return nullptr;
	}

	gpos::Ref<CExpression> pexprIndexLookupKeyOnLeft =
		PexprIndexLookupKeyOnLeft(mp, md_accessor, pexprScalar, pmdindex,
								  pdrgpcrIndex, outer_refs);
	if (nullptr != pexprIndexLookupKeyOnLeft)
	{
		return pexprIndexLookupKeyOnLeft;
	}

	gpos::Ref<CExpression> pexprIndexLookupKeyOnRight =
		PexprIndexLookupKeyOnRight(mp, md_accessor, pexprScalar, pmdindex,
								   pdrgpcrIndex, outer_refs);
	if (nullptr != pexprIndexLookupKeyOnRight)
	{
		return pexprIndexLookupKeyOnRight;
	}

	return nullptr;
}

// split predicates into those that refer to an index key, and those that don't
void
CPredicateUtils::ExtractIndexPredicates(
	CMemoryPool *mp, CMDAccessor *md_accessor,
	CExpressionArray *pdrgpexprPredicate, const IMDIndex *pmdindex,
	CColRefArray *pdrgpcrIndex, CExpressionArray *pdrgpexprIndex,
	CExpressionArray *pdrgpexprResidual,
	CColRefSet *
		pcrsAcceptedOuterRefs,	// outer refs that are acceptable in an index predicate
	BOOL allowArrayCmpForBTreeIndexes)
{
	const ULONG length = pdrgpexprPredicate->Size();

	gpos::Ref<CColRefSet> pcrsIndex = GPOS_NEW(mp) CColRefSet(mp, pdrgpcrIndex);

	for (ULONG ul = 0; ul < length; ul++)
	{
		gpos::Ref<CExpression> pexprCond = (*pdrgpexprPredicate)[ul];

		;

		gpos::Ref<CColRefSet> pcrsUsed =
			GPOS_NEW(mp) CColRefSet(mp, *pexprCond->DeriveUsedColumns());
		if (nullptr != pcrsAcceptedOuterRefs)
		{
			// filter out all accepted outer references
			pcrsUsed->Difference(pcrsAcceptedOuterRefs);
		}

		BOOL fSubset =
			(0 < pcrsUsed->Size()) && (pcrsIndex->ContainsAll(pcrsUsed.get()));
		;

		if (!fSubset)
		{
			pdrgpexprResidual->Append(pexprCond);
			continue;
		}

		CExpressionArray *pdrgpexprTarget = pdrgpexprIndex;

		if (CUtils::FScalarIdentBoolType(pexprCond.get()))
		{
			// expression is a column identifier of boolean type: convert to "col = true"
			pexprCond = CUtils::PexprScalarEqCmp(
				mp, pexprCond,
				CUtils::PexprScalarConstBool(mp, true /*value*/,
											 false /*is_null*/));
		}
		else if (FNot(pexprCond.get()) &&
				 CUtils::FScalarIdentBoolType((*pexprCond)[0]))
		{
			// expression is of the form "not(col) for a column identifier of boolean type: convert to "col = false"
			CExpression *pexprScId = (*pexprCond)[0];
			;
			;
			pexprCond = CUtils::PexprScalarEqCmp(
				mp, pexprScId,
				CUtils::PexprScalarConstBool(mp, false /*value*/,
											 false /*is_null*/));
		}
		else
		{
			// attempt building index lookup predicate
			gpos::Ref<CExpression> pexprLookupPred = PexprIndexLookup(
				mp, md_accessor, pexprCond.get(), pmdindex, pdrgpcrIndex,
				pcrsAcceptedOuterRefs, allowArrayCmpForBTreeIndexes);
			if (nullptr != pexprLookupPred)
			{
				;
				pexprCond = pexprLookupPred;
			}
			else
			{
				// not a supported predicate
				pdrgpexprTarget = pdrgpexprResidual;
			}
		}

		pdrgpexprTarget->Append(pexprCond);
	}

	;
}

// split given scalar expression into two conjunctions; without outer
// references and with outer references
void
CPredicateUtils::SeparateOuterRefs(CMemoryPool *mp, CExpression *pexprScalar,
								   CColRefSet *outer_refs,
								   gpos::Ref<CExpression> *ppexprLocal,
								   gpos::Ref<CExpression> *ppexprOuterRef)
{
	GPOS_ASSERT(nullptr != pexprScalar);
	GPOS_ASSERT(nullptr != outer_refs);
	GPOS_ASSERT(nullptr != ppexprLocal);
	GPOS_ASSERT(nullptr != ppexprOuterRef);

	CColRefSet *pcrsUsed = pexprScalar->DeriveUsedColumns();
	if (pcrsUsed->IsDisjoint(outer_refs))
	{
		// if used columns are disjoint from outer references, return input expression
		;
		*ppexprLocal = pexprScalar;
		*ppexprOuterRef = CUtils::PexprScalarConstBool(mp, true /*fval*/);
		return;
	}

	if (COperator::EopScalarNAryJoinPredList == pexprScalar->Pop()->Eopid())
	{
		// for a ScalarNAryJoinPredList we have to preserve that operator and
		// separate the outer refs from each of its children
		gpos::Ref<CExpressionArray> localChildren =
			GPOS_NEW(mp) CExpressionArray(mp);
		gpos::Ref<CExpressionArray> outerRefChildren =
			GPOS_NEW(mp) CExpressionArray(mp);

		for (ULONG c = 0; c < pexprScalar->Arity(); c++)
		{
			gpos::Ref<CExpression> childLocalExpr = nullptr;
			gpos::Ref<CExpression> childOuterRefExpr = nullptr;

			SeparateOuterRefs(mp, (*pexprScalar)[c], outer_refs,
							  &childLocalExpr, &childOuterRefExpr);
			localChildren->Append(childLocalExpr);
			outerRefChildren->Append(childOuterRefExpr);
		}

		// reassemble the CScalarNAryJoinPredList with its new children without outer refs
		;
		*ppexprLocal = GPOS_NEW(mp)
			CExpression(mp, pexprScalar->Pop(), std::move(localChildren));

		// do the same with the outer refs
		;
		*ppexprOuterRef = GPOS_NEW(mp)
			CExpression(mp, pexprScalar->Pop(), std::move(outerRefChildren));

		return;
	}

	gpos::Ref<CExpressionArray> pdrgpexpr = PdrgpexprConjuncts(mp, pexprScalar);
	gpos::Ref<CExpressionArray> pdrgpexprLocal =
		GPOS_NEW(mp) CExpressionArray(mp);
	gpos::Ref<CExpressionArray> pdrgpexprOuterRefs =
		GPOS_NEW(mp) CExpressionArray(mp);

	const ULONG size = pdrgpexpr->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		CExpression *pexprPred = (*pdrgpexpr)[ul];
		CColRefSet *pcrsPredUsed = pexprPred->DeriveUsedColumns();
		;
		if (0 == pcrsPredUsed->Size() || outer_refs->IsDisjoint(pcrsPredUsed))
		{
			pdrgpexprLocal->Append(pexprPred);
		}
		else
		{
			pdrgpexprOuterRefs->Append(pexprPred);
		}
	};

	*ppexprLocal = PexprConjunction(mp, std::move(pdrgpexprLocal));
	*ppexprOuterRef = PexprConjunction(mp, std::move(pdrgpexprOuterRefs));
}

// convert predicates of the form (a Cmp b) into (a InvCmp b);
// where InvCmp is the inverse comparison (e.g., '=' --> '<>')
gpos::Ref<CExpression>
CPredicateUtils::PexprInverseComparison(CMemoryPool *mp, CExpression *pexprCmp)
{
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	IMDId *mdid_op = gpos::dyn_cast<CScalarCmp>(pexprCmp->Pop())->MdIdOp();
	IMDId *pmdidInverseOp =
		md_accessor->RetrieveScOp(mdid_op)->GetInverseOpMdid();
	const CWStringConst *pstrFirst =
		md_accessor->RetrieveScOp(pmdidInverseOp)->Mdname().GetMDName();

	// generate a predicate for the inversion of the comparison involved in the subquery
	;
	;
	;

	return CUtils::PexprScalarCmp(mp, (*pexprCmp)[0], (*pexprCmp)[1],
								  *pstrFirst, pmdidInverseOp);
}

// convert predicates of the form (true = (a Cmp b)) into (a Cmp b);
// do this operation recursively on deep expression tree
gpos::Ref<CExpression>
CPredicateUtils::PexprPruneSuperfluosEquality(CMemoryPool *mp,
											  CExpression *pexpr)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(pexpr->Pop()->FScalar());

	if (CUtils::FSubquery(pexpr->Pop()))
	{
		// cannot recurse below subquery
		;
		return pexpr;
	}

	if (IsEqualityOp(pexpr))
	{
		BOOL fConstTrueLeftChild = CUtils::FScalarConstTrue((*pexpr)[0]);
		BOOL fConstTrueRightChild = CUtils::FScalarConstTrue((*pexpr)[1]);
		BOOL fConstFalseLeftChild = CUtils::FScalarConstFalse((*pexpr)[0]);
		BOOL fConstFalseRightChild = CUtils::FScalarConstFalse((*pexpr)[1]);

		BOOL fCmpLeftChild = CUtils::FScalarCmp((*pexpr)[0]);
		BOOL fCmpRightChild = CUtils::FScalarCmp((*pexpr)[1]);

		if (fCmpRightChild)
		{
			if (fConstTrueLeftChild)
			{
				return PexprPruneSuperfluosEquality(mp, (*pexpr)[1]);
			}

			if (fConstFalseLeftChild)
			{
				gpos::Ref<CExpression> pexprInverse =
					PexprInverseComparison(mp, (*pexpr)[1]);
				gpos::Ref<CExpression> pexprPruned =
					PexprPruneSuperfluosEquality(mp, pexprInverse.get());
				;
				return pexprPruned;
			}
		}

		if (fCmpLeftChild)
		{
			if (fConstTrueRightChild)
			{
				return PexprPruneSuperfluosEquality(mp, (*pexpr)[0]);
			}

			if (fConstFalseRightChild)
			{
				gpos::Ref<CExpression> pexprInverse =
					PexprInverseComparison(mp, (*pexpr)[0]);
				gpos::Ref<CExpression> pexprPruned =
					PexprPruneSuperfluosEquality(mp, pexprInverse.get());
				;

				return pexprPruned;
			}
		}
	}

	// process children
	gpos::Ref<CExpressionArray> pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG ulChildren = pexpr->Arity();
	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		gpos::Ref<CExpression> pexprChild =
			PexprPruneSuperfluosEquality(mp, (*pexpr)[ul]);
		pdrgpexpr->Append(pexprChild);
	}

	gpos::Ref<COperator> pop = pexpr->Pop();
	;
	return GPOS_NEW(mp) CExpression(mp, std::move(pop), std::move(pdrgpexpr));
}

// determine if we should test predicate implication for statistics computation
BOOL
CPredicateUtils::FCheckPredicateImplication(CExpression *pexprPred)
{
	// currently restrict testing implication to only equality of column references on scalar
	// ident and binary coercible casted idents
	if (COperator::EopScalarCmp == pexprPred->Pop()->Eopid() &&
		IMDType::EcmptEq ==
			gpos::dyn_cast<CScalarCmp>(pexprPred->Pop())->ParseCmpType())
	{
		CExpression *pexprLeft = (*pexprPred)[0];
		CExpression *pexprRight = (*pexprPred)[1];
		return ((COperator::EopScalarIdent == pexprLeft->Pop()->Eopid() ||
				 CCastUtils::FBinaryCoercibleCastedScId(pexprLeft)) &&
				(COperator::EopScalarIdent == pexprRight->Pop()->Eopid() ||
				 CCastUtils::FBinaryCoercibleCastedScId(pexprRight)));
	}
	return false;
}

// Given a predicate and a list of equivalence classes, return true if that predicate is
// implied by given equivalence classes
BOOL
CPredicateUtils::FImpliedPredicate(CExpression *pexprPred,
								   CColRefSetArray *pdrgpcrsEquivClasses)
{
	GPOS_ASSERT(pexprPred->Pop()->FScalar());
	GPOS_ASSERT(FCheckPredicateImplication(pexprPred));

	CColRefSet *pcrsUsed = pexprPred->DeriveUsedColumns();
	const ULONG size = pdrgpcrsEquivClasses->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		CColRefSet *pcrs = (*pdrgpcrsEquivClasses)[ul].get();
		if (pcrs->ContainsAll(pcrsUsed))
		{
			// predicate is implied by given equivalence classes
			return true;
		}
	}

	return false;
}

// Remove conjuncts that are implied based on child equivalence classes,
// the main use case is minimizing join/selection predicates to avoid
// cardinality under-estimation,
//
// for example, in the expression ((a=b) AND (a=c)), if a child
// equivalence class is {b,c}, then we remove the conjunct (a=c)
// since it can be implied from {b,c}, {a,b}
gpos::Ref<CExpression>
CPredicateUtils::PexprRemoveImpliedConjuncts(CMemoryPool *mp,
											 CExpression *pexprScalar,
											 CExpressionHandle &exprhdl)
{
	if (COperator::EopScalarNAryJoinPredList == pexprScalar->Pop()->Eopid())
	{
		// for a ScalarNAryJoinPredList we have to preserve that operator and
		// remove implied preds from each child individually
		gpos::Ref<CExpressionArray> newChildren =
			GPOS_NEW(mp) CExpressionArray(mp);

		for (ULONG c = 0; c < pexprScalar->Arity(); c++)
		{
			newChildren->Append(
				PexprRemoveImpliedConjuncts(mp, (*pexprScalar)[c], exprhdl));
		}

		// reassemble the CScalarNAryJoinPredList with its new children without implied conjuncts
		;

		return GPOS_NEW(mp)
			CExpression(mp, pexprScalar->Pop(), std::move(newChildren));
	}

	// extract equivalence classes from logical children
	gpos::Ref<CColRefSetArray> pdrgpcrs =
		CUtils::PdrgpcrsCopyChildEquivClasses(mp, exprhdl);

	// extract all the conjuncts
	gpos::Ref<CExpressionArray> pdrgpexprConjuncts =
		PdrgpexprConjuncts(mp, pexprScalar);
	const ULONG size = pdrgpexprConjuncts->Size();
	gpos::Ref<CExpressionArray> pdrgpexprNewConjuncts =
		GPOS_NEW(mp) CExpressionArray(mp);
	for (ULONG ul = 0; ul < size; ul++)
	{
		CExpression *pexprConj = (*pdrgpexprConjuncts)[ul];
		if (FCheckPredicateImplication(pexprConj) &&
			FImpliedPredicate(pexprConj, pdrgpcrs.get()))
		{
			// skip implied conjunct
			continue;
		}

		// add predicate to current equivalence classes
		gpos::Ref<CColRefSetArray> pdrgpcrsConj = nullptr;
		gpos::Ref<CConstraint> pcnstr =
			CConstraint::PcnstrFromScalarExpr(mp, pexprConj, &pdrgpcrsConj);
		;
		if (nullptr != pdrgpcrsConj)
		{
			gpos::Ref<CColRefSetArray> pdrgpcrsMerged =
				CUtils::PdrgpcrsMergeEquivClasses(mp, pdrgpcrs,
												  pdrgpcrsConj.get());
			;
			;
			pdrgpcrs = pdrgpcrsMerged;
		}

		// add conjunct to new conjuncts array
		;
		pdrgpexprNewConjuncts->Append(pexprConj);
	}

	;
	;

	return PexprConjunction(mp, std::move(pdrgpexprNewConjuncts));
}

// check if given correlations are valid for (anti)semi-joins;
// we disallow correlations referring to inner child, since inner
// child columns are not visible above (anti)semi-join
BOOL
CPredicateUtils::FValidSemiJoinCorrelations(
	CMemoryPool *mp, CExpression *pexprOuter, CExpression *pexprInner,
	CExpressionArray *pdrgpexprCorrelations)
{
	GPOS_ASSERT(nullptr != pexprOuter);
	GPOS_ASSERT(nullptr != pexprInner);

	CColRefSet *pcrsOuterOuput = pexprOuter->DeriveOutputColumns();
	CColRefSet *pcrsInnerOuput = pexprInner->DeriveOutputColumns();

	// collect output columns of both children
	gpos::Ref<CColRefSet> pcrsChildren =
		GPOS_NEW(mp) CColRefSet(mp, *pcrsOuterOuput);
	pcrsChildren->Union(pcrsInnerOuput);

	const ULONG ulCorrs = pdrgpexprCorrelations->Size();
	BOOL fValid = true;
	for (ULONG ul = 0; fValid && ul < ulCorrs; ul++)
	{
		CExpression *pexprPred = (*pdrgpexprCorrelations)[ul].get();
		CColRefSet *pcrsUsed = pexprPred->DeriveUsedColumns();
		if (0 < pcrsUsed->Size() && !pcrsChildren->ContainsAll(pcrsUsed) &&
			!pcrsUsed->IsDisjoint(pcrsInnerOuput))
		{
			// disallow correlations referring to inner child
			fValid = false;
		}
	};

	return fValid;
}

// check if given expression is (a conjunction of) simple column
// equality that use columns from the given column set
BOOL
CPredicateUtils::FSimpleEqualityUsingCols(CMemoryPool *mp,
										  CExpression *pexprScalar,
										  CColRefSet *pcrs)
{
	GPOS_ASSERT(nullptr != pexprScalar);
	GPOS_ASSERT(pexprScalar->Pop()->FScalar());
	GPOS_ASSERT(nullptr != pcrs);
	GPOS_ASSERT(0 < pcrs->Size());

	// break expression into conjuncts
	gpos::Ref<CExpressionArray> pdrgpexpr = PdrgpexprConjuncts(mp, pexprScalar);
	const ULONG size = pdrgpexpr->Size();
	BOOL fSuccess = true;
	for (ULONG ul = 0; fSuccess && ul < size; ul++)
	{
		// join predicate must be an equality of scalar idents and uses columns from given set
		CExpression *pexprConj = (*pdrgpexpr)[ul].get();
		CColRefSet *pcrsUsed = pexprConj->DeriveUsedColumns();
		fSuccess = IsEqualityOp(pexprConj) &&
				   CUtils::FScalarIdent((*pexprConj)[0]) &&
				   CUtils::FScalarIdent((*pexprConj)[1]) &&
				   !pcrs->IsDisjoint(pcrsUsed);
	};

	return fSuccess;
}

// for all columns in the given expression and are members of the given column set, replace columns with NULLs
gpos::Ref<CExpression>
CPredicateUtils::PexprReplaceColsWithNulls(CMemoryPool *mp,
										   CExpression *pexprScalar,
										   CColRefSet *pcrs)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(nullptr != pexprScalar);

	COperator *pop = pexprScalar->Pop();
	GPOS_ASSERT(pop->FScalar());

	if (CUtils::FSubquery(pop))
	{
		// do not recurse into subqueries
		;
		return pexprScalar;
	}

	if (COperator::EopScalarIdent == pop->Eopid() &&
		pcrs->FMember(gpos::dyn_cast<CScalarIdent>(pop)->Pcr()))
	{
		// replace column with NULL constant
		return CUtils::PexprScalarConstBool(mp, false /*value*/,
											true /*is_null*/);
	}

	// process children recursively
	gpos::Ref<CExpressionArray> pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG ulChildren = pexprScalar->Arity();
	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		gpos::Ref<CExpression> pexprChild =
			PexprReplaceColsWithNulls(mp, (*pexprScalar)[ul], pcrs);
		pdrgpexpr->Append(pexprChild);
	}

	;
	return GPOS_NEW(mp) CExpression(mp, pop, std::move(pdrgpexpr));
}

// check if scalar expression evaluates to (NOT TRUE) when
// all columns in the given set that are included in the expression
// are set to NULL
BOOL
CPredicateUtils::FNullRejecting(CMemoryPool *mp, CExpression *pexprScalar,
								CColRefSet *pcrs)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(nullptr != pexprScalar);
	GPOS_ASSERT(pexprScalar->Pop()->FScalar());

	BOOL fHasVolatileFunctions =
		(IMDFunction::EfsVolatile ==
		 pexprScalar->DeriveScalarFunctionProperties()->Efs());

	if (fHasVolatileFunctions || pexprScalar->DeriveHasNonScalarFunction())
	{
		// scalar expression must not have volatile functions, subquery or non-scalar functions
		return false;
	}

	// create another expression copy where we replace columns included in the set with NULL values
	gpos::Ref<CExpression> pexprColsReplacedWithNulls =
		PexprReplaceColsWithNulls(mp, pexprScalar, pcrs);

	// evaluate the resulting expression
	CScalar::EBoolEvalResult eber =
		CScalar::EberEvaluate(mp, pexprColsReplacedWithNulls.get());
	;

	// return TRUE if expression evaluation  result is (NOT TRUE), which means we need to
	// check if result is NULL or result is False,
	// in these two cases, a predicate will filter out incoming tuple, which means it is Null-Rejecting
	return (CScalar::EberNull == eber || CScalar::EberFalse == eber ||
			CScalar::EberNotTrue == eber);
}

// returns true iff the given expression is a Not operator whose child is an identifier
BOOL
CPredicateUtils::FNotIdent(CExpression *pexpr)
{
	return FNot(pexpr) &&
		   COperator::EopScalarIdent == (*pexpr)[0]->Pop()->Eopid();
}

// returns true iff all predicates in the given array are compatible with the given index.
BOOL
CPredicateUtils::FCompatiblePredicates(CExpressionArray *pdrgpexprPred,
									   const IMDIndex *pmdindex,
									   CColRefArray *pdrgpcrIndex,
									   CMDAccessor *md_accessor)
{
	GPOS_ASSERT(nullptr != pdrgpexprPred);
	GPOS_ASSERT(nullptr != pmdindex);

	const ULONG ulNumPreds = pdrgpexprPred->Size();
	for (ULONG ul = 0; ul < ulNumPreds; ul++)
	{
		if (!FCompatibleIndexPredicate((*pdrgpexprPred)[ul].get(), pmdindex,
									   pdrgpcrIndex, md_accessor))
		{
			return false;
		}
	}

	return true;
}

// returns true iff the given predicate 'pexprPred' is compatible with the given index 'pmdindex'.
BOOL
CPredicateUtils::FCompatibleIndexPredicate(CExpression *pexprPred,
										   const IMDIndex *pmdindex,
										   CColRefArray *pdrgpcrIndex,
										   CMDAccessor *md_accessor)
{
	GPOS_ASSERT(nullptr != pexprPred);
	GPOS_ASSERT(nullptr != pmdindex);

	const IMDScalarOp *pmdobjScCmp = nullptr;
	if (COperator::EopScalarCmp == pexprPred->Pop()->Eopid())
	{
		CScalarCmp *popScCmp = gpos::dyn_cast<CScalarCmp>(pexprPred->Pop());
		pmdobjScCmp = md_accessor->RetrieveScOp(popScCmp->MdIdOp());
	}
	else if (COperator::EopScalarArrayCmp == pexprPred->Pop()->Eopid())
	{
		CScalarArrayCmp *popScArrCmp =
			gpos::dyn_cast<CScalarArrayCmp>(pexprPred->Pop());
		pmdobjScCmp = md_accessor->RetrieveScOp(popScArrCmp->MdIdOp());
	}
	else
	{
		return false;
	}

	CExpression *pexprLeft = (*pexprPred)[0];
	CColRefSet *pcrsUsed = pexprLeft->DeriveUsedColumns();
	GPOS_ASSERT(1 == pcrsUsed->Size());

	CColRef *pcrIndexKey = pcrsUsed->PcrFirst();
	ULONG ulKeyPos = pdrgpcrIndex->IndexOf(pcrIndexKey);
	GPOS_ASSERT(gpos::ulong_max != ulKeyPos);

	return (pmdindex->IsCompatible(pmdobjScCmp, ulKeyPos));
}

// check if given array of expressions contain a volatile function like random().
BOOL
CPredicateUtils::FContainsVolatileFunction(CExpressionArray *pdrgpexprPred)
{
	GPOS_ASSERT(nullptr != pdrgpexprPred);

	const ULONG ulNumPreds = pdrgpexprPred->Size();
	for (ULONG ul = 0; ul < ulNumPreds; ul++)
	{
		CExpression *pexpr = (CExpression *) (*pdrgpexprPred)[ul].get();

		if (FContainsVolatileFunction(pexpr))
		{
			return true;
		}
	}

	return false;
}

// check if the expression contains a volatile function like random().
BOOL
CPredicateUtils::FContainsVolatileFunction(CExpression *pexpr)
{
	GPOS_ASSERT(nullptr != pexpr);


	COperator *popCurrent = pexpr->Pop();
	GPOS_ASSERT(nullptr != popCurrent);

	if (COperator::EopScalarFunc == popCurrent->Eopid())
	{
		CScalarFunc *pCurrentFunction = gpos::dyn_cast<CScalarFunc>(popCurrent);
		return IMDFunction::EfsVolatile ==
			   pCurrentFunction->EfsGetFunctionStability();
	}

	// recursively check children
	const ULONG ulChildren = pexpr->Arity();
	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		BOOL isVolatile = FContainsVolatileFunction((*pexpr)[ul]);
		if (isVolatile)
		{
			return true;
		}
	}

	// cannot find any
	return false;
}

// check if the expensive CNF conversion is beneficial in finding predicate for hash join
BOOL
CPredicateUtils::FConvertToCNF(CExpression *pexprOuter, CExpression *pexprInner,
							   CExpression *pexprScalar)
{
	GPOS_ASSERT(nullptr != pexprScalar);

	if (FComparison(pexprScalar))
	{
		return CPhysicalJoin::FHashJoinCompatible(pexprScalar, pexprOuter,
												  pexprInner);
	}

	BOOL fOr = FOr(pexprScalar);
	BOOL fAllChidrenDoCNF = true;
	BOOL fExistsChildDoCNF = false;

	// recursively check children
	const ULONG arity = pexprScalar->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		BOOL fCNFConversion =
			FConvertToCNF(pexprOuter, pexprInner, (*pexprScalar)[ul]);

		if (fCNFConversion)
		{
			fExistsChildDoCNF = true;
		}
		else
		{
			fAllChidrenDoCNF = false;
		}
	}

	if (fOr)
	{
		// an OR predicate can only be beneficial is all of it
		// children state that there is a benefit for CNF conversion
		// eg: ((a1 > b1) AND (a2 > b2)) OR ((a3 == b3) AND (a4 == b4))
		// one of the OR children has no equality condition and thus when
		// we convert the expression into CNF none of then will be useful to
		// for hash join

		return fAllChidrenDoCNF;
	}
	else
	{
		// at least one one child states that CNF conversion is beneficial

		return fExistsChildDoCNF;
	}
}

// if the nth child of the given union/union all expression is also a
// union / union all expression, then collect the latter's children and
// set the input columns of the new n-ary union/unionall operator
void
CPredicateUtils::CollectGrandChildrenUnionUnionAll(
	CMemoryPool *mp, CExpression *pexpr, ULONG child_index,
	CExpressionArray *pdrgpexprResult, CColRef2dArray *pdrgdrgpcrResult)
{
	GPOS_ASSERT(nullptr != pexpr);
	GPOS_ASSERT(child_index < pexpr->Arity());
	GPOS_ASSERT(nullptr != pdrgpexprResult);
	GPOS_ASSERT(nullptr != pdrgdrgpcrResult);
	GPOS_ASSERT(
		CPredicateUtils::FCollapsibleChildUnionUnionAll(pexpr, child_index));


	CExpression *pexprChild = (*pexpr)[child_index];
	GPOS_ASSERT(nullptr != pexprChild);

	CLogicalSetOp *pop = gpos::dyn_cast<CLogicalSetOp>(pexpr->Pop());
	CLogicalSetOp *popChild = gpos::dyn_cast<CLogicalSetOp>(pexprChild->Pop());

	// the parent setop's expected input columns and the child setop's output columns
	// may have different size or order or both. We need to ensure that the new
	// n-ary setop has the right order of the input columns from its grand children
	CColRef2dArray *pdrgpdrgpcrInput = pop->PdrgpdrgpcrInput();
	CColRefArray *pdrgpcrInputExpected = (*pdrgpdrgpcrInput)[child_index].get();

	const ULONG num_cols = pdrgpcrInputExpected->Size();

	CColRefArray *pdrgpcrOuputChild = popChild->PdrgpcrOutput();
	GPOS_ASSERT(num_cols <= pdrgpcrOuputChild->Size());

	gpos::Ref<ULongPtrArray> pdrgpul = GPOS_NEW(mp) ULongPtrArray(mp);
	for (ULONG ulColIdx = 0; ulColIdx < num_cols; ulColIdx++)
	{
		const CColRef *colref = (*pdrgpcrInputExpected)[ulColIdx];
		ULONG ulPos = pdrgpcrOuputChild->IndexOf(colref);
		GPOS_ASSERT(gpos::ulong_max != ulPos);
		pdrgpul->Append(GPOS_NEW(mp) ULONG(ulPos));
	}

	CColRef2dArray *pdrgdrgpcrChild = popChild->PdrgpdrgpcrInput();
	const ULONG ulArityChild = pexprChild->Arity();
	GPOS_ASSERT(pdrgdrgpcrChild->Size() == ulArityChild);

	for (ULONG ul = 0; ul < ulArityChild; ul++)
	{
		// collect the grand child expression
		CExpression *pexprGrandchild = (*pexprChild)[ul];
		GPOS_ASSERT(pexprGrandchild->Pop()->FLogical());

		;
		pdrgpexprResult->Append(pexprGrandchild);

		// collect the correct input columns
		CColRefArray *pdrgpcrOld = (*pdrgdrgpcrChild)[ul].get();
		gpos::Ref<CColRefArray> pdrgpcrNew = GPOS_NEW(mp) CColRefArray(mp);
		for (ULONG ulColIdx = 0; ulColIdx < num_cols; ulColIdx++)
		{
			ULONG ulPos = *(*pdrgpul)[ulColIdx];
			CColRef *colref = (*pdrgpcrOld)[ulPos];
			pdrgpcrNew->Append(colref);
		}

		pdrgdrgpcrResult->Append(pdrgpcrNew);
	}

	;
}

// check if we can collapse the nth child of the given union / union all operator
BOOL
CPredicateUtils::FCollapsibleChildUnionUnionAll(CExpression *pexpr,
												ULONG child_index)
{
	GPOS_ASSERT(nullptr != pexpr);

	if (!CPredicateUtils::FUnionOrUnionAll(pexpr))
	{
		return false;
	}

	CExpression *pexprChild = (*pexpr)[child_index];
	GPOS_ASSERT(nullptr != pexprChild);

	// we can only collapse when the parent and child operator are of the same kind
	return (pexprChild->Pop()->Eopid() == pexpr->Pop()->Eopid());
}

// check if the input predicate is a simple predicate that can be used for bitmap
// index lookup directly without separating it's children, such as
// * A simple scalar (array) cmp between an ident and a constant (array)
// * A conjunct with children of the form:
//		+--CScalarCmp (op)
//		|--CScalarIdent
//		+--CScalarConst
// OR
//		+--CScalarArrayCmp (op)
//		|--CScalarIdent
//		+--CScalarConstArray
// OR
//		+--CScalarBoolOp (EboolopNot)
//			+--CScalarIdent (boolean type)
// OR
//		+--CScalarIdent   (boolean type)

// Note: Casted idents, constants or const arrays are allowed, such as
//		+--CScalarArrayCmp (op)
//		|--CScalarCast
//		|  +--CScalarIdent
//		+--CScalarConstArray
BOOL
CPredicateUtils::FBitmapLookupSupportedPredicateOrConjunct(
	CExpression *pexpr, CColRefSet *outer_refs)
{
	if (CPredicateUtils::FAnd(pexpr))
	{
		const ULONG ulArity = pexpr->Arity();
		BOOL result = true;
		for (ULONG ul = 0; ul < ulArity && result; ul++)
		{
			result = result &&
					 CPredicateUtils::FBitmapLookupSupportedPredicateOrConjunct(
						 (*pexpr)[ul], outer_refs);
		}

		return result;
	}

	// indexes allow ident cmp const and ident cmp outerref
	if (CPredicateUtils::FIdentCompareConstIgnoreCast(
			pexpr, COperator::EopScalarCmp) ||
		CPredicateUtils::FIdentCompareOuterRefExprIgnoreCast(pexpr,
															 outer_refs) ||
		CPredicateUtils::FArrayCompareIdentToConstIgnoreCast(pexpr) ||
		CUtils::FScalarIdentBoolType(pexpr) ||
		(!CUtils::FScalarIdentBoolType(pexpr) &&
		 CPredicateUtils::FNotIdent(pexpr)))
	{
		return true;
	}

	return false;
}

BOOL
CPredicateUtils::FBuiltInComparisonIsVeryStrict(IMDId *mdid)
{
	const CMDIdGPDB *const pmdidGPDB = gpos::dyn_cast<CMDIdGPDB>(mdid);

	GPOS_ASSERT(nullptr != pmdidGPDB);

	switch (pmdidGPDB->Oid())
	{
		case GPDB_INT2_EQ_OP:
		case GPDB_INT2_NEQ_OP:
		case GPDB_INT2_LT_OP:
		case GPDB_INT2_LEQ_OP:
		case GPDB_INT2_GT_OP:
		case GPDB_INT2_GEQ_OP:
		case GPDB_INT4_EQ_OP:
		case GPDB_INT4_NEQ_OP:
		case GPDB_INT4_LT_OP:
		case GPDB_INT4_LEQ_OP:
		case GPDB_INT4_GT_OP:
		case GPDB_INT4_GEQ_OP:
		case GPDB_INT8_EQ_OP:
		case GPDB_INT8_NEQ_OP:
		case GPDB_INT8_LT_OP:
		case GPDB_INT8_LEQ_OP:
		case GPDB_INT8_GT_OP:
		case GPDB_INT8_GEQ_OP:
		case GPDB_BOOL_EQ_OP:
		case GPDB_BOOL_NEQ_OP:
		case GPDB_BOOL_LT_OP:
		case GPDB_BOOL_LEQ_OP:
		case GPDB_BOOL_GT_OP:
		case GPDB_BOOL_GEQ_OP:
		case GPDB_TEXT_EQ_OP:
		case GPDB_TEXT_NEQ_OP:
		case GPDB_TEXT_LT_OP:
		case GPDB_TEXT_LEQ_OP:
		case GPDB_TEXT_GT_OP:
		case GPDB_TEXT_GEQ_OP:
			// these built-in operators have well-known behavior, they always
			// return NULL when one of the operands is NULL and they
			// never return NULL when both operands are not NULL
			return true;

		default:
			// this operator might also qualify but unfortunately we can't be sure
			return false;
	}
}

BOOL
CPredicateUtils::ExprContainsOnlyStrictComparisons(CMemoryPool *mp,
												   CExpression *expr)
{
	gpos::Ref<CExpressionArray> conjuncts = PdrgpexprConjuncts(mp, expr);
	BOOL result = ExprsContainsOnlyStrictComparisons(conjuncts.get());
	;
	return result;
}

BOOL
CPredicateUtils::ExprsContainsOnlyStrictComparisons(CExpressionArray *conjuncts)
{
	CMDAccessor *mda = COptCtxt::PoctxtFromTLS()->Pmda();

	BOOL result = true;
	for (ULONG ul = 0; ul < conjuncts->Size(); ++ul)
	{
		CExpression *conjunct = (*conjuncts)[ul].get();
		if (FComparison(conjunct))
		{
			CScalarCmp *pscalarCmp =
				gpos::dyn_cast<CScalarCmp>(conjunct->Pop());
			if (!CMDAccessorUtils::FScalarOpReturnsNullOnNullInput(
					mda, pscalarCmp->MdIdOp()))
			{
				result = false;
				break;
			}
		}
		else
		{
			result = false;
			break;
		}
	}

	return result;
}

// EOF
