//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 VMware, Inc. or its affiliates.
//
//	@filename:
//		CXformSplitDQA.h
//
//	@doc:
//		Split an aggregate into a three level aggregation
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformSplitDQA_H
#define GPOPT_CXformSplitDQA_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CLogicalGbAgg.h"
#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformSplitDQA
//
//	@doc:
//		Split an aggregate into a three level aggregation
//
//---------------------------------------------------------------------------
class CXformSplitDQA : public CXformExploration
{
private:
	// hash map between expression and a column reference
	typedef CHashMap<CExpression, CColRef, CExpression::HashValue,
					 CUtils::Equals, CleanupRelease<CExpression>,
					 CleanupNULL<CColRef> >
		ExprToColRefMap;

	// generate an expression with multi-level aggregation
	static gpos::owner<CExpression *> PexprMultiLevelAggregation(
		CMemoryPool *mp, CExpression *pexprRelational,
		gpos::owner<CExpressionArray *> pdrgpexprPrElFirstStage,
		CExpressionArray *pdrgpexprPrElSecondStage,
		gpos::owner<CExpressionArray *> pdrgpexprPrElThirdStage,
		CColRefArray *pdrgpcrArgDQA, CColRefArray *pdrgpcrLastStage,
		BOOL fSplit2LevelsOnly, BOOL fAddDistinctColToLocalGb,
		CLogicalGbAgg::EAggStage aggStage);

	// split DQA into a local DQA and global non-DQA aggregate function
	static gpos::owner<CExpression *> PexprSplitIntoLocalDQAGlobalAgg(
		CMemoryPool *mp, CColumnFactory *col_factory, CMDAccessor *md_accessor,
		gpos::pointer<CExpression *> pexpr, CExpression *pexprRelational,
		gpos::pointer<ExprToColRefMap *> phmexprcr, CColRefArray *pdrgpcrArgDQA,
		CLogicalGbAgg::EAggStage aggStage);

	// helper function to split DQA
	static gpos::owner<CExpression *> PexprSplitHelper(
		CMemoryPool *mp, CColumnFactory *col_factory, CMDAccessor *md_accessor,
		gpos::pointer<CExpression *> pexpr, CExpression *pexprRelational,
		gpos::pointer<ExprToColRefMap *> phmexprcr, CColRefArray *pdrgpcrArgDQA,
		CLogicalGbAgg::EAggStage aggStage);

	// given a scalar aggregate generate the local, intermediate and global
	// aggregate functions. Then add them to the project list of the
	// corresponding aggregate operator at each stage of the multi-stage
	// aggregation
	static void PopulatePrLMultiPhaseAgg(
		CMemoryPool *mp, CColumnFactory *col_factory, CMDAccessor *md_accessor,
		gpos::pointer<CExpression *> pexprPrEl,
		gpos::pointer<CExpressionArray *> pdrgpexprPrElFirstStage,
		gpos::pointer<CExpressionArray *> pdrgpexprPrElSecondStage,
		gpos::pointer<CExpressionArray *> pdrgpexprPrElLastStage,
		BOOL fSplit2LevelsOnly);

	// create project element for the aggregate function of a particular level
	static gpos::owner<CExpression *> PexprPrElAgg(
		CMemoryPool *mp, gpos::pointer<CExpression *> pexprAggFunc,
		EAggfuncStage eaggfuncstage, CColRef *pcrPreviousStage,
		CColRef *pcrGlobal);

	// extract arguments of distinct aggs
	static void ExtractDistinctCols(
		CMemoryPool *mp, CColumnFactory *col_factory, CMDAccessor *md_accessor,
		gpos::pointer<CExpression *> pexpr,
		gpos::pointer<CExpressionArray *> pdrgpexprChildPrEl,
		gpos::pointer<ExprToColRefMap *> phmexprcr,
		gpos::owner<CColRefArray *> *ppdrgpcrArgDQA);

	// return the column reference of the argument to the aggregate function
	static CColRef *PcrAggFuncArgument(
		CMemoryPool *mp, CMDAccessor *md_accessor, CColumnFactory *col_factory,
		CExpression *pexprArg,
		gpos::pointer<CExpressionArray *> pdrgpexprChildPrEl);

public:
	CXformSplitDQA(const CXformSplitDQA &) = delete;

	// ctor
	explicit CXformSplitDQA(CMemoryPool *mp);

	// dtor
	~CXformSplitDQA() override = default;

	// ident accessors
	EXformId
	Exfid() const override
	{
		return ExfSplitDQA;
	}

	// return a string for xform name
	const CHAR *
	SzId() const override
	{
		return "CXformSplitDQA";
	}

	// Compatibility function for splitting aggregates
	BOOL
	FCompatible(CXform::EXformId exfid) override
	{
		return (CXform::ExfSplitDQA != exfid) &&
			   (CXform::ExfSplitGbAgg != exfid);
	}

	// compute xform promise for a given expression handle
	EXformPromise Exfp(CExpressionHandle &exprhdl) const override;

	// actual transform
	void Transform(gpos::pointer<CXformContext *>,
				   gpos::pointer<CXformResult *>,
				   gpos::pointer<CExpression *>) const override;

};	// class CXformSplitDQA

}  // namespace gpopt

#endif	// !GPOPT_CXformSplitDQA_H

// EOF
