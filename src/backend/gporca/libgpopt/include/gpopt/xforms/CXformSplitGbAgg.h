//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformSplitGbAgg.h
//
//	@doc:
//		Split an aggregate into a pair of local and global aggregate
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformSplitGbAgg_H
#define GPOPT_CXformSplitGbAgg_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformSplitGbAgg
//
//	@doc:
//		Split an aggregate operator into pair of local and global aggregate
//
//---------------------------------------------------------------------------
class CXformSplitGbAgg : public CXformExploration
{
private:
protected:
	// check if the transformation is applicable;
	static BOOL FApplicable(gpos::pointer<CExpression *> pexpr);

	// generate a project lists for the local and global aggregates
	// from the original aggregate
	static void PopulateLocalGlobalProjectList(
		CMemoryPool *mp,  // memory pool
		gpos::pointer<CExpression *>
			pexprProjListOrig,	// project list of the original global aggregate
		gpos::owner<CExpression *>
			*ppexprProjListLocal,  // project list of the new local aggregate
		gpos::owner<CExpression *>
			*ppexprProjListGlobal  // project list of the new global aggregate
	);

public:
	CXformSplitGbAgg(const CXformSplitGbAgg &) = delete;

	// ctor
	explicit CXformSplitGbAgg(CMemoryPool *mp);

	// ctor
	explicit CXformSplitGbAgg(gpos::owner<CExpression *> pexprPattern);

	// dtor
	~CXformSplitGbAgg() override = default;

	// ident accessors
	EXformId
	Exfid() const override
	{
		return ExfSplitGbAgg;
	}

	// return a string for xform name
	const CHAR *
	SzId() const override
	{
		return "CXformSplitGbAgg";
	}

	// Compatibility function for splitting aggregates
	BOOL
	FCompatible(CXform::EXformId exfid) override
	{
		return ((CXform::ExfSplitDQA != exfid) &&
				(CXform::ExfSplitGbAgg != exfid) &&
				(CXform::ExfEagerAgg != exfid));
	}

	// compute xform promise for a given expression handle
	EXformPromise Exfp(CExpressionHandle &exprhdl) const override;

	// actual transform
	void Transform(gpos::pointer<CXformContext *> pxfctxt,
				   gpos::pointer<CXformResult *> pxfres,
				   gpos::pointer<CExpression *> pexpr) const override;

};	// class CXformSplitGbAgg

}  // namespace gpopt

#endif	// !GPOPT_CXformSplitGbAgg_H

// EOF
