//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2019 Pivotal Software, Inc.
//
#ifndef GPOPT_CXformImplementFullOuterMergeJoin_H
#define GPOPT_CXformImplementFullOuterMergeJoin_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

class CXformImplementFullOuterMergeJoin : public CXformExploration
{
private:
	CXformImplementFullOuterMergeJoin(
		const CXformImplementFullOuterMergeJoin &) = delete;

public:
	// ctor
	explicit CXformImplementFullOuterMergeJoin(CMemoryPool *mp);

	// dtor
	virtual ~CXformImplementFullOuterMergeJoin() = default;

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfImplementFullOuterMergeJoin;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformImplementFullOuterMergeJoin";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	virtual void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
						   CExpression *pexpr) const;

};	// class CXformImplementFullOuterMergeJoin
}  // namespace gpopt

#endif	// !GPOPT_CXformImplementFullOuterMergeJoin_H

// EOF
