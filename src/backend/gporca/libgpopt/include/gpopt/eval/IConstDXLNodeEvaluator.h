//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		IConstDXLNodeEvaluator.h
//
//	@doc:
//		Interface for evaluating constant expressions given as DXL
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPOPT_IConstDXLNodeEvaluator_H
#define GPOPT_IConstDXLNodeEvaluator_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "naucrates/dxl/operators/CDXLNode.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		IConstExprEvaluator
//
//	@doc:
//		Interface for evaluating constant expressions given as DXL
//
//---------------------------------------------------------------------------
class IConstDXLNodeEvaluator
{
public:
	// dtor
	virtual ~IConstDXLNodeEvaluator() = default;

	// evaluate the given DXL node representing an expression and return the result
	// as DXL. caller takes ownership of returned DXL node
	virtual gpos::owner<gpdxl::CDXLNode *> EvaluateExpr(
		gpos::pointer<const gpdxl::CDXLNode *> pdxlnExpr) = 0;

	// returns true iff the evaluator can evaluate constant expressions without
	// subqueries
	virtual gpos::BOOL FCanEvalExpressions() = 0;
};
}  // namespace gpopt

#endif	// !GPOPT_IConstDXLNodeEvaluator_H

// EOF
