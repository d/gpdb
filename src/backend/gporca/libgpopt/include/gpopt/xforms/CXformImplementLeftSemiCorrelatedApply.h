//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc..
//
//	@filename:
//		CXformImplementLeftSemiCorrelatedApply.h
//
//	@doc:
//		Transform left semi correlated apply to physical left semi
//		correlated join
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformImplementLeftSemiCorrelatedApply_H
#define GPOPT_CXformImplementLeftSemiCorrelatedApply_H

#include "gpos/base.h"
#include "gpopt/operators/CLogicalLeftSemiCorrelatedApply.h"
#include "gpopt/operators/CPhysicalCorrelatedLeftSemiNLJoin.h"
#include "gpopt/xforms/CXformImplementCorrelatedApply.h"

namespace gpopt
{
using namespace gpos;

//-------------------------------------------------------------------------
//	@class:
//		CXformImplementLeftSemiCorrelatedApply
//
//	@doc:
//		Transform left semi correlated apply to physical left semi
//		correlated join
//
//-------------------------------------------------------------------------
class CXformImplementLeftSemiCorrelatedApply
	: public CXformImplementCorrelatedApply<CLogicalLeftSemiCorrelatedApply,
											CPhysicalCorrelatedLeftSemiNLJoin>
{
private:
	CXformImplementLeftSemiCorrelatedApply(
		const CXformImplementLeftSemiCorrelatedApply &) = delete;

public:
	// ctor
	explicit CXformImplementLeftSemiCorrelatedApply(CMemoryPool *mp)
		: CXformImplementCorrelatedApply<CLogicalLeftSemiCorrelatedApply,
										 CPhysicalCorrelatedLeftSemiNLJoin>(mp)
	{
	}

	// dtor
	virtual ~CXformImplementLeftSemiCorrelatedApply() = default;

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfImplementLeftSemiCorrelatedApply;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformImplementLeftSemiCorrelatedApply";
	}

};	// class CXformImplementLeftSemiCorrelatedApply

}  // namespace gpopt

#endif	// !GPOPT_CXformImplementLeftSemiCorrelatedApply_H

// EOF
