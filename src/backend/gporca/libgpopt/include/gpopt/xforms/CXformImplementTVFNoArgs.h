//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformImplementTVFNoArgs.h
//
//	@doc:
//		Implement logical TVF with a physical TVF with no arguments
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformImplementTVFNoArgs_H
#define GPOPT_CXformImplementTVFNoArgs_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformImplementTVF.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformImplementTVFNoArgs
//
//	@doc:
//		Implement TVF with no arguments
//
//---------------------------------------------------------------------------
class CXformImplementTVFNoArgs : public CXformImplementTVF
{
private:
	CXformImplementTVFNoArgs(const CXformImplementTVFNoArgs &) = delete;

public:
	// ctor
	explicit CXformImplementTVFNoArgs(CMemoryPool *mp);

	// dtor
	virtual ~CXformImplementTVFNoArgs() = default;

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfImplementTVFNoArgs;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformImplementTVFNoArgs";
	}

};	// class CXformImplementTVFNoArgs

}  // namespace gpopt

#endif	// !GPOPT_CXformImplementTVFNoArgs_H

// EOF
