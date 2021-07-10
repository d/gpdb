//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CAutoOptCtxt.h
//
//	@doc:
//		Optimizer context object; contains all global objects pertaining to
//		one optimization
//---------------------------------------------------------------------------
#ifndef GPOPT_CAutoOptCtxt_H
#define GPOPT_CAutoOptCtxt_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/base/CColumnFactory.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/cost/ICostModel.h"
#include "gpopt/eval/IConstExprEvaluator.h"
#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/optimizer/COptimizerConfig.h"

namespace gpopt
{
using namespace gpos;

// forward declaration
class CCostParams;

//---------------------------------------------------------------------------
//	@class:
//		CAutoOptCtxt
//
//	@doc:
//		Auto optimizer context object creates and installs optimizer context
//		for unittesting
//
//---------------------------------------------------------------------------
class CAutoOptCtxt
{
private:
public:
	CAutoOptCtxt(CAutoOptCtxt &) = delete;

	// ctor
	CAutoOptCtxt(CMemoryPool *mp, CMDAccessor *md_accessor,
				 gpos::Ref<IConstExprEvaluator> pceeval,
				 gpos::Ref<COptimizerConfig> optimizer_config);

	// ctor
	CAutoOptCtxt(CMemoryPool *mp, CMDAccessor *md_accessor,
				 gpos::Ref<IConstExprEvaluator> pceeval,
				 gpos::Ref<ICostModel> pcm);

	// dtor
	~CAutoOptCtxt();

};	// class CAutoOptCtxt
}  // namespace gpopt


#endif	// !GPOPT_CAutoOptCtxt_H

// EOF
