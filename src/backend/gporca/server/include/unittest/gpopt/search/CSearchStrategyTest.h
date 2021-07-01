//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CSearchStrategyTest.h
//
//	@doc:
//		Test for search strategy
//---------------------------------------------------------------------------
#ifndef GPOPT_CSearchStrategyTest_H
#define GPOPT_CSearchStrategyTest_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/operators/CExpression.h"
#include "gpopt/search/CSearchStage.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CSearchStrategyTest
//
//	@doc:
//		unittest for search strategy
//
//---------------------------------------------------------------------------
class CSearchStrategyTest
{
private:
	// type definition for of expression generator
	typedef gpos::owner<CExpression *> (*Pfpexpr)(CMemoryPool *);

	// type definition for of optimize function
	typedef void (*PfnOptimize)(CMemoryPool *, CExpression *,
								gpos::owner<CSearchStageArray *>);

	// generate random search strategy
	static gpos::owner<CSearchStageArray *> PdrgpssRandom(CMemoryPool *mp);

	// run optimize function on given expression
	static void Optimize(CMemoryPool *mp, Pfpexpr pfnGenerator,
						 gpos::owner<CSearchStageArray *> search_stage_array,
						 PfnOptimize pfnOptimize);

	static void BuildMemo(CMemoryPool *mp, CExpression *pexprInput,
						  gpos::owner<CSearchStageArray *> search_stage_array);

public:
	// unittests driver
	static GPOS_RESULT EresUnittest();

#ifdef GPOS_DEBUG
	// test search strategy with recursive optimization
	static GPOS_RESULT EresUnittest_RecursiveOptimize();
#endif	// GPOS_DEBUG

	// test search strategy with multi-threaded optimization
	static GPOS_RESULT EresUnittest_MultiThreadedOptimize();

	// test reading search strategy from XML file
	static GPOS_RESULT EresUnittest_Parsing();

	// test search strategy that times out
	static GPOS_RESULT EresUnittest_Timeout();

	// test exception handling when parsing search strategy
	static GPOS_RESULT EresUnittest_ParsingWithException();

};	// CSearchStrategyTest

}  // namespace gpopt

#endif	// !GPOPT_CSearchStrategyTest_H


// EOF
