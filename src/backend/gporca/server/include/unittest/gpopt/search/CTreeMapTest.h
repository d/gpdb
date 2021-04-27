//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CTreeMapTest.h
//
//	@doc:
//		Test for tree map implementation
//---------------------------------------------------------------------------
#ifndef GPOPT_CTreeMapTest_H
#define GPOPT_CTreeMapTest_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/search/CTreeMap.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CTreeMapTest
//
//	@doc:
//		unittest for tree map
//
//---------------------------------------------------------------------------
class CTreeMapTest
{
	// fwd declaration
	class CNode;
	typedef CDynamicPtrArray<CNode, CleanupRelease<CNode> > CNodeArray;

	// struct for resulting trees
	class CNode : public CRefCount
	{
	private:
		// element number
		ULONG m_ulData;

		// children
		gpos::owner<CNodeArray *> m_pdrgpnd;

	public:
		CNode(const CNode &) = delete;

		// ctor
		CNode(CMemoryPool *mp, gpos::pointer<const ULONG *> pulData,
			  gpos::owner<CNodeArray *> pdrgpnd);

		// dtor
		~CNode() override;

		// debug print
		IOstream &OsPrintWithIndent(IOstream &os, ULONG ulIndent = 0) const;
	};


private:
	// counter used to mark last successful test
	static ULONG m_ulTestCounter;

	// factory function for result object
	static gpos::owner<CNode *> Pnd(CMemoryPool *mp, gpos::pointer<ULONG *> pul,
									gpos::owner<CNodeArray *> pdrgpnd,
									BOOL *fTestTrue);

	// shorthand for tests
	typedef CTreeMap<ULONG, CNode, BOOL, HashValue<ULONG>, Equals<ULONG> >
		TestMap;

	// helper to generate loaded the tree map
	static TestMap *PtmapLoad(CMemoryPool *mp);

public:
	// unittests
	static GPOS_RESULT EresUnittest();
	static GPOS_RESULT EresUnittest_Basic();
	static GPOS_RESULT EresUnittest_Count();
	static GPOS_RESULT EresUnittest_Unrank();
	static GPOS_RESULT EresUnittest_Memo();


#ifdef GPOS_DEBUG
	static GPOS_RESULT EresUnittest_Cycle();
#endif	// GPOS_DEBUG

};	// CTreeMapTest

}  // namespace gpopt

#endif	// !GPOPT_CTreeMapTest_H


// EOF
