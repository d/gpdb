//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CJoinOrderDP.h
//
//	@doc:
//		Dynamic programming-based join order generation
//---------------------------------------------------------------------------
#ifndef GPOPT_CJoinOrderDP_H
#define GPOPT_CJoinOrderDP_H

#include "gpos/base.h"
#include "gpos/common/CBitSet.h"
#include "gpos/common/CHashMap.h"
#include "gpos/common/DbgPrintMixin.h"
#include "gpos/common/owner.h"
#include "gpos/io/IOstream.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/xforms/CJoinOrder.h"


namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CJoinOrderDP
//
//	@doc:
//		Helper class for creating join orders using dynamic programming
//
//---------------------------------------------------------------------------
class CJoinOrderDP : public CJoinOrder, public gpos::DbgPrintMixin<CJoinOrderDP>
{
private:
	//---------------------------------------------------------------------------
	//	@struct:
	//		SComponentPair
	//
	//	@doc:
	//		Struct to capture a pair of components
	//
	//---------------------------------------------------------------------------
	struct SComponentPair : public CRefCount
	{
		// first component
		gpos::owner<CBitSet *> m_pbsFst;

		// second component
		gpos::owner<CBitSet *> m_pbsSnd;

		// ctor
		SComponentPair(gpos::owner<CBitSet *> pbsFst,
					   gpos::owner<CBitSet *> pbsSnd);

		// dtor
		~SComponentPair() override;

		// hashing function
		static ULONG HashValue(gpos::pointer<const SComponentPair *> pcomppair);

		// equality function
		static BOOL Equals(gpos::pointer<const SComponentPair *> pcomppairFst,
						   gpos::pointer<const SComponentPair *> pcomppairSnd);
	};

	// hashing function
	static ULONG
	UlHashBitSet(gpos::pointer<const CBitSet *> pbs)
	{
		GPOS_ASSERT(nullptr != pbs);

		return pbs->HashValue();
	}

	// equality function
	static BOOL
	FEqualBitSet(gpos::pointer<const CBitSet *> pbsFst,
				 gpos::pointer<const CBitSet *> pbsSnd)
	{
		GPOS_ASSERT(nullptr != pbsFst);
		GPOS_ASSERT(nullptr != pbsSnd);

		return pbsFst->Equals(pbsSnd);
	}

	// hash map from component to best join order
	typedef CHashMap<CBitSet, CExpression, UlHashBitSet, FEqualBitSet,
					 CleanupRelease<CBitSet>, CleanupRelease<CExpression> >
		BitSetToExpressionMap;

	// hash map from component pair to connecting edges
	typedef CHashMap<SComponentPair, CExpression, SComponentPair::HashValue,
					 SComponentPair::Equals, CleanupRelease<SComponentPair>,
					 CleanupRelease<CExpression> >
		ComponentPairToExpressionMap;

	// hash map from expression to cost of best join order
	typedef CHashMap<CExpression, CDouble, CExpression::HashValue,
					 CUtils::Equals, CleanupRelease<CExpression>,
					 CleanupDelete<CDouble> >
		ExpressionToCostMap;

	// lookup table for links
	gpos::owner<ComponentPairToExpressionMap *> m_phmcomplink;

	// dynamic programming table
	gpos::owner<BitSetToExpressionMap *> m_phmbsexpr;

	// map of expressions to its cost
	gpos::owner<ExpressionToCostMap *> m_phmexprcost;

	// array of top-k join expression
	gpos::owner<CExpressionArray *> m_pdrgpexprTopKOrders;

	// dummy expression to used for non-joinable components
	gpos::owner<CExpression *> m_pexprDummy;

	// build expression linking given components
	gpos::owner<CExpression *> PexprBuildPred(gpos::pointer<CBitSet *> pbsFst,
											  gpos::pointer<CBitSet *> pbsSnd);

	// lookup best join order for given set
	gpos::pointer<CExpression *> PexprLookup(gpos::pointer<CBitSet *> pbs);

	// extract predicate joining the two given sets
	gpos::pointer<CExpression *> PexprPred(gpos::pointer<CBitSet *> pbsFst,
										   gpos::pointer<CBitSet *> pbsSnd);

	// join expressions in the given two sets
	gpos::owner<CExpression *> PexprJoin(gpos::pointer<CBitSet *> pbsFst,
										 gpos::pointer<CBitSet *> pbsSnd);

	// join expressions in the given set
	gpos::pointer<CExpression *> PexprJoin(gpos::pointer<CBitSet *> pbs);

	// find best join order for given component using dynamic programming
	gpos::pointer<CExpression *> PexprBestJoinOrderDP(
		gpos::pointer<CBitSet *> pbs);

	// find best join order for given component
	CExpression *PexprBestJoinOrder(gpos::pointer<CBitSet *> pbs);

	// generate cross product for the given components
	gpos::pointer<CExpression *> PexprCross(gpos::pointer<CBitSet *> pbs);

	// join a covered subset with uncovered subset
	gpos::pointer<CExpression *> PexprJoinCoveredSubsetWithUncoveredSubset(
		gpos::pointer<CBitSet *> pbs, gpos::pointer<CBitSet *> pbsCovered,
		gpos::pointer<CBitSet *> pbsUncovered);

	// return a subset of the given set covered by one or more edges
	gpos::owner<CBitSet *> PbsCovered(gpos::pointer<CBitSet *> pbsInput);

	// add given join order to best results
	void AddJoinOrder(CExpression *pexprJoin, CDouble dCost);

	// compute cost of given join expression
	CDouble DCost(gpos::pointer<CExpression *> pexpr);

	// derive stats on given expression
	void DeriveStats(gpos::pointer<CExpression *> pexpr) override;

	// add expression to cost map
	void InsertExpressionCost(CExpression *pexpr, CDouble dCost,
							  BOOL fValidateInsert);

	// generate all subsets of the given array of elements
	static void GenerateSubsets(CMemoryPool *mp,
								gpos::owner<CBitSet *> pbsCurrent,
								ULONG *pulElems, ULONG size, ULONG ulIndex,
								gpos::pointer<CBitSetArray *> pdrgpbsSubsets);

	// driver of subset generation
	static gpos::owner<CBitSetArray *> PdrgpbsSubsets(
		CMemoryPool *mp, gpos::pointer<CBitSet *> pbs);

public:
	// ctor
	CJoinOrderDP(CMemoryPool *mp,
				 gpos::owner<CExpressionArray *> pdrgpexprComponents,
				 gpos::owner<CExpressionArray *> pdrgpexprConjuncts);

	// dtor
	~CJoinOrderDP() override;

	// main handler
	virtual CExpression *PexprExpand();

	// best join orders
	gpos::pointer<CExpressionArray *>
	PdrgpexprTopK() const
	{
		return m_pdrgpexprTopKOrders;
	}

	// print function
	IOstream &OsPrint(IOstream &) const;


};	// class CJoinOrderDP

}  // namespace gpopt

#endif	// !GPOPT_CJoinOrderDP_H

// EOF
