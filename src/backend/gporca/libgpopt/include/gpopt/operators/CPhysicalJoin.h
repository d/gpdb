//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalJoin.h
//
//	@doc:
//		Physical join base class
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalJoin_H
#define GPOPT_CPhysicalJoin_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/base/CDistributionSpec.h"
#include "gpopt/operators/CPhysical.h"

namespace gpopt
{
// fwd declarations
class CDistributionSpecSingleton;

//---------------------------------------------------------------------------
//	@class:
//		CPhysicalJoin
//
//	@doc:
//		Inner nested-loops join operator
//
//---------------------------------------------------------------------------
class CPhysicalJoin : public CPhysical
{
private:
	// private copy ctor
	CPhysicalJoin(const CPhysicalJoin &);

	// check whether the child being processed is the child that has the part consumer
	static BOOL FProcessingChildWithPartConsumer(BOOL fOuterPartConsumerTest,
												 ULONG ulChildIndexToTestFirst,
												 ULONG ulChildIndexToTestSecond,
												 ULONG child_index);

protected:
	// ctor
	explicit CPhysicalJoin(CMemoryPool *mp);

	// dtor
	~CPhysicalJoin() override = default;

	// helper to check if given child index correspond to first child to be optimized
	BOOL FFirstChildToOptimize(ULONG child_index) const;

	// helper to compute required distribution of correlated join's children
	gpos::owner<CEnfdDistribution *> PedCorrelatedJoin(
		CMemoryPool *mp, CExpressionHandle &exprhdl,
		gpos::pointer<CReqdPropPlan *> prppInput, ULONG child_index,
		gpos::pointer<CDrvdPropArray *> pdrgpdpCtxt, ULONG ulOptReq);

	// helper to compute required rewindability of correlated join's children
	static gpos::owner<CRewindabilitySpec *> PrsRequiredCorrelatedJoin(
		CMemoryPool *mp, CExpressionHandle &exprhdl,
		gpos::pointer<CRewindabilitySpec *> prsRequired, ULONG child_index,
		gpos::pointer<CDrvdPropArray *> pdrgpdpCtxt, ULONG ulOptReq);

	// helper for propagating required sort order to outer child
	static gpos::owner<COrderSpec *> PosPropagateToOuter(
		CMemoryPool *mp, CExpressionHandle &exprhdl,
		gpos::pointer<COrderSpec *> posRequired);

	// helper for checking if required sort columns come from outer child
	static BOOL FSortColsInOuterChild(CMemoryPool *mp,
									  CExpressionHandle &exprhdl,
									  gpos::pointer<COrderSpec *> pos);

	// helper for checking if the outer input of a binary join operator
	// includes the required columns
	static BOOL FOuterProvidesReqdCols(
		CExpressionHandle &exprhdl, gpos::pointer<CColRefSet *> pcrsRequired);


	// Do each of the given predicate children use columns from a different
	// join child?
	static BOOL FPredKeysSeparated(gpos::pointer<CExpression *> pexprOuter,
								   gpos::pointer<CExpression *> pexprInner,
								   gpos::pointer<CExpression *> pexprPredOuter,
								   gpos::pointer<CExpression *> pexprPredInner);

public:
	// match function
	BOOL Matches(gpos::pointer<COperator *> pop) const override;

	// sensitivity to order of inputs
	BOOL
	FInputOrderSensitive() const override
	{
		return true;
	}

	//-------------------------------------------------------------------------------------
	// Required Plan Properties
	//-------------------------------------------------------------------------------------

	// compute required output columns of the n-th child
	gpos::owner<CColRefSet *> PcrsRequired(
		CMemoryPool *mp, CExpressionHandle &exprhdl,
		gpos::pointer<CColRefSet *> pcrsRequired, ULONG child_index,
		gpos::pointer<CDrvdPropArray *> pdrgpdpCtxt, ULONG ulOptReq) override;

	// compute required ctes of the n-th child
	CCTEReq *PcteRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
						  gpos::pointer<CCTEReq *> pcter, ULONG child_index,
						  gpos::pointer<CDrvdPropArray *> pdrgpdpCtxt,
						  ULONG ulOptReq) const override;

	// compute required distribution of the n-th child
	gpos::owner<CDistributionSpec *> PdsRequired(
		CMemoryPool *mp, CExpressionHandle &exprhdl,
		gpos::pointer<CDistributionSpec *> pdsRequired, ULONG child_index,
		gpos::pointer<CDrvdPropArray *> pdrgpdpCtxt,
		ULONG ulOptReq) const override;

	gpos::owner<CEnfdDistribution *> Ped(
		CMemoryPool *mp, CExpressionHandle &exprhdl,
		gpos::pointer<CReqdPropPlan *> prppInput, ULONG child_index,
		gpos::pointer<CDrvdPropArray *> pdrgpdpCtxt, ULONG ulDistrReq) override;

	// check if required columns are included in output columns
	BOOL FProvidesReqdCols(CExpressionHandle &exprhdl,
						   gpos::pointer<CColRefSet *> pcrsRequired,
						   ULONG ulOptReq) const override;

	// distribution matching type
	CEnfdDistribution::EDistributionMatching Edm(
		gpos::pointer<CReqdPropPlan *> prppInput, ULONG child_index,
		gpos::pointer<CDrvdPropArray *> pdrgpdpCtxt, ULONG ulOptReq) override;

	//-------------------------------------------------------------------------------------
	// Derived Plan Properties
	//-------------------------------------------------------------------------------------

	// derive sort order from outer child
	gpos::owner<COrderSpec *>
	PosDerive(CMemoryPool *,  // mp
			  CExpressionHandle &exprhdl) const override
	{
		return PosDerivePassThruOuter(exprhdl);
	}

	// derive distribution
	gpos::owner<CDistributionSpec *> PdsDerive(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

	// derive rewindability
	gpos::owner<CRewindabilitySpec *> PrsDerive(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const override;


	//-------------------------------------------------------------------------------------
	// Enforced Properties
	//-------------------------------------------------------------------------------------

	// return rewindability property enforcing type for this operator
	CEnfdProp::EPropEnforcingType EpetRewindability(
		CExpressionHandle &exprhdl,
		gpos::pointer<const CEnfdRewindability *> per) const override;

	// return true if operator passes through stats obtained from children,
	// this is used when computing stats during costing
	BOOL
	FPassThruStats() const override
	{
		return false;
	}

	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------

	// is given predicate hash-join compatible
	static BOOL FHashJoinCompatible(gpos::pointer<CExpression *> pexprPred,
									gpos::pointer<CExpression *> pexprOuter,
									gpos::pointer<CExpression *> pexprInner);

	// is given predicate merge-join compatible
	static BOOL FMergeJoinCompatible(gpos::pointer<CExpression *> pexprPred,
									 gpos::pointer<CExpression *> pexprOuter,
									 gpos::pointer<CExpression *> pexprInner);

	// return number of distribution requests for correlated join
	static ULONG UlDistrRequestsForCorrelatedJoin();

	static void AlignJoinKeyOuterInner(
		gpos::pointer<CExpression *> pexprConjunct,
		gpos::pointer<CExpression *> pexprOuter,
		gpos::pointer<CExpression *> pexprInner, CExpression **ppexprKeyOuter,
		CExpression **ppexprKeyInner, IMDId **mdid_scop);

	static CRewindabilitySpec *PrsRequiredForNLJoinOuterChild(
		CMemoryPool *pmp, CExpressionHandle &exprhdl,
		CRewindabilitySpec *prsRequired);

};	// class CPhysicalJoin

}  // namespace gpopt

#endif	// !GPOPT_CPhysicalJoin_H

// EOF
