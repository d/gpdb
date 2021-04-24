//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalAgg.h
//
//	@doc:
//		Basic physical aggregate operator
//---------------------------------------------------------------------------
#ifndef GPOS_CPhysicalAgg_H
#define GPOS_CPhysicalAgg_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/base/CDistributionSpec.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CLogicalGbAgg.h"
#include "gpopt/operators/CPhysical.h"


namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalAgg
//
//	@doc:
//		Aggregate operator
//
//---------------------------------------------------------------------------
class CPhysicalAgg : public CPhysical
{
private:
	// array of grouping columns
	gpos::owner<CColRefArray *> m_pdrgpcr;

	// aggregate type (local / intermediate / global)
	COperator::EGbAggType m_egbaggtype;

	BOOL m_isAggFromSplitDQA;

	CLogicalGbAgg::EAggStage m_aggStage;

	// compute required distribution of the n-th child of an intermediate aggregate
	gpos::owner<CDistributionSpec *> PdsRequiredIntermediateAgg(
		CMemoryPool *mp, ULONG ulOptReq) const;

	// compute required distribution of the n-th child of a global aggregate
	gpos::owner<CDistributionSpec *> PdsRequiredGlobalAgg(
		CMemoryPool *mp, CExpressionHandle &exprhdl,
		gpos::pointer<CDistributionSpec *> pdsInput, ULONG child_index,
		gpos::pointer<CColRefArray *> pdrgpcrGrp,
		gpos::pointer<CColRefArray *> pdrgpcrGrpMinimal, ULONG ulOptReq) const;

	// compute a maximal hashed distribution using the given columns,
	// if no such distribution can be created, return a Singleton distribution
	static gpos::owner<CDistributionSpec *> PdsMaximalHashed(
		CMemoryPool *mp, gpos::pointer<CColRefArray *> colref_array);

protected:
	// array of minimal grouping columns based on FDs
	gpos::owner<CColRefArray *> m_pdrgpcrMinimal;

	// could the local / intermediate / global aggregate generate
	// duplicate values for the same group across segments
	BOOL m_fGeneratesDuplicates;

	// array of columns used in distinct qualified aggregates (DQA)
	// used only in the case of intermediate aggregates
	gpos::owner<CColRefArray *> m_pdrgpcrArgDQA;

	// is agg part of multi-stage aggregation
	BOOL m_fMultiStage;

	// should distribution enforcement be enabled on this agg?
	// By default, global and local aggregate are created with same
	// grouping columns. In such cases, if local derives the same
	// distribution as global then we need no motion in-between, which
	// implies that a single aggregate is enough. Hence, such plans are
	// prohibited. In CXformEagerAgg, however, the local agg is created
	// with different grouping columns but can have the same
	// distribution as the global. We don't need to prohibit such plans,
	// since the global agg is applied with different grouping columns
	// compared to to the local and is still necessary.
	BOOL m_should_enforce_distribution;

	// compute required columns of the n-th child
	gpos::owner<CColRefSet *> PcrsRequiredAgg(
		CMemoryPool *mp, CExpressionHandle &exprhdl,
		gpos::pointer<CColRefSet *> pcrsRequired, ULONG child_index,
		gpos::pointer<CColRefArray *> pdrgpcrGrp);

	// compute required distribution of the n-th child
	gpos::owner<CDistributionSpec *> PdsRequiredAgg(
		CMemoryPool *mp, CExpressionHandle &exprhdl,
		gpos::pointer<CDistributionSpec *> pdsInput, ULONG child_index,
		ULONG ulOptReq, gpos::pointer<CColRefArray *> pdrgpcgGrp,
		gpos::pointer<CColRefArray *> pdrgpcrGrpMinimal) const;

public:
	CPhysicalAgg(const CPhysicalAgg &) = delete;

	// ctor
	CPhysicalAgg(CMemoryPool *mp, gpos::owner<CColRefArray *> colref_array,
				 gpos::pointer<CColRefArray *>
					 pdrgpcrMinimal,  // FD's on grouping columns
				 COperator::EGbAggType egbaggtype, BOOL fGeneratesDuplicates,
				 gpos::owner<CColRefArray *> pdrgpcrArgDQA, BOOL fMultiStage,
				 BOOL isAggFromSplitDQA, CLogicalGbAgg::EAggStage aggStage,
				 BOOL should_enforce_distribution);

	// is this agg generated by CXformSplitDQA
	BOOL IsAggFromSplitDQA() const;

	// is this part of Two Stage Scalar DQA
	BOOL IsTwoStageScalarDQA() const;

	// is this part of Three Stage Scalar DQA
	BOOL IsThreeStageScalarDQA() const;

	// dtor
	~CPhysicalAgg() override;

	// does this aggregate generate duplicate values for the same group
	virtual BOOL
	FGeneratesDuplicates() const
	{
		return m_fGeneratesDuplicates;
	}

	virtual gpos::pointer<const CColRefArray *>
	PdrgpcrGroupingCols() const
	{
		return m_pdrgpcr;
	}

	// array of columns used in distinct qualified aggregates (DQA)
	virtual gpos::pointer<const CColRefArray *>
	PdrgpcrArgDQA() const
	{
		return m_pdrgpcrArgDQA;
	}

	// aggregate type
	COperator::EGbAggType
	Egbaggtype() const
	{
		return m_egbaggtype;
	}

	// is a global aggregate?
	BOOL
	FGlobal() const
	{
		return (COperator::EgbaggtypeGlobal == m_egbaggtype);
	}

	// is agg part of multi-stage aggregation
	BOOL
	FMultiStage() const
	{
		return m_fMultiStage;
	}

	// match function
	BOOL Matches(gpos::pointer<COperator *> pop) const override;

	// hash function
	ULONG HashValue() const override;

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
	gpos::owner<CCTEReq *> PcteRequired(
		CMemoryPool *mp, CExpressionHandle &exprhdl,
		gpos::pointer<CCTEReq *> pcter, ULONG child_index,
		gpos::pointer<CDrvdPropArray *> pdrgpdpCtxt,
		ULONG ulOptReq) const override;

	// compute required distribution of the n-th child
	gpos::owner<CDistributionSpec *>
	PdsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
				gpos::pointer<CDistributionSpec *> pdsRequired,
				ULONG child_index,
				gpos::pointer<CDrvdPropArray *>,  //pdrgpdpCtxt,
				ULONG ulOptReq) const override
	{
		return PdsRequiredAgg(mp, exprhdl, pdsRequired, child_index, ulOptReq,
							  m_pdrgpcr, m_pdrgpcrMinimal);
	}

	// compute required rewindability of the n-th child
	gpos::owner<CRewindabilitySpec *> PrsRequired(
		CMemoryPool *mp, CExpressionHandle &exprhdl,
		gpos::pointer<CRewindabilitySpec *> prsRequired, ULONG child_index,
		gpos::pointer<CDrvdPropArray *> pdrgpdpCtxt,
		ULONG ulOptReq) const override;

	// check if required columns are included in output columns
	BOOL FProvidesReqdCols(CExpressionHandle &exprhdl,
						   gpos::pointer<CColRefSet *> pcrsRequired,
						   ULONG ulOptReq) const override;


	//-------------------------------------------------------------------------------------
	// Derived Plan Properties
	//-------------------------------------------------------------------------------------

	// derive distribution
	gpos::owner<CDistributionSpec *> PdsDerive(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

	// derive rewindability
	gpos::owner<CRewindabilitySpec *> PrsDerive(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

	//-------------------------------------------------------------------------------------
	// Enforced Properties
	//-------------------------------------------------------------------------------------

	// return distribution property enforcing type for this operator
	CEnfdProp::EPropEnforcingType EpetDistribution(
		CExpressionHandle &exprhdl,
		gpos::pointer<const CEnfdDistribution *> ped) const override;

	// return rewindability property enforcing type for this operator
	CEnfdProp::EPropEnforcingType EpetRewindability(
		CExpressionHandle &,					   // exprhdl
		gpos::pointer<const CEnfdRewindability *>  // per
	) const override;

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

	// conversion function
	static gpos::cast_func<CPhysicalAgg *>
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(CUtils::FPhysicalAgg(pop));

		return dynamic_cast<CPhysicalAgg *>(pop);
	}

	// debug print
	IOstream &OsPrint(IOstream &os) const override;

};	// class CPhysicalAgg

}  // namespace gpopt


#endif	// !GPOS_CPhysicalAgg_H

// EOF
