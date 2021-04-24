//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalComputeScalar.h
//
//	@doc:
//		ComputeScalar operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalComputeScalar_H
#define GPOPT_CPhysicalComputeScalar_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/operators/CPhysical.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalComputeScalar
//
//	@doc:
//		ComputeScalar operator
//
//---------------------------------------------------------------------------
class CPhysicalComputeScalar : public CPhysical
{
private:
public:
	CPhysicalComputeScalar(const CPhysicalComputeScalar &) = delete;

	// ctor
	explicit CPhysicalComputeScalar(CMemoryPool *mp);

	// dtor
	~CPhysicalComputeScalar() override;

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopPhysicalComputeScalar;
	}

	const CHAR *
	SzId() const override
	{
		return "CPhysicalComputeScalar";
	}

	// match function
	BOOL Matches(gpos::pointer<COperator *>) const override;

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

	// compute required sort order of the n-th child
	gpos::owner<COrderSpec *> PosRequired(
		CMemoryPool *mp, CExpressionHandle &exprhdl,
		gpos::pointer<COrderSpec *> posRequired, ULONG child_index,
		gpos::pointer<CDrvdPropArray *> pdrgpdpCtxt,
		ULONG ulOptReq) const override;

	// compute required distribution of the n-th child
	gpos::owner<CDistributionSpec *> PdsRequired(
		CMemoryPool *mp, CExpressionHandle &exprhdl,
		gpos::pointer<CDistributionSpec *> pdsRequired, ULONG child_index,
		gpos::pointer<CDrvdPropArray *> pdrgpdpCtxt,
		ULONG ulOptReq) const override;

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

	// derive sort order
	gpos::owner<COrderSpec *> PosDerive(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

	// derive distribution
	gpos::owner<CDistributionSpec *> PdsDerive(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

	// derive rewindability
	gpos::owner<CRewindabilitySpec *> PrsDerive(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

	//-------------------------------------------------------------------------------------
	// Enforced Properties
	//-------------------------------------------------------------------------------------

	// return order property enforcing type for this operator
	CEnfdProp::EPropEnforcingType EpetOrder(
		CExpressionHandle &exprhdl,
		gpos::pointer<const CEnfdOrder *> peo) const override;

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
	static gpos::cast_func<CPhysicalComputeScalar *>
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(EopPhysicalComputeScalar == pop->Eopid());

		return dynamic_cast<CPhysicalComputeScalar *>(pop);
	}

};	// class CPhysicalComputeScalar

}  // namespace gpopt

#endif	// !GPOPT_CPhysicalComputeScalar_H

// EOF
