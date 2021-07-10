//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CPhysicalCorrelatedNotInLeftAntiSemiNLJoin.h
//
//	@doc:
//		Physical Left Anti Semi NLJ operator capturing correlated execution
//		with NOT-IN/ALL semantics
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalCorrelatedNotInLeftAntiSemiNLJoin_H
#define GPOPT_CPhysicalCorrelatedNotInLeftAntiSemiNLJoin_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/base/CDistributionSpecReplicated.h"
#include "gpopt/operators/CPhysicalLeftAntiSemiNLJoinNotIn.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalCorrelatedNotInLeftAntiSemiNLJoin
//
//	@doc:
//		Physical left semi NLJ operator capturing correlated execution with
//		ANY/IN semantics
//
//---------------------------------------------------------------------------
class CPhysicalCorrelatedNotInLeftAntiSemiNLJoin
	: public CPhysicalLeftAntiSemiNLJoinNotIn
{
private:
	// columns from inner child used in correlated execution
	gpos::owner<CColRefArray *> m_pdrgpcrInner;

	// origin subquery id
	EOperatorId m_eopidOriginSubq;

public:
	CPhysicalCorrelatedNotInLeftAntiSemiNLJoin(
		const CPhysicalCorrelatedNotInLeftAntiSemiNLJoin &) = delete;

	// ctor
	CPhysicalCorrelatedNotInLeftAntiSemiNLJoin(
		CMemoryPool *mp, gpos::owner<CColRefArray *> pdrgpcrInner,
		EOperatorId eopidOriginSubq)
		: CPhysicalLeftAntiSemiNLJoinNotIn(mp),
		  m_pdrgpcrInner(std::move(pdrgpcrInner)),
		  m_eopidOriginSubq(eopidOriginSubq)
	{
		GPOS_ASSERT(nullptr != m_pdrgpcrInner);

		SetDistrRequests(UlDistrRequestsForCorrelatedJoin());
		GPOS_ASSERT(0 < UlDistrRequests());
	}

	// dtor
	~CPhysicalCorrelatedNotInLeftAntiSemiNLJoin() override
	{
		m_pdrgpcrInner->Release();
	}

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopPhysicalCorrelatedNotInLeftAntiSemiNLJoin;
	}

	// return a string for operator name
	const CHAR *
	SzId() const override
	{
		return "CPhysicalCorrelatedNotInLeftAntiSemiNLJoin";
	}

	// match function
	BOOL
	Matches(gpos::pointer<COperator *> pop) const override
	{
		if (pop->Eopid() == Eopid())
		{
			return m_pdrgpcrInner->Equals(
				gpos::dyn_cast<CPhysicalCorrelatedNotInLeftAntiSemiNLJoin>(pop)
					->PdrgPcrInner());
		}

		return false;
	}

	// distribution matching type
	CEnfdDistribution::EDistributionMatching Edm(
		gpos::pointer<CReqdPropPlan *>,	  // prppInput
		ULONG,							  // child_index
		gpos::pointer<CDrvdPropArray *>,  //pdrgpdpCtxt
		ULONG							  // ulOptReq
		) override
	{
		return CEnfdDistribution::EdmSatisfy;
	}

	gpos::owner<CEnfdDistribution *>
	Ped(CMemoryPool *mp, CExpressionHandle &exprhdl,
		gpos::pointer<CReqdPropPlan *> prppInput, ULONG child_index,
		gpos::pointer<CDrvdPropArray *> pdrgpdpCtxt, ULONG ulOptReq) override
	{
		return PedCorrelatedJoin(mp, exprhdl, prppInput, child_index,
								 pdrgpdpCtxt, ulOptReq);
	}

	// compute required distribution of the n-th child
	CDistributionSpec *
	PdsRequired(CMemoryPool *,						 // mp
				CExpressionHandle &,				 // exprhdl,
				gpos::pointer<CDistributionSpec *>,	 // pdsRequired,
				ULONG,								 // child_index,
				gpos::pointer<CDrvdPropArray *>,	 // pdrgpdpCtxt,
				ULONG								 //ulOptReq
	) const override
	{
		GPOS_RAISE(
			CException::ExmaInvalid, CException::ExmiInvalid,
			GPOS_WSZ_LIT(
				"PdsRequired should not be called for CPhysicalCorrelatedNotInLeftAntiSemiNLJoin"));
		return nullptr;
	}

	// compute required rewindability of the n-th child
	gpos::owner<CRewindabilitySpec *>
	PrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
				gpos::pointer<CRewindabilitySpec *> prsRequired,
				ULONG child_index, gpos::pointer<CDrvdPropArray *> pdrgpdpCtxt,
				ULONG ulOptReq) const override
	{
		return PrsRequiredCorrelatedJoin(mp, exprhdl, prsRequired, child_index,
										 pdrgpdpCtxt, ulOptReq);
	}

	// conversion function
	static gpos::cast_func<CPhysicalCorrelatedNotInLeftAntiSemiNLJoin *>
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(EopPhysicalCorrelatedNotInLeftAntiSemiNLJoin ==
					pop->Eopid());

		return dynamic_cast<CPhysicalCorrelatedNotInLeftAntiSemiNLJoin *>(pop);
	}

	// return true if operator is a correlated NL Join
	BOOL
	FCorrelated() const override
	{
		return true;
	}

	// return required inner columns
	gpos::pointer<CColRefArray *>
	PdrgPcrInner() const override
	{
		return m_pdrgpcrInner;
	}


	// origin subquery id
	EOperatorId
	EopidOriginSubq() const
	{
		return m_eopidOriginSubq;
	}

	// print
	IOstream &
	OsPrint(IOstream &os) const override
	{
		os << this->SzId() << "(";
		(void) CUtils::OsPrintDrgPcr(os, m_pdrgpcrInner);
		os << ")";

		return os;
	}

};	// class CPhysicalCorrelatedNotInLeftAntiSemiNLJoin

}  // namespace gpopt


#endif	// !GPOPT_CPhysicalCorrelatedNotInLeftAntiSemiNLJoin_H

// EOF
