//	Greenplum Database
//	Copyright (C) 2016 VMware, Inc. or its affiliates.

#ifndef GPOPT_CPhysicalSerialUnionAll_H
#define GPOPT_CPhysicalSerialUnionAll_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/operators/CPhysicalUnionAll.h"

namespace gpopt
{
// fwd declaration
class CDistributionSpecHashed;

//---------------------------------------------------------------------------
//	@class:
//		CPhysicalSerialUnionAll
//
//	@doc:
//		Physical union all operator. Executes each child serially.
//
//---------------------------------------------------------------------------
class CPhysicalSerialUnionAll : public CPhysicalUnionAll
{
private:
public:
	CPhysicalSerialUnionAll(const CPhysicalSerialUnionAll &) = delete;

	// ctor
	CPhysicalSerialUnionAll(CMemoryPool *mp,
							gpos::owner<CColRefArray *> pdrgpcrOutput,
							gpos::owner<CColRef2dArray *> pdrgpdrgpcrInput);

	// dtor
	~CPhysicalSerialUnionAll() override;

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopPhysicalSerialUnionAll;
	}

	const CHAR *
	SzId() const override
	{
		return "CPhysicalSerialUnionAll";
	}

	// distribution matching type
	CEnfdDistribution::EDistributionMatching
	Edm(gpos::pointer<CReqdPropPlan *> prppInput,
		ULONG,							  // child_index
		gpos::pointer<CDrvdPropArray *>,  //pdrgpdpCtxt
		ULONG ulOptReq) override
	{
		if (0 == ulOptReq && CDistributionSpec::EdtHashed ==
								 prppInput->Ped()->PdsRequired()->Edt())
		{
			// use exact matching if optimizing first request
			return CEnfdDistribution::EdmExact;
		}

		// use relaxed matching if optimizing other requests
		return CEnfdDistribution::EdmSatisfy;
	}


	// compute required distribution of the n-th child
	gpos::owner<CDistributionSpec *> PdsRequired(
		CMemoryPool *mp, CExpressionHandle &exprhdl,
		gpos::pointer<CDistributionSpec *> pdsRequired, ULONG child_index,
		gpos::pointer<CDrvdPropArray *> pdrgpdpCtxt,
		ULONG ulOptReq) const override;

};	// class CPhysicalSerialUnionAll

}  // namespace gpopt

#endif	// !GPOPT_CPhysicalSerialUnionAll_H

// EOF
