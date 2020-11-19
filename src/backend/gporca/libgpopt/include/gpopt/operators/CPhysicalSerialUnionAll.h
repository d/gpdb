//	Greenplum Database
//	Copyright (C) 2016 Pivotal Software, Inc.

#ifndef GPOPT_CPhysicalSerialUnionAll_H
#define GPOPT_CPhysicalSerialUnionAll_H

#include "gpos/base.h"
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
	CPhysicalSerialUnionAll(CMemoryPool *mp, CColRefArray *pdrgpcrOutput,
							CColRef2dArray *pdrgpdrgpcrInput,
							ULONG ulScanIdPartialIndex);

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


	CEnfdDistribution *Ped(CMemoryPool *mp, CExpressionHandle &exprhdl,
						   CReqdPropPlan *prppInput, ULONG child_index,
						   CDrvdPropArray *pdrgpdpCtxt,
						   ULONG ulDistrReq) const override;

};	// class CPhysicalSerialUnionAll

}  // namespace gpopt

#endif	// !GPOPT_CPhysicalSerialUnionAll_H
