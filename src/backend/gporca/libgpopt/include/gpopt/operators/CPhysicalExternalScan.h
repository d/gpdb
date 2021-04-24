//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 VMware, Inc. or its affiliates.
//
//	@filename:
//		CPhysicalExternalScan.h
//
//	@doc:
//		External scan operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalExternalScan_H
#define GPOPT_CPhysicalExternalScan_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/operators/CPhysicalTableScan.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalExternalScan
//
//	@doc:
//		External scan operator
//
//---------------------------------------------------------------------------
class CPhysicalExternalScan : public CPhysicalTableScan
{
private:
public:
	CPhysicalExternalScan(const CPhysicalExternalScan &) = delete;

	// ctor
	CPhysicalExternalScan(CMemoryPool *, const CName *,
						  gpos::owner<CTableDescriptor *>, CColRefArray *);

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopPhysicalExternalScan;
	}

	// return a string for operator name
	const CHAR *
	SzId() const override
	{
		return "CPhysicalExternalScan";
	}

	// match function
	BOOL Matches(gpos::pointer<COperator *>) const override;

	//-------------------------------------------------------------------------------------
	// Derived Plan Properties
	//-------------------------------------------------------------------------------------

	// derive rewindability
	gpos::owner<CRewindabilitySpec *>
	PrsDerive(CMemoryPool *mp,
			  CExpressionHandle &  // exprhdl
	) const override
	{
		// external tables are neither rewindable nor rescannable
		return GPOS_NEW(mp) CRewindabilitySpec(
			CRewindabilitySpec::ErtNone, CRewindabilitySpec::EmhtNoMotion);
	}

	//-------------------------------------------------------------------------------------
	// Enforced Properties
	//-------------------------------------------------------------------------------------

	// return rewindability property enforcing type for this operator
	CEnfdProp::EPropEnforcingType EpetRewindability(
		CExpressionHandle &exprhdl,
		gpos::pointer<const CEnfdRewindability *> per) const override;

	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------

	// conversion function
	static gpos::cast_func<CPhysicalExternalScan *>
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(EopPhysicalExternalScan == pop->Eopid());

		return dynamic_cast<CPhysicalExternalScan *>(pop);
	}

};	// class CPhysicalExternalScan

}  // namespace gpopt

#endif	// !GPOPT_CPhysicalExternalScan_H

// EOF
