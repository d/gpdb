//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDistributionSpecExternal.cpp
//
//	@doc:
//		Specification of external distribution, where data stored externally
//---------------------------------------------------------------------------

#include "gpopt/base/CDistributionSpecExternal.h"

#include "gpos/common/owner.h"

#include "gpopt/base/CUtils.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecExternal::CDistributionSpecExternal
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDistributionSpecExternal::CDistributionSpecExternal() = default;

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecExternal::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CDistributionSpecExternal::Matches(
	gpos::pointer<const CDistributionSpec *> pds) const
{
	return (Edt() == pds->Edt());
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecExternal::FSatisfies
//
//	@doc:
//		Check if this distribution spec satisfies the given one
//
//---------------------------------------------------------------------------
BOOL
CDistributionSpecExternal::FSatisfies(
	gpos::pointer<const CDistributionSpec *> pds) const
{
	if (Matches(pds))
	{
		return true;
	}

	return EdtAny == pds->Edt();
}

void
CDistributionSpecExternal::AppendEnforcers(CMemoryPool *, CExpressionHandle &,
										   gpos::pointer<CReqdPropPlan *>,
										   gpos::pointer<CExpressionArray *>,
										   gpos::pointer<CExpression *>)
{
	GPOS_ASSERT(!"EXTERNAL distribution cannot be enforced, it's derive only.");
}

CDistributionSpec::EDistributionPartitioningType
CDistributionSpecExternal::Edpt() const
{
	return EdptPartitioned;
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecExternal::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream &
CDistributionSpecExternal::OsPrint(IOstream &os) const
{
	return os << this->SzId();
}

// EOF
