//	Greenplum Database
//	Copyright (C) 2016 VMware, Inc. or its affiliates.

#include "gpopt/base/CDistributionSpecUniversal.h"

#include "gpos/common/owner.h"

namespace gpopt
{
gpopt::CDistributionSpecUniversal::CDistributionSpecUniversal() = default;

CDistributionSpec::EDistributionType
CDistributionSpecUniversal::Edt() const
{
	return CDistributionSpec::EdtUniversal;
}

BOOL
CDistributionSpecUniversal::FSatisfies(
	gpos::pointer<const CDistributionSpec *> pds) const
{
	// universal distribution does not satisfy duplicate-sensitive
	// hash distributions
	if (CDistributionSpec::EdtHashed == pds->Edt() &&
		(CDistributionSpecHashed::PdsConvert(pds))->IsDuplicateSensitive())
	{
		return false;
	}

	// universal distribution does not satisfy duplicate-sensitive
	// random distributions
	if (CDistributionSpec::EdtRandom == pds->Edt() &&
		(CDistributionSpecRandom::PdsConvert(pds))->IsDuplicateSensitive())
	{
		return false;
	}

	if (CDistributionSpec::EdtNonSingleton == pds->Edt())
	{
		// universal distribution does not satisfy non-singleton distribution
		return false;
	}

	return true;
}

BOOL
CDistributionSpecUniversal::FRequirable() const
{
	return false;
}

BOOL
CDistributionSpecUniversal::Matches(
	gpos::pointer<const CDistributionSpec *> pds) const
{
	// universal distribution needs to match replicated / singleton requests
	// to avoid generating duplicates
	EDistributionType edt = pds->Edt();
	return (CDistributionSpec::EdtUniversal == edt ||
			CDistributionSpec::EdtSingleton == edt ||
			CDistributionSpec::EdtStrictReplicated == edt ||
			CDistributionSpec::EdtReplicated == edt);
}

void
CDistributionSpecUniversal::AppendEnforcers(CMemoryPool *, CExpressionHandle &,
											gpos::pointer<CReqdPropPlan *>,
											gpos::pointer<CExpressionArray *>,
											gpos::pointer<CExpression *>)
{
	GPOS_ASSERT(!"attempt to enforce UNIVERSAL distribution");
}

IOstream &
CDistributionSpecUniversal::OsPrint(IOstream &os) const
{
	return os << "UNIVERSAL ";
}

CDistributionSpec::EDistributionPartitioningType
CDistributionSpecUniversal::Edpt() const
{
	return EdptNonPartitioned;
}

gpos::cast_func<CDistributionSpecUniversal *>
CDistributionSpecUniversal::PdsConvert(CDistributionSpec *pds)
{
	GPOS_ASSERT(nullptr != pds);
	GPOS_ASSERT(EdtAny == pds->Edt());

	return dynamic_cast<CDistributionSpecUniversal *>(pds);
}
}  // namespace gpopt
