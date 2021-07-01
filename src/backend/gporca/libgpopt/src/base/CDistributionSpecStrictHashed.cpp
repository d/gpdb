//	Greenplum Database
//	Copyright (C) 2016 VMware, Inc. or its affiliates.

#include "gpopt/base/CDistributionSpecStrictHashed.h"

#include "gpos/common/owner.h"

namespace gpopt
{
CDistributionSpecStrictHashed::CDistributionSpecStrictHashed(
	gpos::owner<CExpressionArray *> pdrgpexpr, BOOL fNullsColocated)
	: CDistributionSpecHashed(std::move(pdrgpexpr), fNullsColocated)
{
}

CDistributionSpec::EDistributionType
CDistributionSpecStrictHashed::Edt() const
{
	return CDistributionSpec::EdtStrictHashed;
}

}  // namespace gpopt
