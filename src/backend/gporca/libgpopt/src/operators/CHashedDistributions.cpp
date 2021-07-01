//	Greenplum Database
//	Copyright (C) 2016 VMware, Inc. or its affiliates.

#include "gpopt/operators/CHashedDistributions.h"

#include "gpos/common/owner.h"

#include "gpopt/base/CUtils.h"

using namespace gpopt;
CHashedDistributions::CHashedDistributions(CMemoryPool *mp,
										   CColRefArray *pdrgpcrOutput,
										   CColRef2dArray *pdrgpdrgpcrInput)
	: CDistributionSpecArray(mp)
{
	const ULONG num_cols = pdrgpcrOutput->Size();
	const ULONG arity = pdrgpdrgpcrInput->Size();
	for (ULONG ulChild = 0; ulChild < arity; ulChild++)
	{
		CColRefArray *colref_array = (*pdrgpdrgpcrInput)[ulChild].get();
		gpos::Ref<CExpressionArray> pdrgpexpr =
			GPOS_NEW(mp) CExpressionArray(mp);
		for (ULONG ulCol = 0; ulCol < num_cols; ulCol++)
		{
			CColRef *colref = (*colref_array)[ulCol];
			gpos::Ref<CExpression> pexpr = CUtils::PexprScalarIdent(mp, colref);
			pdrgpexpr->Append(pexpr);
		}

		// create a hashed distribution on input columns of the current child
		BOOL fNullsColocated = true;
		gpos::Ref<CDistributionSpec> pdshashed =
			GPOS_NEW(mp) CDistributionSpecHashed(pdrgpexpr, fNullsColocated);
		Append(pdshashed);
	}
}
