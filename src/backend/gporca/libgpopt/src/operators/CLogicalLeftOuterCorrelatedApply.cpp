//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalLeftOuterCorrelatedApply.cpp
//
//	@doc:
//		Implementation of left outer correlated apply operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CLogicalLeftOuterCorrelatedApply.h"

#include "gpos/base.h"
#include "gpos/common/owner.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftOuterCorrelatedApply::CLogicalLeftOuterCorrelatedApply
//
//	@doc:
//		Ctor - for patterns
//
//---------------------------------------------------------------------------
CLogicalLeftOuterCorrelatedApply::CLogicalLeftOuterCorrelatedApply(
	CMemoryPool *mp)
	: CLogicalLeftOuterApply(mp)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftOuterCorrelatedApply::CLogicalLeftOuterCorrelatedApply
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalLeftOuterCorrelatedApply::CLogicalLeftOuterCorrelatedApply(
	CMemoryPool *mp, gpos::owner<CColRefArray *> pdrgpcrInner,
	EOperatorId eopidOriginSubq)
	: CLogicalLeftOuterApply(mp, std::move(pdrgpcrInner), eopidOriginSubq)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftOuterCorrelatedApply::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
gpos::owner<CXformSet *>
CLogicalLeftOuterCorrelatedApply::PxfsCandidates(CMemoryPool *mp) const
{
	gpos::owner<CXformSet *> xform_set = GPOS_NEW(mp) CXformSet(mp);
	(void) xform_set->ExchangeSet(CXform::ExfImplementLeftOuterCorrelatedApply);

	return xform_set;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftOuterCorrelatedApply::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CLogicalLeftOuterCorrelatedApply::Matches(gpos::pointer<COperator *> pop) const
{
	if (pop->Eopid() == Eopid())
	{
		return m_pdrgpcrInner->Equals(
			gpos::dyn_cast<CLogicalLeftOuterCorrelatedApply>(pop)
				->PdrgPcrInner());
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftOuterCorrelatedApply::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
gpos::owner<COperator *>
CLogicalLeftOuterCorrelatedApply::PopCopyWithRemappedColumns(
	CMemoryPool *mp, gpos::pointer<UlongToColRefMap *> colref_mapping,
	BOOL must_exist)
{
	gpos::owner<CColRefArray *> pdrgpcrInner =
		CUtils::PdrgpcrRemap(mp, m_pdrgpcrInner, colref_mapping, must_exist);

	return GPOS_NEW(mp) CLogicalLeftOuterCorrelatedApply(
		mp, std::move(pdrgpcrInner), m_eopidOriginSubq);
}

// EOF
