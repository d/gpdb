//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CLogicalLeftAntiSemiCorrelatedApplyNotIn.cpp
//
//	@doc:
//		Implementation of left anti semi correlated apply with NOT-IN/ANY semantics
//---------------------------------------------------------------------------

#include "gpopt/operators/CLogicalLeftAntiSemiCorrelatedApplyNotIn.h"

#include "gpos/base.h"
#include "gpos/common/owner.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftAntiSemiCorrelatedApplyNotIn::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
gpos::Ref<CXformSet>
CLogicalLeftAntiSemiCorrelatedApplyNotIn::PxfsCandidates(CMemoryPool *mp) const
{
	gpos::Ref<CXformSet> xform_set = GPOS_NEW(mp) CXformSet(mp);
	(void) xform_set->ExchangeSet(
		CXform::ExfImplementLeftAntiSemiCorrelatedApplyNotIn);

	return xform_set;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftAntiSemiCorrelatedApplyNotIn::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
gpos::Ref<COperator>
CLogicalLeftAntiSemiCorrelatedApplyNotIn::PopCopyWithRemappedColumns(
	CMemoryPool *mp, UlongToColRefMap *colref_mapping, BOOL must_exist)
{
	gpos::Ref<CColRefArray> pdrgpcrInner = CUtils::PdrgpcrRemap(
		mp, m_pdrgpcrInner.get(), colref_mapping, must_exist);

	return GPOS_NEW(mp) CLogicalLeftAntiSemiCorrelatedApplyNotIn(
		mp, std::move(pdrgpcrInner), m_eopidOriginSubq);
}


// EOF
