//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalDynamicGet.cpp
//
//	@doc:
//		Implementation of dynamic table access
//---------------------------------------------------------------------------

#include "gpopt/operators/CLogicalDynamicGet.h"

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CColRefTable.h"
#include "gpopt/base/CConstraintInterval.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/metadata/CName.h"
#include "gpopt/metadata/CPartConstraint.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "naucrates/statistics/CStatistics.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGet::CLogicalDynamicGet
//
//	@doc:
//		ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalDynamicGet::CLogicalDynamicGet(CMemoryPool *mp)
	: CLogicalDynamicGetBase(mp)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGet::CLogicalDynamicGet
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CLogicalDynamicGet::CLogicalDynamicGet(
	CMemoryPool *mp, const CName *pnameAlias,
	gpos::Ref<CTableDescriptor> ptabdesc, ULONG ulPartIndex,
	gpos::Ref<CColRefArray> pdrgpcrOutput,
	gpos::Ref<CColRef2dArray> pdrgpdrgpcrPart,
	gpos::Ref<IMdIdArray> partition_mdids)
	: CLogicalDynamicGetBase(mp, pnameAlias, std::move(ptabdesc), ulPartIndex,
							 std::move(pdrgpcrOutput),
							 std::move(pdrgpdrgpcrPart),
							 std::move(partition_mdids))
{
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGet::CLogicalDynamicGet
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CLogicalDynamicGet::CLogicalDynamicGet(CMemoryPool *mp, const CName *pnameAlias,
									   gpos::Ref<CTableDescriptor> ptabdesc,
									   ULONG ulPartIndex,
									   gpos::Ref<IMdIdArray> partition_mdids)
	: CLogicalDynamicGetBase(mp, pnameAlias, std::move(ptabdesc), ulPartIndex,
							 std::move(partition_mdids))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGet::~CLogicalDynamicGet
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CLogicalDynamicGet::~CLogicalDynamicGet() = default;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGet::HashValue
//
//	@doc:
//		Operator specific hash function
//
//---------------------------------------------------------------------------
ULONG
CLogicalDynamicGet::HashValue() const
{
	ULONG ulHash = gpos::CombineHashes(COperator::HashValue(),
									   m_ptabdesc->MDId()->HashValue());
	ulHash = gpos::CombineHashes(ulHash,
								 CUtils::UlHashColArray(m_pdrgpcrOutput.get()));

	return ulHash;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGet::Matches
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CLogicalDynamicGet::Matches(COperator *pop) const
{
	return CUtils::FMatchDynamicScan(this, pop);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGet::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
gpos::Ref<COperator>
CLogicalDynamicGet::PopCopyWithRemappedColumns(CMemoryPool *mp,
											   UlongToColRefMap *colref_mapping,
											   BOOL must_exist)
{
	gpos::Ref<CColRefArray> pdrgpcrOutput = nullptr;
	if (must_exist)
	{
		pdrgpcrOutput = CUtils::PdrgpcrRemapAndCreate(mp, m_pdrgpcrOutput.get(),
													  colref_mapping);
	}
	else
	{
		pdrgpcrOutput = CUtils::PdrgpcrRemap(mp, m_pdrgpcrOutput.get(),
											 colref_mapping, must_exist);
	}
	gpos::Ref<CColRef2dArray> pdrgpdrgpcrPart = PdrgpdrgpcrCreatePartCols(
		mp, pdrgpcrOutput.get(), m_ptabdesc->PdrgpulPart());
	CName *pnameAlias = GPOS_NEW(mp) CName(mp, *m_pnameAlias);
	;
	;

	return GPOS_NEW(mp) CLogicalDynamicGet(
		mp, pnameAlias, m_ptabdesc, m_scan_id, std::move(pdrgpcrOutput),
		std::move(pdrgpdrgpcrPart), m_partition_mdids);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGet::FInputOrderSensitive
//
//	@doc:
//		Not called for leaf operators
//
//---------------------------------------------------------------------------
BOOL
CLogicalDynamicGet::FInputOrderSensitive() const
{
	GPOS_ASSERT(!"Unexpected function call of FInputOrderSensitive");
	return false;
}

CMaxCard
CLogicalDynamicGet::DeriveMaxCard(CMemoryPool *mp,
								  CExpressionHandle &exprhdl) const
{
	if (nullptr == GetPartitionMdids() || GetPartitionMdids()->Size() == 0)
	{
		return CMaxCard(0);
	}

	return CLogical::DeriveMaxCard(mp, exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGet::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
gpos::Ref<CXformSet>
CLogicalDynamicGet::PxfsCandidates(CMemoryPool *mp) const
{
	gpos::Ref<CXformSet> xform_set = GPOS_NEW(mp) CXformSet(mp);
	(void) xform_set->ExchangeSet(CXform::ExfDynamicGet2DynamicTableScan);
	return xform_set;
}



//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGet::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalDynamicGet::OsPrint(IOstream &os) const
{
	if (m_fPattern)
	{
		return COperator::OsPrint(os);
	}
	else
	{
		os << SzId() << " ";
		// alias of table as referenced in the query
		m_pnameAlias->OsPrint(os);

		// actual name of table in catalog and columns
		os << " (";
		m_ptabdesc->Name().OsPrint(os);
		os << "), ";
		os << "Columns: [";
		CUtils::OsPrintDrgPcr(os, m_pdrgpcrOutput.get());
		os << "] Scan Id: " << m_scan_id;
	}

	return os;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGet::PstatsDerive
//
//	@doc:
//		Load up statistics from metadata
//
//---------------------------------------------------------------------------
gpos::Ref<IStatistics>
CLogicalDynamicGet::PstatsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl,
								 IStatisticsArray *	 // not used
) const
{
	CReqdPropRelational *prprel =
		gpos::dyn_cast<CReqdPropRelational>(exprhdl.Prp());
	gpos::Ref<IStatistics> stats =
		PstatsDeriveFilter(mp, exprhdl, prprel->PexprPartPred());

	gpos::Ref<CColRefSet> pcrs =
		GPOS_NEW(mp) CColRefSet(mp, m_pdrgpcrOutput.get());
	CUpperBoundNDVs *upper_bound_NDVs =
		GPOS_NEW(mp) CUpperBoundNDVs(std::move(pcrs), stats->Rows());
	gpos::dyn_cast<CStatistics>(stats)->AddCardUpperBound(upper_bound_NDVs);

	return stats;
}

// EOF
