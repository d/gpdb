//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CLogicalDynamicGetBase.cpp
//
//	@doc:
//		Implementation of dynamic table access base class
//---------------------------------------------------------------------------

#include "gpopt/operators/CLogicalDynamicGetBase.h"

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
#include "gpopt/operators/CLogicalDynamicGet.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "naucrates/statistics/CFilterStatsProcessor.h"
#include "naucrates/statistics/CStatistics.h"
#include "naucrates/statistics/CStatsPredUtils.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGetBase::CLogicalDynamicGetBase
//
//	@doc:
//		ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalDynamicGetBase::CLogicalDynamicGetBase(CMemoryPool *mp)
	: CLogical(mp),
	  m_pnameAlias(nullptr),
	  m_ptabdesc(nullptr),
	  m_scan_id(0),
	  m_pdrgpcrOutput(nullptr),
	  m_pdrgpdrgpcrPart(nullptr),
	  m_pcrsDist(nullptr)
{
	m_fPattern = true;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGetBase::CLogicalDynamicGetBase
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CLogicalDynamicGetBase::CLogicalDynamicGetBase(
	CMemoryPool *mp, const CName *pnameAlias,
	gpos::Ref<CTableDescriptor> ptabdesc, ULONG scan_id,
	gpos::Ref<CColRefArray> pdrgpcrOutput,
	gpos::Ref<CColRef2dArray> pdrgpdrgpcrPart,
	gpos::Ref<IMdIdArray> partition_mdids)
	: CLogical(mp),
	  m_pnameAlias(pnameAlias),
	  m_ptabdesc(std::move(ptabdesc)),
	  m_scan_id(scan_id),
	  m_pdrgpcrOutput(std::move(pdrgpcrOutput)),
	  m_pdrgpdrgpcrPart(std::move(pdrgpdrgpcrPart)),
	  m_pcrsDist(nullptr),
	  m_partition_mdids(std::move(partition_mdids))

{
	GPOS_ASSERT(nullptr != m_ptabdesc);
	GPOS_ASSERT(nullptr != pnameAlias);
	GPOS_ASSERT(nullptr != m_pdrgpcrOutput);
	GPOS_ASSERT(nullptr != m_pdrgpdrgpcrPart);

	m_pcrsDist =
		CLogical::PcrsDist(mp, m_ptabdesc.get(), m_pdrgpcrOutput.get());
	m_root_col_mapping_per_part = ConstructRootColMappingPerPart(
		mp, m_pdrgpcrOutput.get(), m_partition_mdids.get());
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGetBase::CLogicalDynamicGetBase
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CLogicalDynamicGetBase::CLogicalDynamicGetBase(
	CMemoryPool *mp, const CName *pnameAlias,
	gpos::Ref<CTableDescriptor> ptabdesc, ULONG scan_id,
	gpos::Ref<IMdIdArray> partition_mdids)
	: CLogical(mp),
	  m_pnameAlias(pnameAlias),
	  m_ptabdesc(std::move(ptabdesc)),
	  m_scan_id(scan_id),
	  m_pdrgpcrOutput(nullptr),
	  m_pcrsDist(nullptr),
	  m_partition_mdids(std::move(partition_mdids))
{
	GPOS_ASSERT(nullptr != m_ptabdesc);
	GPOS_ASSERT(nullptr != pnameAlias);

	// generate a default column set for the table descriptor
	m_pdrgpcrOutput = PdrgpcrCreateMapping(mp, m_ptabdesc->Pdrgpcoldesc(),
										   UlOpId(), m_ptabdesc->MDId());
	m_pdrgpdrgpcrPart = PdrgpdrgpcrCreatePartCols(mp, m_pdrgpcrOutput.get(),
												  m_ptabdesc->PdrgpulPart());
	m_pcrsDist =
		CLogical::PcrsDist(mp, m_ptabdesc.get(), m_pdrgpcrOutput.get());

	m_root_col_mapping_per_part = ConstructRootColMappingPerPart(
		mp, m_pdrgpcrOutput.get(), m_partition_mdids.get());
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGetBase::~CLogicalDynamicGetBase
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CLogicalDynamicGetBase::~CLogicalDynamicGetBase()
{
	;
	;
	;
	;
	;
	;

	GPOS_DELETE(m_pnameAlias);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGetBase::DeriveOutputColumns
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
gpos::Ref<CColRefSet>
CLogicalDynamicGetBase::DeriveOutputColumns(CMemoryPool *mp,
											CExpressionHandle &	 // exprhdl
)
{
	gpos::Ref<CColRefSet> pcrs = GPOS_NEW(mp) CColRefSet(mp);
	pcrs->Include(m_pdrgpcrOutput.get());

	return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGetBase::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
gpos::Ref<CKeyCollection>
CLogicalDynamicGetBase::DeriveKeyCollection(CMemoryPool *mp,
											CExpressionHandle &	 // exprhdl
) const
{
	const CBitSetArray *pdrgpbs = m_ptabdesc->PdrgpbsKeys();

	return CLogical::PkcKeysBaseTable(mp, pdrgpbs, m_pdrgpcrOutput.get());
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGetBase::DerivePropertyConstraint
//
//	@doc:
//		Derive constraint property
//
//---------------------------------------------------------------------------
gpos::Ref<CPropConstraint>
CLogicalDynamicGetBase::DerivePropertyConstraint(CMemoryPool *mp,
												 CExpressionHandle &  // exprhdl
) const
{
	return PpcDeriveConstraintFromTable(mp, m_ptabdesc.get(),
										m_pdrgpcrOutput.get());
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGetBase::PpartinfoDerive
//
//	@doc:
//		Derive partition consumer info
//
//---------------------------------------------------------------------------
gpos::Ref<CPartInfo>
CLogicalDynamicGetBase::DerivePartitionInfo(CMemoryPool *mp,
											CExpressionHandle &	 // exprhdl
) const
{
	gpos::Ref<IMDId> mdid = m_ptabdesc->MDId();
	;
	;

	gpos::Ref<CPartInfo> ppartinfo = GPOS_NEW(mp) CPartInfo(mp);
	ppartinfo->AddPartConsumer(mp, m_scan_id, std::move(mdid),
							   m_pdrgpdrgpcrPart);

	return ppartinfo;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGetBase::PstatsDeriveFilter
//
//	@doc:
//		Derive stats from base table using filters on partition and/or index columns
//
//---------------------------------------------------------------------------
gpos::Ref<IStatistics>
CLogicalDynamicGetBase::PstatsDeriveFilter(CMemoryPool *mp,
										   CExpressionHandle &exprhdl,
										   CExpression *pexprFilter) const
{
	gpos::Ref<CExpression> pexprFilterNew = nullptr;

	if (nullptr != pexprFilter)
	{
		pexprFilterNew = pexprFilter;
		;
	}

	gpos::Ref<CColRefSet> pcrsStat = GPOS_NEW(mp) CColRefSet(mp);

	if (nullptr != pexprFilterNew)
	{
		pcrsStat->Include(pexprFilterNew->DeriveUsedColumns());
	}

	// requesting statistics on distribution columns to estimate data skew
	if (nullptr != m_pcrsDist)
	{
		pcrsStat->Include(m_pcrsDist.get());
	}


	gpos::Ref<CStatistics> pstatsFullTable = gpos::dyn_cast<CStatistics>(
		PstatsBaseTable(mp, exprhdl, m_ptabdesc.get(), pcrsStat.get()));

	;

	if (nullptr == pexprFilterNew || pexprFilterNew->DeriveHasSubquery())
	{
		return pstatsFullTable;
	}

	gpos::Ref<CStatsPred> pred_stats = CStatsPredUtils::ExtractPredStats(
		mp, pexprFilterNew.get(), nullptr /*outer_refs*/
	);
	;

	gpos::Ref<IStatistics> result_stats =
		CFilterStatsProcessor::MakeStatsFilter(mp, pstatsFullTable.get(),
											   pred_stats.get(),
											   true /* do_cap_NDVs */);
	;
	;

	return result_stats;
}

// Construct a mapping from each column in root table to an index in each child
// partition's table descr by matching column names
gpos::Ref<ColRefToUlongMapArray>
CLogicalDynamicGetBase::ConstructRootColMappingPerPart(
	CMemoryPool *mp, CColRefArray *root_cols, IMdIdArray *partition_mdids)
{
	CMDAccessor *mda = COptCtxt::PoctxtFromTLS()->Pmda();

	gpos::Ref<ColRefToUlongMapArray> part_maps =
		GPOS_NEW(mp) ColRefToUlongMapArray(mp);
	for (ULONG ul = 0; ul < partition_mdids->Size(); ++ul)
	{
		IMDId *part_mdid = (*partition_mdids)[ul].get();
		const IMDRelation *partrel = mda->RetrieveRel(part_mdid);

		GPOS_ASSERT(nullptr != partrel);

		gpos::Ref<ColRefToUlongMap> mapping = GPOS_NEW(mp) ColRefToUlongMap(mp);

		for (ULONG i = 0; i < root_cols->Size(); ++i)
		{
			CColRef *root_colref = (*root_cols)[i];

			BOOL found_mapping = false;
			for (ULONG j = 0, idx = 0; j < partrel->ColumnCount(); ++j, ++idx)
			{
				const IMDColumn *coldesc = partrel->GetMdCol(j);
				const CWStringConst *colname = coldesc->Mdname().GetMDName();

				if (coldesc->IsDropped())
				{
					--idx;
					continue;
				}

				if (colname->Equals(root_colref->Name().Pstr()))
				{
					// Found the corresponding column in the child partition
					// Save the index in the mapping
					mapping->Insert(root_colref, GPOS_NEW(mp) ULONG(idx));
					found_mapping = true;
					break;
				}
			}

			if (!found_mapping)
			{
				GPOS_RAISE(
					CException::ExmaInvalid, CException::ExmiInvalid,
					GPOS_WSZ_LIT(
						"Cannot generate root to child partition column mapping"));
			}
		}
		part_maps->Append(mapping);
	}
	return part_maps;
}
