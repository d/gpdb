//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 VMware, Inc. or its affiliates.
//
//	@filename:
//		CPhysicalDynamicScan.cpp
//
//	@doc:
//		Base class for physical dynamic scan operators
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalDynamicScan.h"

#include "gpos/common/owner.h"

#include "gpopt/base/CDrvdPropCtxtPlan.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/metadata/CName.h"
#include "gpopt/metadata/CPartConstraint.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CExpressionHandle.h"

using namespace gpopt;
using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalDynamicScan::CPhysicalDynamicScan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalDynamicScan::CPhysicalDynamicScan(
	CMemoryPool *mp, gpos::Ref<CTableDescriptor> ptabdesc, ULONG ulOriginOpId,
	const CName *pnameAlias, ULONG scan_id,
	gpos::Ref<CColRefArray> pdrgpcrOutput,
	gpos::Ref<CColRef2dArray> pdrgpdrgpcrParts,
	gpos::Ref<IMdIdArray> partition_mdids,
	gpos::Ref<ColRefToUlongMapArray> root_col_mapping_per_part)
	: CPhysicalScan(mp, pnameAlias, ptabdesc, pdrgpcrOutput),
	  m_ulOriginOpId(ulOriginOpId),
	  m_scan_id(scan_id),
	  m_pdrgpdrgpcrPart(pdrgpdrgpcrParts),
	  m_partition_mdids(partition_mdids),
	  m_root_col_mapping_per_part(root_col_mapping_per_part)

{
	GPOS_ASSERT(nullptr != m_pdrgpdrgpcrPart);
	GPOS_ASSERT(0 < m_pdrgpdrgpcrPart->Size());

	CMDAccessor *mda = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDRelation *root_rel = mda->RetrieveRel(ptabdesc->MDId());
	IMdIdArray *all_partition_mdids = root_rel->ChildPartitionMdids();
	ULONG part_ptr = 0;
	for (ULONG ul = 0; ul < m_partition_mdids->Size(); ul++)
	{
		IMDId *part_mdid = (*m_partition_mdids)[ul].get();
		while (part_mdid != (*all_partition_mdids)[part_ptr])
		{
			part_ptr++;
		}
		COptCtxt::PoctxtFromTLS()->AddPartForScanId(scan_id, part_ptr);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalDynamicScan::~CPhysicalDynamicScan
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalDynamicScan::~CPhysicalDynamicScan()
{
	;
	;
	;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalDynamicScan::HashValue
//
//	@doc:
//		Combine part index, pointer for table descriptor, Eop and output columns
//
//---------------------------------------------------------------------------
ULONG
CPhysicalDynamicScan::HashValue() const
{
	ULONG ulHash = gpos::CombineHashes(
		COperator::HashValue(),
		gpos::CombineHashes(gpos::HashValue(&m_scan_id),
							m_ptabdesc->MDId()->HashValue()));
	ulHash = gpos::CombineHashes(ulHash,
								 CUtils::UlHashColArray(m_pdrgpcrOutput.get()));

	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalDynamicScan::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CPhysicalDynamicScan::OsPrint(IOstream &os) const
{
	os << SzId() << " ";

	// alias of table as referenced in the query
	m_pnameAlias->OsPrint(os);

	// actual name of table in catalog and columns
	os << " (";
	m_ptabdesc->Name().OsPrint(os);
	os << "), Columns: [";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcrOutput.get());
	os << "] Scan Id: " << m_scan_id;


	return os;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalDynamicScan::PopConvert
//
//	@doc:
//		conversion function
//
//---------------------------------------------------------------------------
CPhysicalDynamicScan *
CPhysicalDynamicScan::PopConvert(COperator *pop)
{
	GPOS_ASSERT(nullptr != pop);
	GPOS_ASSERT(CUtils::FPhysicalScan(pop) &&
				gpos::dyn_cast<CPhysicalScan>(pop)->FDynamicScan());

	return dynamic_cast<CPhysicalDynamicScan *>(pop);
}


// EOF
