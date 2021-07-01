//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 VMware, Inc. or its affiliates.
//
//	@filename:
//		CPhysicalDynamicScan.h
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

#ifndef GPOPT_CPhysicalDynamicScan_H
#define GPOPT_CPhysicalDynamicScan_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/operators/CPhysicalScan.h"

namespace gpopt
{
// fwd declarations
class CTableDescriptor;
class CName;
class CPartConstraint;

//---------------------------------------------------------------------------
//	@class:
//		CPhysicalDynamicScan
//
//	@doc:
//		Base class for physical dynamic scan operators
//
//---------------------------------------------------------------------------
class CPhysicalDynamicScan : public CPhysicalScan
{
private:
	// origin operator id -- gpos::ulong_max if operator was not generated via a transformation
	ULONG m_ulOriginOpId;

	// id of the dynamic scan
	ULONG m_scan_id;

	// partition keys
	gpos::Ref<CColRef2dArray> m_pdrgpdrgpcrPart;

	// child partitions
	gpos::Ref<IMdIdArray> m_partition_mdids;

	// Map of Root colref -> col index in child tabledesc
	// per child partition in m_partition_mdid
	gpos::Ref<ColRefToUlongMapArray> m_root_col_mapping_per_part = nullptr;

public:
	CPhysicalDynamicScan(const CPhysicalDynamicScan &) = delete;

	// ctor
	CPhysicalDynamicScan(
		CMemoryPool *mp, gpos::Ref<CTableDescriptor> ptabdesc,
		ULONG ulOriginOpId, const CName *pnameAlias, ULONG scan_id,
		gpos::Ref<CColRefArray> pdrgpcrOutput,
		gpos::Ref<CColRef2dArray> pdrgpdrgpcrParts,
		gpos::Ref<IMdIdArray> partition_mdids,
		gpos::Ref<ColRefToUlongMapArray> root_col_mapping_per_part);

	// dtor
	~CPhysicalDynamicScan() override;

	// origin operator id -- gpos::ulong_max if operator was not generated via a transformation
	ULONG
	UlOriginOpId() const
	{
		return m_ulOriginOpId;
	}

	// return scan id
	ULONG
	ScanId() const
	{
		return m_scan_id;
	}

	// partition keys
	CColRef2dArray *
	PdrgpdrgpcrPart() const
	{
		return m_pdrgpdrgpcrPart.get();
	}

	// sensitivity to order of inputs
	BOOL
	FInputOrderSensitive() const override
	{
		return true;
	}

	// operator specific hash function
	ULONG HashValue() const override;

	// return true if operator is dynamic scan
	BOOL
	FDynamicScan() const override
	{
		return true;
	}

	IMdIdArray *
	GetPartitionMdids() const
	{
		return m_partition_mdids.get();
	}

	ColRefToUlongMapArray *
	GetRootColMappingPerPart() const
	{
		return m_root_col_mapping_per_part.get();
	}

	// debug print
	IOstream &OsPrint(IOstream &) const override;

	// conversion function
	static CPhysicalDynamicScan *PopConvert(COperator *pop);
};
}  // namespace gpopt

#endif	// !GPOPT_CPhysicalDynamicScan_H

// EOF
