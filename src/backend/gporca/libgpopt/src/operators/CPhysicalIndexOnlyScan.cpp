//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2020 VMware, Inc.
//
//	@filename:
//		CPhysicalIndexOnlyScan.cpp
//
//	@doc:
//		Implementation of index scan operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalIndexOnlyScan.h"

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalIndexOnlyScan::CPhysicalIndexOnlyScan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalIndexOnlyScan::CPhysicalIndexOnlyScan(
	CMemoryPool *mp, gpos::Ref<CIndexDescriptor> pindexdesc,
	gpos::Ref<CTableDescriptor> ptabdesc, ULONG ulOriginOpId,
	const CName *pnameAlias, gpos::Ref<CColRefArray> pdrgpcrOutput,
	gpos::Ref<COrderSpec> pos)
	: CPhysicalScan(mp, pnameAlias, std::move(ptabdesc),
					std::move(pdrgpcrOutput)),
	  m_pindexdesc(std::move(pindexdesc)),
	  m_ulOriginOpId(ulOriginOpId),
	  m_pos(std::move(pos))
{
	GPOS_ASSERT(nullptr != m_pindexdesc);
	GPOS_ASSERT(nullptr != m_pos);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalIndexOnlyScan::~CPhysicalIndexOnlyScan
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalIndexOnlyScan::~CPhysicalIndexOnlyScan()
{
	;
	;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalIndexOnlyScan::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalIndexOnlyScan::EpetOrder(CExpressionHandle &,	// exprhdl
								  const CEnfdOrder *peo) const
{
	GPOS_ASSERT(nullptr != peo);
	GPOS_ASSERT(!peo->PosRequired()->IsEmpty());

	if (peo->FCompatible(m_pos.get()))
	{
		// required order is already established by the index
		return CEnfdProp::EpetUnnecessary;
	}

	return CEnfdProp::EpetRequired;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalIndexOnlyScan::HashValue
//
//	@doc:
//		Combine pointers for table descriptor, index descriptor and Eop
//
//---------------------------------------------------------------------------
ULONG
CPhysicalIndexOnlyScan::HashValue() const
{
	ULONG ulHash = gpos::CombineHashes(
		COperator::HashValue(),
		gpos::CombineHashes(m_pindexdesc->MDId()->HashValue(),
							gpos::HashPtr<CTableDescriptor>(m_ptabdesc.get())));
	ulHash = gpos::CombineHashes(ulHash,
								 CUtils::UlHashColArray(m_pdrgpcrOutput.get()));

	return ulHash;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalIndexOnlyScan::Matches
//
//	@doc:
//		match operator
//
//---------------------------------------------------------------------------
BOOL
CPhysicalIndexOnlyScan::Matches(COperator *pop) const
{
	return CUtils::FMatchIndex(this, pop);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalIndexOnlyScan::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CPhysicalIndexOnlyScan::OsPrint(IOstream &os) const
{
	if (m_fPattern)
	{
		return COperator::OsPrint(os);
	}

	os << SzId() << " ";
	// index name
	os << "  Index Name: (";
	m_pindexdesc->Name().OsPrint(os);
	// table name
	os << ")";
	os << ", Table Name: (";
	m_ptabdesc->Name().OsPrint(os);
	os << ")";
	os << ", Columns: [";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcrOutput.get());
	os << "]";

	return os;
}

// EOF
