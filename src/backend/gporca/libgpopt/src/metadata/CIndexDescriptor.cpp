//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CIndexDescriptor.cpp
//
//	@doc:
//		Implementation of index description
//---------------------------------------------------------------------------

#include "gpopt/metadata/CIndexDescriptor.h"

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/base/CColumnFactory.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CUtils.h"

using namespace gpopt;

FORCE_GENERATE_DBGSTR(CIndexDescriptor);

//---------------------------------------------------------------------------
//	@function:
//		CIndexDescriptor::CIndexDescriptor
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CIndexDescriptor::CIndexDescriptor(
	CMemoryPool *mp, gpos::owner<IMDId *> pmdidIndex, const CName &name,
	gpos::owner<CColumnDescriptorArray *> pdrgcoldescKeyCols,
	gpos::owner<CColumnDescriptorArray *> pdrgcoldescIncludedCols,
	BOOL is_clustered, IMDIndex::EmdindexType index_type)
	: m_pmdidIndex(std::move(pmdidIndex)),
	  m_name(mp, name),
	  m_pdrgpcoldescKeyCols(std::move(pdrgcoldescKeyCols)),
	  m_pdrgpcoldescIncludedCols(std::move(pdrgcoldescIncludedCols)),
	  m_clustered(is_clustered),
	  m_index_type(index_type)
{
	GPOS_ASSERT(nullptr != mp);
	GPOS_ASSERT(m_pmdidIndex->IsValid());
	GPOS_ASSERT(nullptr != m_pdrgpcoldescKeyCols);
	GPOS_ASSERT(nullptr != m_pdrgpcoldescIncludedCols);
}

//---------------------------------------------------------------------------
//	@function:
//		CIndexDescriptor::~CIndexDescriptor
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CIndexDescriptor::~CIndexDescriptor()
{
	m_pmdidIndex->Release();

	m_pdrgpcoldescKeyCols->Release();
	m_pdrgpcoldescIncludedCols->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CIndexDescriptor::Keys
//
//	@doc:
//		number of key columns
//
//---------------------------------------------------------------------------
ULONG
CIndexDescriptor::Keys() const
{
	return m_pdrgpcoldescKeyCols->Size();
}

//---------------------------------------------------------------------------
//	@function:
//		CIndexDescriptor::UlIncludedColumns
//
//	@doc:
//		Number of included columns
//
//---------------------------------------------------------------------------
ULONG
CIndexDescriptor::UlIncludedColumns() const
{
	// array allocated in ctor
	GPOS_ASSERT(nullptr != m_pdrgpcoldescIncludedCols);

	return m_pdrgpcoldescIncludedCols->Size();
}

//---------------------------------------------------------------------------
//	@function:
//		CIndexDescriptor::Pindexdesc
//
//	@doc:
//		Create the index descriptor from the table descriptor and index
//		information from the catalog
//
//---------------------------------------------------------------------------
gpos::owner<CIndexDescriptor *>
CIndexDescriptor::Pindexdesc(CMemoryPool *mp,
							 gpos::pointer<const CTableDescriptor *> ptabdesc,
							 gpos::pointer<const IMDIndex *> pmdindex)
{
	CWStringConst strIndexName(mp, pmdindex->Mdname().GetMDName()->GetBuffer());

	gpos::pointer<CColumnDescriptorArray *> pdrgpcoldesc =
		ptabdesc->Pdrgpcoldesc();

	pmdindex->MDId()->AddRef();

	// array of index column descriptors
	gpos::owner<CColumnDescriptorArray *> pdrgcoldescKey =
		GPOS_NEW(mp) CColumnDescriptorArray(mp);

	for (ULONG ul = 0; ul < pmdindex->Keys(); ul++)
	{
		gpos::owner<CColumnDescriptor *> pcoldesc = (*pdrgpcoldesc)[ul];
		pcoldesc->AddRef();
		pdrgcoldescKey->Append(pcoldesc);
	}

	// array of included column descriptors
	gpos::owner<CColumnDescriptorArray *> pdrgcoldescIncluded =
		GPOS_NEW(mp) CColumnDescriptorArray(mp);
	for (ULONG ul = 0; ul < pmdindex->IncludedCols(); ul++)
	{
		gpos::owner<CColumnDescriptor *> pcoldesc = (*pdrgpcoldesc)[ul];
		pcoldesc->AddRef();
		pdrgcoldescIncluded->Append(pcoldesc);
	}


	// create the index descriptors
	gpos::owner<CIndexDescriptor *> pindexdesc = GPOS_NEW(mp) CIndexDescriptor(
		mp, pmdindex->MDId(), CName(&strIndexName), std::move(pdrgcoldescKey),
		std::move(pdrgcoldescIncluded), pmdindex->IsClustered(),
		pmdindex->IndexType());
	return pindexdesc;
}

BOOL
CIndexDescriptor::SupportsIndexOnlyScan() const
{
	return m_index_type == IMDIndex::EmdindBtree;
}

//---------------------------------------------------------------------------
//	@function:
//		CIndexDescriptor::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CIndexDescriptor::OsPrint(IOstream &os) const
{
	m_name.OsPrint(os);
	os << ": (Keys :";
	CUtils::OsPrintDrgPcoldesc(os, m_pdrgpcoldescKeyCols,
							   m_pdrgpcoldescKeyCols->Size());
	os << "); ";

	os << "(Included Columns :";
	CUtils::OsPrintDrgPcoldesc(os, m_pdrgpcoldescIncludedCols,
							   m_pdrgpcoldescIncludedCols->Size());
	os << ")";

	os << " [ Clustered :";
	if (m_clustered)
	{
		os << "true";
	}
	else
	{
		os << "false";
	}
	os << " ]";
	return os;
}

// EOF
