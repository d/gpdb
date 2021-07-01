//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CKeyCollection.cpp
//
//	@doc:
//		Implementation of key collections
//---------------------------------------------------------------------------

#include "gpopt/base/CKeyCollection.h"

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/base/CUtils.h"

using namespace gpopt;

FORCE_GENERATE_DBGSTR(CKeyCollection);

//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::CKeyCollection
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CKeyCollection::CKeyCollection(CMemoryPool *mp) : m_pdrgpcrs(nullptr)
{
	GPOS_ASSERT(nullptr != mp);

	m_pdrgpcrs = GPOS_NEW(mp) CColRefSetArray(mp);
}


//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::CKeyCollection
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CKeyCollection::CKeyCollection(CMemoryPool *mp, gpos::owner<CColRefSet *> pcrs)
	: m_pdrgpcrs(nullptr)
{
	GPOS_ASSERT(nullptr != pcrs && 0 < pcrs->Size());

	m_pdrgpcrs = GPOS_NEW(mp) CColRefSetArray(mp);

	// we own the set
	Add(std::move(pcrs));
}


//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::CKeyCollection
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CKeyCollection::CKeyCollection(CMemoryPool *mp,
							   gpos::owner<CColRefArray *> colref_array)
	: m_pdrgpcrs(nullptr)
{
	GPOS_ASSERT(nullptr != mp);
	GPOS_ASSERT(nullptr != colref_array && 0 < colref_array->Size());

	m_pdrgpcrs = GPOS_NEW(mp) CColRefSetArray(mp);

	gpos::owner<CColRefSet *> pcrs = GPOS_NEW(mp) CColRefSet(mp);
	pcrs->Include(colref_array);
	Add(std::move(pcrs));

	// we own the array
	colref_array->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::~CKeyCollection
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CKeyCollection::~CKeyCollection()
{
	m_pdrgpcrs->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::Add
//
//	@doc:
//		Add key set to collection; takes ownership
//
//---------------------------------------------------------------------------
void
CKeyCollection::Add(gpos::owner<CColRefSet *> pcrs)
{
	GPOS_ASSERT(!FKey(pcrs) && "no duplicates allowed");

	m_pdrgpcrs->Append(std::move(pcrs));
}


//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::FKey
//
//	@doc:
//		Check if set constitutes key
//
//---------------------------------------------------------------------------
BOOL
CKeyCollection::FKey(gpos::pointer<const CColRefSet *> pcrs,
					 BOOL fExactMatch  // true: match keys exactly,
									   //  false: match keys by inclusion
) const
{
	const ULONG ulSets = m_pdrgpcrs->Size();
	for (ULONG ul = 0; ul < ulSets; ul++)
	{
		if (fExactMatch)
		{
			// accept only exact matches
			if (pcrs->Equals((*m_pdrgpcrs)[ul]))
			{
				return true;
			}
		}
		else
		{
			// if given column set includes a key, then it is also a key
			if (pcrs->ContainsAll((*m_pdrgpcrs)[ul]))
			{
				return true;
			}
		}
	}

	return false;
}



//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::FKey
//
//	@doc:
//		Check if array constitutes key
//
//---------------------------------------------------------------------------
BOOL
CKeyCollection::FKey(CMemoryPool *mp,
					 gpos::pointer<const CColRefArray *> colref_array) const
{
	gpos::owner<CColRefSet *> pcrs = GPOS_NEW(mp) CColRefSet(mp);
	pcrs->Include(colref_array);

	BOOL fKey = FKey(pcrs);
	pcrs->Release();

	return fKey;
}


//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::PdrgpcrTrim
//
//	@doc:
//		Return first subsumed key as column array
//
//---------------------------------------------------------------------------
gpos::owner<CColRefArray *>
CKeyCollection::PdrgpcrTrim(
	CMemoryPool *mp, gpos::pointer<const CColRefArray *> colref_array) const
{
	gpos::owner<CColRefArray *> pdrgpcrTrim = nullptr;
	gpos::owner<CColRefSet *> pcrs = GPOS_NEW(mp) CColRefSet(mp);
	pcrs->Include(colref_array);

	const ULONG ulSets = m_pdrgpcrs->Size();
	for (ULONG ul = 0; ul < ulSets; ul++)
	{
		gpos::pointer<CColRefSet *> pcrsKey = (*m_pdrgpcrs)[ul];
		if (pcrs->ContainsAll(pcrsKey))
		{
			pdrgpcrTrim = pcrsKey->Pdrgpcr(mp);
			break;
		}
	}
	pcrs->Release();

	return pdrgpcrTrim;
}

//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::PdrgpcrKey
//
//	@doc:
//		Extract a key
//
//---------------------------------------------------------------------------
gpos::owner<CColRefArray *>
CKeyCollection::PdrgpcrKey(CMemoryPool *mp) const
{
	if (0 == m_pdrgpcrs->Size())
	{
		return nullptr;
	}

	GPOS_ASSERT(nullptr != (*m_pdrgpcrs)[0]);

	gpos::owner<CColRefArray *> colref_array = (*m_pdrgpcrs)[0]->Pdrgpcr(mp);
	return colref_array;
}


//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::PdrgpcrHashableKey
//
//	@doc:
//		Extract a hashable key
//
//---------------------------------------------------------------------------
gpos::owner<CColRefArray *>
CKeyCollection::PdrgpcrHashableKey(CMemoryPool *mp) const
{
	const ULONG ulSets = m_pdrgpcrs->Size();
	for (ULONG ul = 0; ul < ulSets; ul++)
	{
		gpos::owner<CColRefArray *> pdrgpcrKey = (*m_pdrgpcrs)[ul]->Pdrgpcr(mp);
		if (CUtils::IsHashable(pdrgpcrKey))
		{
			return pdrgpcrKey;
		}
		pdrgpcrKey->Release();
	}

	// no hashable key is found
	return nullptr;
}


//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::PdrgpcrKey
//
//	@doc:
//		Extract the key at a position
//
//---------------------------------------------------------------------------
gpos::owner<CColRefArray *>
CKeyCollection::PdrgpcrKey(CMemoryPool *mp, ULONG ulIndex) const
{
	if (0 == m_pdrgpcrs->Size())
	{
		return nullptr;
	}

	GPOS_ASSERT(nullptr != (*m_pdrgpcrs)[ulIndex]);

	gpos::owner<CColRefArray *> colref_array =
		(*m_pdrgpcrs)[ulIndex]->Pdrgpcr(mp);
	return colref_array;
}


//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::PcrsKey
//
//	@doc:
//		Extract key at given position
//
//---------------------------------------------------------------------------
gpos::owner<CColRefSet *>
CKeyCollection::PcrsKey(CMemoryPool *mp, ULONG ulIndex) const
{
	if (0 == m_pdrgpcrs->Size())
	{
		return nullptr;
	}

	GPOS_ASSERT(nullptr != (*m_pdrgpcrs)[ulIndex]);

	gpos::pointer<CColRefSet *> pcrsKey = (*m_pdrgpcrs)[ulIndex];
	return GPOS_NEW(mp) CColRefSet(mp, *pcrsKey);
}


//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CKeyCollection::OsPrint(IOstream &os) const
{
	os << " Keys: (";

	const ULONG ulSets = m_pdrgpcrs->Size();
	for (ULONG ul = 0; ul < ulSets; ul++)
	{
		if (0 < ul)
		{
			os << ", ";
		}

		GPOS_ASSERT(nullptr != (*m_pdrgpcrs)[ul]);
		os << "[" << (*(*m_pdrgpcrs)[ul]) << "]";
	}

	return os << ")";
}


// EOF
