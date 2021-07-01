//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDistributionSpecRouted.cpp
//
//	@doc:
//		Specification of routed distribution
//---------------------------------------------------------------------------

#include "gpopt/base/CDistributionSpecRouted.h"

#include "gpos/common/owner.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CPhysicalMotionBroadcast.h"
#include "gpopt/operators/CPhysicalMotionRoutedDistribute.h"
#include "naucrates/traceflags/traceflags.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecRouted::CDistributionSpecRouted
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDistributionSpecRouted::CDistributionSpecRouted(CColRef *pcrSegmentId)
	: m_pcrSegmentId(pcrSegmentId)
{
	GPOS_ASSERT(nullptr != pcrSegmentId);
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecRouted::~CDistributionSpecRouted
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDistributionSpecRouted::~CDistributionSpecRouted() = default;


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecRouted::FSatisfies
//
//	@doc:
//		Check if this distribution spec satisfies the given one
//
//---------------------------------------------------------------------------
BOOL
CDistributionSpecRouted::FSatisfies(
	gpos::pointer<const CDistributionSpec *> pds) const
{
	if (Matches(pds))
	{
		// exact match implies satisfaction
		return true;
	}

	if (EdtAny == pds->Edt())
	{
		// routed distribution satisfies the "any" distribution requirement
		return true;
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecRouted::PdsCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the distribution spec with remapped columns
//
//---------------------------------------------------------------------------
gpos::owner<CDistributionSpec *>
CDistributionSpecRouted::PdsCopyWithRemappedColumns(
	CMemoryPool *mp, gpos::pointer<UlongToColRefMap *> colref_mapping,
	BOOL must_exist)
{
	ULONG id = m_pcrSegmentId->Id();
	CColRef *pcrSegmentId = colref_mapping->Find(&id);
	if (nullptr == pcrSegmentId)
	{
		if (must_exist)
		{
			// not found in hashmap, so create a new colref and add to hashmap
			CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
			pcrSegmentId = col_factory->PcrCopy(m_pcrSegmentId);

			BOOL result GPOS_ASSERTS_ONLY =
				colref_mapping->Insert(GPOS_NEW(mp) ULONG(id), pcrSegmentId);
			GPOS_ASSERT(result);
		}
		else
		{
			pcrSegmentId = m_pcrSegmentId;
		}
	}

	return GPOS_NEW(mp) CDistributionSpecRouted(pcrSegmentId);
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecRouted::AppendEnforcers
//
//	@doc:
//		Add required enforcers to dynamic array
//
//---------------------------------------------------------------------------
void
CDistributionSpecRouted::AppendEnforcers(
	CMemoryPool *mp,
	CExpressionHandle &,  // exprhdl
	gpos::pointer<CReqdPropPlan *>
#ifdef GPOS_DEBUG
		prpp
#endif	// GPOS_DEBUG
	,
	gpos::pointer<CExpressionArray *> pdrgpexpr,
	gpos::pointer<CExpression *> pexpr)
{
	GPOS_ASSERT(nullptr != mp);
	GPOS_ASSERT(nullptr != prpp);
	GPOS_ASSERT(nullptr != pdrgpexpr);
	GPOS_ASSERT(nullptr != pexpr);
	GPOS_ASSERT(!GPOS_FTRACE(EopttraceDisableMotions));
	GPOS_ASSERT(
		this == prpp->Ped()->PdsRequired() &&
		"required plan properties don't match enforced distribution spec");

	if (GPOS_FTRACE(EopttraceDisableMotionRountedDistribute))
	{
		// routed-distribute Motion is disabled
		return;
	}

	// add a routed distribution enforcer
	AddRef();
	pexpr->AddRef();
	gpos::owner<CExpression *> pexprMotion = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CPhysicalMotionRoutedDistribute(mp, this), pexpr);
	pdrgpexpr->Append(std::move(pexprMotion));
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecRouted::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CDistributionSpecRouted::HashValue() const
{
	return gpos::CombineHashes((ULONG) Edt(),
							   gpos::HashPtr<CColRef>(m_pcrSegmentId));
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecRouted::PcrsUsed
//
//	@doc:
//		Extract columns used by the distribution spec
//
//---------------------------------------------------------------------------
gpos::owner<CColRefSet *>
CDistributionSpecRouted::PcrsUsed(CMemoryPool *mp) const
{
	gpos::owner<CColRefSet *> pcrs = GPOS_NEW(mp) CColRefSet(mp);
	pcrs->Include(m_pcrSegmentId);

	return pcrs;
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecRouted::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CDistributionSpecRouted::Matches(
	gpos::pointer<const CDistributionSpec *> pds) const
{
	if (Edt() != pds->Edt())
	{
		return false;
	}

	gpos::pointer<const CDistributionSpecRouted *> pdsRouted =
		CDistributionSpecRouted::PdsConvert(pds);
	return m_pcrSegmentId == pdsRouted->Pcr();
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecRouted::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream &
CDistributionSpecRouted::OsPrint(IOstream &os) const
{
	os << "ROUTED: [ ";
	m_pcrSegmentId->OsPrint(os);
	return os << " ]";
}

// EOF
