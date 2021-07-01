//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalCTEConsumer.cpp
//
//	@doc:
//		Implementation of CTE consumer operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalCTEConsumer.h"

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/base/CCTEMap.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalCTEProducer.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::CPhysicalCTEConsumer
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalCTEConsumer::CPhysicalCTEConsumer(
	CMemoryPool *mp, ULONG id, gpos::Ref<CColRefArray> colref_array,
	gpos::Ref<UlongToColRefMap> colref_mapping)
	: CPhysical(mp),
	  m_id(id),
	  m_pdrgpcr(std::move(colref_array)),
	  m_phmulcr(std::move(colref_mapping))
{
	GPOS_ASSERT(nullptr != m_pdrgpcr);
	GPOS_ASSERT(nullptr != m_phmulcr);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::~CPhysicalCTEConsumer
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalCTEConsumer::~CPhysicalCTEConsumer()
{
	;
	;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::PcrsRequired
//
//	@doc:
//		Compute required output columns of the n-th child
//
//---------------------------------------------------------------------------
gpos::Ref<CColRefSet>
CPhysicalCTEConsumer::PcrsRequired(CMemoryPool *,		 // mp,
								   CExpressionHandle &,	 // exprhdl,
								   CColRefSet *,		 // pcrsRequired,
								   ULONG,				 // child_index,
								   CDrvdPropArray *,	 // pdrgpdpCtxt
								   ULONG				 // ulOptReq
)
{
	GPOS_ASSERT(!"CPhysicalCTEConsumer has no relational children");
	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
gpos::Ref<COrderSpec>
CPhysicalCTEConsumer::PosRequired(CMemoryPool *,		// mp,
								  CExpressionHandle &,	// exprhdl,
								  COrderSpec *,			// posRequired,
								  ULONG,				// child_index,
								  CDrvdPropArray *,		// pdrgpdpCtxt
								  ULONG					// ulOptReq
) const
{
	GPOS_ASSERT(!"CPhysicalCTEConsumer has no relational children");
	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::PdsRequired
//
//	@doc:
//		Compute required distribution of the n-th child
//
//---------------------------------------------------------------------------
gpos::Ref<CDistributionSpec>
CPhysicalCTEConsumer::PdsRequired(CMemoryPool *,		// mp,
								  CExpressionHandle &,	// exprhdl,
								  CDistributionSpec *,	// pdsRequired,
								  ULONG,				//child_index
								  CDrvdPropArray *,		// pdrgpdpCtxt
								  ULONG					// ulOptReq
) const
{
	GPOS_ASSERT(!"CPhysicalCTEConsumer has no relational children");
	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::PrsRequired
//
//	@doc:
//		Compute required rewindability of the n-th child
//
//---------------------------------------------------------------------------
gpos::Ref<CRewindabilitySpec>
CPhysicalCTEConsumer::PrsRequired(CMemoryPool *,		 // mp,
								  CExpressionHandle &,	 // exprhdl,
								  CRewindabilitySpec *,	 // prsRequired,
								  ULONG,				 // child_index,
								  CDrvdPropArray *,		 // pdrgpdpCtxt
								  ULONG					 // ulOptReq
) const
{
	GPOS_ASSERT(!"CPhysicalCTEConsumer has no relational children");
	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
gpos::Ref<CCTEReq>
CPhysicalCTEConsumer::PcteRequired(CMemoryPool *,		 //mp,
								   CExpressionHandle &,	 //exprhdl,
								   CCTEReq *,			 //pcter,
								   ULONG,				 //child_index,
								   CDrvdPropArray *,	 //pdrgpdpCtxt,
								   ULONG				 //ulOptReq
) const
{
	GPOS_ASSERT(!"CPhysicalCTEConsumer has no relational children");
	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
gpos::Ref<COrderSpec>
CPhysicalCTEConsumer::PosDerive(CMemoryPool *,		 // mp
								CExpressionHandle &	 //exprhdl
) const
{
	GPOS_ASSERT(!"Unexpected call to CTE consumer order property derivation");

	return nullptr;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::PdsDerive
//
//	@doc:
//		Derive distribution
//
//---------------------------------------------------------------------------
gpos::Ref<CDistributionSpec>
CPhysicalCTEConsumer::PdsDerive(CMemoryPool *,		 // mp
								CExpressionHandle &	 //exprhdl
) const
{
	GPOS_ASSERT(
		!"Unexpected call to CTE consumer distribution property derivation");

	return nullptr;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::PrsDerive
//
//	@doc:
//		Derive rewindability
//
//---------------------------------------------------------------------------
gpos::Ref<CRewindabilitySpec>
CPhysicalCTEConsumer::PrsDerive(CMemoryPool *,		 //mp
								CExpressionHandle &	 //exprhdl
) const
{
	GPOS_ASSERT(
		!"Unexpected call to CTE consumer rewindability property derivation");

	return nullptr;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::PcmDerive
//
//	@doc:
//		Derive cte map
//
//---------------------------------------------------------------------------
gpos::Ref<CCTEMap>
CPhysicalCTEConsumer::PcmDerive(CMemoryPool *mp, CExpressionHandle &
#ifdef GPOS_DEBUG
													 exprhdl
#endif
) const
{
	GPOS_ASSERT(0 == exprhdl.Arity());

	gpos::Ref<CCTEMap> pcmConsumer = GPOS_NEW(mp) CCTEMap(mp);
	pcmConsumer->Insert(m_id, CCTEMap::EctConsumer, nullptr /*pdpplan*/);

	return pcmConsumer;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalCTEConsumer::FProvidesReqdCols(CExpressionHandle &exprhdl,
										CColRefSet *pcrsRequired,
										ULONG  // ulOptReq
) const
{
	GPOS_ASSERT(nullptr != pcrsRequired);

	CColRefSet *pcrsOutput = exprhdl.DeriveOutputColumns();
	return pcrsOutput->ContainsAll(pcrsRequired);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalCTEConsumer::EpetOrder(CExpressionHandle &exprhdl,
								const CEnfdOrder *peo) const
{
	GPOS_ASSERT(nullptr != peo);
	GPOS_ASSERT(!peo->PosRequired()->IsEmpty());

	COrderSpec *pos = gpos::dyn_cast<CDrvdPropPlan>(exprhdl.Pdp())->Pos();
	if (peo->FCompatible(pos))
	{
		return CEnfdProp::EpetUnnecessary;
	}

	return CEnfdProp::EpetRequired;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::EpetRewindability
//
//	@doc:
//		Return the enforcing type for rewindability property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalCTEConsumer::EpetRewindability(CExpressionHandle &exprhdl,
										const CEnfdRewindability *per) const
{
	GPOS_ASSERT(nullptr != per);

	CRewindabilitySpec *prs =
		gpos::dyn_cast<CDrvdPropPlan>(exprhdl.Pdp())->Prs();
	if (per->FCompatible(prs))
	{
		return CEnfdProp::EpetUnnecessary;
	}

	return CEnfdProp::EpetRequired;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CPhysicalCTEConsumer::Matches(COperator *pop) const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}

	CPhysicalCTEConsumer *popCTEConsumer =
		gpos::dyn_cast<CPhysicalCTEConsumer>(pop);

	return m_id == popCTEConsumer->UlCTEId() &&
		   m_pdrgpcr->Equals(popCTEConsumer->Pdrgpcr());
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CPhysicalCTEConsumer::HashValue() const
{
	ULONG ulHash = gpos::CombineHashes(COperator::HashValue(), m_id);
	ulHash =
		gpos::CombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcr.get()));

	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CPhysicalCTEConsumer::OsPrint(IOstream &os) const
{
	os << SzId() << " (";
	os << m_id;
	os << "), Columns: [";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcr.get());
	os << "]";

	return os;
}

// EOF
