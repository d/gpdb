//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CPhysicalPartitionSelector.cpp
//
//	@doc:
//		Implementation of physical partition selector
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalPartitionSelector.h"

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/base/CColRef.h"
#include "gpopt/base/CDistributionSpecAny.h"
#include "gpopt/base/CDrvdPropCtxtPlan.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::CPhysicalPartitionSelector
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalPartitionSelector::CPhysicalPartitionSelector(
	CMemoryPool *mp, ULONG scan_id, ULONG selector_id,
	gpos::owner<IMDId *> mdid, gpos::owner<CExpression *> pexprScalar)
	: CPhysical(mp),
	  m_scan_id(scan_id),
	  m_selector_id(selector_id),
	  m_mdid(std::move(mdid)),
	  m_filter_expr(std::move(pexprScalar))
{
	GPOS_ASSERT(0 < scan_id);
	GPOS_ASSERT(m_mdid->IsValid());
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::~CPhysicalPartitionSelector
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalPartitionSelector::~CPhysicalPartitionSelector()
{
	m_mdid->Release();
	CRefCount::SafeRelease(m_filter_expr);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::Matches
//
//	@doc:
//		Match operators
//
//---------------------------------------------------------------------------
BOOL
CPhysicalPartitionSelector::Matches(gpos::pointer<COperator *> pop) const
{
	if (Eopid() != pop->Eopid())
	{
		return false;
	}

	gpos::pointer<CPhysicalPartitionSelector *> popPartSelector =
		gpos::dyn_cast<CPhysicalPartitionSelector>(pop);

	BOOL fScanIdCmp = popPartSelector->ScanId() == m_scan_id;
	BOOL fMdidCmp = popPartSelector->MDId()->Equals(MDId());

	return fScanIdCmp && fMdidCmp;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::HashValue
//
//	@doc:
//		Hash operator
//
//---------------------------------------------------------------------------
ULONG
CPhysicalPartitionSelector::HashValue() const
{
	return gpos::CombineHashes(
		Eopid(), gpos::CombineHashes(m_scan_id, MDId()->HashValue()));
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PcrsRequired
//
//	@doc:
//		Compute required columns of the n-th child;
//		we only compute required columns for the relational child;
//
//---------------------------------------------------------------------------
gpos::owner<CColRefSet *>
CPhysicalPartitionSelector::PcrsRequired(
	CMemoryPool *mp, CExpressionHandle &exprhdl,
	gpos::pointer<CColRefSet *> pcrsInput, ULONG child_index,
	gpos::pointer<CDrvdPropArray *>,  // pdrgpdpCtxt
	ULONG							  // ulOptReq
)
{
	GPOS_ASSERT(
		0 == child_index &&
		"Required properties can only be computed on the relational child");

	gpos::owner<CColRefSet *> pcrs = GPOS_NEW(mp) CColRefSet(mp, *pcrsInput);
	pcrs->Union(m_filter_expr->DeriveUsedColumns());
	pcrs->Intersection(exprhdl.DeriveOutputColumns(child_index));

	return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
gpos::owner<COrderSpec *>
CPhysicalPartitionSelector::PosRequired(
	CMemoryPool *mp, CExpressionHandle &exprhdl,
	gpos::pointer<COrderSpec *> posRequired, ULONG child_index,
	gpos::pointer<CDrvdPropArray *>,  // pdrgpdpCtxt
	ULONG							  // ulOptReq
) const
{
	GPOS_ASSERT(0 == child_index);

	return PosPassThru(mp, exprhdl, posRequired, child_index);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PdsRequired
//
//	@doc:
//		Compute required distribution of the n-th child
//
//---------------------------------------------------------------------------
gpos::owner<CDistributionSpec *>
CPhysicalPartitionSelector::PdsRequired(
	CMemoryPool *mp, CExpressionHandle &exprhdl,
	gpos::pointer<CDistributionSpec *> pdsInput, ULONG child_index,
	gpos::pointer<CDrvdPropArray *>,  // pdrgpdpCtxt
	ULONG							  // ulOptReq
) const
{
	GPOS_ASSERT(0 == child_index);

	gpos::pointer<CPartInfo *> ppartinfo = exprhdl.DerivePartitionInfo();
	BOOL fCovered = ppartinfo->FContainsScanId(m_scan_id);

	if (fCovered)
	{
		// if partition consumer is defined below, do not pass distribution
		// requirements down as this will cause the consumer and enforcer to be
		// in separate slices
		return GPOS_NEW(mp) CDistributionSpecAny(this->Eopid());
	}

	return PdsPassThru(mp, exprhdl, pdsInput, child_index);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PrsRequired
//
//	@doc:
//		Compute required rewindability of the n-th child
//
//---------------------------------------------------------------------------
gpos::owner<CRewindabilitySpec *>
CPhysicalPartitionSelector::PrsRequired(
	CMemoryPool *mp, CExpressionHandle &exprhdl,
	gpos::pointer<CRewindabilitySpec *> prsRequired, ULONG child_index,
	gpos::pointer<CDrvdPropArray *>,  // pdrgpdpCtxt
	ULONG							  // ulOptReq
) const
{
	GPOS_ASSERT(0 == child_index);

	return PrsPassThru(mp, exprhdl, prsRequired, child_index);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
gpos::owner<CCTEReq *>
CPhysicalPartitionSelector::PcteRequired(
	CMemoryPool *,		  //mp,
	CExpressionHandle &,  //exprhdl,
	gpos::pointer<CCTEReq *> pcter,
	ULONG
#ifdef GPOS_DEBUG
		child_index
#endif
	,
	gpos::pointer<CDrvdPropArray *>,  //pdrgpdpCtxt,
	ULONG							  //ulOptReq
) const
{
	GPOS_ASSERT(0 == child_index);
	return PcterPushThru(pcter);
}

gpos::owner<CPartitionPropagationSpec *>
CPhysicalPartitionSelector::PppsRequired(
	CMemoryPool *mp, CExpressionHandle &,
	gpos::pointer<CPartitionPropagationSpec *> pppsRequired,
	ULONG child_index GPOS_ASSERTS_ONLY, gpos::pointer<CDrvdPropArray *>,
	ULONG) const
{
	GPOS_ASSERT(child_index == 0);

	gpos::owner<CPartitionPropagationSpec *> pps_result =
		GPOS_NEW(mp) CPartitionPropagationSpec(mp);
	pps_result->InsertAllExcept(pppsRequired, m_scan_id);
	return pps_result;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalPartitionSelector::FProvidesReqdCols(
	CExpressionHandle &exprhdl, gpos::pointer<CColRefSet *> pcrsRequired,
	ULONG  // ulOptReq
) const
{
	return FUnaryProvidesReqdCols(exprhdl, pcrsRequired);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
gpos::owner<COrderSpec *>
CPhysicalPartitionSelector::PosDerive(CMemoryPool *,  // mp
									  CExpressionHandle &exprhdl) const
{
	return PosDerivePassThruOuter(exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PdsDerive
//
//	@doc:
//		Derive distribution
//
//---------------------------------------------------------------------------
gpos::owner<CDistributionSpec *>
CPhysicalPartitionSelector::PdsDerive(CMemoryPool *,  // mp
									  CExpressionHandle &exprhdl) const
{
	return PdsDerivePassThruOuter(exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PrsDerive
//
//	@doc:
//		Derive rewindability
//
//---------------------------------------------------------------------------
gpos::owner<CRewindabilitySpec *>
CPhysicalPartitionSelector::PrsDerive(CMemoryPool *mp,
									  CExpressionHandle &exprhdl) const
{
	return PrsDerivePassThruOuter(mp, exprhdl);
}

gpos::owner<CPartitionPropagationSpec *>
CPhysicalPartitionSelector::PppsDerive(CMemoryPool *mp,
									   CExpressionHandle &exprhdl) const
{
	gpos::owner<CPartitionPropagationSpec *> pps_result =
		GPOS_NEW(mp) CPartitionPropagationSpec(mp);
	gpos::pointer<CPartitionPropagationSpec *> pps_child =
		exprhdl.Pdpplan(0 /* child_index */)->Ppps();

	gpos::owner<CBitSet *> selector_ids = GPOS_NEW(mp) CBitSet(mp);
	selector_ids->ExchangeSet(m_selector_id);

	pps_result->InsertAll(pps_child);
	pps_result->Insert(m_scan_id, CPartitionPropagationSpec::EpptPropagator,
					   m_mdid, selector_ids, nullptr);
	selector_ids->Release();

	return pps_result;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::EpetDistribution
//
//	@doc:
//		Return the enforcing type for distribution property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalPartitionSelector::EpetDistribution(
	CExpressionHandle &exprhdl,
	gpos::pointer<const CEnfdDistribution *> ped) const
{
	gpos::pointer<CDrvdPropPlan *> pdpplan =
		exprhdl.Pdpplan(0 /* child_index */);

	if (ped->FCompatible(pdpplan->Pds()))
	{
		// required distribution established by the operator
		return CEnfdProp::EpetUnnecessary;
	}

	// GPDB_12_MERGE_FIXME: Check part propagation spec
#if 0
	CPartIndexMap *ppimDrvd = pdpplan->Ppim();
	if (!ppimDrvd->Contains(m_scan_id))
	{
		// part consumer is defined above: prohibit adding a motion on top of the
		// part resolver as this will create two slices
		return CEnfdProp::EpetProhibited;
	}
#endif

	// part consumer found below: enforce distribution on top of part resolver
	return CEnfdProp::EpetRequired;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::EpetRewindability
//
//	@doc:
//		Return the enforcing type for rewindability property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalPartitionSelector::EpetRewindability(
	CExpressionHandle &exprhdl,
	gpos::pointer<const CEnfdRewindability *> per) const
{
	// get rewindability delivered by the node
	gpos::pointer<CRewindabilitySpec *> prs =
		gpos::dyn_cast<CDrvdPropPlan>(exprhdl.Pdp())->Prs();
	if (per->FCompatible(prs))
	{
		// required rewindability is already provided
		return CEnfdProp::EpetUnnecessary;
	}

	// always force spool to be on top of filter
	return CEnfdProp::EpetRequired;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalPartitionSelector::EpetOrder(CExpressionHandle &,	// exprhdl,
									  gpos::pointer<const CEnfdOrder *>	 // ped
) const
{
	return CEnfdProp::EpetOptional;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CPhysicalPartitionSelector::OsPrint(IOstream &os) const
{
	os << SzId() << ", Id: " << SelectorId() << ", Scan Id: " << m_scan_id
	   << ", Part Table: ";
	MDId()->OsPrint(os);

	return os;
}

// EOF
