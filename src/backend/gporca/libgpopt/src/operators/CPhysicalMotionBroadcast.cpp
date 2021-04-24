//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalMotionBroadcast.cpp
//
//	@doc:
//		Implementation of broadcast motion operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalMotionBroadcast.h"

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/base/CDistributionSpecReplicated.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CExpressionHandle.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionBroadcast::CPhysicalMotionBroadcast
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalMotionBroadcast::CPhysicalMotionBroadcast(CMemoryPool *mp)
	: CPhysicalMotion(mp), m_pdsReplicated(nullptr)
{
	m_pdsReplicated = GPOS_NEW(mp)
		CDistributionSpecReplicated(CDistributionSpec::EdtStrictReplicated);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionBroadcast::~CPhysicalMotionBroadcast
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalMotionBroadcast::~CPhysicalMotionBroadcast()
{
	m_pdsReplicated->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionBroadcast::Matches
//
//	@doc:
//		Match operators
//
//---------------------------------------------------------------------------
BOOL
CPhysicalMotionBroadcast::Matches(gpos::pointer<COperator *> pop) const
{
	return Eopid() == pop->Eopid();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionBroadcast::PcrsRequired
//
//	@doc:
//		Compute required columns of the n-th child;
//
//---------------------------------------------------------------------------
gpos::owner<CColRefSet *>
CPhysicalMotionBroadcast::PcrsRequired(
	CMemoryPool *mp, CExpressionHandle &exprhdl,
	gpos::pointer<CColRefSet *> pcrsRequired, ULONG child_index,
	gpos::pointer<CDrvdPropArray *>,  // pdrgpdpCtxt
	ULONG							  // ulOptReq
)
{
	GPOS_ASSERT(0 == child_index);

	gpos::owner<CColRefSet *> pcrs = GPOS_NEW(mp) CColRefSet(mp, *pcrsRequired);

	gpos::owner<CColRefSet *> pcrsChildReqd =
		PcrsChildReqd(mp, exprhdl, pcrs, child_index, gpos::ulong_max);
	pcrs->Release();

	return pcrsChildReqd;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionBroadcast::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalMotionBroadcast::FProvidesReqdCols(
	CExpressionHandle &exprhdl, gpos::pointer<CColRefSet *> pcrsRequired,
	ULONG  // ulOptReq
) const
{
	return FUnaryProvidesReqdCols(exprhdl, pcrsRequired);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionBroadcast::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalMotionBroadcast::EpetOrder(CExpressionHandle &,  // exprhdl
									gpos::pointer<const CEnfdOrder *>  // peo
) const
{
	// broadcast motion is not order-preserving
	return CEnfdProp::EpetRequired;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionBroadcast::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
gpos::owner<COrderSpec *>
CPhysicalMotionBroadcast::PosRequired(
	CMemoryPool *mp,
	CExpressionHandle &,		  // exprhdl
	gpos::pointer<COrderSpec *>,  //posInput
	ULONG
#ifdef GPOS_DEBUG
		child_index
#endif	// GPOS_DEBUG
	,
	gpos::pointer<CDrvdPropArray *>,  // pdrgpdpCtxt
	ULONG							  // ulOptReq
) const
{
	GPOS_ASSERT(0 == child_index);

	// no order required from child expression
	return GPOS_NEW(mp) COrderSpec(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionBroadcast::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
gpos::owner<COrderSpec *>
CPhysicalMotionBroadcast::PosDerive(CMemoryPool *mp,
									CExpressionHandle &	 // exprhdl
) const
{
	// broadcast motion is not order-preserving
	return GPOS_NEW(mp) COrderSpec(mp);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionBroadcast::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CPhysicalMotionBroadcast::OsPrint(IOstream &os) const
{
	os << SzId() << " ";
	return os;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionBroadcast::PopConvert
//
//	@doc:
//		Conversion function
//
//---------------------------------------------------------------------------
gpos::cast_func<CPhysicalMotionBroadcast *>
CPhysicalMotionBroadcast::PopConvert(COperator *pop)
{
	GPOS_ASSERT(nullptr != pop);
	GPOS_ASSERT(EopPhysicalMotionBroadcast == pop->Eopid());

	return dynamic_cast<CPhysicalMotionBroadcast *>(pop);
}

// EOF
