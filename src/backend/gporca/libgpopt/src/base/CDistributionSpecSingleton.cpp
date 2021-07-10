//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CDistributionSpecSingleton.cpp
//
//	@doc:
//		Specification of singleton distribution
//---------------------------------------------------------------------------

#include "gpopt/base/CDistributionSpecSingleton.h"

#include "gpos/common/owner.h"

#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CPhysicalMotionGather.h"
#include "naucrates/traceflags/traceflags.h"


using namespace gpopt;


// initialization of static variables
const CHAR *CDistributionSpecSingleton::m_szSegmentType[EstSentinel] = {
	"master", "segment"};


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecSingleton::CDistributionSpecSingleton
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDistributionSpecSingleton::CDistributionSpecSingleton(ESegmentType est)
	: m_est(est)
{
	GPOS_ASSERT(EstSentinel != est);
}

CDistributionSpecSingleton::CDistributionSpecSingleton()
{
	m_est = EstMaster;

	if (COptCtxt::PoctxtFromTLS()->OptimizeDMLQueryWithSingletonSegment())
	{
		m_est = EstSegment;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecSingleton::FSatisfies
//
//	@doc:
//		Check if this distribution spec satisfies the given one
//
//---------------------------------------------------------------------------
BOOL
CDistributionSpecSingleton::FSatisfies(
	gpos::pointer<const CDistributionSpec *> pds) const
{
	if (Matches(pds))
	{
		// exact match implies satisfaction
		return true;
	}

	if (EdtNonSingleton == pds->Edt())
	{
		// singleton does not satisfy non-singleton requirements
		return false;
	}

	if (EdtAny == pds->Edt())
	{
		// a singleton distribution satisfies "any" distributions
		return true;
	}

	if (EdtHashed == pds->Edt() &&
		CDistributionSpecHashed::PdsConvert(pds)->FSatisfiedBySingleton())
	{
		// a singleton distribution satisfies hashed distributions, if the hashed distribution allows satisfaction
		return true;
	}

	return (EdtSingleton == pds->Edt() &&
			m_est == (const_cast<CDistributionSpecSingleton *>(
						  gpos::cast<CDistributionSpecSingleton>(pds)))
						 ->Est());
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecSingleton::AppendEnforcers
//
//	@doc:
//		Add required enforcers to dynamic array
//
//---------------------------------------------------------------------------
void
CDistributionSpecSingleton::AppendEnforcers(
	CMemoryPool *mp,
	CExpressionHandle &,  // exprhdl
	gpos::pointer<CReqdPropPlan *> prpp,
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


	if (GPOS_FTRACE(EopttraceDisableMotionGather))
	{
		// gather Motion is disabled
		return;
	}

	pexpr->AddRef();
	gpos::owner<CExpression *> pexprMotion = GPOS_NEW(mp)
		CExpression(mp, GPOS_NEW(mp) CPhysicalMotionGather(mp, m_est), pexpr);
	pdrgpexpr->Append(pexprMotion);

	if (!prpp->Peo()->PosRequired()->IsEmpty() &&
		CDistributionSpecSingleton::EstMaster == m_est)
	{
		gpos::owner<COrderSpec *> pos = prpp->Peo()->PosRequired();
		pos->AddRef();
		pexpr->AddRef();

		gpos::owner<CExpression *> pexprGatherMerge = GPOS_NEW(mp) CExpression(
			mp, GPOS_NEW(mp) CPhysicalMotionGather(mp, m_est, std::move(pos)),
			pexpr);
		pdrgpexpr->Append(std::move(pexprGatherMerge));
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecSingleton::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream &
CDistributionSpecSingleton::OsPrint(IOstream &os) const
{
	return os << "SINGLETON (" << m_szSegmentType[m_est] << ")";
}


// EOF
