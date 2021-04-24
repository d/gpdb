//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Greenplum, Inc.
//
//	@filename:
//		CPhysicalInnerIndexNLJoin.cpp
//
//	@doc:
//		Implementation of index inner nested-loops join operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalInnerIndexNLJoin.h"

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/base/CDistributionSpecAny.h"
#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CDistributionSpecNonSingleton.h"
#include "gpopt/base/CDistributionSpecReplicated.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPredicateUtils.h"



using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerIndexNLJoin::CPhysicalInnerIndexNLJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalInnerIndexNLJoin::CPhysicalInnerIndexNLJoin(
	CMemoryPool *mp, gpos::owner<CColRefArray *> colref_array,
	gpos::owner<CExpression *> origJoinPred)
	: CPhysicalInnerNLJoin(mp),
	  m_pdrgpcrOuterRefs(colref_array),
	  m_origJoinPred(origJoinPred)
{
	GPOS_ASSERT(nullptr != m_pdrgpcrOuterRefs);
	if (nullptr != m_origJoinPred)
	{
		m_origJoinPred->AddRef();
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerIndexNLJoin::~CPhysicalInnerIndexNLJoin
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalInnerIndexNLJoin::~CPhysicalInnerIndexNLJoin()
{
	m_pdrgpcrOuterRefs->Release();
	CRefCount::SafeRelease(m_origJoinPred);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerIndexNLJoin::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CPhysicalInnerIndexNLJoin::Matches(gpos::pointer<COperator *> pop) const
{
	if (pop->Eopid() == Eopid())
	{
		return m_pdrgpcrOuterRefs->Equals(
			gpos::dyn_cast<CPhysicalInnerIndexNLJoin>(pop)->PdrgPcrOuterRefs());
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerIndexNLJoin::PdsRequired
//
//	@doc:
//		Compute required distribution of the n-th child;
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalInnerIndexNLJoin::PdsRequired(
	CMemoryPool *mp GPOS_UNUSED, CExpressionHandle &exprhdl GPOS_UNUSED,
	gpos::pointer<CDistributionSpec *>,	 //pdsRequired,
	ULONG child_index GPOS_UNUSED,
	gpos::pointer<CDrvdPropArray *> pdrgpdpCtxt GPOS_UNUSED,
	ULONG  // ulOptReq
) const
{
	GPOS_RAISE(
		CException::ExmaInvalid, CException::ExmiInvalid,
		GPOS_WSZ_LIT(
			"PdsRequired should not be called for CPhysicalInnerIndexNLJoin"));
	return nullptr;
}

gpos::owner<CEnfdDistribution *>
CPhysicalInnerIndexNLJoin::Ped(CMemoryPool *mp, CExpressionHandle &exprhdl,
							   gpos::pointer<CReqdPropPlan *> prppInput,
							   ULONG child_index,
							   gpos::pointer<CDrvdPropArray *> pdrgpdpCtxt,
							   ULONG ulDistrReq)
{
	GPOS_ASSERT(2 > child_index);

	CEnfdDistribution::EDistributionMatching dmatch =
		Edm(prppInput, child_index, pdrgpdpCtxt, ulDistrReq);

	if (1 == child_index)
	{
		// inner (index-scan side) is requested for Any distribution,
		// we allow outer references on the inner child of the join since it needs
		// to refer to columns in join's outer child
		return GPOS_NEW(mp) CEnfdDistribution(
			GPOS_NEW(mp)
				CDistributionSpecAny(this->Eopid(), true /*fAllowOuterRefs*/),
			dmatch);
	}

	// we need to match distribution of inner
	gpos::pointer<CDistributionSpec *> pdsInner =
		gpos::dyn_cast<CDrvdPropPlan>((*pdrgpdpCtxt)[0])->Pds();
	CDistributionSpec::EDistributionType edtInner = pdsInner->Edt();
	if (CDistributionSpec::EdtSingleton == edtInner ||
		CDistributionSpec::EdtStrictSingleton == edtInner ||
		CDistributionSpec::EdtUniversal == edtInner)
	{
		// enforce executing on a single host
		return GPOS_NEW(mp) CEnfdDistribution(
			GPOS_NEW(mp) CDistributionSpecSingleton(), dmatch);
	}

	if (CDistributionSpec::EdtHashed == edtInner)
	{
		// check if we could create an equivalent hashed distribution request to the inner child
		gpos::pointer<CDistributionSpecHashed *> pdshashed =
			gpos::dyn_cast<CDistributionSpecHashed>(pdsInner);
		gpos::pointer<CDistributionSpecHashed *> pdshashedEquiv =
			pdshashed->PdshashedEquiv();

		// If the inner child is a IndexScan on a multi-key distributed index, it
		// may derive an incomplete equiv spec (see CPhysicalScan::PdsDerive()).
		// However, there is no point to using that here since there will be no
		// operator above this that can complete it.
		if (pdshashed->HasCompleteEquivSpec(mp))
		{
			// request hashed distribution from outer
			pdshashedEquiv->Pdrgpexpr()->AddRef();
			gpos::owner<CDistributionSpecHashed *> pdsHashedRequired =
				GPOS_NEW(mp)
					CDistributionSpecHashed(pdshashedEquiv->Pdrgpexpr(),
											pdshashedEquiv->FNullsColocated());
			pdsHashedRequired->ComputeEquivHashExprs(mp, exprhdl);

			return GPOS_NEW(mp)
				CEnfdDistribution(std::move(pdsHashedRequired), dmatch);
		}
	}

	// otherwise, require outer child to be replicated
	// The match type for this request has to be "Satisfy" since EdtReplicated
	// is required only property. Since a Broadcast motion will always
	// derive a EdtStrictReplicated distribution spec, it will never "Match"
	// the required distribution spec and hence will not be optimized.
	return GPOS_NEW(mp) CEnfdDistribution(
		GPOS_NEW(mp)
			CDistributionSpecReplicated(CDistributionSpec::EdtReplicated),
		CEnfdDistribution::EdmSatisfy);
}


// EOF
