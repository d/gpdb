//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalInnerHashJoin.cpp
//
//	@doc:
//		Implementation of inner hash join operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalInnerHashJoin.h"

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/CScalarCmp.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerHashJoin::CPhysicalInnerHashJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalInnerHashJoin::CPhysicalInnerHashJoin(
	CMemoryPool *mp, gpos::owner<CExpressionArray *> pdrgpexprOuterKeys,
	gpos::owner<CExpressionArray *> pdrgpexprInnerKeys,
	gpos::owner<IMdIdArray *> hash_opfamilies)
	: CPhysicalHashJoin(mp, std::move(pdrgpexprOuterKeys),
						std::move(pdrgpexprInnerKeys),
						std::move(hash_opfamilies))
{
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerHashJoin::~CPhysicalInnerHashJoin
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalInnerHashJoin::~CPhysicalInnerHashJoin() = default;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerHashJoin::PdshashedCreateMatching
//
//	@doc:
//		Helper function for creating a matching hashed distribution
//
//---------------------------------------------------------------------------
gpos::owner<CDistributionSpecHashed *>
CPhysicalInnerHashJoin::PdshashedCreateMatching(
	CMemoryPool *mp, CDistributionSpecHashed *pdshashed,
	ULONG
		ulSourceChild  // index of child that delivered the given hashed distribution
) const
{
	GPOS_ASSERT(nullptr != pdshashed);

	gpos::owner<CDistributionSpecHashed *> pdshashedMatching =
		PdshashedMatching(mp, pdshashed, ulSourceChild);

	// create a new spec with input and the output spec as equivalents, as you don't want to lose
	// the already existing equivalent specs of pdshashed
	// NB: The matching spec is added at the beginning.
	pdshashedMatching->Pdrgpexpr()->AddRef();
	pdshashed->AddRef();
	if (nullptr != pdshashedMatching->Opfamilies())
	{
		pdshashedMatching->Opfamilies()->AddRef();
	}
	gpos::owner<CDistributionSpecHashed *> pdsHashedMatchingEquivalents =
		GPOS_NEW(mp) CDistributionSpecHashed(
			pdshashedMatching->Pdrgpexpr(),
			pdshashedMatching->FNullsColocated(),
			pdshashed,	// matching distribution spec is equivalent to passed distribution spec
			pdshashedMatching->Opfamilies());
	pdshashedMatching->Release();
	return pdsHashedMatchingEquivalents;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerHashJoin::PdsDeriveFromHashedChildren
//
//	@doc:
//		Derive hash join distribution from hashed children;
//		return nullptr if derivation failed
//
//---------------------------------------------------------------------------
gpos::owner<CDistributionSpec *>
CPhysicalInnerHashJoin::PdsDeriveFromHashedChildren(
	CMemoryPool *mp, gpos::pointer<CDistributionSpec *> pdsOuter,
	gpos::pointer<CDistributionSpec *> pdsInner) const
{
	GPOS_ASSERT(nullptr != pdsOuter);
	GPOS_ASSERT(nullptr != pdsInner);

	gpos::pointer<CDistributionSpecHashed *> pdshashedOuter =
		gpos::dyn_cast<CDistributionSpecHashed>(pdsOuter);
	gpos::pointer<CDistributionSpecHashed *> pdshashedInner =
		gpos::dyn_cast<CDistributionSpecHashed>(pdsInner);

	if (pdshashedOuter->IsCoveredBy(PdrgpexprOuterKeys()) &&
		pdshashedInner->IsCoveredBy(PdrgpexprInnerKeys()))
	{
		// if both sides are hashed on subsets of hash join keys, join's output can be
		// seen as distributed on outer spec or (equivalently) on inner spec,
		// so create a new spec and mark outer and inner as equivalent
		gpos::owner<CDistributionSpecHashed *> combined_hashed_spec =
			pdshashedOuter->Combine(mp, pdshashedInner);
		return combined_hashed_spec;
	}

	return nullptr;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerHashJoin::PdsDeriveFromReplicatedOuter
//
//	@doc:
//		Derive hash join distribution from a replicated outer child;
//
//---------------------------------------------------------------------------
gpos::owner<CDistributionSpec *>
CPhysicalInnerHashJoin::PdsDeriveFromReplicatedOuter(
	CMemoryPool *mp,
	gpos::pointer<CDistributionSpec *>
#ifdef GPOS_DEBUG
		pdsOuter
#endif	// GPOS_DEBUG
	,
	gpos::pointer<CDistributionSpec *> pdsInner) const
{
	GPOS_ASSERT(nullptr != pdsOuter);
	GPOS_ASSERT(nullptr != pdsInner);
	GPOS_ASSERT(CDistributionSpec::EdtStrictReplicated == pdsOuter->Edt());

	// if outer child is replicated, join results distribution is defined by inner child
	if (CDistributionSpec::EdtHashed == pdsInner->Edt())
	{
		CDistributionSpecHashed *pdshashedInner =
			gpos::dyn_cast<CDistributionSpecHashed>(pdsInner);
		if (pdshashedInner->IsCoveredBy(PdrgpexprInnerKeys()))
		{
			// inner child is hashed on a subset of inner hashkeys,
			// return a hashed distribution equivalent to a matching outer distribution
			return PdshashedCreateMatching(mp, pdshashedInner,
										   1 /*ulSourceChild*/);
		}
	}

	// otherwise, pass-through inner distribution
	pdsInner->AddRef();
	return pdsInner;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerHashJoin::PdsDeriveFromHashedOuter
//
//	@doc:
//		Derive hash join distribution from a hashed outer child;
//		return nullptr if derivation failed
//
//---------------------------------------------------------------------------
gpos::owner<CDistributionSpec *>
CPhysicalInnerHashJoin::PdsDeriveFromHashedOuter(
	CMemoryPool *mp, CDistributionSpec *pdsOuter,
	gpos::pointer<CDistributionSpec *>
#ifdef GPOS_DEBUG
		pdsInner
#endif	// GPOS_DEBUG
) const
{
	GPOS_ASSERT(nullptr != pdsOuter);
	GPOS_ASSERT(nullptr != pdsInner);

	GPOS_ASSERT(CDistributionSpec::EdtHashed == pdsOuter->Edt());

	CDistributionSpecHashed *pdshashedOuter =
		gpos::dyn_cast<CDistributionSpecHashed>(pdsOuter);
	if (pdshashedOuter->IsCoveredBy(PdrgpexprOuterKeys()))
	{
		// outer child is hashed on a subset of outer hashkeys,
		// return a hashed distribution equivalent to a matching outer distribution
		return PdshashedCreateMatching(mp, pdshashedOuter, 0 /*ulSourceChild*/);
	}

	return nullptr;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerHashJoin::PdsDerive
//
//	@doc:
//		Derive distribution
//
//---------------------------------------------------------------------------
gpos::owner<CDistributionSpec *>
CPhysicalInnerHashJoin::PdsDerive(CMemoryPool *mp,
								  CExpressionHandle &exprhdl) const
{
	gpos::pointer<CDistributionSpec *> pdsOuter =
		exprhdl.Pdpplan(0 /*child_index*/)->Pds();
	gpos::pointer<CDistributionSpec *> pdsInner =
		exprhdl.Pdpplan(1 /*child_index*/)->Pds();

	if (CDistributionSpec::EdtUniversal == pdsOuter->Edt())
	{
		// if outer is universal, pass through inner distribution
		pdsInner->AddRef();
		return pdsInner;
	}

	if (CDistributionSpec::EdtHashed == pdsOuter->Edt() &&
		CDistributionSpec::EdtHashed == pdsInner->Edt())
	{
		gpos::owner<CDistributionSpec *> pdsDerived =
			PdsDeriveFromHashedChildren(mp, pdsOuter, pdsInner);
		if (nullptr != pdsDerived)
		{
			return pdsDerived;
		}
	}

	if (CDistributionSpec::EdtStrictReplicated == pdsOuter->Edt())
	{
		return PdsDeriveFromReplicatedOuter(mp, pdsOuter, pdsInner);
	}

	if (CDistributionSpec::EdtHashed == pdsOuter->Edt())
	{
		gpos::owner<CDistributionSpec *> pdsDerived =
			PdsDeriveFromHashedOuter(mp, pdsOuter, pdsInner);
		if (nullptr != pdsDerived)
		{
			return pdsDerived;
		}
	}

	// otherwise, pass through outer distribution
	pdsOuter->AddRef();
	return pdsOuter;
}

gpos::owner<CExpression *>
PexprJoinPredOnPartKeys(CMemoryPool *mp, CExpression *pexprScalar,
						gpos::pointer<CPartKeysArray *> pdrgppartkeys,
						gpos::pointer<CColRefSet *> pcrsAllowedRefs)
{
	GPOS_ASSERT(nullptr != pcrsAllowedRefs);

	gpos::owner<CExpression *> pexprPred = nullptr;
	for (ULONG ulKey = 0; nullptr == pexprPred && ulKey < pdrgppartkeys->Size();
		 ulKey++)
	{
		// get partition key
		CColRef2dArray *pdrgpdrgpcrPartKeys =
			(*pdrgppartkeys)[ulKey]->Pdrgpdrgpcr();

		// try to generate a request with dynamic partition selection
		pexprPred = CPredicateUtils::PexprExtractPredicatesOnPartKeys(
			mp, pexprScalar, pdrgpdrgpcrPartKeys, pcrsAllowedRefs,
			true  // fUseConstraints
		);
	}

	return pexprPred;
}

gpos::owner<CPartitionPropagationSpec *>
CPhysicalInnerHashJoin::PppsRequired(
	CMemoryPool *mp, CExpressionHandle &exprhdl,
	gpos::pointer<CPartitionPropagationSpec *> pppsRequired, ULONG child_index,
	gpos::pointer<CDrvdPropArray *> pdrgpdpCtxt, ULONG ulOptReq) const
{
	GPOS_ASSERT(nullptr != pppsRequired);
	GPOS_ASSERT(nullptr != pdrgpdpCtxt);

	CExpression *pexprScalar = exprhdl.PexprScalarExactChild(2 /*child_index*/);

	// CColRefSet *pcrsOutputOuter = exprhdl.DeriveOutputColumns(0);
	gpos::pointer<CColRefSet *> pcrsOutputInner =
		exprhdl.DeriveOutputColumns(1);

	// CPartInfo *part_info_outer = exprhdl.DerivePartitionInfo(0);
	// CPartInfo *part_info_inner = exprhdl.DerivePartitionInfo(1);

	gpos::owner<CPartitionPropagationSpec *> pps_result;
	if (ulOptReq == 0)
	{
		// DPE: create a new request
		pps_result = GPOS_NEW(mp) CPartitionPropagationSpec(mp);
		gpos::pointer<CPartInfo *> part_info_outer =
			exprhdl.DerivePartitionInfo(0);
		for (ULONG ul = 0; ul < part_info_outer->UlConsumers(); ++ul)
		{
			ULONG scan_id = part_info_outer->ScanId(ul);
			IMDId *rel_mdid = part_info_outer->GetRelMdId(ul);
			gpos::pointer<CPartKeysArray *> part_keys_array =
				part_info_outer->Pdrgppartkeys(ul);

			gpos::owner<CExpression *> pexprCmp =
				PexprJoinPredOnPartKeys(mp, pexprScalar, part_keys_array,
										pcrsOutputInner /* pcrsAllowedRefs*/);
			if (pexprCmp == nullptr)
			{
				continue;
			}

			if (child_index == 0)
			{
				gpos::pointer<CPartitionPropagationSpec *> pps_inner =
					gpos::dyn_cast<CDrvdPropPlan>((*pdrgpdpCtxt)[0])->Ppps();

				gpos::owner<CBitSet *> selector_ids =
					GPOS_NEW(mp) CBitSet(mp, *pps_inner->SelectorIds(scan_id));
				pps_result->Insert(scan_id,
								   CPartitionPropagationSpec::EpptConsumer,
								   rel_mdid, selector_ids, nullptr /* expr */);
				selector_ids->Release();
			}
			else
			{
				GPOS_ASSERT(child_index == 1);
				pps_result->Insert(scan_id,
								   CPartitionPropagationSpec::EpptPropagator,
								   rel_mdid, nullptr, pexprCmp);
			}
			pexprCmp->Release();
		}

		gpos::owner<CBitSet *> allowed_scan_ids = GPOS_NEW(mp) CBitSet(mp);
		gpos::pointer<CPartInfo *> part_info =
			exprhdl.DerivePartitionInfo(child_index);
		for (ULONG ul = 0; ul < part_info->UlConsumers(); ++ul)
		{
			ULONG scan_id = part_info->ScanId(ul);
			allowed_scan_ids->ExchangeSet(scan_id);
		}

		pps_result->InsertAllowedConsumers(pppsRequired, allowed_scan_ids);
		allowed_scan_ids->Release();
	}
	else
	{
		// No DPE: pass through requests
		pps_result = CPhysical::PppsRequired(
			mp, exprhdl, pppsRequired, child_index, pdrgpdpCtxt, ulOptReq);
	}
	return pps_result;
}

gpos::owner<CPartitionPropagationSpec *>
CPhysicalInnerHashJoin::PppsDerive(CMemoryPool *mp,
								   CExpressionHandle &exprhdl) const
{
	gpos::pointer<CPartitionPropagationSpec *> pps_outer =
		exprhdl.Pdpplan(0)->Ppps();
	gpos::pointer<CPartitionPropagationSpec *> pps_inner =
		exprhdl.Pdpplan(1)->Ppps();

	gpos::owner<CPartitionPropagationSpec *> pps_result =
		GPOS_NEW(mp) CPartitionPropagationSpec(mp);
	pps_result->InsertAll(pps_outer);
	pps_result->InsertAllResolve(pps_inner);

	return pps_result;
}

// EOF
