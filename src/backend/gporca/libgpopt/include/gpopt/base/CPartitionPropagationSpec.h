//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPartitionPropagationSpec.h
//
//	@doc:
//		Partition Propagation spec in required properties
//---------------------------------------------------------------------------
#ifndef GPOPT_CPartitionPropagationSpec_H
#define GPOPT_CPartitionPropagationSpec_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"
#include "gpos/common/owner.h"

#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CPropSpec.h"


namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CPartitionPropagationSpec
//
//	@doc:
//		Partition Propagation specification
//
//---------------------------------------------------------------------------
class CPartitionPropagationSpec : public CPropSpec
{
public:
	enum EPartPropSpecInfoType
	{
		EpptPropagator,
		EpptConsumer,
		EpptSentinel
	};

private:
	struct SPartPropSpecInfo : public CRefCount
	{
		// scan id of the DynamicScan
		ULONG m_scan_id;

		// info type: consumer or propagator
		EPartPropSpecInfoType m_type;

		// relation id of the DynamicScan
		gpos::owner<IMDId *> m_root_rel_mdid;

		//  partition selector ids to use (reqd only)
		gpos::owner<CBitSet *> m_selector_ids = nullptr;

		// filter expressions to generate partition pruning data in the translator (reqd only)
		gpos::owner<CExpression *> m_filter_expr = nullptr;

		SPartPropSpecInfo(ULONG scan_id, EPartPropSpecInfoType type,
						  gpos::owner<IMDId *> rool_rel_mdid)
			: m_scan_id(scan_id),
			  m_type(type),
			  m_root_rel_mdid(std::move(rool_rel_mdid))
		{
			GPOS_ASSERT(m_root_rel_mdid != nullptr);

			CMemoryPool *mp = COptCtxt::PoctxtFromTLS()->Pmp();
			m_selector_ids = GPOS_NEW(mp) CBitSet(mp);
		}

		~SPartPropSpecInfo() override
		{
			m_root_rel_mdid->Release();
			CRefCount::SafeRelease(m_selector_ids);
			CRefCount::SafeRelease(m_filter_expr);
		}

		IOstream &OsPrint(IOstream &os) const;

		// used for determining equality in memo (e.g in optimization contexts)
		BOOL Equals(gpos::pointer<const SPartPropSpecInfo *>) const;

		BOOL FSatisfies(gpos::pointer<const SPartPropSpecInfo *>) const;

		// used for sorting SPartPropSpecInfo in an array
		static INT CmpFunc(const void *val1, const void *val2);
	};

	typedef CDynamicPtrArray<SPartPropSpecInfo, CleanupRelease>
		SPartPropSpecInfoArray;

	// partition required/derived info, sorted by scanid
	gpos::owner<SPartPropSpecInfoArray *> m_part_prop_spec_infos = nullptr;

	// Present scanids (for easy lookup)
	gpos::owner<CBitSet *> m_scan_ids = nullptr;

public:
	CPartitionPropagationSpec(const CPartitionPropagationSpec &) = delete;

	// ctor
	CPartitionPropagationSpec(CMemoryPool *mp);

	// dtor
	~CPartitionPropagationSpec() override;

	// append enforcers to dynamic array for the given plan properties
	void AppendEnforcers(CMemoryPool *mp, CExpressionHandle &exprhdl,
						 gpos::pointer<CReqdPropPlan *> prpp,
						 gpos::pointer<CExpressionArray *> pdrgpexpr,
						 gpos::pointer<CExpression *> pexpr) override;

	// hash function
	ULONG
	HashValue() const override
	{
		// GPDB_12_MERGE_FIXME: Implement this, even if it's unused
		GPOS_RTL_ASSERT(!"Unused");
	}

	// extract columns used by the partition propagation spec
	gpos::owner<CColRefSet *>
	PcrsUsed(CMemoryPool *mp) const override
	{
		// return an empty set
		return GPOS_NEW(mp) CColRefSet(mp);
	}

	// property type
	EPropSpecType
	Epst() const override
	{
		return EpstPartPropagation;
	}

	BOOL
	Contains(ULONG scan_id) const
	{
		return m_scan_ids->Get(scan_id);
	}

	BOOL ContainsAnyConsumers() const;

	// equality check to determine compatibility of derived & required properties
	BOOL Equals(gpos::pointer<const CPartitionPropagationSpec *> ppps) const;

	// satisfies function
	BOOL FSatisfies(
		gpos::pointer<const CPartitionPropagationSpec *> pps_reqd) const;


	SPartPropSpecInfo *FindPartPropSpecInfo(ULONG scan_id) const;

	void Insert(ULONG scan_id, EPartPropSpecInfoType type, IMDId *rool_rel_mdid,
				gpos::pointer<CBitSet *> selector_ids,
				gpos::pointer<CExpression *> expr);

	void Insert(gpos::pointer<SPartPropSpecInfo *> other);

	void InsertAll(gpos::pointer<CPartitionPropagationSpec *> pps);

	void InsertAllowedConsumers(gpos::pointer<CPartitionPropagationSpec *> pps,
								gpos::pointer<CBitSet *> allowed_scan_ids);

	void InsertAllExcept(gpos::pointer<CPartitionPropagationSpec *> pps,
						 ULONG scan_id);

	gpos::pointer<const CBitSet *> SelectorIds(ULONG scan_id) const;

	// is partition propagation required
	BOOL
	FPartPropagationReqd() const
	{
		return true;
	}

	// print
	IOstream &OsPrint(IOstream &os) const override;

	void InsertAllResolve(gpos::pointer<CPartitionPropagationSpec *> pSpec);
};	// class CPartitionPropagationSpec

}  // namespace gpopt

#endif	// !GPOPT_CPartitionPropagationSpec_H

// EOF
