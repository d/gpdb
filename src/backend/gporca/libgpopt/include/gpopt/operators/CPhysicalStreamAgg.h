//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalStreamAgg.h
//
//	@doc:
//		Sort-based stream Aggregate operator
//---------------------------------------------------------------------------
#ifndef GPOS_CPhysicalStreamAgg_H
#define GPOS_CPhysicalStreamAgg_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/operators/CPhysicalAgg.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalStreamAgg
//
//	@doc:
//		Sort-based aggregate operator
//
//---------------------------------------------------------------------------
class CPhysicalStreamAgg : public CPhysicalAgg
{
private:
	// local order spec
	gpos::owner<COrderSpec *> m_pos;

	// set representation of minimal grouping columns
	gpos::owner<CColRefSet *> m_pcrsMinimalGrpCols;

	// construct order spec on grouping column so that it covers required order spec
	static gpos::owner<COrderSpec *> PosCovering(
		CMemoryPool *mp, gpos::pointer<COrderSpec *> posRequired,
		gpos::pointer<CColRefArray *> pdrgpcrGrp);

protected:
	// compute required sort columns of the n-th child
	gpos::owner<COrderSpec *> PosRequiredStreamAgg(
		CMemoryPool *mp, CExpressionHandle &exprhdl,
		gpos::pointer<COrderSpec *> posRequired, ULONG child_index,
		gpos::pointer<CColRefArray *> pdrgpcrGrp) const;

	// initialize the order spec using the given array of columns
	void InitOrderSpec(CMemoryPool *mp,
					   gpos::pointer<CColRefArray *> pdrgpcrOrder);

public:
	CPhysicalStreamAgg(const CPhysicalStreamAgg &) = delete;

	// ctor
	CPhysicalStreamAgg(
		CMemoryPool *mp, gpos::owner<CColRefArray *> colref_array,
		gpos::pointer<CColRefArray *>
			pdrgpcrMinimal,	 // minimal grouping columns based on FD's
		COperator::EGbAggType egbaggtype, BOOL fGeneratesDuplicates,
		gpos::owner<CColRefArray *> pdrgpcrArgDQA, BOOL fMultiStage,
		BOOL isAggFromSplitDQA, CLogicalGbAgg::EAggStage aggStage,
		BOOL should_enforce_distribution = true
		// should_enforce_distribution should be set to false if
		// 'local' and 'global' splits don't need to have different
		// distributions. This flag is set to false if the local
		// aggregate has been created by CXformEagerAgg.
	);

	// dtor
	~CPhysicalStreamAgg() override;


	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopPhysicalStreamAgg;
	}

	// return a string for operator name
	const CHAR *
	SzId() const override
	{
		return "CPhysicalStreamAgg";
	}

	//-------------------------------------------------------------------------------------
	// Required Plan Properties
	//-------------------------------------------------------------------------------------

	// compute required sort columns of the n-th child
	gpos::owner<COrderSpec *>
	PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
				gpos::pointer<COrderSpec *> posRequired, ULONG child_index,
				gpos::pointer<CDrvdPropArray *>,  //pdrgpdpCtxt,
				ULONG							  //ulOptReq
	) const override
	{
		return PosRequiredStreamAgg(mp, exprhdl, posRequired, child_index,
									m_pdrgpcrMinimal);
	}

	//-------------------------------------------------------------------------------------
	// Derived Plan Properties
	//-------------------------------------------------------------------------------------

	// derive sort order
	gpos::owner<COrderSpec *> PosDerive(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

	//-------------------------------------------------------------------------------------
	// Enforced Properties
	//-------------------------------------------------------------------------------------

	// return order property enforcing type for this operator
	CEnfdProp::EPropEnforcingType EpetOrder(
		CExpressionHandle &exprhdl,
		gpos::pointer<const CEnfdOrder *> peo) const override;

	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------

	// conversion function
	static gpos::cast_func<CPhysicalStreamAgg *>
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(EopPhysicalStreamAgg == pop->Eopid() ||
					EopPhysicalStreamAggDeduplicate == pop->Eopid());

		return dynamic_cast<CPhysicalStreamAgg *>(pop);
	}

};	// class CPhysicalStreamAgg

}  // namespace gpopt


#endif	// !GPOS_CPhysicalStreamAgg_H

// EOF
