//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 VMware, Inc. or its affiliates.
//
//	@filename:
//		CLogicalGbAggDeduplicate.h
//
//	@doc:
//		Group Aggregate operator for deduplicating join outputs. This is used
//		for example when we transform left semijoin to inner join. E.g. the
//		following expression:
//			LSJ
//			|-- A
//			|-- B
//			+-- A.a1 <> B.b1
//		can be transformed to:
//			GbAggDedup (group on keys of A)
//			+-- InnerJoin
//				|-- A
//				|-- B
//				+-- A.a1 <> B.b1
//---------------------------------------------------------------------------
#ifndef GPOS_CLogicalGbAggDeduplicate_H
#define GPOS_CLogicalGbAggDeduplicate_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalGbAgg.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CLogicalGbAggDeduplicate
//
//	@doc:
//		aggregate operator for deduplicating join outputs
//
//---------------------------------------------------------------------------
class CLogicalGbAggDeduplicate : public CLogicalGbAgg
{
private:
	// array of keys from the join's child
	gpos::Ref<CColRefArray> m_pdrgpcrKeys;

public:
	CLogicalGbAggDeduplicate(const CLogicalGbAggDeduplicate &) = delete;

	// ctor
	explicit CLogicalGbAggDeduplicate(CMemoryPool *mp);

	// ctor
	CLogicalGbAggDeduplicate(CMemoryPool *mp,
							 gpos::Ref<CColRefArray> colref_array,
							 COperator::EGbAggType egbaggtype,
							 gpos::Ref<CColRefArray> pdrgpcrKeys = nullptr);

	// ctor
	CLogicalGbAggDeduplicate(CMemoryPool *mp,
							 gpos::Ref<CColRefArray> colref_array,
							 gpos::Ref<CColRefArray> pdrgpcrMinimal,
							 COperator::EGbAggType egbaggtype,
							 gpos::Ref<CColRefArray> pdrgpcrKeys = nullptr);

	// dtor
	~CLogicalGbAggDeduplicate() override;

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopLogicalGbAggDeduplicate;
	}

	// return a string for operator name
	const CHAR *
	SzId() const override
	{
		return "CLogicalGbAggDeduplicate";
	}

	// array of keys from the join's child that needs to be deduped
	CColRefArray *
	PdrgpcrKeys() const
	{
		return m_pdrgpcrKeys.get();
	}

	// match function
	BOOL Matches(COperator *pop) const override;

	// hash function
	ULONG HashValue() const override;

	// return a copy of the operator with remapped columns
	gpos::Ref<COperator> PopCopyWithRemappedColumns(
		CMemoryPool *mp, UlongToColRefMap *colref_mapping,
		BOOL must_exist) override;

	//-------------------------------------------------------------------------------------
	// Derived Relational Properties
	//-------------------------------------------------------------------------------------

	// derive key collections
	gpos::Ref<CKeyCollection> DeriveKeyCollection(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

	// compute required stats columns of the n-th child
	//-------------------------------------------------------------------------------------
	// Required Relational Properties
	//-------------------------------------------------------------------------------------

	// compute required stat columns of the n-th child
	gpos::Ref<CColRefSet> PcrsStat(CMemoryPool *mp, CExpressionHandle &exprhdl,
								   CColRefSet *pcrsInput,
								   ULONG child_index) const override;

	//-------------------------------------------------------------------------------------
	// Transformations
	//-------------------------------------------------------------------------------------

	// candidate set of xforms
	gpos::Ref<CXformSet> PxfsCandidates(CMemoryPool *mp) const override;

	// derive statistics
	gpos::Ref<IStatistics> PstatsDerive(
		CMemoryPool *mp, CExpressionHandle &exprhdl,
		IStatisticsArray *stats_ctxt) const override;

	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------

	// conversion function
	static CLogicalGbAggDeduplicate *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(EopLogicalGbAggDeduplicate == pop->Eopid());

		return dynamic_cast<CLogicalGbAggDeduplicate *>(pop);
	}


	// debug print
	IOstream &OsPrint(IOstream &os) const override;

};	// class CLogicalGbAggDeduplicate

}  // namespace gpopt


#endif	// !GPOS_CLogicalGbAggDeduplicate_H

// EOF
