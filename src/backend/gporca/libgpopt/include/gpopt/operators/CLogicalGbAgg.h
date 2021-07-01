//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CLogicalGbAgg.h
//
//	@doc:
//		Group Aggregate operator
//---------------------------------------------------------------------------
#ifndef GPOS_CLogicalGbAgg_H
#define GPOS_CLogicalGbAgg_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalUnary.h"

namespace gpopt
{
// fwd declaration
class CLogicalGbAgg;

//---------------------------------------------------------------------------
//	@class:
//		CLogicalGbAgg
//
//	@doc:
//		aggregate operator
//
//---------------------------------------------------------------------------
class CLogicalGbAgg : public CLogicalUnary
{
protected:
	// does local / intermediate / global aggregate generate duplicate values for the same group
	BOOL m_fGeneratesDuplicates;

	// array of columns used in distinct qualified aggregates (DQA)
	// used only in the case of intermediate aggregates
	gpos::owner<CColRefArray *> m_pdrgpcrArgDQA;

	// compute required stats columns for a GbAgg
	gpos::owner<CColRefSet *> PcrsStatGbAgg(
		CMemoryPool *mp, CExpressionHandle &exprhdl,
		gpos::pointer<CColRefSet *> pcrsInput, ULONG child_index,
		gpos::pointer<CColRefArray *> pdrgpcrGrp) const;

public:
	CLogicalGbAgg(const CLogicalGbAgg &) = delete;

	// the below enum specifically covers only 2 & 3 stage
	// scalar dqa, as they are used to find the one having
	// better cost context, rest of the aggs falls in others
	// category.
	enum EAggStage
	{
		EasTwoStageScalarDQA,
		EasThreeStageScalarDQA,
		EasOthers,

		EasSentinel
	};

	// ctor
	explicit CLogicalGbAgg(CMemoryPool *mp);

	// ctor
	CLogicalGbAgg(CMemoryPool *mp, gpos::owner<CColRefArray *> colref_array,
				  COperator::EGbAggType egbaggtype, EAggStage aggStage);

	// ctor
	CLogicalGbAgg(CMemoryPool *mp, gpos::owner<CColRefArray *> colref_array,
				  COperator::EGbAggType egbaggtype, BOOL fGeneratesDuplicates,
				  gpos::owner<CColRefArray *> pdrgpcrArgDQA,
				  EAggStage aggStage);

	// ctor
	CLogicalGbAgg(CMemoryPool *mp, gpos::owner<CColRefArray *> colref_array,
				  COperator::EGbAggType egbaggtype);

	// ctor
	CLogicalGbAgg(CMemoryPool *mp, gpos::owner<CColRefArray *> colref_array,
				  COperator::EGbAggType egbaggtype, BOOL fGeneratesDuplicates,
				  gpos::owner<CColRefArray *> pdrgpcrArgDQA);

	// ctor
	CLogicalGbAgg(CMemoryPool *mp, gpos::owner<CColRefArray *> colref_array,
				  gpos::owner<CColRefArray *> pdrgpcrMinimal,
				  COperator::EGbAggType egbaggtype);

	// ctor
	CLogicalGbAgg(CMemoryPool *mp, gpos::owner<CColRefArray *> colref_array,
				  gpos::owner<CColRefArray *> pdrgpcrMinimal,
				  COperator::EGbAggType egbaggtype, BOOL fGeneratesDuplicates,
				  gpos::owner<CColRefArray *> pdrgpcrArgDQA);

	// is this part of Two Stage Scalar DQA
	BOOL IsTwoStageScalarDQA() const;

	// is this part of Three Stage Scalar DQA
	BOOL IsThreeStageScalarDQA() const;

	// return the m_aggStage
	EAggStage
	AggStage() const
	{
		return m_aggStage;
	}

	// dtor
	~CLogicalGbAgg() override;

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopLogicalGbAgg;
	}

	// return a string for operator name
	const CHAR *
	SzId() const override
	{
		return "CLogicalGbAgg";
	}

	// does this aggregate generate duplicate values for the same group
	virtual BOOL
	FGeneratesDuplicates() const
	{
		return m_fGeneratesDuplicates;
	}

	// match function
	BOOL Matches(gpos::pointer<COperator *> pop) const override;

	// hash function
	ULONG HashValue() const override;

	// grouping columns accessor
	gpos::pointer<CColRefArray *>
	Pdrgpcr() const
	{
		return m_pdrgpcr;
	}

	// array of columns used in distinct qualified aggregates (DQA)
	gpos::pointer<CColRefArray *>
	PdrgpcrArgDQA() const
	{
		return m_pdrgpcrArgDQA;
	}

	// aggregate type
	COperator::EGbAggType
	Egbaggtype() const
	{
		return m_egbaggtype;
	}

	// is a global aggregate?
	BOOL
	FGlobal() const
	{
		return (COperator::EgbaggtypeGlobal == m_egbaggtype);
	}

	// minimal grouping columns accessor
	gpos::pointer<CColRefArray *>
	PdrgpcrMinimal() const
	{
		return m_pdrgpcrMinimal;
	}

	// return a copy of the operator with remapped columns
	gpos::owner<COperator *> PopCopyWithRemappedColumns(
		CMemoryPool *mp, gpos::pointer<UlongToColRefMap *> colref_mapping,
		BOOL must_exist) override;

	//-------------------------------------------------------------------------------------
	// Derived Relational Properties
	//-------------------------------------------------------------------------------------

	// derive output columns
	gpos::owner<CColRefSet *> DeriveOutputColumns(CMemoryPool *,
												  CExpressionHandle &) override;

	// derive outer references
	gpos::owner<CColRefSet *> DeriveOuterReferences(
		CMemoryPool *mp, CExpressionHandle &exprhdl) override;

	// derive not null columns
	gpos::owner<CColRefSet *> DeriveNotNullColumns(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

	// derive key collections
	gpos::owner<CKeyCollection *> DeriveKeyCollection(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

	// derive max card
	CMaxCard DeriveMaxCard(CMemoryPool *mp,
						   CExpressionHandle &exprhdl) const override;

	// derive constraint property
	gpos::owner<CPropConstraint *> DerivePropertyConstraint(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

	// compute required stats columns of the n-th child
	//-------------------------------------------------------------------------------------
	// Required Relational Properties
	//-------------------------------------------------------------------------------------

	// compute required stat columns of the n-th child
	gpos::owner<CColRefSet *> PcrsStat(CMemoryPool *mp,
									   CExpressionHandle &exprhdl,
									   gpos::pointer<CColRefSet *> pcrsInput,
									   ULONG child_index) const override;

	//-------------------------------------------------------------------------------------
	// Transformations
	//-------------------------------------------------------------------------------------

	// candidate set of xforms
	gpos::owner<CXformSet *> PxfsCandidates(CMemoryPool *mp) const override;

	// derive statistics
	gpos::owner<IStatistics *> PstatsDerive(
		CMemoryPool *mp, CExpressionHandle &exprhdl,
		gpos::pointer<IStatisticsArray *> stats_ctxt) const override;

	// stat promise
	EStatPromise
	Esp(CExpressionHandle &) const override
	{
		return CLogical::EspHigh;
	}

	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------

	// conversion function
	static gpos::cast_func<CLogicalGbAgg *>
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(EopLogicalGbAgg == pop->Eopid() ||
					EopLogicalGbAggDeduplicate == pop->Eopid());

		return dynamic_cast<CLogicalGbAgg *>(pop);
	}

	// debug print
	IOstream &OsPrint(IOstream &os) const override;

	// derive statistics
	static gpos::owner<IStatistics *> PstatsDerive(
		CMemoryPool *mp, gpos::pointer<IStatistics *> child_stats,
		gpos::pointer<CColRefArray *> pdrgpcrGroupingCols,
		gpos::pointer<ULongPtrArray *> pdrgpulComputedCols,
		gpos::pointer<CBitSet *> keys);

	// print group by aggregate type
	static IOstream &OsPrintGbAggType(IOstream &os,
									  COperator::EGbAggType egbaggtype);

private:
	// array of grouping columns
	gpos::owner<CColRefArray *> m_pdrgpcr;

	// minimal grouping columns based on FD's
	gpos::owner<CColRefArray *> m_pdrgpcrMinimal;

	// local / intermediate / global aggregate
	COperator::EGbAggType m_egbaggtype;

	// which type of multi-stage agg it is
	EAggStage m_aggStage;

};	// class CLogicalGbAgg

}  // namespace gpopt


#endif	// !GPOS_CLogicalGbAgg_H

// EOF
