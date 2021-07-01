//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright 2011 EMC Corp.
//
//	@filename:
//		CLogicalLeftAntiSemiApply.h
//
//	@doc:
//		Logical Left Anti Semi Apply operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalLeftAntiSemiApply_H
#define GPOPT_CLogicalLeftAntiSemiApply_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalApply.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CLogicalLeftAntiSemiApply
//
//	@doc:
//		Logical Left Anti Semi Apply operator
//
//---------------------------------------------------------------------------
class CLogicalLeftAntiSemiApply : public CLogicalApply
{
private:
public:
	CLogicalLeftAntiSemiApply(const CLogicalLeftAntiSemiApply &) = delete;

	// ctor
	explicit CLogicalLeftAntiSemiApply(CMemoryPool *mp) : CLogicalApply(mp)
	{
	}

	// ctor
	CLogicalLeftAntiSemiApply(CMemoryPool *mp,
							  gpos::Ref<CColRefArray> pdrgpcrInner,
							  EOperatorId eopidOriginSubq)
		: CLogicalApply(mp, std::move(pdrgpcrInner), eopidOriginSubq)
	{
	}

	// dtor
	~CLogicalLeftAntiSemiApply() override = default;

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopLogicalLeftAntiSemiApply;
	}

	// return a string for operator name
	const CHAR *
	SzId() const override
	{
		return "CLogicalLeftAntiSemiApply";
	}

	// return true if we can pull projections up past this operator from its given child
	BOOL
	FCanPullProjectionsUp(ULONG child_index) const override
	{
		return (0 == child_index);
	}

	//-------------------------------------------------------------------------------------
	// Derived Relational Properties
	//-------------------------------------------------------------------------------------

	// derive output columns
	gpos::Ref<CColRefSet>
	DeriveOutputColumns(CMemoryPool *,	// mp
						CExpressionHandle &exprhdl) override
	{
		GPOS_ASSERT(3 == exprhdl.Arity());

		return PcrsDeriveOutputPassThru(exprhdl);
	}

	// derive not nullable output columns
	gpos::Ref<CColRefSet>
	DeriveNotNullColumns(CMemoryPool *,	 // mp
						 CExpressionHandle &exprhdl) const override
	{
		return PcrsDeriveNotNullPassThruOuter(exprhdl);
	}

	// dervive keys
	gpos::Ref<CKeyCollection> DeriveKeyCollection(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

	// derive max card
	CMaxCard DeriveMaxCard(CMemoryPool *mp,
						   CExpressionHandle &exprhdl) const override;

	// derive constraint property
	gpos::Ref<CPropConstraint>
	DerivePropertyConstraint(CMemoryPool *,	 //mp,
							 CExpressionHandle &exprhdl) const override
	{
		return PpcDeriveConstraintPassThru(exprhdl, 0 /*ulChild*/);
	}

	//-------------------------------------------------------------------------------------
	// Transformations
	//-------------------------------------------------------------------------------------

	// candidate set of xforms
	gpos::Ref<CXformSet> PxfsCandidates(CMemoryPool *mp) const override;

	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------

	// return true if operator is a left anti semi apply
	BOOL
	FLeftAntiSemiApply() const override
	{
		return true;
	}

	// return a copy of the operator with remapped columns
	gpos::Ref<COperator> PopCopyWithRemappedColumns(
		CMemoryPool *mp, UlongToColRefMap *colref_mapping,
		BOOL must_exist) override;

	// conversion function
	static CLogicalLeftAntiSemiApply *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(CUtils::FLeftAntiSemiApply(pop));

		return dynamic_cast<CLogicalLeftAntiSemiApply *>(pop);
	}

};	// class CLogicalLeftAntiSemiApply

}  // namespace gpopt


#endif	// !GPOPT_CLogicalLeftAntiSemiApply_H

// EOF
