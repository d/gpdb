//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CLogicalUnion.h
//
//	@doc:
//		Logical Union operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalUnion_H
#define GPOPT_CLogicalUnion_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalSetOp.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CLogicalUnion
//
//	@doc:
//		union operators
//
//---------------------------------------------------------------------------
class CLogicalUnion : public CLogicalSetOp
{
private:
public:
	CLogicalUnion(const CLogicalUnion &) = delete;

	// ctor
	explicit CLogicalUnion(CMemoryPool *mp);

	CLogicalUnion(CMemoryPool *mp, gpos::owner<CColRefArray *> pdrgpcrOutput,
				  gpos::owner<CColRef2dArray *> pdrgpdrgpcrInput);

	// dtor
	~CLogicalUnion() override;

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopLogicalUnion;
	}

	// return a string for operator name
	const CHAR *
	SzId() const override
	{
		return "CLogicalUnion";
	}

	// sensitivity to order of inputs
	BOOL
	FInputOrderSensitive() const override
	{
		return true;
	}

	// return a copy of the operator with remapped columns
	gpos::owner<COperator *> PopCopyWithRemappedColumns(
		CMemoryPool *mp, gpos::pointer<UlongToColRefMap *> colref_mapping,
		BOOL must_exist) override;

	//-------------------------------------------------------------------------------------
	// Derived Relational Properties
	//-------------------------------------------------------------------------------------

	// derive max card
	CMaxCard DeriveMaxCard(CMemoryPool *mp,
						   CExpressionHandle &exprhdl) const override;

	// derive constraint property
	gpos::owner<CPropConstraint *>
	DerivePropertyConstraint(CMemoryPool *mp,
							 CExpressionHandle &exprhdl) const override
	{
		return PpcDeriveConstraintSetop(mp, exprhdl, false /*fIntersect*/);
	}

	//-------------------------------------------------------------------------------------
	// Transformations
	//-------------------------------------------------------------------------------------

	// candidate set of xforms
	gpos::owner<CXformSet *> PxfsCandidates(CMemoryPool *mp) const override;

	//-------------------------------------------------------------------------------------
	// Derived Stats
	//-------------------------------------------------------------------------------------

	// stat promise
	EStatPromise
	Esp(CExpressionHandle &) const override
	{
		return CLogical::EspHigh;
	}

	// derive statistics
	gpos::owner<IStatistics *> PstatsDerive(
		CMemoryPool *mp, CExpressionHandle &exprhdl,
		gpos::pointer<IStatisticsArray *> stats_ctxt) const override;

	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------

	// conversion function
	static gpos::cast_func<CLogicalUnion *>
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(EopLogicalUnion == pop->Eopid());

		return dynamic_cast<CLogicalUnion *>(pop);
	}

};	// class CLogicalUnion

}  // namespace gpopt


#endif	// !GPOPT_CLogicalUnion_H

// EOF
