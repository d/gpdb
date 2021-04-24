//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalAssert.h
//
//	@doc:
//		Assert operator
//---------------------------------------------------------------------------
#ifndef GPOS_CLogicalAssert_H
#define GPOS_CLogicalAssert_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalUnary.h"
#include "naucrates/dxl/errorcodes.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CLogicalAssert
//
//	@doc:
//		Assert operator
//
//---------------------------------------------------------------------------
class CLogicalAssert : public CLogicalUnary
{
private:
	// exception
	CException *m_pexc;

public:
	CLogicalAssert(const CLogicalAssert &) = delete;

	// ctors
	explicit CLogicalAssert(CMemoryPool *mp);

	CLogicalAssert(CMemoryPool *mp, CException *pexc);

	// dtor
	~CLogicalAssert() override
	{
		GPOS_DELETE(m_pexc);
	}

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopLogicalAssert;
	}

	// name of operator
	const CHAR *
	SzId() const override
	{
		return "CLogicalAssert";
	}

	// exception
	CException *
	Pexc() const
	{
		return m_pexc;
	}

	// match function;
	BOOL Matches(gpos::pointer<COperator *> pop) const override;

	//-------------------------------------------------------------------------------------
	// Derived Relational Properties
	//-------------------------------------------------------------------------------------

	// derive output columns
	gpos::owner<CColRefSet *> DeriveOutputColumns(CMemoryPool *,
												  CExpressionHandle &) override;

	// dervive keys
	gpos::owner<CKeyCollection *> DeriveKeyCollection(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

	// derive max card
	CMaxCard DeriveMaxCard(CMemoryPool *mp,
						   CExpressionHandle &exprhdl) const override;

	// derive constraint property
	gpos::owner<CPropConstraint *>
	DerivePropertyConstraint(CMemoryPool *mp,
							 CExpressionHandle &exprhdl) const override
	{
		return PpcDeriveConstraintFromPredicates(mp, exprhdl);
	}

	//-------------------------------------------------------------------------------------
	// Transformations
	//-------------------------------------------------------------------------------------

	// candidate set of xforms
	gpos::owner<CXformSet *> PxfsCandidates(CMemoryPool *) const override;

	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------

	// conversion function
	static gpos::cast_func<CLogicalAssert *>
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(EopLogicalAssert == pop->Eopid());

		return dynamic_cast<CLogicalAssert *>(pop);
	}

	// derive statistics
	gpos::owner<IStatistics *> PstatsDerive(
		CMemoryPool *mp, CExpressionHandle &exprhdl,
		gpos::pointer<IStatisticsArray *> stats_ctxt) const override;

	// debug print
	IOstream &OsPrint(IOstream &os) const override;

};	// class CLogicalAssert

}  // namespace gpopt

#endif	// !GPOS_CLogicalAssert_H

// EOF
