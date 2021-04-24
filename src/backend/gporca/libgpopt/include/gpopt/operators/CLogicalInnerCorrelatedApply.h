//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalInnerCorrelatedApply.h
//
//	@doc:
//		Logical Inner Correlated Apply operator;
//		a variant of inner apply that captures the need to implement a
//		correlated-execution strategy on the physical side
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalInnerCorrelatedApply_H
#define GPOPT_CLogicalInnerCorrelatedApply_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/operators/CLogicalInnerApply.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CLogicalInnerCorrelatedApply
//
//	@doc:
//		Logical Apply operator used in scalar subquery transformations
//
//---------------------------------------------------------------------------
class CLogicalInnerCorrelatedApply : public CLogicalInnerApply
{
private:
public:
	CLogicalInnerCorrelatedApply(const CLogicalInnerCorrelatedApply &) = delete;

	// ctor
	CLogicalInnerCorrelatedApply(CMemoryPool *mp,
								 gpos::owner<CColRefArray *> pdrgpcrInner,
								 EOperatorId eopidOriginSubq);

	// ctor for patterns
	explicit CLogicalInnerCorrelatedApply(CMemoryPool *mp);

	// dtor
	~CLogicalInnerCorrelatedApply() override = default;

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopLogicalInnerCorrelatedApply;
	}

	// return a string for operator name
	const CHAR *
	SzId() const override
	{
		return "CLogicalInnerCorrelatedApply";
	}

	// applicable transformations
	gpos::owner<CXformSet *> PxfsCandidates(CMemoryPool *mp) const override;

	// match function
	BOOL Matches(gpos::pointer<COperator *> pop) const override;

	// return a copy of the operator with remapped columns
	gpos::owner<COperator *> PopCopyWithRemappedColumns(
		CMemoryPool *mp, gpos::pointer<UlongToColRefMap *> colref_mapping,
		BOOL must_exist) override;

	// return true if operator is a correlated apply
	BOOL
	FCorrelated() const override
	{
		return true;
	}

	// conversion function
	static gpos::cast_func<CLogicalInnerCorrelatedApply *>
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(EopLogicalInnerCorrelatedApply == pop->Eopid());

		return dynamic_cast<CLogicalInnerCorrelatedApply *>(pop);
	}

};	// class CLogicalInnerCorrelatedApply

}  // namespace gpopt


#endif	// !GPOPT_CLogicalInnerCorrelatedApply_H

// EOF
