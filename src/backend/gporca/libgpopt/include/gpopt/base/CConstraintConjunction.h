//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CConstraintConjunction.h
//
//	@doc:
//		Representation of a conjunction constraint. A conjunction is a number
//		of ANDed constraints
//---------------------------------------------------------------------------
#ifndef GPOPT_CConstraintConjunction_H
#define GPOPT_CConstraintConjunction_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/base/CConstraint.h"
#include "gpopt/base/CRange.h"

namespace gpopt
{
using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		CConstraintConjunction
//
//	@doc:
//		Representation of a conjunction constraint
//
//---------------------------------------------------------------------------
class CConstraintConjunction : public CConstraint
{
private:
	// array of constraints
	gpos::owner<CConstraintArray *> m_pdrgpcnstr;

	// mapping colref -> array of child constraints
	gpos::owner<ColRefToConstraintArrayMap *> m_phmcolconstr;

public:
	CConstraintConjunction(const CConstraintConjunction &) = delete;

	// ctor
	CConstraintConjunction(CMemoryPool *mp,
						   gpos::owner<CConstraintArray *> pdrgpcnstr);

	// dtor
	~CConstraintConjunction() override;

	// constraint type accessor
	EConstraintType
	Ect() const override
	{
		return CConstraint::EctConjunction;
	}

	// all constraints in conjunction
	gpos::pointer<CConstraintArray *>
	Pdrgpcnstr() const
	{
		return m_pdrgpcnstr;
	}

	// is this constraint a contradiction
	BOOL FContradiction() const override;

	// scalar expression
	gpos::pointer<CExpression *> PexprScalar(CMemoryPool *mp) override;

	// check if there is a constraint on the given column
	BOOL FConstraint(const CColRef *colref) const override;

	// return a copy of the constraint with remapped columns
	gpos::owner<CConstraint *> PcnstrCopyWithRemappedColumns(
		CMemoryPool *mp, gpos::pointer<UlongToColRefMap *> colref_mapping,
		BOOL must_exist) override;

	// return constraint on a given column
	gpos::owner<CConstraint *> Pcnstr(CMemoryPool *mp,
									  const CColRef *colref) override;

	// return constraint on a given column set
	gpos::owner<CConstraint *> Pcnstr(
		CMemoryPool *mp, gpos::pointer<CColRefSet *> pcrs) override;

	// return a clone of the constraint for a different column
	gpos::owner<CConstraint *> PcnstrRemapForColumn(
		CMemoryPool *mp, CColRef *colref) const override;

	// print
	IOstream &
	OsPrint(IOstream &os) const override
	{
		return PrintConjunctionDisjunction(os, m_pdrgpcnstr);
	}

};	// class CConstraintConjunction
}  // namespace gpopt

#endif	// !GPOPT_CConstraintConjunction_H

// EOF
