//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CLogicalSetOp.h
//
//	@doc:
//		Base for set operations
//---------------------------------------------------------------------------
#ifndef GPOS_CLogicalSetOp_H
#define GPOS_CLogicalSetOp_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/operators/CLogical.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CLogicalSetOp
//
//	@doc:
//		Base for all set operations
//
//---------------------------------------------------------------------------
class CLogicalSetOp : public CLogical
{
protected:
	// output column array
	gpos::owner<CColRefArray *> m_pdrgpcrOutput;

	// input column array
	gpos::owner<CColRef2dArray *> m_pdrgpdrgpcrInput;

	// set representation of output columns
	gpos::owner<CColRefSet *> m_pcrsOutput;

	// set representation of input columns
	gpos::owner<CColRefSetArray *> m_pdrgpcrsInput;

	// private copy ctor
	CLogicalSetOp(const CLogicalSetOp &);

	// build set representation of input/output columns for faster set operations
	void BuildColumnSets(CMemoryPool *mp);

	// output equivalence classes
	gpos::owner<CColRefSetArray *> PdrgpcrsOutputEquivClasses(
		CMemoryPool *mp, CExpressionHandle &exprhdl, BOOL fIntersect) const;

	// equivalence classes from one input child, mapped to output columns
	gpos::owner<CColRefSetArray *> PdrgpcrsInputMapped(
		CMemoryPool *mp, CExpressionHandle &exprhdl, ULONG ulChild) const;

	// constraints for a given output column from all children
	gpos::owner<CConstraintArray *> PdrgpcnstrColumn(CMemoryPool *mp,
													 CExpressionHandle &exprhdl,
													 ULONG ulColIndex,
													 ULONG ulStart) const;

	// get constraint for a given output column from a given children
	gpos::owner<CConstraint *> PcnstrColumn(CMemoryPool *mp,
											CExpressionHandle &exprhdl,
											ULONG ulColIndex,
											ULONG ulChild) const;

	// derive constraint property for intersect and union operators
	gpos::owner<CPropConstraint *> PpcDeriveConstraintIntersectUnion(
		CMemoryPool *mp, CExpressionHandle &exprhdl, BOOL fIntersect) const;

public:
	// ctor
	explicit CLogicalSetOp(CMemoryPool *mp);

	CLogicalSetOp(CMemoryPool *mp, gpos::owner<CColRefArray *> pdrgOutput,
				  gpos::owner<CColRefArray *> pdrgpcrLeft,
				  gpos::owner<CColRefArray *> pdrgpcrRight);

	CLogicalSetOp(CMemoryPool *mp, gpos::owner<CColRefArray *> pdrgpcrOutput,
				  gpos::owner<CColRef2dArray *> pdrgpdrgpcrInput);

	// dtor
	~CLogicalSetOp() override;

	// ident accessors
	EOperatorId Eopid() const override = 0;

	// return a string for operator name
	const CHAR *SzId() const override = 0;

	// accessor of output column array
	gpos::pointer<CColRefArray *>
	PdrgpcrOutput() const
	{
		GPOS_ASSERT(nullptr != m_pdrgpcrOutput);
		return m_pdrgpcrOutput;
	}

	// accessor of input column array
	gpos::pointer<CColRef2dArray *>
	PdrgpdrgpcrInput() const
	{
		GPOS_ASSERT(nullptr != m_pdrgpdrgpcrInput);
		return m_pdrgpdrgpcrInput;
	}

	// return true if we can pull projections up past this operator from its given child
	BOOL FCanPullProjectionsUp(ULONG  //child_index
	) const override
	{
		return false;
	}

	// match function
	BOOL Matches(gpos::pointer<COperator *> pop) const override;

	IOstream &OsPrint(IOstream &os) const override;

	//-------------------------------------------------------------------------------------
	// Derived Relational Properties
	//-------------------------------------------------------------------------------------

	// derive output columns
	gpos::owner<CColRefSet *> DeriveOutputColumns(CMemoryPool *,
												  CExpressionHandle &) override;

	// derive key collections
	gpos::owner<CKeyCollection *> DeriveKeyCollection(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

	// derive partition consumer info
	gpos::owner<CPartInfo *> DerivePartitionInfo(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

	//-------------------------------------------------------------------------------------
	// Required Relational Properties
	//-------------------------------------------------------------------------------------

	// compute required stat columns of the n-th child
	gpos::owner<CColRefSet *> PcrsStat(CMemoryPool *,		 // mp
									   CExpressionHandle &,	 // exprhdl
									   gpos::pointer<CColRefSet *> pcrsInput,
									   ULONG  // child_index
	) const override;

	// conversion function
	static gpos::cast_func<CLogicalSetOp *>
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(CUtils::FLogicalSetOp(pop));

		return dynamic_cast<CLogicalSetOp *>(pop);
	}

};	// class CLogicalSetOp

}  // namespace gpopt


#endif	// !GPOS_CLogicalSetOp_H

// EOF
