//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalSequence.cpp
//
//	@doc:
//		Implementation of logical sequence operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CLogicalSequence.h"

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequence::CLogicalSequence
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CLogicalSequence::CLogicalSequence(CMemoryPool *mp) : CLogical(mp)
{
	GPOS_ASSERT(nullptr != mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequence::Matches
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CLogicalSequence::Matches(gpos::pointer<COperator *> pop) const
{
	return pop->Eopid() == Eopid();
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequence::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
gpos::owner<CXformSet *>
CLogicalSequence::PxfsCandidates(CMemoryPool *mp) const
{
	gpos::owner<CXformSet *> xform_set = GPOS_NEW(mp) CXformSet(mp);
	(void) xform_set->ExchangeSet(CXform::ExfImplementSequence);
	return xform_set;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequence::DeriveOutputColumns
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
gpos::owner<CColRefSet *>
CLogicalSequence::DeriveOutputColumns(CMemoryPool *,  // mp
									  CExpressionHandle &exprhdl)
{
	GPOS_ASSERT(1 <= exprhdl.Arity());

	// get output columns of last child
	gpos::owner<CColRefSet *> pcrs =
		exprhdl.DeriveOutputColumns(exprhdl.Arity() - 1);
	pcrs->AddRef();

	return pcrs;
}



//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequence::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
gpos::owner<CKeyCollection *>
CLogicalSequence::DeriveKeyCollection(CMemoryPool *,  // mp
									  CExpressionHandle &exprhdl) const
{
	// return key of last child
	const ULONG arity = exprhdl.Arity();
	return PkcDeriveKeysPassThru(exprhdl, arity - 1 /* ulChild */);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequence::DeriveMaxCard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalSequence::DeriveMaxCard(CMemoryPool *,	// mp
								CExpressionHandle &exprhdl) const
{
	// pass on max card of last child
	return exprhdl.DeriveMaxCard(exprhdl.Arity() - 1);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSequence::DerivePartitionInfo
//
//	@doc:
//		Derive part consumers
//
//---------------------------------------------------------------------------
gpos::owner<CPartInfo *>
CLogicalSequence::DerivePartitionInfo(CMemoryPool *mp,
									  CExpressionHandle &exprhdl) const
{
	return PpartinfoDeriveCombine(mp, exprhdl);
}


// EOF
