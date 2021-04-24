//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CScalarIsDistinctFrom.h
//
//	@doc:
//		Is distinct from operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarIsDistinctFrom_H
#define GPOPT_CScalarIsDistinctFrom_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/operators/CScalarCmp.h"


namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CScalarIsDistinctFrom
//
//	@doc:
//		Is distinct from operator
//
//---------------------------------------------------------------------------
class CScalarIsDistinctFrom : public CScalarCmp
{
private:
public:
	CScalarIsDistinctFrom(const CScalarIsDistinctFrom &) = delete;

	// ctor
	CScalarIsDistinctFrom(CMemoryPool *mp, gpos::owner<IMDId *> mdid_op,
						  const CWStringConst *pstrOp)
		: CScalarCmp(mp, std::move(mdid_op), pstrOp, IMDType::EcmptIDF)
	{
		GPOS_ASSERT(mdid_op->IsValid());
	}

	// dtor
	~CScalarIsDistinctFrom() override = default;

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopScalarIsDistinctFrom;
	}

	// boolean expression evaluation
	EBoolEvalResult Eber(
		gpos::pointer<ULongPtrArray *> pdrgpulChildren) const override;

	// return a string for operator name
	const CHAR *
	SzId() const override
	{
		return "CScalarIsDistinctFrom";
	}

	BOOL Matches(gpos::pointer<COperator *> pop) const override;

	// conversion function
	static gpos::cast_func<CScalarIsDistinctFrom *> PopConvert(COperator *pop);

	// get commuted scalar IDF operator
	gpos::owner<CScalarCmp *> PopCommutedOp(CMemoryPool *mp) override;

};	// class CScalarIsDistinctFrom

}  // namespace gpopt

#endif	// !GPOPT_CScalarIsDistinctFrom_H

// EOF
