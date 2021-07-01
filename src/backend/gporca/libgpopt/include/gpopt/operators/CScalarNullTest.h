//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CScalarNullTest.h
//
//	@doc:
//		Scalar null test
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarNullTest_H
#define GPOPT_CScalarNullTest_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/base/CDrvdProp.h"
#include "gpopt/operators/CScalar.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CScalarNullTest
//
//	@doc:
//		Scalar null test operator
//
//---------------------------------------------------------------------------
class CScalarNullTest : public CScalar
{
private:
public:
	CScalarNullTest(const CScalarNullTest &) = delete;

	// ctor
	explicit CScalarNullTest(CMemoryPool *mp) : CScalar(mp)
	{
	}

	// dtor
	~CScalarNullTest() override = default;


	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopScalarNullTest;
	}

	// return a string for operator name
	const CHAR *
	SzId() const override
	{
		return "CScalarNullTest";
	}

	// match function
	BOOL Matches(gpos::pointer<COperator *>) const override;

	// sensitivity to order of inputs
	BOOL
	FInputOrderSensitive() const override
	{
		return false;
	}

	// return a copy of the operator with remapped columns
	gpos::owner<COperator *>
	PopCopyWithRemappedColumns(
		CMemoryPool *,						//mp,
		gpos::pointer<UlongToColRefMap *>,	//colref_mapping,
		BOOL								//must_exist
		) override
	{
		return PopCopyDefault();
	}

	// the type of the scalar expression
	gpos::pointer<IMDId *> MdidType() const override;

	// boolean expression evaluation
	EBoolEvalResult Eber(
		gpos::pointer<ULongPtrArray *> pdrgpulChildren) const override;

	// conversion function
	static gpos::cast_func<CScalarNullTest *>
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(EopScalarNullTest == pop->Eopid());

		return dynamic_cast<CScalarNullTest *>(pop);
	}

};	// class CScalarNullTest

}  // namespace gpopt


#endif	// !GPOPT_CScalarNullTest_H

// EOF
