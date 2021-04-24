//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CScalarNullIf.h
//
//	@doc:
//		Scalar NullIf Operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarNullIf_H
#define GPOPT_CScalarNullIf_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/operators/CScalar.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CScalarNullIf
//
//	@doc:
//		Scalar NullIf operator
//
//---------------------------------------------------------------------------
class CScalarNullIf : public CScalar
{
private:
	// operator id
	gpos::owner<IMDId *> m_mdid_op;

	// return type
	gpos::owner<IMDId *> m_mdid_type;

	// does operator return NULL on NULL input?
	BOOL m_returns_null_on_null_input;

	// is operator return type BOOL?
	BOOL m_fBoolReturnType;

public:
	CScalarNullIf(const CScalarNullIf &) = delete;

	// ctor
	CScalarNullIf(CMemoryPool *mp, gpos::owner<IMDId *> mdid_op,
				  gpos::owner<IMDId *> mdid_type);

	// dtor
	~CScalarNullIf() override;

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopScalarNullIf;
	}

	// operator id
	virtual gpos::pointer<IMDId *>
	MdIdOp() const
	{
		return m_mdid_op;
	}

	// return type
	gpos::pointer<IMDId *>
	MdidType() const override
	{
		return m_mdid_type;
	}

	// return a string for operator name
	const CHAR *
	SzId() const override
	{
		return "CScalarNullIf";
	}

	// operator specific hash function
	ULONG HashValue() const override;

	// match function
	BOOL Matches(gpos::pointer<COperator *> pop) const override;

	// sensitivity to order of inputs
	BOOL
	FInputOrderSensitive() const override
	{
		return true;
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

	// boolean expression evaluation
	EBoolEvalResult Eber(
		gpos::pointer<ULongPtrArray *> pdrgpulChildren) const override;

	// conversion function
	static gpos::cast_func<CScalarNullIf *>
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(EopScalarNullIf == pop->Eopid());

		return dynamic_cast<CScalarNullIf *>(pop);
	}

};	// class CScalarNullIf

}  // namespace gpopt

#endif	// !GPOPT_CScalarNullIf_H

// EOF
