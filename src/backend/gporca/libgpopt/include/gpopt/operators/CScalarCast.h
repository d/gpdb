//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CScalarCast.h
//
//	@doc:
//		Scalar casting operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarCast_H
#define GPOPT_CScalarCast_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/base/CDrvdProp.h"
#include "gpopt/operators/CScalar.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CScalarCast
//
//	@doc:
//		Scalar casting operator
//
//---------------------------------------------------------------------------
class CScalarCast : public CScalar
{
private:
	// return type metadata id in the catalog
	gpos::owner<IMDId *> m_return_type_mdid;

	// function to be used for casting
	gpos::owner<IMDId *> m_func_mdid;

	// whether or not this cast is binary coercible
	BOOL m_is_binary_coercible;

	// does operator return NULL on NULL input?
	BOOL m_returns_null_on_null_input;

	// is operator's return type BOOL?
	BOOL m_fBoolReturnType;

public:
	CScalarCast(const CScalarCast &) = delete;

	// ctor
	CScalarCast(CMemoryPool *mp, gpos::owner<IMDId *> return_type_mdid,
				gpos::owner<IMDId *> mdid_func, BOOL is_binary_coercible);

	// dtor
	~CScalarCast() override
	{
		m_func_mdid->Release();
		m_return_type_mdid->Release();
	}


	// ident accessors

	// the type of the scalar expression
	gpos::pointer<IMDId *>
	MdidType() const override
	{
		return m_return_type_mdid;
	}

	// func that casts
	gpos::pointer<IMDId *>
	FuncMdId() const
	{
		return m_func_mdid;
	}

	EOperatorId
	Eopid() const override
	{
		return EopScalarCast;
	}

	// return a string for operator name
	const CHAR *
	SzId() const override
	{
		return "CScalarCast";
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

	// whether or not this cast is binary coercible
	BOOL
	IsBinaryCoercible() const
	{
		return m_is_binary_coercible;
	}

	// boolean expression evaluation
	EBoolEvalResult
	Eber(gpos::pointer<ULongPtrArray *> pdrgpulChildren) const override
	{
		return EberNullOnAllNullChildren(pdrgpulChildren);
	}

	// conversion function
	static gpos::cast_func<CScalarCast *>
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(EopScalarCast == pop->Eopid());

		return dynamic_cast<CScalarCast *>(pop);
	}

};	// class CScalarCast

}  // namespace gpopt


#endif	// !GPOPT_CScalarCast_H

// EOF
