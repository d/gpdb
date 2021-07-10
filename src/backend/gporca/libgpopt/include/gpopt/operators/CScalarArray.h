//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CScalarArray.h
//
//	@doc:
//		Class for scalar arrays
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarArray_H
#define GPOPT_CScalarArray_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/operators/CScalar.h"
#include "gpopt/operators/CScalarConst.h"
#include "naucrates/md/IMDId.h"

namespace gpopt
{
using namespace gpos;
using namespace gpmd;

typedef CDynamicPtrArray<CScalarConst, CleanupRelease> CScalarConstArray;

//---------------------------------------------------------------------------
//	@class:
//		CScalarArray
//
//	@doc:
//		Scalar array
//
//---------------------------------------------------------------------------
class CScalarArray : public CScalar
{
private:
	// element type id
	gpos::owner<IMDId *> m_pmdidElem;

	// array type id
	gpos::owner<IMDId *> m_pmdidArray;

	// is array multidimensional
	BOOL m_fMultiDimensional;

	// const values
	gpos::owner<CScalarConstArray *> m_pdrgPconst;

public:
	CScalarArray(const CScalarArray &) = delete;

	// ctor
	CScalarArray(CMemoryPool *mp, IMDId *elem_type_mdid, IMDId *array_type_mdid,
				 BOOL is_multidimenstional);

	// ctor
	CScalarArray(CMemoryPool *mp, IMDId *elem_type_mdid, IMDId *array_type_mdid,
				 BOOL is_multidimenstional, CScalarConstArray *pdrgPconst);

	// dtor
	~CScalarArray() override;

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopScalarArray;
	}

	// return a string for aggregate function
	const CHAR *
	SzId() const override
	{
		return "CScalarArray";
	}


	// operator specific hash function
	ULONG HashValue() const override;

	// match function
	BOOL Matches(COperator *pop) const override;

	// sensitivity to order of inputs
	BOOL
	FInputOrderSensitive() const override
	{
		return true;
	}

	// return a copy of the operator with remapped columns
	COperator *
	PopCopyWithRemappedColumns(
		CMemoryPool *,						//mp,
		gpos::pointer<UlongToColRefMap *>,	//colref_mapping,
		BOOL								//must_exist
		) override
	{
		return PopCopyDefault();
	}

	// conversion function
	static gpos::cast_func<CScalarArray *>
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(EopScalarArray == pop->Eopid());

		return dynamic_cast<CScalarArray *>(pop);
	}

	// element type id
	gpos::pointer<IMDId *> PmdidElem() const;

	// array type id
	gpos::pointer<IMDId *> PmdidArray() const;

	// is array multi-dimensional
	BOOL FMultiDimensional() const;

	// type of expression's result
	gpos::pointer<IMDId *> MdidType() const override;

	// CScalarConst array
	gpos::pointer<CScalarConstArray *> PdrgPconst() const;

	// print
	IOstream &OsPrint(IOstream &os) const override;

};	// class CScalarArray

}  // namespace gpopt


#endif	// !GPOPT_CScalarArray_H

// EOF
