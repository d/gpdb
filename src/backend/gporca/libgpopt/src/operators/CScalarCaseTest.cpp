//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CScalarCaseTest.cpp
//
//	@doc:
//		Implementation of scalar case test operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CScalarCaseTest.h"

#include "gpos/base.h"
#include "gpos/common/owner.h"

using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CScalarCaseTest::CScalarCaseTest
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarCaseTest::CScalarCaseTest(CMemoryPool *mp,
								 gpos::owner<IMDId *> mdid_type)
	: CScalar(mp), m_mdid_type(std::move(mdid_type))
{
	GPOS_ASSERT(m_mdid_type->IsValid());
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarCaseTest::~CScalarCaseTest
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CScalarCaseTest::~CScalarCaseTest()
{
	m_mdid_type->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarCaseTest::HashValue
//
//	@doc:
//		Operator specific hash function; combined hash of operator id and
//		return type id
//
//---------------------------------------------------------------------------
ULONG
CScalarCaseTest::HashValue() const
{
	return gpos::CombineHashes(COperator::HashValue(),
							   m_mdid_type->HashValue());
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarCaseTest::FInputOrderSensitive
//
//	@doc:
//		Not called for leaf operators
//
//---------------------------------------------------------------------------
BOOL
CScalarCaseTest::FInputOrderSensitive() const
{
	GPOS_ASSERT(!"Unexpected call of function FInputOrderSensitive");
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarCaseTest::Matches
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarCaseTest::Matches(gpos::pointer<COperator *> pop) const
{
	if (pop->Eopid() == Eopid())
	{
		gpos::pointer<CScalarCaseTest *> popScCaseTest =
			gpos::dyn_cast<CScalarCaseTest>(pop);

		// match if return types are identical
		return popScCaseTest->MdidType()->Equals(m_mdid_type);
	}

	return false;
}

// EOF
