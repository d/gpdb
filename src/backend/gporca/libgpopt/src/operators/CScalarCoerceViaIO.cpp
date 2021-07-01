//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CScalarCoerceViaIO.cpp
//
//	@doc:
//		Implementation of scalar CoerceViaIO operators
//
//	@owner:
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpopt/operators/CScalarCoerceViaIO.h"

#include "gpos/base.h"
#include "gpos/common/owner.h"

using namespace gpopt;
using namespace gpmd;


//---------------------------------------------------------------------------
//	@function:
//		CScalarCoerceViaIO::CScalarCoerceViaIO
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarCoerceViaIO::CScalarCoerceViaIO(CMemoryPool *mp,
									   gpos::Ref<IMDId> mdid_type,
									   INT type_modifier, ECoercionForm ecf,
									   INT location)
	: CScalarCoerceBase(mp, std::move(mdid_type), type_modifier, ecf, location)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarCoerceViaIO::Matches
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarCoerceViaIO::Matches(COperator *pop) const
{
	if (pop->Eopid() == Eopid())
	{
		CScalarCoerceViaIO *popCoerce = gpos::dyn_cast<CScalarCoerceViaIO>(pop);

		return popCoerce->MdidType()->Equals(MdidType()) &&
			   popCoerce->TypeModifier() == TypeModifier() &&
			   popCoerce->Ecf() == Ecf() && popCoerce->Location() == Location();
	}

	return false;
}


// EOF
