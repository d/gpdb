//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalLeftOuterHashJoin.cpp
//
//	@doc:
//		Implementation of left outer hash join operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalLeftOuterHashJoin.h"

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/base/CDistributionSpecHashed.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalLeftOuterHashJoin::CPhysicalLeftOuterHashJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalLeftOuterHashJoin::CPhysicalLeftOuterHashJoin(
	CMemoryPool *mp, gpos::Ref<CExpressionArray> pdrgpexprOuterKeys,
	gpos::Ref<CExpressionArray> pdrgpexprInnerKeys,
	gpos::Ref<IMdIdArray> hash_opfamilies)
	: CPhysicalHashJoin(mp, std::move(pdrgpexprOuterKeys),
						std::move(pdrgpexprInnerKeys),
						std::move(hash_opfamilies))
{
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalLeftOuterHashJoin::~CPhysicalLeftOuterHashJoin
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalLeftOuterHashJoin::~CPhysicalLeftOuterHashJoin() = default;

// EOF
