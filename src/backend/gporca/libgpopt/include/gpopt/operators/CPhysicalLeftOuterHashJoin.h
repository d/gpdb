//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalLeftOuterHashJoin.h
//
//	@doc:
//		Left outer hash join operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalLeftOuterHashJoin_H
#define GPOPT_CPhysicalLeftOuterHashJoin_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/operators/CPhysicalHashJoin.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalLeftOuterHashJoin
//
//	@doc:
//		Left outer hash join operator
//
//---------------------------------------------------------------------------
class CPhysicalLeftOuterHashJoin : public CPhysicalHashJoin
{
private:
public:
	CPhysicalLeftOuterHashJoin(const CPhysicalLeftOuterHashJoin &) = delete;

	// ctor
	CPhysicalLeftOuterHashJoin(
		CMemoryPool *mp, gpos::owner<CExpressionArray *> pdrgpexprOuterKeys,
		gpos::owner<CExpressionArray *> pdrgpexprInnerKeys,
		gpos::owner<IMdIdArray *> hash_opfamilies);

	// dtor
	~CPhysicalLeftOuterHashJoin() override;

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopPhysicalLeftOuterHashJoin;
	}

	// return a string for operator name
	const CHAR *
	SzId() const override
	{
		return "CPhysicalLeftOuterHashJoin";
	}

	// conversion function
	static gpos::cast_func<CPhysicalLeftOuterHashJoin *>
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(EopPhysicalLeftOuterHashJoin == pop->Eopid());

		return dynamic_cast<CPhysicalLeftOuterHashJoin *>(pop);
	}


};	// class CPhysicalLeftOuterHashJoin

}  // namespace gpopt

#endif	// !GPOPT_CPhysicalLeftOuterHashJoin_H

// EOF
