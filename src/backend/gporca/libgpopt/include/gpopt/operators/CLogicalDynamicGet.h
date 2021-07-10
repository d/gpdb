//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalDynamicGet.h
//
//	@doc:
//		Dynamic table accessor for partitioned tables
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalDynamicGet_H
#define GPOPT_CLogicalDynamicGet_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/metadata/CName.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CLogicalDynamicGetBase.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CLogicalDynamicGet
//
//	@doc:
//		Dynamic table accessor
//
//---------------------------------------------------------------------------
class CLogicalDynamicGet : public CLogicalDynamicGetBase
{
public:
	CLogicalDynamicGet(const CLogicalDynamicGet &) = delete;

	// ctors
	explicit CLogicalDynamicGet(CMemoryPool *mp);

	CLogicalDynamicGet(CMemoryPool *mp, const CName *pnameAlias,
					   gpos::owner<CTableDescriptor *> ptabdesc,
					   ULONG ulPartIndex,
					   gpos::owner<CColRefArray *> pdrgpcrOutput,
					   gpos::owner<CColRef2dArray *> pdrgpdrgpcrPart,
					   gpos::owner<IMdIdArray *> partition_mdids);

	CLogicalDynamicGet(CMemoryPool *mp, const CName *pnameAlias,
					   gpos::owner<CTableDescriptor *> ptabdesc,
					   ULONG ulPartIndex,
					   gpos::owner<IMdIdArray *> partition_mdids);

	// dtor
	~CLogicalDynamicGet() override;

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopLogicalDynamicGet;
	}

	// return a string for operator name
	const CHAR *
	SzId() const override
	{
		return "CLogicalDynamicGet";
	}

	// operator specific hash function
	ULONG HashValue() const override;

	// match function
	BOOL Matches(gpos::pointer<COperator *> pop) const override;

	// sensitivity to order of inputs
	BOOL FInputOrderSensitive() const override;

	// return a copy of the operator with remapped columns
	gpos::owner<COperator *> PopCopyWithRemappedColumns(
		CMemoryPool *mp, gpos::pointer<UlongToColRefMap *> colref_mapping,
		BOOL must_exist) override;

	//-------------------------------------------------------------------------------------
	// Derived Relational Properties
	//-------------------------------------------------------------------------------------


	// derive join depth
	ULONG
	DeriveJoinDepth(CMemoryPool *,		 // mp
					CExpressionHandle &	 // exprhdl
	) const override
	{
		return 1;
	}

	// derive table descriptor
	gpos::pointer<CTableDescriptor *>
	DeriveTableDescriptor(CMemoryPool *,	   // mp
						  CExpressionHandle &  // exprhdl
	) const override
	{
		return m_ptabdesc;
	}

	// derive max card
	CMaxCard DeriveMaxCard(CMemoryPool *mp,
						   CExpressionHandle &exprhdl) const override;


	//-------------------------------------------------------------------------------------
	// Required Relational Properties
	//-------------------------------------------------------------------------------------

	// compute required stat columns of the n-th child
	gpos::owner<CColRefSet *>
	PcrsStat(CMemoryPool *,				   // mp,
			 CExpressionHandle &,		   // exprhdl
			 gpos::pointer<CColRefSet *>,  //pcrsInput
			 ULONG						   // child_index
	) const override
	{
		GPOS_ASSERT(!"CLogicalDynamicGet has no children");
		return nullptr;
	}

	//-------------------------------------------------------------------------------------
	// Transformations
	//-------------------------------------------------------------------------------------

	// candidate set of xforms
	gpos::owner<CXformSet *> PxfsCandidates(CMemoryPool *mp) const override;

	//-------------------------------------------------------------------------------------
	// Statistics
	//-------------------------------------------------------------------------------------

	// derive statistics
	gpos::owner<IStatistics *> PstatsDerive(
		CMemoryPool *mp, CExpressionHandle &exprhdl,
		gpos::pointer<IStatisticsArray *> stats_ctxt) const override;

	// stat promise
	EStatPromise
	Esp(CExpressionHandle &) const override
	{
		return CLogical::EspHigh;
	}

	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------

	// conversion function
	static gpos::cast_func<CLogicalDynamicGet *>
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(EopLogicalDynamicGet == pop->Eopid());

		return dynamic_cast<CLogicalDynamicGet *>(pop);
	}

	// debug print
	IOstream &OsPrint(IOstream &) const override;

};	// class CLogicalDynamicGet

}  // namespace gpopt


#endif	// !GPOPT_CLogicalDynamicGet_H

// EOF
