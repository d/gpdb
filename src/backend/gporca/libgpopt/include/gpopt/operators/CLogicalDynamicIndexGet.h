//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalDynamicIndexGet.h
//
//	@doc:
//		Dynamic index get operator for partitioned tables
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalDynamicIndexGet_H
#define GPOPT_CLogicalDynamicIndexGet_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/COrderSpec.h"
#include "gpopt/metadata/CIndexDescriptor.h"
#include "gpopt/metadata/CName.h"
#include "gpopt/operators/CLogicalDynamicGetBase.h"


namespace gpopt
{
// fwd declarations
class CPartConstraint;

//---------------------------------------------------------------------------
//	@class:
//		CLogicalDynamicIndexGet
//
//	@doc:
//		Dynamic index accessor for partitioned tables
//
//---------------------------------------------------------------------------
class CLogicalDynamicIndexGet : public CLogicalDynamicGetBase
{
private:
	// index descriptor
	gpos::owner<CIndexDescriptor *> m_pindexdesc;

	// origin operator id -- gpos::ulong_max if operator was not generated via a transformation
	ULONG m_ulOriginOpId;

	// order spec
	gpos::owner<COrderSpec *> m_pos;

public:
	CLogicalDynamicIndexGet(const CLogicalDynamicIndexGet &) = delete;

	// ctors
	explicit CLogicalDynamicIndexGet(CMemoryPool *mp);

	CLogicalDynamicIndexGet(CMemoryPool *mp,
							gpos::pointer<const IMDIndex *> pmdindex,
							gpos::owner<CTableDescriptor *> ptabdesc,
							ULONG ulOriginOpId, const CName *pnameAlias,
							ULONG ulPartIndex,
							gpos::owner<CColRefArray *> pdrgpcrOutput,
							gpos::owner<CColRef2dArray *> pdrgpdrgpcrPart,
							gpos::owner<IMdIdArray *> partition_mdids);

	// dtor
	~CLogicalDynamicIndexGet() override;

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopLogicalDynamicIndexGet;
	}

	// return a string for operator name
	const CHAR *
	SzId() const override
	{
		return "CLogicalDynamicIndexGet";
	}

	// origin operator id -- gpos::ulong_max if operator was not generated via a transformation
	ULONG
	UlOriginOpId() const
	{
		return m_ulOriginOpId;
	}

	// index name
	const CName &
	Name() const override
	{
		return m_pindexdesc->Name();
	}

	// table alias name
	const CName &
	NameAlias() const
	{
		return *m_pnameAlias;
	}

	// index descriptor
	gpos::pointer<CIndexDescriptor *>
	Pindexdesc() const
	{
		return m_pindexdesc;
	}

	// order spec
	gpos::pointer<COrderSpec *>
	Pos() const
	{
		return m_pos;
	}

	// operator specific hash function
	ULONG HashValue() const override;

	// match function
	BOOL Matches(gpos::pointer<COperator *> pop) const override;

	// derive outer references
	gpos::owner<CColRefSet *> DeriveOuterReferences(
		CMemoryPool *mp, CExpressionHandle &exprhdl) override;

	// sensitivity to order of inputs
	BOOL FInputOrderSensitive() const override;

	// return a copy of the operator with remapped columns
	gpos::owner<COperator *> PopCopyWithRemappedColumns(
		CMemoryPool *mp, gpos::pointer<UlongToColRefMap *> colref_mapping,
		BOOL must_exist) override;

	//-------------------------------------------------------------------------------------
	// Required Relational Properties
	//-------------------------------------------------------------------------------------

	// compute required stat columns of the n-th child
	gpos::owner<CColRefSet *>
	PcrsStat(CMemoryPool *,				   //mp
			 CExpressionHandle &,		   // exprhdl
			 gpos::pointer<CColRefSet *>,  //pcrsInput
			 ULONG						   // child_index
	) const override
	{
		GPOS_ASSERT(!"CLogicalDynamicIndexGet has no children");
		return nullptr;
	}

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
	// Transformations
	//-------------------------------------------------------------------------------------

	// candidate set of xforms
	gpos::owner<CXformSet *> PxfsCandidates(CMemoryPool *mp) const override;

	//-------------------------------------------------------------------------------------
	// conversion function
	//-------------------------------------------------------------------------------------

	static gpos::cast_func<CLogicalDynamicIndexGet *>
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(EopLogicalDynamicIndexGet == pop->Eopid());

		return dynamic_cast<CLogicalDynamicIndexGet *>(pop);
	}


	// debug print
	IOstream &OsPrint(IOstream &) const override;

};	// class CLogicalDynamicIndexGet

}  // namespace gpopt

#endif	// !GPOPT_CLogicalDynamicIndexGet_H

// EOF
