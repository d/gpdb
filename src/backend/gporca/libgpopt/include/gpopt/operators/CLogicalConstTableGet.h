//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CLogicalConstTableGet.h
//
//	@doc:
//		Constant table accessor
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalConstTableGet_H
#define GPOPT_CLogicalConstTableGet_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/operators/CLogical.h"

namespace gpopt
{
// dynamic array of datum arrays -- array owns elements
typedef CDynamicPtrArray<IDatumArray, CleanupRelease> IDatum2dArray;

//---------------------------------------------------------------------------
//	@class:
//		CLogicalConstTableGet
//
//	@doc:
//		Constant table accessor
//
//---------------------------------------------------------------------------
class CLogicalConstTableGet : public CLogical
{
private:
	// array of column descriptors: the schema of the const table
	gpos::owner<CColumnDescriptorArray *> m_pdrgpcoldesc;

	// array of datum arrays
	gpos::owner<IDatum2dArray *> m_pdrgpdrgpdatum;

	// output columns
	gpos::owner<CColRefArray *> m_pdrgpcrOutput;

	// construct column descriptors from column references
	static gpos::owner<CColumnDescriptorArray *> PdrgpcoldescMapping(
		CMemoryPool *mp, gpos::pointer<CColRefArray *> colref_array);

public:
	CLogicalConstTableGet(const CLogicalConstTableGet &) = delete;

	// ctors
	explicit CLogicalConstTableGet(CMemoryPool *mp);

	CLogicalConstTableGet(CMemoryPool *mp,
						  gpos::owner<CColumnDescriptorArray *> pdrgpcoldesc,
						  gpos::owner<IDatum2dArray *> pdrgpdrgpdatum);

	CLogicalConstTableGet(CMemoryPool *mp,
						  gpos::owner<CColRefArray *> pdrgpcrOutput,
						  gpos::owner<IDatum2dArray *> pdrgpdrgpdatum);

	// dtor
	~CLogicalConstTableGet() override;

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopLogicalConstTableGet;
	}

	// return a string for operator name
	const CHAR *
	SzId() const override
	{
		return "CLogicalConstTableGet";
	}

	// col descr accessor
	gpos::pointer<CColumnDescriptorArray *>
	Pdrgpcoldesc() const
	{
		return m_pdrgpcoldesc;
	}

	// const table values accessor
	gpos::pointer<IDatum2dArray *>
	Pdrgpdrgpdatum() const
	{
		return m_pdrgpdrgpdatum;
	}

	// accessors
	gpos::pointer<CColRefArray *>
	PdrgpcrOutput() const
	{
		return m_pdrgpcrOutput;
	}

	// sensitivity to order of inputs
	BOOL FInputOrderSensitive() const override;

	// operator specific hash function
	ULONG HashValue() const override;

	// match function
	BOOL Matches(gpos::pointer<COperator *> pop) const override;

	// return a copy of the operator with remapped columns
	gpos::owner<COperator *> PopCopyWithRemappedColumns(
		CMemoryPool *mp, gpos::pointer<UlongToColRefMap *> colref_mapping,
		BOOL must_exist) override;

	//-------------------------------------------------------------------------------------
	// Derived Relational Properties
	//-------------------------------------------------------------------------------------

	// derive output columns
	gpos::owner<CColRefSet *> DeriveOutputColumns(CMemoryPool *,
												  CExpressionHandle &) override;

	// derive max card
	CMaxCard DeriveMaxCard(CMemoryPool *mp,
						   CExpressionHandle &exprhdl) const override;

	// derive partition consumer info
	gpos::owner<CPartInfo *>
	DerivePartitionInfo(CMemoryPool *mp,
						CExpressionHandle &	 //exprhdl
	) const override
	{
		return GPOS_NEW(mp) CPartInfo(mp);
	}

	// derive constraint property
	gpos::owner<CPropConstraint *>
	DerivePropertyConstraint(CMemoryPool *mp,
							 CExpressionHandle &  // exprhdl
	) const override
	{
		// TODO:  - Jan 11, 2013; compute constraints based on the
		// datum values in this CTG
		return GPOS_NEW(mp) CPropConstraint(
			mp, GPOS_NEW(mp) CColRefSetArray(mp), nullptr /*pcnstr*/);
	}

	//-------------------------------------------------------------------------------------
	// Required Relational Properties
	//-------------------------------------------------------------------------------------

	// compute required stat columns of the n-th child
	gpos::owner<CColRefSet *>
	PcrsStat(CMemoryPool *,				   // mp
			 CExpressionHandle &,		   // exprhdl
			 gpos::pointer<CColRefSet *>,  // pcrsInput
			 ULONG						   // child_index
	) const override
	{
		GPOS_ASSERT(!"CLogicalConstTableGet has no children");
		return nullptr;
	}

	// derive statistics
	gpos::owner<IStatistics *> PstatsDerive(
		CMemoryPool *mp, CExpressionHandle &exprhdl,
		gpos::pointer<IStatisticsArray *> stats_ctxt) const override;

	//-------------------------------------------------------------------------------------
	// Transformations
	//-------------------------------------------------------------------------------------

	// candidate set of xforms
	gpos::owner<CXformSet *> PxfsCandidates(CMemoryPool *mp) const override;

	// stat promise
	EStatPromise
	Esp(CExpressionHandle &) const override
	{
		return CLogical::EspLow;
	}

	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------

	// conversion function
	static gpos::cast_func<CLogicalConstTableGet *>
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(EopLogicalConstTableGet == pop->Eopid());

		return dynamic_cast<CLogicalConstTableGet *>(pop);
	}


	// debug print
	IOstream &OsPrint(IOstream &) const override;

};	// class CLogicalConstTableGet

}  // namespace gpopt


#endif	// !GPOPT_CLogicalConstTableGet_H

// EOF
