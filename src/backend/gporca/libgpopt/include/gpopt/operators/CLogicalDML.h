//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalDML.h
//
//	@doc:
//		Logical DML operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalDML_H
#define GPOPT_CLogicalDML_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/operators/CLogical.h"

namespace gpopt
{
// fwd declarations
class CTableDescriptor;

//---------------------------------------------------------------------------
//	@class:
//		CLogicalDML
//
//	@doc:
//		Logical DML operator
//
//---------------------------------------------------------------------------
class CLogicalDML : public CLogical
{
public:
	// enum of DML operators
	enum EDMLOperator
	{
		EdmlInsert,
		EdmlDelete,
		EdmlUpdate,
		EdmlSentinel
	};

	static const WCHAR m_rgwszDml[EdmlSentinel][10];

private:
	// dml operator
	EDMLOperator m_edmlop;

	// table descriptor
	gpos::owner<CTableDescriptor *> m_ptabdesc;

	// source columns
	gpos::owner<CColRefArray *> m_pdrgpcrSource;

	// set of modified columns from the target table
	gpos::owner<CBitSet *> m_pbsModified;

	// action column
	CColRef *m_pcrAction;

	// table oid column
	CColRef *m_pcrTableOid;

	// ctid column
	CColRef *m_pcrCtid;

	// segmentId column
	CColRef *m_pcrSegmentId;

	// tuple oid column if one exists
	CColRef *m_pcrTupleOid;

public:
	CLogicalDML(const CLogicalDML &) = delete;

	// ctor
	explicit CLogicalDML(CMemoryPool *mp);

	// ctor
	CLogicalDML(CMemoryPool *mp, EDMLOperator edmlop,
				gpos::owner<CTableDescriptor *> ptabdesc,
				gpos::owner<CColRefArray *> colref_array,
				gpos::owner<CBitSet *> pbsModified, CColRef *pcrAction,
				CColRef *pcrTableOid, CColRef *pcrCtid, CColRef *pcrSegmentId,
				CColRef *pcrTupleOid);

	// dtor
	~CLogicalDML() override;

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopLogicalDML;
	}

	// return a string for operator name
	const CHAR *
	SzId() const override
	{
		return "CLogicalDML";
	}

	// dml operator
	EDMLOperator
	Edmlop() const
	{
		return m_edmlop;
	}

	// source columns
	gpos::pointer<CColRefArray *>
	PdrgpcrSource() const
	{
		return m_pdrgpcrSource;
	}

	// modified columns set
	gpos::pointer<CBitSet *>
	PbsModified() const
	{
		return m_pbsModified;
	}

	// action column
	CColRef *
	PcrAction() const
	{
		return m_pcrAction;
	}

	// table oid column
	CColRef *
	PcrTableOid() const
	{
		return m_pcrTableOid;
	}

	// ctid column
	CColRef *
	PcrCtid() const
	{
		return m_pcrCtid;
	}

	// segmentId column
	CColRef *
	PcrSegmentId() const
	{
		return m_pcrSegmentId;
	}

	// return table's descriptor
	gpos::pointer<CTableDescriptor *>
	Ptabdesc() const
	{
		return m_ptabdesc;
	}

	// tuple oid column
	CColRef *
	PcrTupleOid() const
	{
		return m_pcrTupleOid;
	}

	// operator specific hash function
	ULONG HashValue() const override;

	// match function
	BOOL Matches(gpos::pointer<COperator *> pop) const override;

	// sensitivity to order of inputs
	BOOL
	FInputOrderSensitive() const override
	{
		return false;
	}

	// return a copy of the operator with remapped columns
	gpos::owner<COperator *> PopCopyWithRemappedColumns(
		CMemoryPool *mp, gpos::pointer<UlongToColRefMap *> colref_mapping,
		BOOL must_exist) override;

	//-------------------------------------------------------------------------------------
	// Derived Relational Properties
	//-------------------------------------------------------------------------------------

	// derive output columns
	gpos::owner<CColRefSet *> DeriveOutputColumns(
		CMemoryPool *mp, CExpressionHandle &exprhdl) override;

	// derive constraint property
	gpos::owner<CPropConstraint *> DerivePropertyConstraint(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

	// derive max card
	CMaxCard DeriveMaxCard(CMemoryPool *mp,
						   CExpressionHandle &exprhdl) const override;

	// derive partition consumer info
	gpos::owner<CPartInfo *>
	DerivePartitionInfo(CMemoryPool *,	// mp,
						CExpressionHandle &exprhdl) const override
	{
		return PpartinfoPassThruOuter(exprhdl);
	}

	// compute required stats columns of the n-th child
	gpos::owner<CColRefSet *>
	PcrsStat(CMemoryPool *,		   // mp
			 CExpressionHandle &,  // exprhdl
			 gpos::pointer<CColRefSet *> pcrsInput,
			 ULONG	// child_index
	) const override
	{
		return PcrsStatsPassThru(pcrsInput);
	}

	//-------------------------------------------------------------------------------------
	// Transformations
	//-------------------------------------------------------------------------------------

	// candidate set of xforms
	gpos::owner<CXformSet *> PxfsCandidates(CMemoryPool *mp) const override;

	// derive key collections
	gpos::owner<CKeyCollection *> DeriveKeyCollection(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

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
	static gpos::cast_func<CLogicalDML *>
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(EopLogicalDML == pop->Eopid());

		return dynamic_cast<CLogicalDML *>(pop);
	}

	// debug print
	IOstream &OsPrint(IOstream &) const override;

};	// class CLogicalDML
}  // namespace gpopt

#endif	// !GPOPT_CLogicalDML_H

// EOF
