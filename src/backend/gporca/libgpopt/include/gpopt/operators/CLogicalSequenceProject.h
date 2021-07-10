//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalSequenceProject.h
//
//	@doc:
//		Logical Sequence Project operator
//---------------------------------------------------------------------------
#ifndef GPOS_CLogicalSequenceProject_H
#define GPOS_CLogicalSequenceProject_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/base/CDistributionSpec.h"
#include "gpopt/base/COrderSpec.h"
#include "gpopt/base/CWindowFrame.h"
#include "gpopt/operators/CLogicalUnary.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CLogicalSequenceProject
//
//	@doc:
//		Logical Sequence Project operator
//
//---------------------------------------------------------------------------
class CLogicalSequenceProject : public CLogicalUnary
{
private:
	// partition by keys
	gpos::Ref<CDistributionSpec> m_pds;

	// order specs of child window functions
	gpos::Ref<COrderSpecArray> m_pdrgpos;

	// frames of child window functions
	gpos::Ref<CWindowFrameArray> m_pdrgpwf;

	// flag indicating if current operator has any non-empty order specs
	BOOL m_fHasOrderSpecs;

	// flag indicating if current operator has any non-empty frame specs
	BOOL m_fHasFrameSpecs;

	// set the flag indicating that SeqPrj has specified order specs
	void SetHasOrderSpecs(CMemoryPool *mp);

	// set the flag indicating that SeqPrj has specified frame specs
	void SetHasFrameSpecs(CMemoryPool *mp);

public:
	CLogicalSequenceProject(const CLogicalSequenceProject &) = delete;

	// ctor
	CLogicalSequenceProject(CMemoryPool *mp, gpos::Ref<CDistributionSpec> pds,
							gpos::Ref<COrderSpecArray> pdrgpos,
							gpos::Ref<CWindowFrameArray> pdrgpwf);

	// ctor for pattern
	explicit CLogicalSequenceProject(CMemoryPool *mp);

	// dtor
	~CLogicalSequenceProject() override;

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopLogicalSequenceProject;
	}

	// operator name
	const CHAR *
	SzId() const override
	{
		return "CLogicalSequenceProject";
	}

	// distribution spec
	CDistributionSpec *
	Pds() const
	{
		return m_pds.get();
	}

	// order by keys
	COrderSpecArray *
	Pdrgpos() const
	{
		return m_pdrgpos.get();
	}

	// frame specifications
	CWindowFrameArray *
	Pdrgpwf() const
	{
		return m_pdrgpwf.get();
	}

	// return true if non-empty order specs are used by current operator
	BOOL
	FHasOrderSpecs() const
	{
		return m_fHasOrderSpecs;
	}

	// return true if non-empty frame specs are used by current operator
	BOOL
	FHasFrameSpecs() const
	{
		return m_fHasFrameSpecs;
	}

	// return a copy of the operator with remapped columns
	gpos::Ref<COperator> PopCopyWithRemappedColumns(
		CMemoryPool *mp, UlongToColRefMap *colref_mapping,
		BOOL must_exist) override;

	// return true if we can pull projections up past this operator from its given child
	BOOL FCanPullProjectionsUp(ULONG  //child_index
	) const override
	{
		return false;
	}

	//-------------------------------------------------------------------------------------
	// Derived Relational Properties
	//-------------------------------------------------------------------------------------

	// derive output columns
	gpos::Ref<CColRefSet> DeriveOutputColumns(
		CMemoryPool *mp, CExpressionHandle &exprhdl) override;

	// derive outer references
	gpos::Ref<CColRefSet> DeriveOuterReferences(
		CMemoryPool *mp, CExpressionHandle &exprhdl) override;

	// dervive keys
	gpos::Ref<CKeyCollection> DeriveKeyCollection(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

	// derive max card
	CMaxCard DeriveMaxCard(CMemoryPool *mp,
						   CExpressionHandle &exprhdl) const override;

	// derive constraint property
	gpos::Ref<CPropConstraint>
	DerivePropertyConstraint(CMemoryPool *,	 //mp,
							 CExpressionHandle &exprhdl) const override
	{
		return PpcDeriveConstraintPassThru(exprhdl, 0 /*ulChild*/);
	}

	//-------------------------------------------------------------------------------------
	// Transformations
	//-------------------------------------------------------------------------------------

	// candidate set of xforms
	gpos::Ref<CXformSet> PxfsCandidates(CMemoryPool *mp) const override;

	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------

	// derive statistics
	gpos::Ref<IStatistics> PstatsDerive(
		CMemoryPool *mp, CExpressionHandle &exprhdl,
		IStatisticsArray *stats_ctxt) const override;

	// match function
	BOOL Matches(COperator *pop) const override;

	ULONG HashValue() const override;

	// print
	IOstream &OsPrint(IOstream &os) const override;

	// remove outer references from Order By/ Partition By clauses, and return a new operator
	gpos::Ref<CLogicalSequenceProject> PopRemoveLocalOuterRefs(
		CMemoryPool *mp, CExpressionHandle &exprhdl);

	// check for outer references in Partition/Order, or window frame edges
	BOOL FHasLocalReferencesTo(const CColRefSet *outerRefsToCheck) const;

	// conversion function
	static CLogicalSequenceProject *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(EopLogicalSequenceProject == pop->Eopid());

		return dynamic_cast<CLogicalSequenceProject *>(pop);
	}

};	// class CLogicalSequenceProject

}  // namespace gpopt

#endif	// !GPOS_CLogicalSequenceProject_H

// EOF
