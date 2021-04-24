//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 - 2011 EMC CORP.
//
//	@filename:
//		CExpressionHandle.h
//
//	@doc:
//		Handle to convey context wherever an expression is used in a shallow
//		context, i.e. operator and the properties of its children but no
//		access to the children is needed.
//---------------------------------------------------------------------------
#ifndef GPOPT_CExpressionHandle_H
#define GPOPT_CExpressionHandle_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CDrvdProp.h"
#include "gpopt/base/CDrvdPropRelational.h"
#include "gpopt/base/CReqdProp.h"
#include "gpopt/search/CGroupExpression.h"
#include "naucrates/statistics/IStatistics.h"


namespace gpopt
{
// fwd declaration
class CExpression;
class COperator;
class CDrvdPropPlan;
class CDrvdPropScalar;
class CPropConstraint;
class CCostContext;

using namespace gpos;



//---------------------------------------------------------------------------
//	@class:
//		CExpressionHandle
//
//	@doc:
//		Context for expression; abstraction for group expressions and
//		stand-alone expressions/DAGs;
//		a handle is attached to either an expression or a group expression
//
//---------------------------------------------------------------------------
class CExpressionHandle
{
	friend class CExpression;

private:
	// memory pool
	CMemoryPool *m_mp;

	// attached expression
	gpos::owner<CExpression *> m_pexpr;

	// attached group expression
	gpos::owner<CGroupExpression *> m_pgexpr;

	// attached cost context
	gpos::pointer<CCostContext *> m_pcc;

	// derived plan properties of the gexpr attached by a CostContext under
	// the default CDrvdPropCtxtPlan. See DerivePlanPropsForCostContext()
	// NB: does NOT support on-demand property derivation
	gpos::owner<CDrvdProp *> m_pdpplan;

	// statistics of attached expr/gexpr;
	// set during derived stats computation
	gpos::owner<IStatistics *> m_pstats;

	// required properties of attached expr/gexpr;
	// set during required property computation
	gpos::owner<CReqdProp *> m_prp;

	// array of children's derived stats
	gpos::owner<IStatisticsArray *> m_pdrgpstat;

	// array of children's required properties
	gpos::owner<CReqdPropArray *> m_pdrgprp;

	// return an array of stats objects starting from the first stats object referenced by child
	gpos::owner<IStatisticsArray *> PdrgpstatOuterRefs(
		gpos::pointer<IStatisticsArray *> statistics_array, ULONG child_index);

	// check if stats are derived for attached expression and its children
	BOOL FStatsDerived() const;

	// copy stats from attached expression/group expression to local stats members
	void CopyStats();

	// return True if handle is attached to a leaf pattern
	BOOL FAttachedToLeafPattern() const;

	// stat derivation at root operator where handle is attached
	void DeriveRootStats(IStatisticsArray *stats_ctxt);

public:
	CExpressionHandle(const CExpressionHandle &) = delete;

	// ctor
	explicit CExpressionHandle(CMemoryPool *mp);

	// dtor
	~CExpressionHandle();

	// attach handle to a given expression
	void Attach(gpos::pointer<CExpression *> pexpr);

	// attach handle to a given group expression
	void Attach(gpos::pointer<CGroupExpression *> pgexpr);

	// attach handle to a given cost context
	void Attach(gpos::pointer<CCostContext *> pcc);

	// recursive property derivation,
	void DeriveProps(CDrvdPropCtxt *pdpctxt);

	// recursive stats derivation
	void DeriveStats(IStatisticsArray *stats_ctxt,
					 BOOL fComputeRootStats = true);

	// stats derivation for attached cost context
	void DeriveCostContextStats();

	// stats derivation using given properties and context
	void DeriveStats(CMemoryPool *pmpLocal, CMemoryPool *pmpGlobal,
					 CReqdPropRelational *prprel,
					 IStatisticsArray *stats_ctxt) const;

	// derive the properties of the plan carried by attached cost context,
	// using default CDrvdPropCtxtPlan
	void DerivePlanPropsForCostContext();

	// initialize required properties container
	void InitReqdProps(gpos::pointer<CReqdProp *> prpInput);

	// compute required properties of the n-th child
	void ComputeChildReqdProps(ULONG child_index, CDrvdPropArray *pdrgpdpCtxt,
							   ULONG ulOptReq);

	// copy required properties of the n-th child
	void CopyChildReqdProps(ULONG child_index, gpos::owner<CReqdProp *> prp);

	// compute required columns of the n-th child
	void ComputeChildReqdCols(ULONG child_index, CDrvdPropArray *pdrgpdpCtxt);

	// required properties computation of all children
	void ComputeReqdProps(gpos::pointer<CReqdProp *> prpInput, ULONG ulOptReq);

	// derived relational props of n-th child
	CDrvdPropRelational *GetRelationalProperties(ULONG child_index) const;

	// derived stats of n-th child
	gpos::pointer<IStatistics *> Pstats(ULONG child_index) const;

	// derived plan props of n-th child
	CDrvdPropPlan *Pdpplan(ULONG child_index) const;

	// derived scalar props of n-th child
	CDrvdPropScalar *GetDrvdScalarProps(ULONG child_index) const;

	// derived properties of attached expr/gexpr
	gpos::pointer<CDrvdProp *> Pdp() const;

	// derived relational properties of attached expr/gexpr
	CDrvdPropRelational *GetRelationalProperties() const;

	// stats of attached expr/gexpr
	gpos::pointer<IStatistics *> Pstats();

	// required properties of attached expr/gexpr
	gpos::pointer<CReqdProp *>
	Prp() const
	{
		return m_prp;
	}

	// check if given child is a scalar
	BOOL FScalarChild(ULONG child_index) const;

	// required relational props of n-th child
	CReqdPropRelational *GetReqdRelationalProps(ULONG child_index) const;

	// required plan props of n-th child
	CReqdPropPlan *Prpp(ULONG child_index) const;

	// arity function
	ULONG Arity() const;

	// index of the last non-scalar child
	ULONG UlLastNonScalarChild() const;

	// index of the first non-scalar child
	ULONG UlFirstNonScalarChild() const;

	// number of non-scalar children
	ULONG UlNonScalarChildren() const;

	// accessor for operator
	COperator *Pop() const;

	// accessor for child operator
	COperator *Pop(ULONG child_index) const;

	// accessor for grandchild operator
	COperator *PopGrandchild(ULONG child_index, ULONG grandchild_index,
							 CCostContext **grandchildContext) const;

	// accessor for expression
	gpos::pointer<CExpression *>
	Pexpr() const
	{
		return m_pexpr;
	}

	// accessor for group expression
	gpos::pointer<CGroupExpression *>
	Pgexpr() const
	{
		return m_pgexpr;
	}

	// check for outer references
	BOOL
	HasOuterRefs() const
	{
		return (0 < DeriveOuterReferences()->Size());
	}

	// check if attached expression must execute on a single host
	BOOL
	NeedsSingletonExecution() const
	{
		return DeriveFunctionProperties()->NeedsSingletonExecution();
	}

	// check for outer references in the given child
	BOOL
	HasOuterRefs(ULONG child_index) const
	{
		return (0 < DeriveOuterReferences(child_index)->Size());
	}

	// get next child index based on child optimization order, return true if such index could be found
	BOOL FNextChildIndex(ULONG *pulChildIndex  // output: index to be changed
	) const;

	// return the index of first child to be optimized
	ULONG UlFirstOptimizedChildIndex() const;

	// return the index of last child to be optimized
	ULONG UlLastOptimizedChildIndex() const;

	// return the index of child to be optimized next to the given child
	ULONG UlNextOptimizedChildIndex(ULONG child_index) const;

	// return the index of child optimized before the given child
	ULONG UlPreviousOptimizedChildIndex(ULONG child_index) const;

	// get the function properties of a child
	CFunctionProp *PfpChild(ULONG child_index) const;

	// check whether an expression's children have a volatile function
	BOOL FChildrenHaveVolatileFuncScan() const;

	// return a representative (inexact) scalar child at given index
	CExpression *PexprScalarRepChild(ULONG child_index) const;

	// return a representative (inexact) scalar expression attached to handle
	gpos::pointer<CExpression *> PexprScalarRep() const;

	// return an exact scalar child at given index or return null if not possible
	CExpression *PexprScalarExactChild(ULONG child_index,
									   BOOL error_on_null_return = false) const;

	// return an exact scalar expression attached to handle or null if not possible
	CExpression *PexprScalarExact() const;

	void DeriveProducerStats(ULONG child_index,
							 gpos::pointer<CColRefSet *> pcrsStat) const;

	// return the columns used by a logical operator internally as well
	// as columns used by all its scalar children
	gpos::owner<CColRefSet *> PcrsUsedColumns(CMemoryPool *mp) const;

	CColRefSet *DeriveOuterReferences() const;
	CColRefSet *DeriveOuterReferences(ULONG child_index) const;

	CColRefSet *DeriveOutputColumns() const;
	CColRefSet *DeriveOutputColumns(ULONG child_index) const;

	CColRefSet *DeriveNotNullColumns() const;
	CColRefSet *DeriveNotNullColumns(ULONG child_index) const;

	CColRefSet *DeriveCorrelatedApplyColumns() const;
	CColRefSet *DeriveCorrelatedApplyColumns(ULONG child_index) const;

	CMaxCard DeriveMaxCard() const;
	CMaxCard DeriveMaxCard(ULONG child_index) const;

	CKeyCollection *DeriveKeyCollection() const;
	CKeyCollection *DeriveKeyCollection(ULONG child_index) const;

	CPropConstraint *DerivePropertyConstraint() const;
	CPropConstraint *DerivePropertyConstraint(ULONG child_index) const;

	ULONG DeriveJoinDepth() const;
	ULONG DeriveJoinDepth(ULONG child_index) const;

	CFunctionProp *DeriveFunctionProperties() const;
	CFunctionProp *DeriveFunctionProperties(ULONG child_index) const;

	CFunctionalDependencyArray *Pdrgpfd() const;
	CFunctionalDependencyArray *Pdrgpfd(ULONG child_index) const;

	CPartInfo *DerivePartitionInfo() const;
	CPartInfo *DerivePartitionInfo(ULONG child_index) const;

	CTableDescriptor *DeriveTableDescriptor() const;
	CTableDescriptor *DeriveTableDescriptor(ULONG child_index) const;

	// Scalar property accessors
	CColRefSet *DeriveDefinedColumns(ULONG child_index) const;
	CColRefSet *DeriveUsedColumns(ULONG child_index) const;
	CColRefSet *DeriveSetReturningFunctionColumns(ULONG child_index) const;
	BOOL DeriveHasSubquery(ULONG child_index) const;
	CPartInfo *DeriveScalarPartitionInfo(ULONG child_index) const;
	CFunctionProp *DeriveScalarFunctionProperties(ULONG child_index) const;
	BOOL DeriveHasNonScalarFunction(ULONG child_index) const;
	ULONG DeriveTotalDistinctAggs(ULONG child_index) const;
	BOOL DeriveHasMultipleDistinctAggs(ULONG child_index) const;
	BOOL DeriveHasScalarArrayCmp(ULONG child_index) const;

};	// class CExpressionHandle

}  // namespace gpopt


#endif	// !GPOPT_CExpressionHandle_H

// EOF
