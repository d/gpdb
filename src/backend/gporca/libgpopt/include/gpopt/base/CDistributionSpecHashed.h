//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CDistributionSpecHashed.h
//
//	@doc:
//		Description of a hashed distribution;
//		Can be used as required or derived property;
//---------------------------------------------------------------------------
#ifndef GPOPT_CDistributionSpecHashed_H
#define GPOPT_CDistributionSpecHashed_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/base/CColRef.h"
#include "gpopt/base/CDistributionSpecRandom.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CDistributionSpecHashed
//
//	@doc:
//		Class for representing hashed distribution specification.
//
//---------------------------------------------------------------------------
class CDistributionSpecHashed : public CDistributionSpecRandom
{
private:
	// array of distribution expressions
	gpos::owner<CExpressionArray *> m_pdrgpexpr;

	gpos::owner<IMdIdArray *> m_opfamilies;

	// are NULLS consistently distributed
	BOOL m_fNullsColocated;

	// equivalent hashed distribution introduced by a hash join
	gpos::owner<CDistributionSpecHashed *> m_pdshashedEquiv;

	// array of expression arrays, an array at position n
	// is the list of expression which are equivalent to the distribution expr
	// at position n in m_pdrgpexpr array.
	gpos::owner<CExpressionArrays *> m_equiv_hash_exprs;

	// check if specs are compatible wrt to co-location of nulls;
	// HD1 satisfies HD2 if:
	//	* HD1 colocates NULLs or
	//  * HD2 doesn't care about NULLs
	BOOL
	FNullsColocatedCompatible(
		gpos::pointer<const CDistributionSpecHashed *> pds) const
	{
		return (m_fNullsColocated || !pds->m_fNullsColocated);
	}

	// check if specs are compatible wrt to duplicate sensitivity
	// HD1 satisfies HD2 if:
	//	* HD1 is duplicate sensitive, or
	//  * HD2 doesn't care about duplicates
	BOOL
	FDuplicateSensitiveCompatible(
		gpos::pointer<const CDistributionSpecHashed *> pds) const
	{
		return (m_is_duplicate_sensitive || !pds->m_is_duplicate_sensitive);
	}

	// exact match against given hashed distribution
	BOOL FMatchHashedDistribution(
		gpos::pointer<const CDistributionSpecHashed *> pdshashed) const;

	BOOL FDistributionSpecHashedOnlyOnGpSegmentId() const;

public:
	CDistributionSpecHashed(const CDistributionSpecHashed &) = delete;

	// ctor
	CDistributionSpecHashed(CExpressionArray *pdrgpexpr, BOOL fNullsColocated,
							IMdIdArray *opfamilies = nullptr);

	// ctor
	CDistributionSpecHashed(CExpressionArray *pdrgpexpr, BOOL fNullsColocated,
							CDistributionSpecHashed *pdshashedEquiv,
							IMdIdArray *opfamilies = nullptr);

	static gpos::owner<CDistributionSpecHashed *> MakeHashedDistrSpec(
		CMemoryPool *mp, CExpressionArray *pdrgpexpr, BOOL fNullsColocated,
		CDistributionSpecHashed *pdshashedEquiv,
		gpos::owner<IMdIdArray *> opfamilies);

	// dtor
	~CDistributionSpecHashed() override;

	void PopulateDefaultOpfamilies();

	// distribution type accessor
	EDistributionType
	Edt() const override
	{
		return CDistributionSpec::EdtHashed;
	}

	const CHAR *
	SzId() const override
	{
		return "HASHED";
	}

	// expression array accessor
	gpos::pointer<CExpressionArray *>
	Pdrgpexpr() const
	{
		return m_pdrgpexpr;
	}

	// is distribution nulls colocated
	BOOL
	FNullsColocated() const
	{
		return m_fNullsColocated;
	}

	// equivalent hashed distribution
	gpos::pointer<CDistributionSpecHashed *>
	PdshashedEquiv() const
	{
		return m_pdshashedEquiv;
	}

	gpos::pointer<IMdIdArray *>
	Opfamilies() const
	{
		return m_opfamilies;
	}

	// columns used by distribution expressions
	CColRefSet *PcrsUsed(CMemoryPool *mp) const override;

	// return a copy of the distribution spec after excluding the given columns
	virtual gpos::owner<CDistributionSpecHashed *> PdshashedExcludeColumns(
		CMemoryPool *mp, CColRefSet *pcrs);

	// does this distribution match the given one
	BOOL Matches(gpos::pointer<const CDistributionSpec *> pds) const override;

	// does this distribution satisfy the given one
	BOOL FSatisfies(
		gpos::pointer<const CDistributionSpec *> pds) const override;

	// check if the columns of passed spec match a subset of the
	// object's columns
	BOOL FMatchSubset(gpos::pointer<const CDistributionSpecHashed *> pds) const;

	// equality function
	BOOL Equals(gpos::pointer<const CDistributionSpec *> pds) const override;

	// return a copy of the distribution spec with remapped columns
	gpos::owner<CDistributionSpec *> PdsCopyWithRemappedColumns(
		CMemoryPool *mp, UlongToColRefMap *colref_mapping,
		BOOL must_exist) override;

	// append enforcers to dynamic array for the given plan properties
	void AppendEnforcers(CMemoryPool *mp, CExpressionHandle &exprhdl,
						 gpos::pointer<CReqdPropPlan *> prpp,
						 CExpressionArray *pdrgpexpr,
						 CExpression *pexpr) override;

	// hash function for hashed distribution spec
	ULONG HashValue() const override;

	// return distribution partitioning type
	EDistributionPartitioningType
	Edpt() const override
	{
		return EdptPartitioned;
	}

	// print
	IOstream &OsPrint(IOstream &os) const override;
	IOstream &OsPrintWithPrefix(IOstream &os, const char *prefix) const;

	// return a hashed distribution on the maximal hashable subset of given columns
	static CDistributionSpecHashed *PdshashedMaximal(CMemoryPool *mp,
													 CColRefArray *colref_array,
													 BOOL fNullsColocated);

	// conversion function
	static gpos::cast_func<CDistributionSpecHashed *>
	PdsConvert(CDistributionSpec *pds)
	{
		GPOS_ASSERT(nullptr != pds);
		CDistributionSpecHashed *pdsHashed =
			dynamic_cast<CDistributionSpecHashed *>(pds);
		GPOS_ASSERT(nullptr != pdsHashed);

		return pdsHashed;
	}

	// conversion function: const argument
	static gpos::pointer<const CDistributionSpecHashed *>
	PdsConvert(gpos::pointer<const CDistributionSpec *> pds)
	{
		GPOS_ASSERT(nullptr != pds);
		gpos::pointer<const CDistributionSpecHashed *> pdsHashed =
			dynamic_cast<const CDistributionSpecHashed *>(pds);
		GPOS_ASSERT(nullptr != pdsHashed);

		return pdsHashed;
	}

	gpos::pointer<CExpressionArrays *>
	HashSpecEquivExprs() const
	{
		return m_equiv_hash_exprs;
	}

	void ComputeEquivHashExprs(CMemoryPool *mp,
							   CExpressionHandle &expression_handle);

	// does the current spec or equivalent spec cover the input expression array
	BOOL IsCoveredBy(
		gpos::pointer<const CExpressionArray *> dist_cols_expr_array) const;

	// create a copy of the distribution spec
	CDistributionSpecHashed *Copy(CMemoryPool *mp);

	// get distribution expr array from the current and its equivalent spec
	CExpressionArrays *GetAllDistributionExprs(CMemoryPool *mp);

	// return a new spec created after merging the current spec with the input spec as equivalents
	gpos::owner<CDistributionSpecHashed *> Combine(
		CMemoryPool *mp, CDistributionSpecHashed *other_spec);

	// check if the equivalent spec (if any) has no matching columns with the main spec
	BOOL HasCompleteEquivSpec(CMemoryPool *mp) const;

	// use given predicates to complete an incomplete spec, if possible
	static CDistributionSpecHashed *TryToCompleteEquivSpec(
		CMemoryPool *mp, CDistributionSpecHashed *pdshashed,
		CExpression *pexprPred, CColRefSet *outerRefs);
};	// class CDistributionSpecHashed

}  // namespace gpopt

#endif	// !GPOPT_CDistributionSpecHashed_H

// EOF
