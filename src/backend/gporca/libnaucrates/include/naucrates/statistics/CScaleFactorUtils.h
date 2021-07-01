//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CScaleFactorUtils.h
//
//	@doc:
//		Utility functions for computing scale factors used in stats computation
//---------------------------------------------------------------------------
#ifndef GPOPT_CScaleFactorUtils_H
#define GPOPT_CScaleFactorUtils_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/engine/CStatisticsConfig.h"
#include "naucrates/statistics/CHistogram.h"
#include "naucrates/statistics/CStatisticsUtils.h"

namespace gpnaucrates
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CScaleFactorUtils
//
//	@doc:
//		Utility functions for computing scale factors used in stats computation
//
//---------------------------------------------------------------------------
class CScaleFactorUtils
{
public:
	struct SJoinCondition
	{
		// scale factor
		CDouble m_scale_factor;

		// mdid pair for the predicate
		gpos::owner<IMdIdArray *> m_oid_pair;

		// true if both sides of the predicate are distribution keys
		BOOL m_dist_keys;

		//ctor
		SJoinCondition(CDouble scale_factor,
					   gpos::owner<IMdIdArray *> mdid_pair, BOOL both_dist_keys)
			: m_scale_factor(scale_factor),
			  m_oid_pair(std::move(mdid_pair)),
			  m_dist_keys(both_dist_keys)
		{
		}

		~SJoinCondition()
		{
			CRefCount::SafeRelease(m_oid_pair);
		}

		// We hash/compare the pointer rather than the contents of the IMdId, as we want to discern between different instances of IMdIds.
		// For example, when performing a self join, the underlying tables will have different IMdId pointers, but the same contents.
		// We treat them as different instances and assume independence to calculate the correct join cardinality.
		static ULONG
		HashValue(gpos::pointer<const IMdIdArray *> oid_pair)
		{
			return CombineHashes(gpos::HashPtr<IMDId>((*oid_pair)[0]),
								 gpos::HashPtr<IMDId>((*oid_pair)[1]));
		}

		static BOOL
		Equals(gpos::pointer<const IMdIdArray *> first,
			   gpos::pointer<const IMdIdArray *> second)
		{
			return ((*first)[0] == (*second)[0]) &&
				   ((*first)[1] == (*second)[1]);
		}
	};

	typedef CDynamicPtrArray<SJoinCondition, CleanupDelete> SJoinConditionArray;

	typedef CHashMap<IMdIdArray, CDoubleArray, SJoinCondition::HashValue,
					 SJoinCondition::Equals, CleanupRelease<IMdIdArray>,
					 CleanupRelease<CDoubleArray> >
		OIDPairToScaleFactorArrayMap;

	typedef CHashMapIter<IMdIdArray, CDoubleArray, SJoinCondition::HashValue,
						 SJoinCondition::Equals, CleanupRelease<IMdIdArray>,
						 CleanupRelease<CDoubleArray> >
		OIDPairToScaleFactorArrayMapIter;

	// generate the hashmap of scale factors grouped by pred tables, also produces array of complex join preds
	static gpos::owner<OIDPairToScaleFactorArrayMap *> GenerateScaleFactorMap(
		CMemoryPool *mp,
		gpos::pointer<SJoinConditionArray *> join_conds_scale_factors,
		gpos::pointer<CDoubleArray *> complex_join_preds);

	// generate a cumulative scale factor using a modified sqrt algorithm
	static CDouble CalcCumulativeScaleFactorSqrtAlg(
		gpos::pointer<OIDPairToScaleFactorArrayMap *> scale_factor_hashmap,
		gpos::pointer<CDoubleArray *> complex_join_preds);

	// calculate the cumulative join scaling factor
	static CDouble CumulativeJoinScaleFactor(
		CMemoryPool *mp, gpos::pointer<const CStatisticsConfig *> stats_config,
		gpos::pointer<SJoinConditionArray *> join_conds_scale_factors,
		CDouble limit_for_result_scale_factor);

	// return scaling factor of the join predicate after apply damping
	static CDouble DampedJoinScaleFactor(
		gpos::pointer<const CStatisticsConfig *> stats_config,
		ULONG num_columns);

	// return scaling factor of the filter after apply damping
	static CDouble DampedFilterScaleFactor(
		gpos::pointer<const CStatisticsConfig *> stats_config,
		ULONG num_columns);

	// return scaling factor of the group by predicate after apply damping
	static CDouble DampedGroupByScaleFactor(
		gpos::pointer<const CStatisticsConfig *> stats_config,
		ULONG num_columns);

	// sort the array of scaling factor
	static void SortScalingFactor(gpos::pointer<CDoubleArray *> scale_factors,
								  BOOL is_descending);

	// calculate the cumulative scaling factor for conjunction after applying damping multiplier
	static CDouble CalcScaleFactorCumulativeConj(
		gpos::pointer<const CStatisticsConfig *> stats_config,
		gpos::pointer<CDoubleArray *> scale_factors);

	// calculate the cumulative scaling factor for disjunction after applying damping multiplier
	static CDouble CalcScaleFactorCumulativeDisj(
		gpos::pointer<const CStatisticsConfig *> stats_config,
		gpos::pointer<CDoubleArray *> scale_factors, CDouble tota_rows);

	// comparison function in descending order
	static INT DescendingOrderCmpFunc(const void *val1, const void *val2);

	// comparison function for joins in descending order
	static INT DescendingOrderCmpJoinFunc(const void *val1, const void *val2);

	// comparison function in ascending order
	static INT AscendingOrderCmpFunc(const void *val1, const void *val2);

	// helper function for double comparison
	static INT DoubleCmpFunc(const CDouble *double_data1,
							 const CDouble *double_data2, BOOL is_descending);

	// default scaling factor of LIKE predicate
	static const CDouble DDefaultScaleFactorLike;

	// default scaling factor of join predicate
	static const CDouble DefaultJoinPredScaleFactor;

	// default scaling factor of non-equality (<, <=, >=, <=) join predicate
	// Note: scale factor of InEquality (!= also denoted as <>) is computed from scale factor of equi-join
	static const CDouble DefaultInequalityJoinPredScaleFactor;

	// invalid scale factor
	static const CDouble InvalidScaleFactor;
};	// class CScaleFactorUtils
}  // namespace gpnaucrates

#endif	// !GPOPT_CScaleFactorUtils_H

// EOF
