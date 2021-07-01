//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 VMware, Inc. or its affiliates.
//
//	@filename:
//		CLeftSemiJoinStatsProcessor.h
//
//	@doc:
//		Processor for computing statistics for Left Semi Join
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CLeftSemiJoinStatsProcessor_H
#define GPNAUCRATES_CLeftSemiJoinStatsProcessor_H

#include "gpos/common/owner.h"

#include "naucrates/statistics/CJoinStatsProcessor.h"

namespace gpnaucrates
{
class CLeftSemiJoinStatsProcessor : public CJoinStatsProcessor
{
public:
	static gpos::owner<CStatistics *> CalcLSJoinStatsStatic(
		CMemoryPool *mp, gpos::pointer<const IStatistics *> outer_stats,
		gpos::pointer<const IStatistics *> inner_side_stats,
		gpos::pointer<CStatsPredJoinArray *> join_preds_stats);
};
}  // namespace gpnaucrates

#endif	// !GPNAUCRATES_CLeftSemiJoinStatsProcessor_H

// EOF
