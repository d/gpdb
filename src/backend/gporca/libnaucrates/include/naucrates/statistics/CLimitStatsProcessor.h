//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 VMware, Inc. or its affiliates.
//
//	@filename:
//		CLimitStatsProcessor.h
//
//	@doc:
//		Compute statistics for limit operation
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CLimitStatsProcessor_H
#define GPNAUCRATES_CLimitStatsProcessor_H

#include "gpos/common/owner.h"

#include "gpopt/optimizer/COptimizerConfig.h"
#include "naucrates/statistics/CStatisticsUtils.h"

namespace gpnaucrates
{
using namespace gpos;

class CLimitStatsProcessor
{
public:
	// limit
	static gpos::owner<CStatistics *> CalcLimitStats(
		CMemoryPool *mp, gpos::pointer<const CStatistics *> input_stats,
		CDouble input_limit_rows);
};
}  // namespace gpnaucrates

#endif	// !GPNAUCRATES_CLimitStatsProcessor_H

// EOF
