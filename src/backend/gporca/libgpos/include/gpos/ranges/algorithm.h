//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2020 VMware, Inc.
//---------------------------------------------------------------------------

#ifndef GPOS_ranges_algorithm_H
#define GPOS_ranges_algorithm_H

#include <algorithm>
#include <iterator>

namespace gpos
{
namespace ranges
{
template <class Range, class OutputIterator>
auto
copy(Range &&r, OutputIterator result)
{
	using std::begin;
	using std::end;
	return std::copy(begin(r), end(r), result);
}
}  // namespace ranges
}  // namespace gpos
#endif	// GPOS_ranges_algorithm_H
