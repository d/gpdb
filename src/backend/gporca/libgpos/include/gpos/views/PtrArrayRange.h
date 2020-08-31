//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2020 VMware, Inc.
//---------------------------------------------------------------------------

#ifndef GPOS_PtrArrayRange_H
#define GPOS_PtrArrayRange_H
#include <functional>
#include "gpos/common/CDynamicPtrArray.h"

namespace gpos
{
namespace views
{
/// \class PtrArrayRange
///
/// This class presents a range-like view over an underlying \class
/// CDynamicPtrArray. This enables efficient range-based for loop or
/// iterator-based algorithms to operator on CDynamicPtrArray.
///
/// Example:
/// Instead of this:
/// \code
/// void
/// PlusOne(gpos::CDynamicPtrArray<INT, gpos::CleanupDelete<INT>>* ptr_array) {
///   for (ULONG ul = 0; ul < ptr_array->Size(); ++ul) {
///     INT* p = (*ptr_array)[ul];
///     ++*p;
///   }
/// \endcode
///
/// You can now write this:
/// \code
/// void
/// PlusOne(gpos::CDynamicPtrArray<INT, gpos::CleanupDelete<INT>>* ptr_array) {
///   for (auto &elem : ptr_array_range(ptr_array)) {
///     ++elem;
///   }
/// \endcode
template <class T, void (*CleanupFn)(T *)>
struct PtrArrayRange
{
	using ptr_array_type = CDynamicPtrArray<T, CleanupFn>;

	struct iterator
	{
		ptr_array_type *ptr_array_;
		size_t index;

		using value_type = T;
		using reference = T &;
		using difference_type = std::ptrdiff_t;
		using pointer = T *;
		using iterator_category = std::forward_iterator_tag;

		T &
		operator*() const
		{
			return *(*ptr_array_)[index];
		}

		iterator &
		operator++()
		{
			++index;
			return *this;
		}

		iterator
		operator++(int)
		{
			return {ptr_array_, index++};
		}

		bool
		operator!=(const iterator &rhs) const
		{
			return index != rhs.index;
		}
	};

	PtrArrayRange(ptr_array_type *dynamic_ptr_array)
		: ptr_array_(dynamic_ptr_array)
	{
	}

	iterator
	begin() const
	{
		return iterator{ptr_array_, 0};
	}

	iterator
	end() const
	{
		return iterator{ptr_array_, ptr_array_->Size()};
	}

private:
	ptr_array_type *ptr_array_;
};
}  // namespace views

template <class T, void (*CleanupFn)(T *)>
auto
ptr_array_range(CDynamicPtrArray<T, CleanupFn> *dynamic_ptr_array)
{
	return views::PtrArrayRange<T, CleanupFn>{dynamic_ptr_array};
}
}  // namespace gpos
#endif	// GPOS_PtrArrayRange_H
