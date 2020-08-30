//---------------------------------------------------------------------------
// Greenplum Database
// Copyright (c) 2020 VMware, Inc. or its affiliates
//---------------------------------------------------------------------------
#ifndef GPOS_MemoryPoolAllocator_H
#define GPOS_MemoryPoolAllocator_H

#include "gpos/memory/CMemoryPool.h"

namespace gpos
{
template <class T>
struct MemoryPoolAllocator
{
	using value_type = T;

	/// Implicit conversion from a Memory Pool to an allocator. This allows the
	/// idiomatic construction of allocator-aware of standard containers, for
	/// example:
	/// \code
	///   std::vector<int, gpos::MemoryPoolAllocator<int>> v(mp);
	/// \endcode
	// NOLINTNEXTLINE(google-explicit-constructor)
	MemoryPoolAllocator(CMemoryPool *mp) noexcept : mp_(mp)
	{
	}

	value_type *
	allocate(std::size_t n)
	{
		size_t bytes = sizeof(value_type) * n;
		void *addr =
			mp_->NewImpl(bytes, __FILE__, __LINE__, CMemoryPool::EatArray);
		return static_cast<value_type *>(addr);
	}

	void
	deallocate(value_type *p, std::size_t)
	{
		mp_->DeleteImpl(p, CMemoryPool::EatArray);
	}

	bool
	operator==(const MemoryPoolAllocator &rhs) const
	{
		return mp_ == rhs.mp_;
	}

	bool
	operator!=(const MemoryPoolAllocator &rhs) const
	{
		return !(*this == rhs);
	}

private:
	CMemoryPool *mp_;
};

}  // namespace gpos
#endif	// GPOS_MemoryPoolAllocator_H
