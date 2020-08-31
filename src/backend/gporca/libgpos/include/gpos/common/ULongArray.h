//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2020 VMware, Inc.
//---------------------------------------------------------------------------

#ifndef GPOS_ULongArray_H
#define GPOS_ULongArray_H
#include <vector>

#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/common/CRefCount.h"
#include "gpos/memory/CMemoryPool.h"
#include "gpos/memory/MemoryPoolAllocator.h"

namespace gpos
{
class ULongArray : public std::vector<ULONG, gpos::MemoryPoolAllocator<ULONG>>
{
public:
	explicit ULongArray(CMemoryPool *mp);
	explicit ULongArray(
		CDynamicPtrArray<ULONG, gpos::CleanupDelete<ULONG>> *dynamic_ptr_array);
	void Append(ULONG *);

private:
	using vector_base = std::vector<ULONG, gpos::MemoryPoolAllocator<ULONG>>;
};
}  // namespace gpos
#endif	// GPOS_ULongArray_H
