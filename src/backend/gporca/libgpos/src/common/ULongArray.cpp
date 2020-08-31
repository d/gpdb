#include "gpos/common/ULongArray.h"

#include "gpos/ranges/algorithm.h"
#include "gpos/views/PtrArrayRange.h"

namespace gpos
{
ULongArray::ULongArray(CMemoryPool *mp) : vector_base(mp)
{
}

void
ULongArray::Append(ULONG *element)
{
	push_back(*element);
	GPOS_DELETE(element);
}
ULongArray::ULongArray(
	CDynamicPtrArray<ULONG, gpos::CleanupDelete<ULONG>> *dynamic_ptr_array)
	: vector_base(dynamic_ptr_array->m_mp)
{
	ranges::copy(ptr_array_range(dynamic_ptr_array), std::back_inserter(*this));
}
}  // namespace gpos
