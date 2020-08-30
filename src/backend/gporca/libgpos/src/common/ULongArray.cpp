#include "gpos/common/ULongArray.h"

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
}  // namespace gpos
