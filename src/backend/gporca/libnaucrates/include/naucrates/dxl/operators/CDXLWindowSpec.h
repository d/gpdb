//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLWindowSpec.h
//
//	@doc:
//		Class for representing DXL window specification in the DXL
//		representation of the logical query tree
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLWindowSpec_H
#define GPDXL_CDXLWindowSpec_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLWindowFrame.h"
#include "naucrates/md/CMDName.h"

namespace gpdxl
{
using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		CDXLWindowSpec
//
//	@doc:
//		Class for representing DXL window specification in the DXL
//		representation of the logical query tree
//
//---------------------------------------------------------------------------
class CDXLWindowSpec : public CRefCount
{
private:
	// memory pool;
	CMemoryPool *m_mp;

	// partition-by column identifiers
	gpos::Ref<ULongPtrArray> m_partition_by_colid_array;

	// name of window specification
	CMDName *m_mdname;

	// sorting columns
	gpos::Ref<CDXLNode> m_sort_col_list_dxlnode;

	// window frame associated with the window key
	gpos::Ref<CDXLWindowFrame> m_window_frame;

	// private copy ctor
	CDXLWindowSpec(const CDXLWindowSpec &);

public:
	// ctor
	CDXLWindowSpec(CMemoryPool *mp,
				   gpos::Ref<ULongPtrArray> partition_by_colid_array,
				   CMDName *mdname, gpos::Ref<CDXLNode> sort_col_list_dxlnode,
				   gpos::Ref<CDXLWindowFrame> window_frame);

	// dtor
	~CDXLWindowSpec() override;

	// serialize operator in DXL format
	virtual void SerializeToDXL(CXMLSerializer *) const;

	// set window frame definition
	void SetWindowFrame(CDXLWindowFrame *window_frame);

	// return window frame
	CDXLWindowFrame *
	GetWindowFrame() const
	{
		return m_window_frame.get();
	}

	// partition-by column identifiers
	ULongPtrArray *
	GetPartitionByColIdArray() const
	{
		return m_partition_by_colid_array.get();
	}

	// sort columns
	CDXLNode *
	GetSortColListDXL() const
	{
		return m_sort_col_list_dxlnode.get();
	}

	// window specification name
	CMDName *
	MdName() const
	{
		return m_mdname;
	}
};

typedef gpos::Vector<gpos::Ref<CDXLWindowSpec>> CDXLWindowSpecArray;
}  // namespace gpdxl
#endif	// !GPDXL_CDXLWindowSpec_H

// EOF
