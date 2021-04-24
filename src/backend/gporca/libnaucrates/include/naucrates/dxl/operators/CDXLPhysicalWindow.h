//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLPhysicalWindow.h
//
//	@doc:
//		Class for representing DXL window operators
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLPhysicalWindow_H
#define GPDXL_CDXLPhysicalWindow_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLPhysical.h"
#include "naucrates/dxl/operators/CDXLWindowKey.h"

namespace gpdxl
{
// indices of window elements in the children array
enum Edxlwindow
{
	EdxlwindowIndexProjList = 0,
	EdxlwindowIndexFilter,
	EdxlwindowIndexChild,
	EdxlwindowIndexSentinel
};

//---------------------------------------------------------------------------
//	@class:
//		CDXLPhysicalWindow
//
//	@doc:
//		Class for representing DXL window operators
//
//---------------------------------------------------------------------------
class CDXLPhysicalWindow : public CDXLPhysical
{
private:
	// partition columns
	gpos::owner<ULongPtrArray *> m_part_by_colid_array;

	// window keys
	gpos::owner<CDXLWindowKeyArray *> m_dxl_window_key_array;

public:
	CDXLPhysicalWindow(CDXLPhysicalWindow &) = delete;

	//ctor
	CDXLPhysicalWindow(CMemoryPool *mp,
					   gpos::owner<ULongPtrArray *> part_by_colid_array,
					   gpos::owner<CDXLWindowKeyArray *> window_key_array);

	//dtor
	~CDXLPhysicalWindow() override;

	// accessors
	Edxlopid GetDXLOperator() const override;
	const CWStringConst *GetOpNameStr() const override;

	// number of partition columns
	ULONG PartByColsCount() const;

	// return partition columns
	gpos::pointer<const ULongPtrArray *>
	GetPartByColsArray() const
	{
		return m_part_by_colid_array;
	}

	// number of window keys
	ULONG WindowKeysCount() const;

	// return the window key at a given position
	gpos::pointer<CDXLWindowKey *> GetDXLWindowKeyAt(ULONG ulPos) const;

	// serialize operator in DXL format
	void SerializeToDXL(CXMLSerializer *xml_serializer,
						gpos::pointer<const CDXLNode *> dxlnode) const override;

	// conversion function
	static gpos::cast_func<CDXLPhysicalWindow *>
	Cast(CDXLOperator *dxl_op)
	{
		GPOS_ASSERT(nullptr != dxl_op);
		GPOS_ASSERT(EdxlopPhysicalWindow == dxl_op->GetDXLOperator());

		return dynamic_cast<CDXLPhysicalWindow *>(dxl_op);
	}

#ifdef GPOS_DEBUG
	// checks whether the operator has valid structure, i.e. number and
	// types of child nodes
	void AssertValid(gpos::pointer<const CDXLNode *>,
					 BOOL validate_children) const override;
#endif	// GPOS_DEBUG
};
}  // namespace gpdxl
#endif	// !GPDXL_CDXLPhysicalWindow_H

// EOF
