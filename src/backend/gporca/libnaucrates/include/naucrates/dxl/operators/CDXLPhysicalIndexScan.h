//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLPhysicalIndexScan.h
//
//	@doc:
//		Class for representing DXL index scan operators
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLPhysicalIndexScan_H
#define GPDXL_CDXLPhysicalIndexScan_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "naucrates/dxl/operators/CDXLIndexDescr.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLPhysical.h"
#include "naucrates/dxl/operators/CDXLTableDescr.h"

namespace gpdxl
{
// indices of index scan elements in the children array
enum Edxlis
{
	EdxlisIndexProjList = 0,
	EdxlisIndexFilter,
	EdxlisIndexCondition,
	EdxlisSentinel
};

//---------------------------------------------------------------------------
//	@class:
//		CDXLPhysicalIndexScan
//
//	@doc:
//		Class for representing DXL index scan operators
//
//---------------------------------------------------------------------------
class CDXLPhysicalIndexScan : public CDXLPhysical
{
private:
	// table descriptor for the scanned table
	gpos::owner<CDXLTableDescr *> m_dxl_table_descr;

	// index descriptor associated with the scanned table
	gpos::owner<CDXLIndexDescr *> m_dxl_index_descr;

	// scan direction of the index
	EdxlIndexScanDirection m_index_scan_dir;

public:
	CDXLPhysicalIndexScan(CDXLPhysicalIndexScan &) = delete;

	//ctor
	CDXLPhysicalIndexScan(CMemoryPool *mp,
						  gpos::owner<CDXLTableDescr *> table_descr,
						  gpos::owner<CDXLIndexDescr *> dxl_index_descr,
						  EdxlIndexScanDirection idx_scan_direction);

	//dtor
	~CDXLPhysicalIndexScan() override;

	// operator type
	Edxlopid GetDXLOperator() const override;

	// operator name
	const CWStringConst *GetOpNameStr() const override;

	// index descriptor
	virtual gpos::pointer<const CDXLIndexDescr *> GetDXLIndexDescr() const;

	//table descriptor
	virtual gpos::pointer<const CDXLTableDescr *> GetDXLTableDescr() const;

	// scan direction
	virtual EdxlIndexScanDirection GetIndexScanDir() const;

	// serialize operator in DXL format
	void SerializeToDXL(CXMLSerializer *xml_serializer,
						gpos::pointer<const CDXLNode *> node) const override;

	// conversion function
	static gpos::cast_func<CDXLPhysicalIndexScan *>
	Cast(CDXLOperator *dxl_op)
	{
		GPOS_ASSERT(nullptr != dxl_op);
		GPOS_ASSERT(EdxlopPhysicalIndexScan == dxl_op->GetDXLOperator());

		return dynamic_cast<CDXLPhysicalIndexScan *>(dxl_op);
	}

#ifdef GPOS_DEBUG
	// checks whether the operator has valid structure, i.e. number and
	// types of child nodes
	void AssertValid(gpos::pointer<const CDXLNode *>,
					 BOOL validate_children) const override;
#endif	// GPOS_DEBUG
};
}  // namespace gpdxl
#endif	// !GPDXL_CDXLPhysicalIndexScan_H

// EOF
