//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLLogicalCTEProducer.h
//
//	@doc:
//		Class for representing DXL logical CTE producer operators
//---------------------------------------------------------------------------
#ifndef GPDXL_CDXLLogicalCTEProducer_H
#define GPDXL_CDXLLogicalCTEProducer_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "naucrates/dxl/operators/CDXLLogical.h"

namespace gpdxl
{
//---------------------------------------------------------------------------
//	@class:
//		CDXLLogicalCTEProducer
//
//	@doc:
//		Class for representing DXL logical CTE producers
//
//---------------------------------------------------------------------------
class CDXLLogicalCTEProducer : public CDXLLogical
{
private:
	// cte id
	ULONG m_id;

	// output column ids
	gpos::owner<ULongPtrArray *> m_output_colids_array;

public:
	CDXLLogicalCTEProducer(CDXLLogicalCTEProducer &) = delete;

	// ctor
	CDXLLogicalCTEProducer(CMemoryPool *mp, ULONG id,
						   gpos::owner<ULongPtrArray *> output_colids_array);

	// dtor
	~CDXLLogicalCTEProducer() override;

	// operator type
	Edxlopid GetDXLOperator() const override;

	// operator name
	const CWStringConst *GetOpNameStr() const override;

	// cte identifier
	ULONG
	Id() const
	{
		return m_id;
	}

	gpos::pointer<ULongPtrArray *>
	GetOutputColIdsArray() const
	{
		return m_output_colids_array;
	}

	// serialize operator in DXL format
	void SerializeToDXL(CXMLSerializer *xml_serializer,
						gpos::pointer<const CDXLNode *> dxlnode) const override;

#ifdef GPOS_DEBUG
	// checks whether the operator has valid structure, i.e. number and
	// types of child nodes
	void AssertValid(gpos::pointer<const CDXLNode *>,
					 BOOL validate_children) const override;
#endif	// GPOS_DEBUG

	// conversion function
	static gpos::cast_func<CDXLLogicalCTEProducer *>
	Cast(CDXLOperator *dxl_op)
	{
		GPOS_ASSERT(nullptr != dxl_op);
		GPOS_ASSERT(EdxlopLogicalCTEProducer == dxl_op->GetDXLOperator());
		return dynamic_cast<CDXLLogicalCTEProducer *>(dxl_op);
	}
};
}  // namespace gpdxl
#endif	// !GPDXL_CDXLLogicalCTEProducer_H

// EOF
