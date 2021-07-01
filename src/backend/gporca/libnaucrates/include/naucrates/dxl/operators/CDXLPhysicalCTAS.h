//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CDXLPhysicalCTAS.h
//
//	@doc:
//		Class for representing DXL physical CTAS operators
//---------------------------------------------------------------------------
#ifndef GPDXL_CDXLPhysicalCTAS_H
#define GPDXL_CDXLPhysicalCTAS_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "naucrates/dxl/operators/CDXLColDescr.h"
#include "naucrates/dxl/operators/CDXLPhysical.h"
#include "naucrates/md/IMDRelation.h"

namespace gpdxl
{
// fwd decl
class CDXLCtasStorageOptions;

using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		CDXLPhysicalCTAS
//
//	@doc:
//		Class for representing DXL physical CTAS operators
//
//---------------------------------------------------------------------------
class CDXLPhysicalCTAS : public CDXLPhysical
{
private:
	// schema name
	CMDName *m_mdname_schema;

	// table name
	CMDName *m_mdname_rel;

	// list of columns
	gpos::owner<CDXLColDescrArray *> m_col_descr_array;

	// storage options
	gpos::owner<CDXLCtasStorageOptions *> m_dxl_ctas_storage_option;

	// distribution policy
	IMDRelation::Ereldistrpolicy m_rel_distr_policy;

	// list of distribution column positions
	gpos::owner<ULongPtrArray *> m_distr_column_pos_array;

	// list of distriution column opclasses
	gpos::owner<IMdIdArray *> m_distr_opclasses;

	// is this a temporary table
	BOOL m_is_temp_table;

	// does table have oids
	BOOL m_has_oids;

	// storage type
	IMDRelation::Erelstoragetype m_rel_storage_type;

	// list of source column ids
	gpos::owner<ULongPtrArray *> m_src_colids_array;

	// list of vartypmod
	gpos::owner<IntPtrArray *> m_vartypemod_array;

public:
	CDXLPhysicalCTAS(CDXLPhysicalCTAS &) = delete;

	// ctor
	CDXLPhysicalCTAS(
		CMemoryPool *mp, CMDName *mdname_schema, CMDName *mdname_rel,
		gpos::owner<CDXLColDescrArray *> dxl_col_descr_array,
		gpos::owner<CDXLCtasStorageOptions *> dxl_ctas_storage_options,
		IMDRelation::Ereldistrpolicy rel_distr_policy,
		gpos::owner<ULongPtrArray *> distr_column_pos_array,
		gpos::owner<IMdIdArray *> distr_opclasses, BOOL is_temporary,
		BOOL has_oids, IMDRelation::Erelstoragetype rel_storage_type,
		gpos::owner<ULongPtrArray *> src_colids_array,
		gpos::owner<IntPtrArray *> vartypemod_array);

	// dtor
	~CDXLPhysicalCTAS() override;

	// operator type
	Edxlopid GetDXLOperator() const override;

	// operator name
	const CWStringConst *GetOpNameStr() const override;

	// column descriptors
	gpos::pointer<CDXLColDescrArray *>
	GetDXLColumnDescrArray() const
	{
		return m_col_descr_array;
	}

	// distribution type
	IMDRelation::Ereldistrpolicy
	Ereldistrpolicy() const
	{
		return m_rel_distr_policy;
	}

	// distribution column positions
	gpos::pointer<ULongPtrArray *>
	GetDistrColPosArray() const
	{
		return m_distr_column_pos_array;
	}

	// source column ids
	gpos::pointer<ULongPtrArray *>
	GetSrcColidsArray() const
	{
		return m_src_colids_array;
	}

	// list of vartypmod for target expressions
	gpos::pointer<IntPtrArray *>
	GetVarTypeModArray() const
	{
		return m_vartypemod_array;
	}

	// table name
	CMDName *
	GetMdNameSchema() const
	{
		return m_mdname_schema;
	}

	// table name
	CMDName *
	MdName() const
	{
		return m_mdname_rel;
	}

	// is temporary
	BOOL
	IsTemporary() const
	{
		return m_is_temp_table;
	}

	// CTAS storage options
	gpos::pointer<CDXLCtasStorageOptions *>
	GetDxlCtasStorageOption() const
	{
		return m_dxl_ctas_storage_option;
	}

	gpos::pointer<IMdIdArray *>
	GetDistrOpclasses() const
	{
		return m_distr_opclasses;
	}
	// serialize operator in DXL format
	void SerializeToDXL(CXMLSerializer *xml_serializer,
						gpos::pointer<const CDXLNode *> dxlnode) const override;

#ifdef GPOS_DEBUG
	// checks whether the operator has valid structure, i.e. number and
	// types of child nodes
	void AssertValid(gpos::pointer<const CDXLNode *> dxlnode,
					 BOOL validate_children) const override;
#endif	// GPOS_DEBUG

	// conversion function
	static gpos::cast_func<CDXLPhysicalCTAS *>
	Cast(CDXLOperator *dxl_op)
	{
		GPOS_ASSERT(nullptr != dxl_op);
		GPOS_ASSERT(EdxlopPhysicalCTAS == dxl_op->GetDXLOperator());
		return dynamic_cast<CDXLPhysicalCTAS *>(dxl_op);
	}
};
}  // namespace gpdxl
#endif	// !GPDXL_CDXLPhysicalCTAS_H

// EOF
