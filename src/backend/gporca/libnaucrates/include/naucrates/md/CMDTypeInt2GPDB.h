//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CMDTypeInt2GPDB.h
//
//	@doc:
//		Class for representing INT2 types in GPDB
//---------------------------------------------------------------------------



#ifndef GPMD_CMDTypeInt2GPDB_H
#define GPMD_CMDTypeInt2GPDB_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "naucrates/base/IDatumInt2.h"
#include "naucrates/md/CGPDBTypeHelper.h"
#include "naucrates/md/IMDTypeInt2.h"

#define GPDB_INT2_OID OID(21)
#define GPDB_INT2_OPFAMILY OID(1977)
#define GPDB_INT2_LEGACY_OPFAMILY OID(7100)
#define GPDB_INT2_LENGTH 2
#define GPDB_INT2_EQ_OP OID(94)
#define GPDB_INT2_NEQ_OP OID(519)
#define GPDB_INT2_LT_OP OID(95)
#define GPDB_INT2_LEQ_OP OID(522)
#define GPDB_INT2_GT_OP OID(520)
#define GPDB_INT2_GEQ_OP OID(524)
#define GPDB_INT2_COMP_OP OID(350)
#define GPDB_INT2_EQ_FUNC OID(63)
#define GPDB_INT2_ARRAY_TYPE OID(1005)

#define GPDB_INT2_AGG_MIN OID(2133)
#define GPDB_INT2_AGG_MAX OID(2117)
#define GPDB_INT2_AGG_AVG OID(2102)
#define GPDB_INT2_AGG_SUM OID(2109)
#define GPDB_INT2_AGG_COUNT OID(2147)

// fwd decl
namespace gpdxl
{
class CXMLSerializer;
}

namespace gpmd
{
using namespace gpos;
using namespace gpnaucrates;

//---------------------------------------------------------------------------
//	@class:
//		CMDTypeInt2GPDB
//
//	@doc:
//		Class for representing INT2 type in GPDB
//
//---------------------------------------------------------------------------
class CMDTypeInt2GPDB : public IMDTypeInt2
{
	friend class CGPDBTypeHelper<CMDTypeInt2GPDB>;

private:
	// memory pool
	CMemoryPool *m_mp;

	// type id
	gpos::owner<IMDId *> m_mdid;
	gpos::owner<IMDId *> m_distr_opfamily;
	gpos::owner<IMDId *> m_legacy_distr_opfamily;

	// mdids of different operators
	gpos::owner<IMDId *> m_mdid_op_eq;
	gpos::owner<IMDId *> m_mdid_op_neq;
	gpos::owner<IMDId *> m_mdid_op_lt;
	gpos::owner<IMDId *> m_mdid_op_leq;
	gpos::owner<IMDId *> m_mdid_op_gt;
	gpos::owner<IMDId *> m_mdid_op_geq;
	gpos::owner<IMDId *> m_mdid_op_cmp;
	gpos::owner<IMDId *> m_mdid_type_array;

	// min aggregate
	gpos::owner<IMDId *> m_mdid_min;

	// max aggregate
	gpos::owner<IMDId *> m_mdid_max;

	// avg aggregate
	gpos::owner<IMDId *> m_mdid_avg;

	// sum aggregate
	gpos::owner<IMDId *> m_mdid_sum;

	// count aggregate
	gpos::owner<IMDId *> m_mdid_count;

	// DXL for object
	const CWStringDynamic *m_dxl_str;

	// type name and type
	static CWStringConst m_str;
	static CMDName m_mdname;

	// a null datum of this type (used for statistics comparison)
	gpos::owner<IDatum *> m_datum_null;

public:
	CMDTypeInt2GPDB(const CMDTypeInt2GPDB &) = delete;

	// ctor
	explicit CMDTypeInt2GPDB(CMemoryPool *mp);

	// dtor
	~CMDTypeInt2GPDB() override;

	// factory method for creating INT2 datums
	gpos::owner<IDatumInt2 *> CreateInt2Datum(CMemoryPool *mp, SINT value,
											  BOOL is_null) const override;

	// accessors
	const CWStringDynamic *
	GetStrRepr() const override
	{
		return m_dxl_str;
	}

	// accessor of metadata id
	gpos::pointer<IMDId *> MDId() const override;

	gpos::pointer<IMDId *> GetDistrOpfamilyMdid() const override;

	// accessor of type name
	CMDName Mdname() const override;

	// id of specified comparison operator type
	gpos::pointer<IMDId *> GetMdidForCmpType(ECmpType cmp_type) const override;

	// id of specified aggregate type
	gpos::pointer<IMDId *> GetMdidForAggType(EAggType agg_type) const override;

	// is type redistributable
	BOOL
	IsRedistributable() const override
	{
		return true;
	}

	// is type has fixed length
	BOOL
	IsFixedLength() const override
	{
		return true;
	}

	// is type composite
	BOOL
	IsComposite() const override
	{
		return false;
	}

	// size of type
	ULONG
	Length() const override
	{
		return GPDB_INT2_LENGTH;
	}

	// return the GPDB length
	virtual INT
	GetGPDBLength() const
	{
		return GPDB_INT2_LENGTH;
	}

	// is type passed by value
	BOOL
	IsPassedByValue() const override
	{
		return true;
	}

	// metadata id of b-tree lookup operator
	gpos::pointer<const IMDId *>
	CmpOpMdid() const override
	{
		return m_mdid_op_cmp;
	}

	// is type hashable
	BOOL
	IsHashable() const override
	{
		return true;
	}

	// is type merge joinable
	BOOL
	IsMergeJoinable() const override
	{
		return true;
	}

	// metadata id of array type
	gpos::pointer<IMDId *>
	GetArrayTypeMdid() const override
	{
		return m_mdid_type_array;
	}

	// id of the relation corresponding to a composite type
	gpos::pointer<IMDId *>
	GetBaseRelMdid() const override
	{
		return nullptr;
	}

	// serialize object in DXL format
	void Serialize(gpdxl::CXMLSerializer *xml_serializer) const override;

	// return the null constant for this type
	gpos::pointer<IDatum *>
	DatumNull() const override
	{
		return m_datum_null;
	}

	// transformation method for generating datum from CDXLScalarConstValue
	gpos::owner<IDatum *> GetDatumForDXLConstVal(
		gpos::pointer<const CDXLScalarConstValue *> dxl_op) const override;

	// create typed datum from DXL datum
	gpos::owner<IDatum *> GetDatumForDXLDatum(
		CMemoryPool *mp,
		gpos::pointer<const CDXLDatum *> dxl_datum) const override;

	// generate the DXL datum from IDatum
	gpos::owner<CDXLDatum *> GetDatumVal(
		CMemoryPool *mp, gpos::pointer<IDatum *> datum) const override;

	// generate the DXL datum representing null value
	gpos::owner<CDXLDatum *> GetDXLDatumNull(CMemoryPool *mp) const override;

	// generate the DXL scalar constant from IDatum
	gpos::owner<CDXLScalarConstValue *> GetDXLOpScConst(
		CMemoryPool *mp, gpos::pointer<IDatum *> datum) const override;

#ifdef GPOS_DEBUG
	// debug print of the type in the provided stream
	void DebugPrint(IOstream &os) const override;
#endif
};
}  // namespace gpmd

#endif	// !GPMD_CMDTypeInt2GPDB_H

// EOF
