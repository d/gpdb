//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CMDIdScCmp.h
//
//	@doc:
//		Class for representing mdids of scalar comparison operators
//---------------------------------------------------------------------------

#ifndef GPMD_CMDIdScCmpFunc_H
#define GPMD_CMDIdScCmpFunc_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "naucrates/md/CMDIdGPDB.h"
#include "naucrates/md/IMDType.h"

namespace gpmd
{
using namespace gpos;


//---------------------------------------------------------------------------
//	@class:
//		CMDIdScCmp
//
//	@doc:
//		Class for representing ids of scalar comparison operators
//
//---------------------------------------------------------------------------
class CMDIdScCmp : public IMDId
{
private:
	// mdid of source type
	gpos::owner<CMDIdGPDB *> m_mdid_left;

	// mdid of destinatin type
	gpos::owner<CMDIdGPDB *> m_mdid_right;

	// comparison type
	IMDType::ECmpType m_comparision_type;

	// buffer for the serialized mdid
	WCHAR m_mdid_array[GPDXL_MDID_LENGTH];

	// string representation of the mdid
	CWStringStatic m_str;

	// serialize mdid
	void Serialize();

public:
	CMDIdScCmp(const CMDIdScCmp &) = delete;

	// ctor
	CMDIdScCmp(gpos::owner<CMDIdGPDB *> left_mdid,
			   gpos::owner<CMDIdGPDB *> right_mdid, IMDType::ECmpType cmp_type);

	// dtor
	~CMDIdScCmp() override;

	EMDIdType
	MdidType() const override
	{
		return EmdidScCmp;
	}

	// string representation of mdid
	const WCHAR *GetBuffer() const override;

	// source system id
	CSystemId
	Sysid() const override
	{
		return m_mdid_left->Sysid();
	}

	// left type id
	gpos::pointer<IMDId *> GetLeftMdid() const;

	// right type id
	gpos::pointer<IMDId *> GetRightMdid() const;

	IMDType::ECmpType
	ParseCmpType() const
	{
		return m_comparision_type;
	}

	// equality check
	BOOL Equals(gpos::pointer<const IMDId *> mdid) const override;

	// computes the hash value for the metadata id
	ULONG HashValue() const override;

	// is the mdid valid
	BOOL
	IsValid() const override
	{
		return IMDId::IsValid(m_mdid_left) && IMDId::IsValid(m_mdid_right) &&
			   IMDType::EcmptOther != m_comparision_type;
	}

	// serialize mdid in DXL as the value of the specified attribute
	void Serialize(CXMLSerializer *xml_serializer,
				   const CWStringConst *attribute_str) const override;

	// debug print of the metadata id
	IOstream &OsPrint(IOstream &os) const override;

	// const converter
	static gpos::pointer<const CMDIdScCmp *>
	CastMdid(gpos::pointer<const IMDId *> mdid)
	{
		GPOS_ASSERT(nullptr != mdid && EmdidScCmp == mdid->MdidType());

		return dynamic_cast<const CMDIdScCmp *>(mdid);
	}

	// non-const converter
	static gpos::cast_func<CMDIdScCmp *>
	CastMdid(IMDId *mdid)
	{
		GPOS_ASSERT(nullptr != mdid && EmdidScCmp == mdid->MdidType());

		return dynamic_cast<CMDIdScCmp *>(mdid);
	}

	// make a copy in the given memory pool
	gpos::owner<IMDId *>
	Copy(CMemoryPool *mp) const override
	{
		gpos::owner<CMDIdGPDB *> mdid_left =
			gpos::dyn_cast<CMDIdGPDB>(m_mdid_left->Copy(mp));
		gpos::owner<CMDIdGPDB *> mdid_right =
			gpos::dyn_cast<CMDIdGPDB>(m_mdid_right->Copy(mp));

		return GPOS_NEW(mp) CMDIdScCmp(
			std::move(mdid_left), std::move(mdid_right), m_comparision_type);
	}
};
}  // namespace gpmd

#endif	// !GPMD_CMDIdScCmpFunc_H

// EOF
