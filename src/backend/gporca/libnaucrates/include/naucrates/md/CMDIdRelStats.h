//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CMDIdRelStats.h
//
//	@doc:
//		Class for representing mdids for relation statistics
//---------------------------------------------------------------------------



#ifndef GPMD_CMDIdRelStats_H
#define GPMD_CMDIdRelStats_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/common/owner.h"
#include "gpos/string/CWStringConst.h"

#include "naucrates/dxl/gpdb_types.h"
#include "naucrates/md/CMDIdGPDB.h"
#include "naucrates/md/CSystemId.h"

namespace gpmd
{
using namespace gpos;


//---------------------------------------------------------------------------
//	@class:
//		CMDIdRelStats
//
//	@doc:
//		Class for representing ids of relation stats objects
//
//---------------------------------------------------------------------------
class CMDIdRelStats : public IMDId
{
private:
	// mdid of base relation
	gpos::owner<CMDIdGPDB *> m_rel_mdid;

	// buffer for the serialzied mdid
	WCHAR m_mdid_array[GPDXL_MDID_LENGTH];

	// string representation of the mdid
	CWStringStatic m_str;

	// serialize mdid
	void Serialize();

public:
	CMDIdRelStats(const CMDIdRelStats &) = delete;

	// ctor
	explicit CMDIdRelStats(gpos::owner<CMDIdGPDB *> rel_mdid);

	// dtor
	~CMDIdRelStats() override;

	EMDIdType
	MdidType() const override
	{
		return EmdidRelStats;
	}

	// string representation of mdid
	const WCHAR *GetBuffer() const override;

	// source system id
	CSystemId
	Sysid() const override
	{
		return m_rel_mdid->Sysid();
	}

	// accessors
	gpos::pointer<IMDId *> GetRelMdId() const;

	// equality check
	BOOL Equals(gpos::pointer<const IMDId *> mdid) const override;

	// computes the hash value for the metadata id
	ULONG
	HashValue() const override
	{
		return m_rel_mdid->HashValue();
	}

	// is the mdid valid
	BOOL
	IsValid() const override
	{
		return IMDId::IsValid(m_rel_mdid);
	}

	// serialize mdid in DXL as the value of the specified attribute
	void Serialize(CXMLSerializer *xml_serializer,
				   const CWStringConst *attribute_str) const override;

	// debug print of the metadata id
	IOstream &OsPrint(IOstream &os) const override;

	// const converter
	static gpos::pointer<const CMDIdRelStats *>
	CastMdid(gpos::pointer<const IMDId *> mdid)
	{
		GPOS_ASSERT(nullptr != mdid && EmdidRelStats == mdid->MdidType());

		return dynamic_cast<const CMDIdRelStats *>(mdid);
	}

	// non-const converter
	static gpos::cast_func<CMDIdRelStats *>
	CastMdid(IMDId *mdid)
	{
		GPOS_ASSERT(nullptr != mdid && EmdidRelStats == mdid->MdidType());

		return dynamic_cast<CMDIdRelStats *>(mdid);
	}

	// make a copy in the given memory pool
	gpos::owner<IMDId *>
	Copy(CMemoryPool *mp) const override
	{
		gpos::owner<CMDIdGPDB *> mdid_rel =
			gpos::dyn_cast<CMDIdGPDB>(m_rel_mdid->Copy(mp));
		return GPOS_NEW(mp) CMDIdRelStats(std::move(mdid_rel));
	}
};

}  // namespace gpmd



#endif	// !GPMD_CMDIdRelStats_H

// EOF
