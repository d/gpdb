//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Greenplum
//
//	@filename:
//		CGPDBAttOptCol.h
//
//	@doc:
//		Class to represent pair of GPDB var info to optimizer col info
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CGPDBAttOptCol_H
#define GPDXL_CGPDBAttOptCol_H

#include "gpos/common/CRefCount.h"
#include "gpos/common/owner.h"

#include "gpopt/translate/CGPDBAttInfo.h"
#include "gpopt/translate/COptColInfo.h"

namespace gpdxl
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CGPDBAttOptCol
//
//	@doc:
//		Class to represent pair of GPDB var info to optimizer col info
//
//---------------------------------------------------------------------------
class CGPDBAttOptCol : public CRefCount
{
private:
	// gpdb att info
	gpos::Ref<CGPDBAttInfo> m_gpdb_att_info;

	// optimizer col info
	gpos::Ref<COptColInfo> m_opt_col_info;

public:
	CGPDBAttOptCol(const CGPDBAttOptCol &) = delete;

	// ctor
	CGPDBAttOptCol(gpos::Ref<CGPDBAttInfo> gpdb_att_info,
				   gpos::Ref<COptColInfo> opt_col_info)
		: m_gpdb_att_info(std::move(gpdb_att_info)),
		  m_opt_col_info(std::move(opt_col_info))
	{
		GPOS_ASSERT(nullptr != m_gpdb_att_info);
		GPOS_ASSERT(nullptr != m_opt_col_info);
	}

	// d'tor
	~CGPDBAttOptCol() override
	{
		;
		;
	}

	// accessor
	const CGPDBAttInfo *
	GetGPDBAttInfo() const
	{
		return m_gpdb_att_info.get();
	}

	// accessor
	const COptColInfo *
	GetOptColInfo() const
	{
		return m_opt_col_info.get();
	}
};

}  // namespace gpdxl

#endif	// !GPDXL_CGPDBAttOptCol_H

// EOF
