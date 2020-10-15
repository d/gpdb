//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerWindowKey.h
//
//	@doc:
//		SAX parse handler class for parsing the window key
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerWindowKey_H
#define GPDXL_CParseHandlerWindowKey_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLWindowKey.h"
#include "naucrates/dxl/parser/CParseHandlerBase.h"
#include "naucrates/dxl/xml/dxltokens.h"

namespace gpdxl
{
using namespace gpos;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@class:
//		CParseHandlerWindowKey
//
//	@doc:
//		SAX parse handler class for parsing the window key
//
//---------------------------------------------------------------------------
class CParseHandlerWindowKey : public CParseHandlerBase
{
private:
	// window key generated by the parser
	CDXLWindowKey *m_dxl_window_key_gen;

	// process the start of an element
	void StartElement(
		const XMLCh *const element_uri,			// URI of element's namespace
		const XMLCh *const element_local_name,	// local part of element's name
		const XMLCh *const element_qname,		// element's qname
		const Attributes &attr					// element's attributes
	);

	// process the end of an element
	void EndElement(
		const XMLCh *const element_uri,			// URI of element's namespace
		const XMLCh *const element_local_name,	// local part of element's name
		const XMLCh *const element_qname		// element's qname
	);

public:
	CParseHandlerWindowKey(const CParseHandlerWindowKey &) = delete;

	// ctor/dtor
	CParseHandlerWindowKey(CMemoryPool *mp,
						   CParseHandlerManager *parse_handler_mgr,
						   CParseHandlerBase *parse_handler_root);

	// window key generated by the parse handler
	CDXLWindowKey *
	GetDxlWindowKeyGen() const
	{
		return m_dxl_window_key_gen;
	}
};
}  // namespace gpdxl

#endif	// !GPDXL_CParseHandlerWindowKey_H

// EOF
