//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerScalarComp.h
//
//	@doc:
//		SAX parse handler class for parsing scalar comparison nodes.
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerScalarComp_H
#define GPDXL_CParseHandlerScalarComp_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/operators/CDXLScalarComp.h"


namespace gpdxl
{
using namespace gpos;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@class:
//		CParseHandlerScalarComp
//
//	@doc:
//		Parse handler for parsing a scalar comparison operator
//
//---------------------------------------------------------------------------
class CParseHandlerScalarComp : public CParseHandlerScalarOp
{
private:
	// the scalar comparison operator
	CDXLScalarComp *m_dxl_op;

	CParseHandlerScalarComp(const CParseHandlerScalarComp &) = delete;

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
	// ctor/dtor
	CParseHandlerScalarComp(CMemoryPool *mp,
							CParseHandlerManager *parse_handler_mgr,
							CParseHandlerBase *parse_handler_root);

	virtual ~CParseHandlerScalarComp();
};
}  // namespace gpdxl

#endif	// !GPDXL_CParseHandlerScalarComp_H

// EOF
