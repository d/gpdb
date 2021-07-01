//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CDXLScalarArrayRef.cpp
//
//	@doc:
//		Implementation of DXL arrayrefs
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarArrayRef.h"

#include "gpos/common/owner.h"

#include "gpopt/mdcache/CMDAccessor.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayRef::CDXLScalarArrayRef
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLScalarArrayRef::CDXLScalarArrayRef(CMemoryPool *mp,
									   gpos::Ref<IMDId> elem_type_mdid,
									   INT type_modifier,
									   gpos::Ref<IMDId> array_type_mdid,
									   gpos::Ref<IMDId> return_type_mdid)
	: CDXLScalar(mp),
	  m_elem_type_mdid(std::move(elem_type_mdid)),
	  m_type_modifier(type_modifier),
	  m_array_type_mdid(std::move(array_type_mdid)),
	  m_return_type_mdid(std::move(return_type_mdid))
{
	GPOS_ASSERT(m_elem_type_mdid->IsValid());
	GPOS_ASSERT(m_array_type_mdid->IsValid());
	GPOS_ASSERT(m_return_type_mdid->IsValid());
	GPOS_ASSERT(m_return_type_mdid->Equals(m_elem_type_mdid.get()) ||
				m_return_type_mdid->Equals(m_array_type_mdid.get()));
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayRef::~CDXLScalarArrayRef
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLScalarArrayRef::~CDXLScalarArrayRef()
{
	;
	;
	;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayRef::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarArrayRef::GetDXLOperator() const
{
	return EdxlopScalarArrayRef;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayRef::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarArrayRef::GetOpNameStr() const
{
	return CDXLTokens::GetDXLTokenStr(EdxltokenScalarArrayRef);
}

INT
CDXLScalarArrayRef::TypeModifier() const
{
	return m_type_modifier;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayRef::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarArrayRef::SerializeToDXL(CXMLSerializer *xml_serializer,
								   const CDXLNode *dxlnode) const
{
	const CWStringConst *element_name = GetOpNameStr();

	xml_serializer->OpenElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix), element_name);
	m_elem_type_mdid->Serialize(
		xml_serializer, CDXLTokens::GetDXLTokenStr(EdxltokenArrayElementType));
	if (default_type_modifier != TypeModifier())
	{
		xml_serializer->AddAttribute(
			CDXLTokens::GetDXLTokenStr(EdxltokenTypeMod), TypeModifier());
	}
	m_array_type_mdid->Serialize(
		xml_serializer, CDXLTokens::GetDXLTokenStr(EdxltokenArrayType));
	m_return_type_mdid->Serialize(xml_serializer,
								  CDXLTokens::GetDXLTokenStr(EdxltokenTypeId));

	// serialize child nodes
	const ULONG arity = dxlnode->Arity();
	GPOS_ASSERT(3 == arity || 4 == arity);

	// first 2 children are index lists
	(*dxlnode)[0]->SerializeToDXL(xml_serializer);
	(*dxlnode)[1]->SerializeToDXL(xml_serializer);

	// 3rd child is the ref expression
	const CWStringConst *pstrRefExpr =
		CDXLTokens::GetDXLTokenStr(EdxltokenScalarArrayRefExpr);
	xml_serializer->OpenElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix), pstrRefExpr);
	(*dxlnode)[2]->SerializeToDXL(xml_serializer);
	xml_serializer->CloseElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix), pstrRefExpr);

	// 4th child is the optional assign expression
	const CWStringConst *pstrAssignExpr =
		CDXLTokens::GetDXLTokenStr(EdxltokenScalarArrayRefAssignExpr);
	xml_serializer->OpenElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix), pstrAssignExpr);
	if (4 == arity)
	{
		(*dxlnode)[3]->SerializeToDXL(xml_serializer);
	}
	xml_serializer->CloseElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix), pstrAssignExpr);

	xml_serializer->CloseElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix), element_name);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayRef::HasBoolResult
//
//	@doc:
//		Does the operator return boolean result
//
//---------------------------------------------------------------------------
BOOL
CDXLScalarArrayRef::HasBoolResult(CMDAccessor *md_accessor) const
{
	return (
		IMDType::EtiBool ==
		md_accessor->RetrieveType(m_return_type_mdid.get())->GetDatumType());
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayRef::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarArrayRef::AssertValid(const CDXLNode *dxlnode,
								BOOL validate_children) const
{
	const ULONG arity = dxlnode->Arity();
	for (ULONG ul = 0; ul < arity; ++ul)
	{
		CDXLNode *child_dxlnode = (*dxlnode)[ul];
		GPOS_ASSERT(EdxloptypeScalar ==
					child_dxlnode->GetOperator()->GetDXLOperatorType());

		if (validate_children)
		{
			child_dxlnode->GetOperator()->AssertValid(child_dxlnode,
													  validate_children);
		}
	}
}
#endif	// GPOS_DEBUG

// EOF
