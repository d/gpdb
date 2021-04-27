//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CDXLNode.h
//
//	@doc:
//		Basic tree/DAG-based representation for DXL tree nodes
//---------------------------------------------------------------------------
#ifndef GPDXL_CDXLNode_H
#define GPDXL_CDXLNode_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/common/CHashMap.h"
#include "gpos/common/owner.h"

#include "naucrates/dxl/gpdb_types.h"
#include "naucrates/dxl/operators/CDXLDirectDispatchInfo.h"
#include "naucrates/dxl/operators/CDXLOperator.h"
#include "naucrates/dxl/operators/CDXLProperties.h"

namespace gpdxl
{
using namespace gpos;

// fwd decl
class CDXLNode;
class CXMLSerializer;

typedef CDynamicPtrArray<CDXLNode, CleanupRelease> CDXLNodeArray;

// arrays of OID
typedef CDynamicPtrArray<OID, CleanupDelete> OidArray;

typedef CHashMap<ULONG, CDXLNode, gpos::HashValue<ULONG>, gpos::Equals<ULONG>,
				 CleanupDelete<ULONG>, CleanupRelease<CDXLNode> >
	IdToCDXLNodeMap;

//---------------------------------------------------------------------------
//	@class:
//		CDXLNode
//
//	@doc:
//		Class for representing nodes in a DXL tree.
//		Each node specifies an operator at that node, and has an array of children nodes.
//
//---------------------------------------------------------------------------
class CDXLNode : public CRefCount
{
private:
	// dxl tree operator class
	gpos::owner<CDXLOperator *> m_dxl_op;

	// properties of the operator
	gpos::owner<CDXLProperties *> m_dxl_properties;

	// array of children
	gpos::owner<CDXLNodeArray *> m_dxl_array;

	// direct dispatch spec
	gpos::owner<CDXLDirectDispatchInfo *> m_direct_dispatch_info;

public:
	CDXLNode(const CDXLNode &) = delete;

	// ctors

	explicit CDXLNode(CMemoryPool *mp);
	CDXLNode(CMemoryPool *mp, gpos::owner<CDXLOperator *> dxl_op);
	CDXLNode(CMemoryPool *mp, gpos::owner<CDXLOperator *> dxl_op,
			 gpos::owner<CDXLNode *> child_dxlnode);
	CDXLNode(CMemoryPool *mp, gpos::owner<CDXLOperator *> dxl_op,
			 gpos::owner<CDXLNode *> first_child_dxlnode,
			 gpos::owner<CDXLNode *> second_child_dxlnode);
	CDXLNode(CMemoryPool *mp, gpos::owner<CDXLOperator *> dxl_op,
			 gpos::owner<CDXLNode *> first_child_dxlnode,
			 gpos::owner<CDXLNode *> second_child_dxlnode,
			 gpos::owner<CDXLNode *> third_child_dxlnode);
	CDXLNode(gpos::owner<CDXLOperator *> dxl_op,
			 gpos::owner<CDXLNodeArray *> dxl_array);

	// dtor
	~CDXLNode() override;

	// shorthand to access children
	inline CDXLNode *
	operator[](ULONG idx) const
	{
		GPOS_ASSERT(nullptr != m_dxl_array);
		CDXLNode *dxl_node = (*m_dxl_array)[idx];
		GPOS_ASSERT(nullptr != dxl_node);
		return dxl_node;
	};

	// arity function, returns the number of children this node has
	inline ULONG
	Arity() const
	{
		return (m_dxl_array == nullptr) ? 0 : m_dxl_array->Size();
	}

	// accessor for operator
	inline gpos::pointer<CDXLOperator *>
	GetOperator() const
	{
		return m_dxl_op;
	}

	// return properties
	gpos::pointer<CDXLProperties *>
	GetProperties() const
	{
		return m_dxl_properties;
	}

	// accessor for children nodes
	gpos::pointer<const CDXLNodeArray *>
	GetChildDXLNodeArray() const
	{
		return m_dxl_array;
	}

	// accessor to direct dispatch info
	gpos::pointer<CDXLDirectDispatchInfo *>
	GetDXLDirectDispatchInfo() const
	{
		return m_direct_dispatch_info;
	}

	// setters
	void AddChild(gpos::owner<CDXLNode *> child_dxlnode);

	void SetOperator(gpos::owner<CDXLOperator *> dxl_op);

	void SerializeToDXL(CXMLSerializer *) const;

	// replace a given child of this DXL node with the given node
	void ReplaceChild(ULONG idx, gpos::owner<CDXLNode *> child_dxlnode);

	void SerializeChildrenToDXL(CXMLSerializer *xml_serializer) const;

	// setter
	void SetProperties(gpos::owner<CDXLProperties *> dxl_properties);

	// setter for direct dispatch info
	void SetDirectDispatchInfo(
		gpos::owner<CDXLDirectDispatchInfo *> dxl_direct_dispatch_info);

	// serialize properties in DXL format
	void SerializePropertiesToDXL(CXMLSerializer *xml_serializer) const;

#ifdef GPOS_DEBUG
	// checks whether the operator has valid structure, i.e. number and
	// types of child nodes
	void AssertValid(BOOL validate_children) const;
#endif	// GPOS_DEBUG

};	// class CDXLNode
}  // namespace gpdxl


#endif	// !GPDXL_CDXLNode_H

// EOF
