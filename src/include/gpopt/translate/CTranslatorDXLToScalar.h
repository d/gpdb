//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CTranslatorDXLToScalar.h
//
//	@doc:
//		Class providing methods for translating from DXL Scalar Node to
//		GPDB's Expr.
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CTranslatorDXLToScalar_H
#define GPDXL_CTranslatorDXLToScalar_H


#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/translate/CContextDXLToPlStmt.h"
#include "gpopt/translate/CMappingColIdVar.h"
#include "gpopt/translate/CMappingElementColIdParamId.h"
#include "naucrates/dxl/operators/CDXLDatum.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLScalarArrayRefIndexList.h"
#include "naucrates/dxl/operators/CDXLScalarCast.h"

// fwd declarations
namespace gpopt
{
class CMDAccessor;
}

namespace gpmd
{
class IMDId;
}

struct Aggref;
struct BoolExpr;
struct BooleanTest;
struct CaseExpr;
struct Expr;
struct FuncExpr;
struct NullTest;
struct OpExpr;
struct Param;
struct Plan;
struct RelabelType;
struct ScalarArrayOpExpr;
struct Const;
struct List;
struct SubLink;
struct SubPlan;

namespace gpdxl
{
using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		CTranslatorDXLToScalar
//
//	@doc:
//		Class providing methods for translating from DXL Scalar Node to
//		GPDB's Expr.
//
//---------------------------------------------------------------------------
class CTranslatorDXLToScalar
{
private:
	CMemoryPool *m_mp;

	// meta data accessor
	CMDAccessor *m_md_accessor;

	// indicates whether a sublink was encountered during translation of the scalar subtree
	BOOL m_has_subqueries;

	// number of segments
	ULONG m_num_of_segments;

	// translate a CDXLScalarArrayComp into a GPDB ScalarArrayOpExpr
	Expr *TranslateDXLScalarArrayCompToScalar(
		gpos::pointer<const CDXLNode *> scalar_array_cmp_node,
		CMappingColIdVar *colid_var);

	Expr *TranslateDXLScalarOpExprToScalar(
		gpos::pointer<const CDXLNode *> scalar_op_expr_node,
		CMappingColIdVar *colid_var);

	Expr *TranslateDXLScalarDistinctToScalar(
		gpos::pointer<const CDXLNode *> scalar_distinct_cmp_node,
		CMappingColIdVar *colid_var);

	Expr *TranslateDXLScalarBoolExprToScalar(
		gpos::pointer<const CDXLNode *> scalar_bool_expr_node,
		CMappingColIdVar *colid_var);

	Expr *TranslateDXLScalarBooleanTestToScalar(
		gpos::pointer<const CDXLNode *> scalar_boolean_test_node,
		CMappingColIdVar *colid_var);

	Expr *TranslateDXLScalarCastToScalar(
		gpos::pointer<const CDXLNode *> scalar_relabel_type_node,
		CMappingColIdVar *colid_var);

	Expr *TranslateDXLScalarCoerceToDomainToScalar(
		gpos::pointer<const CDXLNode *> coerce_node,
		CMappingColIdVar *colid_var);

	Expr *TranslateDXLScalarCoerceViaIOToScalar(
		gpos::pointer<const CDXLNode *> coerce_node,
		CMappingColIdVar *colid_var);

	Expr *TranslateDXLScalarArrayCoerceExprToScalar(
		gpos::pointer<const CDXLNode *> coerce_node,
		CMappingColIdVar *colid_var);

	Expr *TranslateDXLScalarNullTestToScalar(
		gpos::pointer<const CDXLNode *> scalar_null_test_node,
		CMappingColIdVar *colid_var);

	Expr *TranslateDXLScalarNullIfToScalar(
		gpos::pointer<const CDXLNode *> scalar_null_if_node,
		CMappingColIdVar *colid_var);

	Expr *TranslateDXLScalarIfStmtToScalar(
		gpos::pointer<const CDXLNode *> scalar_if_stmt_node,
		CMappingColIdVar *colid_var);

	Expr *TranslateDXLScalarSwitchToScalar(
		gpos::pointer<const CDXLNode *> scalar_switch_node,
		CMappingColIdVar *colid_var);

	static Expr *TranslateDXLScalarCaseTestToScalar(
		gpos::pointer<const CDXLNode *> scalar_case_test_node,
		CMappingColIdVar *colid_var);

	Expr *TranslateDXLScalarAggrefToScalar(
		gpos::pointer<const CDXLNode *> aggref_node,
		CMappingColIdVar *colid_var);

	Expr *TranslateDXLScalarWindowRefToScalar(
		gpos::pointer<const CDXLNode *> scalar_winref_node,
		CMappingColIdVar *colid_var);

	Expr *TranslateDXLScalarFuncExprToScalar(
		gpos::pointer<const CDXLNode *> scalar_func_expr_node,
		CMappingColIdVar *colid_var);

	// return a GPDB subplan from a DXL subplan
	Expr *TranslateDXLScalarSubplanToScalar(
		gpos::pointer<const CDXLNode *> scalar_sub_plan_node,
		CMappingColIdVar *colid_var);

	// build subplan node
	SubPlan *TranslateSubplanFromChildPlan(
		Plan *plan_child, SubLinkType slink,
		CContextDXLToPlStmt *dxl_to_plstmt_ctxt);

	// translate subplan test expression
	Expr *TranslateDXLSubplanTestExprToScalar(
		gpos::pointer<CDXLNode *> test_expr_node, SubLinkType slink,
		CMappingColIdVar *colid_var, List **param_ids_list);

	// translate subplan parameters
	void TranslateSubplanParams(
		SubPlan *sub_plan, CDXLTranslateContext *dxl_translator_ctxt,
		gpos::pointer<const CDXLColRefArray *> outer_refs,
		CMappingColIdVar *colid_var);

	CHAR *GetSubplanAlias(ULONG plan_id);

	static Param *TranslateParamFromMapping(
		gpos::pointer<const CMappingElementColIdParamId *>
			colid_to_param_id_map);

	// translate a scalar coalesce
	Expr *TranslateDXLScalarCoalesceToScalar(
		gpos::pointer<const CDXLNode *> scalar_coalesce_node,
		CMappingColIdVar *colid_var);

	// translate a scalar minmax
	Expr *TranslateDXLScalarMinMaxToScalar(
		gpos::pointer<const CDXLNode *> scalar_min_max_node,
		CMappingColIdVar *colid_var);

	// translate a scconstval
	Expr *TranslateDXLScalarConstToScalar(
		gpos::pointer<const CDXLNode *> scalar_const_node,
		CMappingColIdVar *colid_var);

	// translate an array expression
	Expr *TranslateDXLScalarArrayToScalar(
		gpos::pointer<const CDXLNode *> scalar_array_node,
		CMappingColIdVar *colid_var);

	// translate an arrayref expression
	Expr *TranslateDXLScalarArrayRefToScalar(
		gpos::pointer<const CDXLNode *> scalar_array_ref_node,
		CMappingColIdVar *colid_var);

	// translate an arrayref index list
	List *TranslateDXLArrayRefIndexListToScalar(
		gpos::pointer<const CDXLNode *> index_list_node,
		CDXLScalarArrayRefIndexList::EIndexListBound index_list_bound,
		CMappingColIdVar *colid_var);

	// translate a DML action expression
	static Expr *TranslateDXLScalarDMLActionToScalar(
		gpos::pointer<const CDXLNode *> dml_action_node,
		CMappingColIdVar *colid_var);


	// translate children of DXL node, and add them to list
	List *TranslateScalarChildren(List *list,
								  gpos::pointer<const CDXLNode *> dxlnode,
								  CMappingColIdVar *colid_var);

	// return the operator return type oid for the given func id.
	OID GetFunctionReturnTypeOid(IMDId *mdid) const;

	// translate dxldatum to GPDB Const
	static Const *ConvertDXLDatumToConstOid(
		gpos::pointer<CDXLDatum *> datum_dxl);
	static Const *ConvertDXLDatumToConstInt2(
		gpos::pointer<CDXLDatum *> datum_dxl);
	static Const *ConvertDXLDatumToConstInt4(
		gpos::pointer<CDXLDatum *> datum_dxl);
	static Const *ConvertDXLDatumToConstInt8(
		gpos::pointer<CDXLDatum *> datum_dxl);
	static Const *ConvertDXLDatumToConstBool(
		gpos::pointer<CDXLDatum *> datum_dxl);
	Const *TranslateDXLDatumGenericToScalar(
		gpos::pointer<CDXLDatum *> datum_dxl);
	Expr *TranslateRelabelTypeOrFuncExprFromDXL(
		gpos::pointer<const CDXLScalarCast *> scalar_cast, Expr *pexprChild);

public:
	CTranslatorDXLToScalar(const CTranslatorDXLToScalar &) = delete;

	struct STypeOidAndTypeModifier
	{
		OID oid_type;
		INT type_modifier;
	};

	// ctor
	CTranslatorDXLToScalar(CMemoryPool *mp, CMDAccessor *md_accessor,
						   ULONG num_segments);

	// translate DXL scalar operator node into an Expr expression
	// This function is called during the translation of DXL->Query or DXL->Query
	Expr *TranslateDXLToScalar(gpos::pointer<const CDXLNode *> scalar_op_node,
							   CMappingColIdVar *colid_var);

	// translate a scalar ident into an Expr
	static Expr *TranslateDXLScalarIdentToScalar(
		gpos::pointer<const CDXLNode *> scalar_id_node,
		CMappingColIdVar *colid_var);

	// translate a scalar comparison into an Expr
	Expr *TranslateDXLScalarCmpToScalar(
		gpos::pointer<const CDXLNode *> scalar_cmp_node,
		CMappingColIdVar *colid_var);


	// checks if the operator return a boolean result
	static BOOL HasBoolResult(gpos::pointer<CDXLNode *> dxlnode,
							  CMDAccessor *md_accessor);

	// check if the operator is a "true" bool constant
	static BOOL HasConstTrue(gpos::pointer<CDXLNode *> dxlnode,
							 CMDAccessor *md_accessor);

	// check if the operator is a NULL constant
	static BOOL HasConstNull(gpos::pointer<CDXLNode *> dxlnode);

	// are there subqueries in the tree
	BOOL
	HasSubqueries() const
	{
		return m_has_subqueries;
	}

	// translate a DXL datum into GPDB const expression
	Expr *TranslateDXLDatumToScalar(gpos::pointer<CDXLDatum *> datum_dxl);
};
}  // namespace gpdxl
#endif	// !GPDXL_CTranslatorDXLToScalar_H

// EOF
