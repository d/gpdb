//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CTranslatorDXLToPlStmt.h
//
//	@doc:
//		Class providing methods for translating from DXL tree to GPDB PlannedStmt
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CTranslatorDxlToPlStmt_H
#define GPDXL_CTranslatorDxlToPlStmt_H

#include "gpos/common/owner.h"
extern "C" {
#include "postgres.h"
}

#include "gpos/base.h"

#include "gpopt/translate/CContextDXLToPlStmt.h"
#include "gpopt/translate/CDXLTranslateContext.h"
#include "gpopt/translate/CDXLTranslateContextBaseTable.h"
#include "gpopt/translate/CMappingColIdVarPlStmt.h"
#include "gpopt/translate/CTranslatorDXLToScalar.h"
#include "naucrates/dxl/CIdGenerator.h"
#include "naucrates/dxl/operators/CDXLCtasStorageOptions.h"
#include "naucrates/dxl/operators/CDXLPhysicalIndexScan.h"
#include "naucrates/md/IMDRelationExternal.h"

#include "access/attnum.h"
#include "nodes/nodes.h"
#include "nodes/plannodes.h"

// fwd declarations
namespace gpopt
{
class CMDAccessor;
}

namespace gpmd
{
class IMDRelation;
class IMDIndex;
}  // namespace gpmd

struct PlannedStmt;
struct Scan;
struct HashJoin;
struct NestLoop;
struct MergeJoin;
struct Hash;
struct RangeTblEntry;
struct Motion;
struct Limit;
struct Agg;
struct Append;
struct Sort;
struct SubqueryScan;
struct SubPlan;
struct Result;
struct Material;
struct ShareInputScan;
//struct Const;
//struct List;

namespace gpdxl
{
using namespace gpopt;

// fwd decl
class CDXLNode;
class CDXLPhysicalCTAS;
class CDXLDirectDispatchInfo;

//---------------------------------------------------------------------------
//	@class:
//		CTranslatorDXLToPlStmt
//
//	@doc:
//		Class providing methods for translating from DXL tree to GPDB PlannedStmt
//
//---------------------------------------------------------------------------
class CTranslatorDXLToPlStmt
{
private:
	// context for fixing index var attno
	struct SContextIndexVarAttno
	{
		// MD relation
		gpos::pointer<const IMDRelation *> m_md_rel;

		// MD index
		gpos::pointer<const IMDIndex *> m_md_index;

		// ctor
		SContextIndexVarAttno(gpos::pointer<const IMDRelation *> md_rel,
							  gpos::pointer<const IMDIndex *> md_index)
			: m_md_rel(md_rel), m_md_index(md_index)
		{
			GPOS_ASSERT(nullptr != md_rel);
			GPOS_ASSERT(nullptr != md_index);
		}
	};	// SContextIndexVarAttno

	// memory pool
	CMemoryPool *m_mp;

	// meta data accessor
	CMDAccessor *m_md_accessor;

	CContextDXLToPlStmt *m_dxl_to_plstmt_context;

	CTranslatorDXLToScalar *m_translator_dxl_to_scalar;

	// command type
	CmdType m_cmd_type;

	// is target table distributed, false when in non DML statements
	BOOL m_is_tgt_tbl_distributed;

	// list of result relations range table indexes for DML statements,
	// or NULL for select queries
	List *m_result_rel_list;

	// number of segments
	ULONG m_num_of_segments;

	// partition selector counter
	ULONG m_partition_selector_counter;

	// private copy ctor
	CTranslatorDXLToPlStmt(const CTranslatorDXLToPlStmt &);

	// walker to set index var attno's
	static BOOL SetIndexVarAttnoWalker(
		Node *node, SContextIndexVarAttno *ctxt_index_var_attno_walker);

public:
	// ctor
	CTranslatorDXLToPlStmt(CMemoryPool *mp, CMDAccessor *md_accessor,
						   CContextDXLToPlStmt *dxl_to_plstmt_context,
						   ULONG num_of_segments);

	// dtor
	~CTranslatorDXLToPlStmt();

	// translate DXL operator node into a Plan node
	Plan *TranslateDXLOperatorToPlan(
		gpos::pointer<const CDXLNode *> dxlnode,
		CDXLTranslateContext *output_context,
		gpos::pointer<CDXLTranslationContextArray *>
			ctxt_translation_prev_siblings	// translation contexts of previous siblings
	);

	// main translation routine for DXL tree -> PlannedStmt
	PlannedStmt *GetPlannedStmtFromDXL(gpos::pointer<const CDXLNode *> dxlnode,
									   const Query *orig_query,
									   bool can_set_tag);

	// translate the join types from its DXL representation to the GPDB one
	static JoinType GetGPDBJoinTypeFromDXLJoinType(EdxlJoinType join_type);

private:
	// Set the bitmapset of a plan to the list of param_ids defined by the plan
	static void SetParamIds(Plan *);

	// translate DXL table scan node into a SeqScan node
	Plan *TranslateDXLTblScan(
		gpos::pointer<const CDXLNode *> tbl_scan_dxlnode,
		CDXLTranslateContext *output_context,
		gpos::pointer<CDXLTranslationContextArray *>
			ctxt_translation_prev_siblings	// translation contexts of previous siblings
	);

	// translate DXL index scan node into a IndexScan node
	Plan *TranslateDXLIndexScan(
		gpos::pointer<const CDXLNode *> index_scan_dxlnode,
		CDXLTranslateContext *output_context,
		CDXLTranslationContextArray *
			ctxt_translation_prev_siblings	// translation contexts of previous siblings
	);

	// translates a DXL index scan node into a IndexScan node
	Plan *TranslateDXLIndexScan(
		gpos::pointer<const CDXLNode *> index_scan_dxlnode,
		gpdxl::CDXLPhysicalIndexScan *dxl_physical_idx_scan_op,
		CDXLTranslateContext *output_context,
		CDXLTranslationContextArray *
			ctxt_translation_prev_siblings	// translation contexts of previous siblings
	);

	// translate DXL index scan node into a IndexOnlyScan node
	Plan *TranslateDXLIndexOnlyScan(
		gpos::pointer<const CDXLNode *> index_scan_dxlnode,
		CDXLTranslateContext *output_context,
		CDXLTranslationContextArray *
			ctxt_translation_prev_siblings	// translation contexts of previous siblings
	);

	// translate DXL hash join into a HashJoin node
	Plan *TranslateDXLHashJoin(
		gpos::pointer<const CDXLNode *> TranslateDXLHashJoin,
		CDXLTranslateContext *output_context,
		CDXLTranslationContextArray *
			ctxt_translation_prev_siblings	// translation contexts of previous siblings
	);

	// translate DXL nested loop join into a NestLoop node
	Plan *TranslateDXLNLJoin(
		gpos::pointer<const CDXLNode *> nl_join_dxlnode,
		CDXLTranslateContext *output_context,
		CDXLTranslationContextArray *
			ctxt_translation_prev_siblings	// translation contexts of previous siblings
	);

	// translate DXL merge join into a MergeJoin node
	Plan *TranslateDXLMergeJoin(
		gpos::pointer<const CDXLNode *> merge_join_dxlnode,
		CDXLTranslateContext *output_context,
		CDXLTranslationContextArray *
			ctxt_translation_prev_siblings	// translation contexts of previous siblings
	);

	// translate DXL motion node into GPDB Motion plan node
	Plan *TranslateDXLMotion(
		gpos::pointer<const CDXLNode *> motion_dxlnode,
		CDXLTranslateContext *output_context,
		CDXLTranslationContextArray *
			ctxt_translation_prev_siblings	// translation contexts of previous siblings
	);

	// translate DXL motion node
	Plan *TranslateDXLDuplicateSensitiveMotion(
		gpos::pointer<const CDXLNode *> motion_dxlnode,
		CDXLTranslateContext *output_context,
		CDXLTranslationContextArray *
			ctxt_translation_prev_siblings	// translation contexts of previous siblings
	);

	// translate DXL duplicate sensitive redistribute motion node into
	// GPDB result node with hash filters
	Plan *TranslateDXLRedistributeMotionToResultHashFilters(
		gpos::pointer<const CDXLNode *> motion_dxlnode,
		CDXLTranslateContext *output_context,
		CDXLTranslationContextArray *
			ctxt_translation_prev_siblings	// translation contexts of previous siblings
	);

	// translate DXL aggregate node into GPDB Agg plan node
	Plan *TranslateDXLAgg(
		gpos::pointer<const CDXLNode *> motion_dxlnode,
		CDXLTranslateContext *output_context,
		CDXLTranslationContextArray *
			ctxt_translation_prev_siblings	// translation contexts of previous siblings
	);

	// translate DXL window node into GPDB window node
	Plan *TranslateDXLWindow(
		gpos::pointer<const CDXLNode *> motion_dxlnode,
		CDXLTranslateContext *output_context,
		CDXLTranslationContextArray *
			ctxt_translation_prev_siblings	// translation contexts of previous siblings
	);

	// translate DXL sort node into GPDB Sort plan node
	Plan *TranslateDXLSort(
		gpos::pointer<const CDXLNode *> sort_dxlnode,
		CDXLTranslateContext *output_context,
		CDXLTranslationContextArray *
			ctxt_translation_prev_siblings	// translation contexts of previous siblings
	);

	// translate a DXL node into a Hash node
	Plan *TranslateDXLHash(
		gpos::pointer<const CDXLNode *> dxlnode,
		CDXLTranslateContext *output_context,
		CDXLTranslationContextArray *
			ctxt_translation_prev_siblings	// translation contexts of previous siblings
	);

	// translate DXL Limit node into a Limit node
	Plan *TranslateDXLLimit(
		gpos::pointer<const CDXLNode *> limit_dxlnode,
		CDXLTranslateContext *output_context,
		CDXLTranslationContextArray *
			ctxt_translation_prev_siblings	// translation contexts of previous siblings
	);

	// translate DXL TVF into a GPDB Function Scan node
	Plan *TranslateDXLTvf(
		gpos::pointer<const CDXLNode *> tvf_dxlnode,
		CDXLTranslateContext *output_context,
		gpos::pointer<CDXLTranslationContextArray *>
			ctxt_translation_prev_siblings	// translation contexts of previous siblings
	);

	Plan *TranslateDXLSubQueryScan(
		gpos::pointer<const CDXLNode *> subquery_scan_dxlnode,
		CDXLTranslateContext *output_context,
		CDXLTranslationContextArray *
			ctxt_translation_prev_siblings	// translation contexts of previous siblings
	);

	Plan *TranslateDXLProjectSet(
		gpos::pointer<const CDXLNode *> result_dxlnode,
		CDXLTranslateContext *output_context,
		CDXLTranslationContextArray *
			ctxt_translation_prev_siblings	// translation contexts of previous siblings
	);

	Plan *TranslateDXLResult(
		gpos::pointer<const CDXLNode *> result_dxlnode,
		CDXLTranslateContext *output_context,
		CDXLTranslationContextArray *
			ctxt_translation_prev_siblings	// translation contexts of previous siblings
	);

	Plan *TranslateDXLAppend(
		gpos::pointer<const CDXLNode *> append_dxlnode,
		CDXLTranslateContext *output_context,
		CDXLTranslationContextArray *
			ctxt_translation_prev_siblings	// translation contexts of previous siblings
	);

	Plan *TranslateDXLMaterialize(
		gpos::pointer<const CDXLNode *> materialize_dxlnode,
		CDXLTranslateContext *output_context,
		CDXLTranslationContextArray *
			ctxt_translation_prev_siblings	// translation contexts of previous siblings
	);

	Plan *TranslateDXLSharedScan(
		gpos::pointer<const CDXLNode *> shared_scan_dxlnode,
		CDXLTranslateContext *output_context,
		CDXLTranslationContextArray *
			ctxt_translation_prev_siblings	// translation contexts of previous siblings
	);

	// translate a sequence operator
	Plan *TranslateDXLSequence(
		gpos::pointer<const CDXLNode *> sequence_dxlnode,
		CDXLTranslateContext *output_context,
		CDXLTranslationContextArray *
			ctxt_translation_prev_siblings	// translation contexts of previous siblings
	);

	// translate a DML operator
	Plan *TranslateDXLDml(
		gpos::pointer<const CDXLNode *> dml_dxlnode,
		CDXLTranslateContext *output_context,
		CDXLTranslationContextArray *
			ctxt_translation_prev_siblings	// translation contexts of previous siblings
	);

	// translate a Split operator
	Plan *TranslateDXLSplit(
		gpos::pointer<const CDXLNode *> split_dxlnode,
		CDXLTranslateContext *output_context,
		CDXLTranslationContextArray *
			ctxt_translation_prev_siblings	// translation contexts of previous siblings
	);

	// translate an Assert operator
	Plan *TranslateDXLAssert(
		gpos::pointer<const CDXLNode *> assert_dxlnode,
		CDXLTranslateContext *output_context,
		CDXLTranslationContextArray *
			ctxt_translation_prev_siblings	// translation contexts of previous siblings
	);

	// translate a CTE producer into a GPDB share input scan
	Plan *TranslateDXLCTEProducerToSharedScan(
		gpos::pointer<const CDXLNode *> cte_producer_dxlnode,
		CDXLTranslateContext *output_context,
		CDXLTranslationContextArray *
			ctxt_translation_prev_siblings	// translation contexts of previous siblings
	);

	// translate a CTE consumer into a GPDB share input scan
	Plan *TranslateDXLCTEConsumerToSharedScan(
		gpos::pointer<const CDXLNode *> cte_consumer_dxlnode,
		CDXLTranslateContext *output_context,
		gpos::pointer<CDXLTranslationContextArray *>
			ctxt_translation_prev_siblings	// translation contexts of previous siblings
	);

	// translate a bitmap table scan operator
	Plan *TranslateDXLBitmapTblScan(
		gpos::pointer<const CDXLNode *> bitmapscan_dxlnode,
		CDXLTranslateContext *output_context,
		CDXLTranslationContextArray *
			ctxt_translation_prev_siblings	// translation contexts of previous siblings
	);

	// translate a DXL PartitionSelector into a GPDB PartitionSelector
	Plan *TranslateDXLPartSelector(
		gpos::pointer<const CDXLNode *> partition_selector_dxlnode,
		CDXLTranslateContext *output_context,
		CDXLTranslationContextArray *
			ctxt_translation_prev_siblings	// translation contexts of previous siblings
	);

	// translate a DXL Value Scan into GPDB Value Scan
	Plan *TranslateDXLValueScan(
		gpos::pointer<const CDXLNode *> value_scan_dxlnode,
		CDXLTranslateContext *output_context,
		gpos::pointer<CDXLTranslationContextArray *>
			ctxt_translation_prev_siblings);

	// translate DXL filter list into GPDB filter list
	List *TranslateDXLFilterList(
		gpos::pointer<const CDXLNode *> filter_list_dxlnode,
		const CDXLTranslateContextBaseTable *base_table_context,
		CDXLTranslationContextArray *child_contexts,
		CDXLTranslateContext *output_context);

	// create range table entry from a CDXLPhysicalTVF node
	RangeTblEntry *TranslateDXLTvfToRangeTblEntry(
		gpos::pointer<const CDXLNode *> tvf_dxlnode,
		CDXLTranslateContext *output_context,
		CDXLTranslateContextBaseTable *base_table_context);

	// create range table entry from a CDXLPhysicalValueScan node
	RangeTblEntry *TranslateDXLValueScanToRangeTblEntry(
		gpos::pointer<const CDXLNode *> value_scan_dxlnode,
		CDXLTranslateContext *output_context,
		CDXLTranslateContextBaseTable *base_table_context);

	// create range table entry from a table descriptor
	RangeTblEntry *TranslateDXLTblDescrToRangeTblEntry(
		gpos::pointer<const CDXLTableDescr *> table_descr, Index index,
		CDXLTranslateContextBaseTable *base_table_context);

	// translate DXL projection list into a target list
	List *TranslateDXLProjList(
		gpos::pointer<const CDXLNode *> project_list_dxlnode,
		const CDXLTranslateContextBaseTable *base_table_context,
		CDXLTranslationContextArray *child_contexts,
		CDXLTranslateContext *output_context);

	// insert NULL values for dropped attributes to construct the target list for a DML statement
	List *CreateTargetListWithNullsForDroppedCols(
		List *target_list, gpos::pointer<const IMDRelation *> md_rel);

	// create a target list containing column references for a hash node from the
	// project list of its child node
	static List *TranslateDXLProjectListToHashTargetList(
		gpos::pointer<const CDXLNode *> project_list_dxlnode,
		CDXLTranslateContext *child_context,
		CDXLTranslateContext *output_context);

	List *TranslateDXLFilterToQual(
		gpos::pointer<const CDXLNode *> filter_dxlnode,
		const CDXLTranslateContextBaseTable *base_table_context,
		CDXLTranslationContextArray *child_contexts,
		CDXLTranslateContext *output_context);


	// translate operator costs from the DXL cost structure into a Plan
	// struct used by GPDB
	void TranslatePlanCosts(gpos::pointer<const CDXLNode *> dxlnode,
							Plan *plan);

	// shortcut for translating both the projection list and the filter
	void TranslateProjListAndFilter(
		gpos::pointer<const CDXLNode *> project_list_dxlnode,
		gpos::pointer<const CDXLNode *> filter_dxlnode,
		const CDXLTranslateContextBaseTable *base_table_context,
		CDXLTranslationContextArray *child_contexts, List **targetlist_out,
		List **qual_out, CDXLTranslateContext *output_context);

	// translate the hash expr list of a redistribute motion node
	void TranslateHashExprList(
		gpos::pointer<const CDXLNode *> hash_expr_list_dxlnode,
		const CDXLTranslateContext *child_context, List **hash_expr_out_list,
		List **hash_expr_types_out_list, CDXLTranslateContext *output_context);

	// translate the tree of bitmap index operators that are under a bitmap table scan
	Plan *TranslateDXLBitmapAccessPath(
		gpos::pointer<const CDXLNode *> bitmap_access_path_dxlnode,
		CDXLTranslateContext *output_context,
		gpos::pointer<const IMDRelation *> md_rel,
		gpos::pointer<const CDXLTableDescr *> table_descr,
		CDXLTranslateContextBaseTable *base_table_context,
		CDXLTranslationContextArray *ctxt_translation_prev_siblings,
		BitmapHeapScan *bitmap_tbl_scan);

	// translate a bitmap bool op expression
	Plan *TranslateDXLBitmapBoolOp(
		gpos::pointer<const CDXLNode *> bitmap_boolop_dxlnode,
		CDXLTranslateContext *output_context,
		gpos::pointer<const IMDRelation *> md_rel,
		gpos::pointer<const CDXLTableDescr *> table_descr,
		CDXLTranslateContextBaseTable *base_table_context,
		CDXLTranslationContextArray *ctxt_translation_prev_siblings,
		BitmapHeapScan *bitmap_tbl_scan);

	// translate CDXLScalarBitmapIndexProbe into BitmapIndexScan
	Plan *TranslateDXLBitmapIndexProbe(
		gpos::pointer<const CDXLNode *> bitmap_index_probe_dxlnode,
		CDXLTranslateContext *output_context,
		gpos::pointer<const IMDRelation *> md_rel,
		gpos::pointer<const CDXLTableDescr *> table_descr,
		CDXLTranslateContextBaseTable *base_table_context,
		CDXLTranslationContextArray *ctxt_translation_prev_siblings,
		BitmapHeapScan *bitmap_tbl_scan);

	static void TranslateSortCols(
		gpos::pointer<const CDXLNode *> sort_col_list_dxl,
		const CDXLTranslateContext *child_context,
		AttrNumber *att_no_sort_colids, Oid *sort_op_oids,
		Oid *sort_collations_oids, bool *is_nulls_first);

	List *TranslateDXLScCondToQual(
		gpos::pointer<const CDXLNode *> filter_dxlnode,
		const CDXLTranslateContextBaseTable *base_table_context,
		CDXLTranslationContextArray *child_contexts,
		CDXLTranslateContext *output_context);

	// parse string value into a Const
	static Cost CostFromStr(const CWStringBase *str);

	// check if the given operator is a DML operator on a distributed table
	BOOL IsTgtTblDistributed(CDXLOperator *dxlop);

	// add a target entry for a junk column with given colid to the target list
	static void AddJunkTargetEntryForColId(
		List **target_list, CDXLTranslateContext *dxl_translate_ctxt,
		ULONG colid, const char *resname);

	// translate the index condition list in an Index scan
	void TranslateIndexConditions(
		CDXLNode *index_cond_list_dxlnode,
		gpos::pointer<const CDXLTableDescr *> dxl_tbl_descr,
		BOOL is_bitmap_index_probe, gpos::pointer<const IMDIndex *> index,
		gpos::pointer<const IMDRelation *> md_rel,
		CDXLTranslateContext *output_context,
		CDXLTranslateContextBaseTable *base_table_context,
		CDXLTranslationContextArray *ctxt_translation_prev_siblings,
		List **index_cond, List **index_orig_cond, List **index_strategy_list,
		List **index_subtype_list);

	// translate the index filters
	List *TranslateDXLIndexFilter(
		CDXLNode *filter_dxlnode, CDXLTranslateContext *output_context,
		CDXLTranslateContextBaseTable *base_table_context,
		CDXLTranslationContextArray *ctxt_translation_prev_siblings);

	// translate the assert constraints
	List *TranslateDXLAssertConstraints(
		CDXLNode *filter_dxlnode, CDXLTranslateContext *output_context,
		CDXLTranslationContextArray *child_contexts);

	// translate a CTAS operator
	Plan *TranslateDXLCtas(
		gpos::pointer<const CDXLNode *> dml_dxlnode,
		CDXLTranslateContext *output_context,
		CDXLTranslationContextArray *ctxt_translation_prev_siblings =
			nullptr	 // translation contexts of previous siblings
	);

	// sets the vartypmod fields in the target entries of the given target list
	static void SetVarTypMod(gpos::pointer<const CDXLPhysicalCTAS *> dxlop,
							 List *target_list);

	// translate the into clause for a DXL physical CTAS operator
	static IntoClause *TranslateDXLPhyCtasToIntoClause(
		gpos::pointer<const CDXLPhysicalCTAS *> dxlop);

	// translate the distribution policy for a DXL physical CTAS operator
	static GpPolicy *TranslateDXLPhyCtasToDistrPolicy(
		gpos::pointer<const CDXLPhysicalCTAS *> dxlop, List *target_list);

	// translate CTAS storage options
	static List *TranslateDXLCtasStorageOptions(
		gpdxl::CDXLCtasStorageOptions::CDXLCtasOptionArray
			*ctas_storage_options);

	// compute directed dispatch segment ids
	List *TranslateDXLDirectDispatchInfo(
		CDXLDirectDispatchInfo *dxl_direct_dispatch_info);

	// hash a DXL datum with GPDB's hash function
	ULONG GetDXLDatumGPDBHash(CDXLDatumArray *dxl_datum_array);

	// translate nest loop colrefs to GPDB nestparams
	static List *TranslateNestLoopParamList(
		CDXLColRefArray *pdrgdxlcrOuterRefs, CDXLTranslateContext *dxltrctxLeft,
		CDXLTranslateContext *dxltrctxRight);
};
}  // namespace gpdxl

#endif	// !GPDXL_CTranslatorDxlToPlStmt_H

// EOF
