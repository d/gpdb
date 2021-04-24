//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CTranslatorRelcacheToDXL.h
//
//	@doc:
//		Class for translating GPDB's relcache entries into DXL MD objects
//
//	@test:
//
//
//---------------------------------------------------------------------------



#ifndef GPDXL_CTranslatorRelcacheToDXL_H
#define GPDXL_CTranslatorRelcacheToDXL_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

extern "C" {
#include "postgres.h"

#include "access/tupdesc.h"
#include "catalog/gp_distribution_policy.h"
}

#include "naucrates/dxl/gpdb_types.h"
#include "naucrates/dxl/operators/CDXLColDescr.h"
#include "naucrates/md/CDXLColStats.h"
#include "naucrates/md/CMDAggregateGPDB.h"
#include "naucrates/md/CMDCheckConstraintGPDB.h"
#include "naucrates/md/CMDFunctionGPDB.h"
#include "naucrates/md/CMDPartConstraintGPDB.h"
#include "naucrates/md/CMDRelationExternalGPDB.h"
#include "naucrates/md/CMDRelationGPDB.h"
#include "naucrates/md/CMDScalarOpGPDB.h"
#include "naucrates/md/IMDFunction.h"
#include "naucrates/md/IMDId.h"
#include "naucrates/md/IMDIndex.h"
#include "naucrates/md/IMDType.h"
#include "naucrates/statistics/CHistogram.h"
#include "naucrates/statistics/CStatisticsUtils.h"

// fwd decl
struct RelationData;
typedef struct RelationData *Relation;
struct LogicalIndexes;

namespace gpdxl
{
using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		CTranslatorRelcacheToDXL
//
//	@doc:
//		Class for translating GPDB's relcache entries into DXL MD objects
//
//---------------------------------------------------------------------------
class CTranslatorRelcacheToDXL
{
private:
	//---------------------------------------------------------------------------
	//	@class:
	//		SFuncProps
	//
	//	@doc:
	//		Internal structure to capture exceptional cases where
	//		function properties are wrongly defined in the catalog,
	//
	//		this information is used to correct function properties during
	//		translation
	//
	//---------------------------------------------------------------------------
	struct SFuncProps
	{
	private:
		// function identifier
		OID m_oid;

		// function stability
		IMDFunction::EFuncStbl m_stability;

		// function data access
		IMDFunction::EFuncDataAcc m_access;

		// is function strict?
		BOOL m_is_strict;

		// can the function return multiple rows?
		BOOL m_returns_set;

	public:
		// ctor
		SFuncProps(OID oid, IMDFunction::EFuncStbl stability,
				   IMDFunction::EFuncDataAcc access, BOOL is_strict,
				   BOOL ReturnsSet)
			: m_oid(oid),
			  m_stability(stability),
			  m_access(access),
			  m_is_strict(is_strict),
			  m_returns_set(ReturnsSet)
		{
		}

		// dtor
		virtual ~SFuncProps() = default;

		// return function identifier
		OID
		Oid() const
		{
			return m_oid;
		}

		// return function stability
		IMDFunction::EFuncStbl
		GetStability() const
		{
			return m_stability;
		}

		// return data access property
		IMDFunction::EFuncDataAcc
		GetDataAccess() const
		{
			return m_access;
		}

		// is function strict?
		BOOL
		IsStrict() const
		{
			return m_is_strict;
		}

		// does function return set?
		BOOL
		ReturnsSet() const
		{
			return m_returns_set;
		}

	};	// struct SFuncProps

	// array of function properties map
	static const SFuncProps m_func_props[];

	// lookup function properties
	static void LookupFuncProps(
		OID func_oid,
		IMDFunction::EFuncStbl *stability,	// output: function stability
		IMDFunction::EFuncDataAcc *access,	// output: function data access
		BOOL *is_strict,					// output: is function strict?
		BOOL *is_ndv_preserving,			// output: preserves NDVs of inputs
		BOOL *ReturnsSet,					// output: does function return set?
		BOOL *
			is_allowed_for_PS  // output: is this an increasing function (lossy cast) allowed for partition selection
	);

	// check and fall back for unsupported relations
	static void CheckUnsupportedRelation(OID rel_oid);

	// get type name from the relcache
	static CMDName *GetTypeName(CMemoryPool *mp, gpos::pointer<IMDId *> mdid);

	// get function stability property from the GPDB character representation
	static CMDFunctionGPDB::EFuncStbl GetFuncStability(CHAR c);

	// get function data access property from the GPDB character representation
	static CMDFunctionGPDB::EFuncDataAcc GetEFuncDataAccess(CHAR c);

	// get type of aggregate's intermediate result from the relcache
	static gpos::owner<IMDId *> RetrieveAggIntermediateResultType(
		CMemoryPool *mp, gpos::pointer<IMDId *> mdid);

	// retrieve a GPDB metadata object from the relcache
	static gpos::owner<IMDCacheObject *> RetrieveObjectGPDB(
		CMemoryPool *mp, CMDAccessor *md_accessor, IMDId *mdid);

	// retrieve relstats object from the relcache
	static gpos::owner<IMDCacheObject *> RetrieveRelStats(CMemoryPool *mp,
														  IMDId *mdid);

	// retrieve column stats object from the relcache
	static gpos::owner<IMDCacheObject *> RetrieveColStats(
		CMemoryPool *mp, CMDAccessor *md_accessor, IMDId *mdid);

	// retrieve cast object from the relcache
	static gpos::owner<IMDCacheObject *> RetrieveCast(CMemoryPool *mp,
													  IMDId *mdid);

	// retrieve scalar comparison object from the relcache
	static gpos::owner<IMDCacheObject *> RetrieveScCmp(CMemoryPool *mp,
													   IMDId *mdid);

	// transform GPDB's MCV information to optimizer's histogram structure
	static CHistogram *TransformMcvToOrcaHistogram(
		CMemoryPool *mp, gpos::pointer<const IMDType *> md_type,
		const Datum *mcv_values, const float4 *mcv_frequencies,
		ULONG num_mcv_values);

	// transform GPDB's hist information to optimizer's histogram structure
	static CHistogram *TransformHistToOrcaHistogram(
		CMemoryPool *mp, gpos::pointer<const IMDType *> md_type,
		const Datum *hist_values, ULONG num_hist_values, CDouble num_distinct,
		CDouble hist_freq);

	// histogram to array of dxl buckets
	static gpos::owner<CDXLBucketArray *> TransformHistogramToDXLBucketArray(
		CMemoryPool *mp, gpos::pointer<const IMDType *> md_type,
		const CHistogram *hist);

	// transform stats from pg_stats form to optimizer's preferred form
	static gpos::owner<CDXLBucketArray *> TransformStatsToDXLBucketArray(
		CMemoryPool *mp, OID att_type, CDouble num_distinct, CDouble null_freq,
		const Datum *mcv_values, const float4 *mcv_frequencies,
		ULONG num_mcv_values, const Datum *hist_values, ULONG num_hist_values);

	// get partition keys and types for a relation
	static void RetrievePartKeysAndTypes(
		CMemoryPool *mp, Relation rel, OID oid,
		gpos::owner<ULongPtrArray *> *part_keys,
		gpos::owner<CharPtrArray *> *part_types);

	// get keysets for relation
	static gpos::owner<ULongPtr2dArray *> RetrieveRelKeysets(
		CMemoryPool *mp, OID oid, BOOL should_add_default_keys,
		BOOL is_partitioned, ULONG *attno_mapping);

	// storage type for a relation
	static IMDRelation::Erelstoragetype RetrieveRelStorageType(Relation rel);

	// fix frequencies if they add up to more than 1.0
	static void NormalizeFrequencies(float4 *pdrgf, ULONG length,
									 CDouble *null_freq);

	// get the relation columns
	static gpos::owner<CMDColumnArray *> RetrieveRelColumns(
		CMemoryPool *mp, CMDAccessor *md_accessor, Relation rel);

	// return the dxl representation of the column's default value
	static gpos::owner<CDXLNode *> GetDefaultColumnValue(
		CMemoryPool *mp, CMDAccessor *md_accessor, TupleDesc rd_att,
		AttrNumber attrno);


	// get the distribution columns
	static gpos::owner<ULongPtrArray *> RetrieveRelDistributionCols(
		CMemoryPool *mp, GpPolicy *gp_policy,
		gpos::pointer<CMDColumnArray *> mdcol_array, ULONG size);

	// construct a mapping GPDB attnos -> position in the column array
	static ULONG *ConstructAttnoMapping(
		CMemoryPool *mp, gpos::pointer<CMDColumnArray *> mdcol_array,
		ULONG max_cols);

	// check if index is supported
	static BOOL IsIndexSupported(Relation index_rel);

	// compute the array of included columns
	static gpos::owner<ULongPtrArray *> ComputeIncludedCols(
		CMemoryPool *mp, gpos::pointer<const IMDRelation *> md_rel);

	// is given level included in the default partitions
	static BOOL LevelHasDefaultPartition(List *default_levels, ULONG level);

	// retrieve part constraint for index
	static gpos::owner<CMDPartConstraintGPDB *> RetrievePartConstraintForIndex(
		CMemoryPool *mp, CMDAccessor *md_accessor,
		gpos::pointer<const IMDRelation *> md_rel, Node *part_constraint,
		ULongPtrArray *level_with_default_part_array, BOOL is_unbounded);

	// retrieve part constraint for relation
	static gpos::owner<CDXLNode *> RetrievePartConstraintForRel(
		CMemoryPool *mp, CMDAccessor *md_accessor, Relation rel,
		gpos::pointer<CMDColumnArray *> mdcol_array);

	// retrieve part constraint from a GPDB node
	static gpos::owner<CMDPartConstraintGPDB *> RetrievePartConstraintFromNode(
		CMemoryPool *mp, CMDAccessor *md_accessor,
		gpos::pointer<CDXLColDescrArray *> dxl_col_descr_array,
		Node *part_constraint, ULongPtrArray *level_with_default_part_array,
		BOOL is_unbounded);

	// return relation name
	static CMDName *GetRelName(CMemoryPool *mp, Relation rel);

	// return the index info list defined on the given relation
	static gpos::owner<CMDIndexInfoArray *> RetrieveRelIndexInfo(
		CMemoryPool *mp, Relation rel);

	// return the check constraints defined on the relation with the given oid
	static gpos::owner<IMdIdArray *> RetrieveRelCheckConstraints(
		CMemoryPool *mp, OID oid);

	// does relation type have system columns
	static BOOL RelHasSystemColumns(char rel_kind);

	// translate Optimizer comparison types to GPDB
	static ULONG GetComparisonType(IMDType::ECmpType cmp_type);

	// retrieve the opfamilies mdids for the given scalar op
	static gpos::owner<IMdIdArray *> RetrieveScOpOpFamilies(
		CMemoryPool *mp, gpos::pointer<IMDId *> mdid_scalar_op);

	// retrieve the opfamilies mdids for the given index
	static gpos::owner<IMdIdArray *> RetrieveIndexOpFamilies(
		CMemoryPool *mp, gpos::pointer<IMDId *> mdid_index);

	static gpos::owner<IMdIdArray *> RetrieveRelDistributionOpFamilies(
		CMemoryPool *mp, GpPolicy *policy);

	// for non-leaf partition tables return the number of child partitions
	// else return 1
	static ULONG RetrieveNumChildPartitions(OID rel_oid);

	// generate statistics for the system level columns
	static gpos::owner<CDXLColStats *> GenerateStatsForSystemCols(
		CMemoryPool *mp, OID rel_oid, CMDIdColStats *mdid_col_stats,
		CMDName *md_colname, OID att_type, AttrNumber attrnum,
		CDXLBucketArray *dxl_stats_bucket_array, CDouble rows);

	static gpos::owner<IMdIdArray *> RetrieveIndexPartitions(CMemoryPool *mp,
															 OID rel_oid);

	static IMDRelation::Erelstoragetype RetrieveStorageTypeForPartitionedTable(
		Relation rel);

public:
	// retrieve a metadata object from the relcache
	static gpos::owner<IMDCacheObject *> RetrieveObject(
		CMemoryPool *mp, CMDAccessor *md_accessor, gpos::pointer<IMDId *> mdid);

	// retrieve a relation from the relcache
	static gpos::owner<IMDRelation *> RetrieveRel(CMemoryPool *mp,
												  CMDAccessor *md_accessor,
												  IMDId *mdid);

	// add system columns (oid, tid, xmin, etc) in table descriptors
	static void AddSystemColumns(CMemoryPool *mp,
								 gpos::pointer<CMDColumnArray *> mdcol_array,
								 Relation rel);

	// retrieve an index from the relcache
	static gpos::owner<IMDIndex *> RetrieveIndex(CMemoryPool *mp,
												 CMDAccessor *md_accessor,
												 IMDId *mdid_index);

	// retrieve a check constraint from the relcache
	static gpos::owner<CMDCheckConstraintGPDB *> RetrieveCheckConstraints(
		CMemoryPool *mp, CMDAccessor *md_accessor, IMDId *mdid);

	// populate the attribute number to position mapping
	static ULONG *PopulateAttnoPositionMap(
		CMemoryPool *mp, gpos::pointer<const IMDRelation *> md_rel, ULONG size);

	// return the position of a given attribute number
	static ULONG GetAttributePosition(INT attno, const ULONG *attno_mapping);

	// retrieve a type from the relcache
	static gpos::owner<IMDType *> RetrieveType(CMemoryPool *mp,
											   gpos::pointer<IMDId *> mdid);

	// retrieve a scalar operator from the relcache
	static gpos::owner<CMDScalarOpGPDB *> RetrieveScOp(CMemoryPool *mp,
													   IMDId *mdid);

	// retrieve a function from the relcache
	static gpos::owner<CMDFunctionGPDB *> RetrieveFunc(CMemoryPool *mp,
													   IMDId *mdid);

	// retrieve an aggregate from the relcache
	static gpos::owner<CMDAggregateGPDB *> RetrieveAgg(CMemoryPool *mp,
													   IMDId *mdid);

	// translate GPDB comparison type
	static IMDType::ECmpType ParseCmpType(ULONG cmpt);

	// get the distribution policy of the relation
	static IMDRelation::Ereldistrpolicy GetRelDistribution(GpPolicy *gp_policy);
};
}  // namespace gpdxl



#endif	// !GPDXL_CTranslatorRelcacheToDXL_H

// EOF
