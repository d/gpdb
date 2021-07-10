//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMDAccessor.h
//
//	@doc:
//		Metadata cache accessor.
//---------------------------------------------------------------------------



#ifndef GPOPT_CMDAccessor_H
#define GPOPT_CMDAccessor_H

#include "gpos/base.h"
#include "gpos/common/owner.h"
#include "gpos/memory/CCache.h"
#include "gpos/memory/CCacheAccessor.h"

#include "gpopt/engine/CStatisticsConfig.h"
#include "gpopt/mdcache/CMDKey.h"
#include "naucrates/md/CSystemId.h"
#include "naucrates/md/IMDFunction.h"
#include "naucrates/md/IMDId.h"
#include "naucrates/md/IMDProvider.h"
#include "naucrates/md/IMDType.h"
#include "naucrates/statistics/IStatistics.h"

// fwd declarations
namespace gpdxl
{
class CDXLDatum;
}

namespace gpmd
{
class IMDCacheObject;
class IMDRelation;
class IMDRelationExternal;
class IMDScalarOp;
class IMDAggregate;
class IMDTrigger;
class IMDIndex;
class IMDCheckConstraint;
class IMDProvider;
class CMDProviderGeneric;
class IMDColStats;
class IMDRelStats;
class CDXLBucket;
class IMDCast;
class IMDScCmp;
}  // namespace gpmd

namespace gpnaucrates
{
class CHistogram;
class CBucket;
class IStatistics;
}  // namespace gpnaucrates

namespace gpopt
{
using namespace gpos;
using namespace gpmd;


typedef IMDId *MdidPtr;

//---------------------------------------------------------------------------
//	@class:
//		CMDAccessor
//
//	@doc:
//		Gives the optimizer access to metadata information of a particular
//		object (e.g., a Table).
//
//		CMDAccessor maintains a cache of metadata objects (IMDCacheObject)
//		keyed on CMDKey (wrapper over IMDId). It also provides various accessor
//		methods (such as RetrieveAgg(), RetrieveRel() etc.) to request for corresponding
//		metadata objects (such as aggregates and relations respectively). These
//		methods in turn call the private method GetImdObj().
//
//		GetImdObj() first looks up the object in the MDCache. If no information
//		is available in the cache, it goes to a CMDProvider (e.g., GPDB
//		relcache or Minidump) to retrieve the required information.
//
//---------------------------------------------------------------------------
class CMDAccessor
{
public:
	// ccache template for mdcache
	typedef CCache<IMDCacheObject *, CMDKey *> MDCache;

private:
	// element in the hashtable of cache accessors maintained by the MD accessor
	struct SMDAccessorElem;
	struct SMDProviderElem;


	// cache accessor for objects in a MD cache
	typedef CCacheAccessor<IMDCacheObject *, CMDKey *> CacheAccessorMD;

	// hashtable for cache accessors indexed by the md id of the accessed object
	typedef CSyncHashtable<SMDAccessorElem, MdidPtr> MDHT;

	typedef CSyncHashtableAccessByKey<SMDAccessorElem, MdidPtr> MDHTAccessor;

	// iterator for the cache accessors hashtable
	typedef CSyncHashtableIter<SMDAccessorElem, MdidPtr> MDHTIter;
	typedef CSyncHashtableAccessByIter<SMDAccessorElem, MdidPtr>
		MDHTIterAccessor;

	// hashtable for MD providers indexed by the source system id
	typedef CSyncHashtable<SMDProviderElem, SMDProviderElem> MDPHT;

	typedef CSyncHashtableAccessByKey<SMDProviderElem, SMDProviderElem>
		MDPHTAccessor;

	// iterator for the providers hashtable
	typedef CSyncHashtableIter<SMDProviderElem, SMDProviderElem> MDPHTIter;
	typedef CSyncHashtableAccessByIter<SMDProviderElem, SMDProviderElem>
		MDPHTIterAccessor;

	// element in the cache accessor hashtable maintained by the MD Accessor
	struct SMDAccessorElem
	{
	private:
		// hashed object
		gpos::owner<IMDCacheObject *> m_imd_obj;

	public:
		// hash key
		gpos::owner<IMDId *> m_mdid;

		// generic link
		SLink m_link;

		// invalid key
		static const MdidPtr m_pmdidInvalid;

		// ctor
		SMDAccessorElem(gpos::owner<IMDCacheObject *> pimdobj,
						gpos::owner<IMDId *> mdid);

		// dtor
		~SMDAccessorElem();

		// hashed object
		gpos::pointer<IMDCacheObject *>
		GetImdObj()
		{
			return m_imd_obj;
		}

		// return the key for this hashtable element
		gpos::pointer<IMDId *> MDId() const;

		// equality function for hash tables
		static BOOL Equals(const MdidPtr &left_mdid,
						   gpos::pointer<const MdidPtr &> right_mdid);

		// hash function for cost contexts hash table
		static ULONG HashValue(const MdidPtr &mdid);
	};

	// element in the MD provider hashtable
	struct SMDProviderElem
	{
	private:
		// source system id
		CSystemId m_sysid;

		// value of the hashed element
		gpos::owner<IMDProvider *> m_pmdp;

	public:
		// generic link
		SLink m_link;

		// invalid key
		static const SMDProviderElem m_mdpelemInvalid;

		// ctor
		SMDProviderElem(CSystemId sysid, gpos::owner<IMDProvider *> pmdp);

		// dtor
		~SMDProviderElem();

		// return the MD provider
		gpos::pointer<IMDProvider *> Pmdp();

		// return the system id
		CSystemId Sysid() const;

		// equality function for hash tables
		static BOOL Equals(const SMDProviderElem &mdpelemLeft,
						   const SMDProviderElem &mdpelemRight);

		// hash function for MD providers hash table
		static ULONG HashValue(const SMDProviderElem &mdpelem);
	};

private:
	// memory pool
	CMemoryPool *m_mp;

	// metadata cache
	MDCache *m_pcache;

	// generic metadata provider
	CMDProviderGeneric *m_pmdpGeneric;

	// hashtable of cache accessors
	MDHT m_shtCacheAccessors;

	// hashtable of MD providers
	MDPHT m_shtProviders;

	// total time consumed in looking up MD objects (including time used to fetch objects from MD provider)
	CDouble m_dLookupTime;

	// total time consumed in fetching MD objects from MD provider,
	// this time is currently dominated by serialization time
	CDouble m_dFetchTime;

	// interface to a MD cache object
	gpos::pointer<const IMDCacheObject *> GetImdObj(
		gpos::pointer<IMDId *> mdid);

	// return the type corresponding to the given type info and source system id
	gpos::pointer<const IMDType *> RetrieveType(CSystemId sysid,
												IMDType::ETypeInfo type_info);

	// return the generic type corresponding to the given type info
	gpos::pointer<const IMDType *> RetrieveType(IMDType::ETypeInfo type_info);

	// destroy accessor element when MDAccessor is destroyed
	static void DestroyAccessorElement(SMDAccessorElem *pmdaccelem);

	// destroy accessor element when MDAccessor is destroyed
	static void DestroyProviderElement(SMDProviderElem *pmdpelem);

	// lookup an MD provider by system id
	IMDProvider *Pmdp(CSystemId sysid);

	// initialize hash tables
	void InitHashtables(CMemoryPool *mp);

	// return the column statistics meta data object for a given column of a table
	gpos::pointer<const IMDColStats *> Pmdcolstats(CMemoryPool *mp,
												   IMDId *rel_mdid,
												   ULONG ulPos);

	// record histogram and width information for a given column of a table
	void RecordColumnStats(
		CMemoryPool *mp, IMDId *rel_mdid, ULONG colid, ULONG ulPos,
		BOOL isSystemCol, BOOL isEmptyTable,
		gpos::pointer<UlongToHistogramMap *> col_histogram_mapping,
		gpos::pointer<UlongToDoubleMap *> colid_width_mapping,
		gpos::pointer<CStatisticsConfig *> stats_config);

	// construct a stats histogram from an MD column stats object
	CHistogram *GetHistogram(CMemoryPool *mp, gpos::pointer<IMDId *> mdid_type,
							 gpos::pointer<const IMDColStats *> pmdcolstats);

	// construct a typed bucket from a DXL bucket
	CBucket *Pbucket(CMemoryPool *mp, gpos::pointer<IMDId *> mdid_type,
					 gpos::pointer<const CDXLBucket *> dxl_bucket);

	// construct a typed datum from a DXL bucket
	gpos::owner<IDatum *> GetDatum(CMemoryPool *mp,
								   gpos::pointer<IMDId *> mdid_type,
								   gpos::pointer<const CDXLDatum *> dxl_datum);

public:
	CMDAccessor(const CMDAccessor &) = delete;

	// ctors
	CMDAccessor(CMemoryPool *mp, MDCache *pcache);
	CMDAccessor(CMemoryPool *mp, MDCache *pcache, CSystemId sysid,
				gpos::owner<IMDProvider *> pmdp);
	CMDAccessor(CMemoryPool *mp, MDCache *pcache,
				gpos::pointer<const CSystemIdArray *> pdrgpsysid,
				gpos::pointer<const CMDProviderArray *> pdrgpmdp);

	//dtor
	~CMDAccessor();

	// return MD cache
	MDCache *
	Pcache() const
	{
		return m_pcache;
	}

	// register a new MD provider
	void RegisterProvider(CSystemId sysid, gpos::owner<IMDProvider *> pmdp);

	// register given MD providers
	void RegisterProviders(gpos::pointer<const CSystemIdArray *> pdrgpsysid,
						   gpos::pointer<const CMDProviderArray *> pdrgpmdp);

	// interface to a relation object from the MD cache
	gpos::pointer<const IMDRelation *> RetrieveRel(gpos::pointer<IMDId *> mdid);

	// interface to type's from the MD cache given the type's mdid
	gpos::pointer<const IMDType *> RetrieveType(gpos::pointer<IMDId *> mdid);

	// obtain the specified base type given by the template parameter
	template <class T>
	const T *
	PtMDType()
	{
		IMDType::ETypeInfo type_info = T::GetTypeInfo();
		GPOS_ASSERT(IMDType::EtiGeneric != type_info);
		return dynamic_cast<const T *>(RetrieveType(type_info));
	}

	// obtain the specified base type given by the template parameter
	template <class T>
	const T *
	PtMDType(CSystemId sysid)
	{
		IMDType::ETypeInfo type_info = T::GetTypeInfo();
		GPOS_ASSERT(IMDType::EtiGeneric != type_info);
		return dynamic_cast<const T *>(RetrieveType(sysid, type_info));
	}

	// interface to a scalar operator from the MD cache
	gpos::pointer<const IMDScalarOp *> RetrieveScOp(
		gpos::pointer<IMDId *> mdid);

	// interface to a function from the MD cache
	gpos::pointer<const IMDFunction *> RetrieveFunc(
		gpos::pointer<IMDId *> mdid);

	// interface to check if the window function from the MD cache is an aggregate window function
	BOOL FAggWindowFunc(gpos::pointer<IMDId *> mdid);

	// interface to an aggregate from the MD cache
	gpos::pointer<const IMDAggregate *> RetrieveAgg(
		gpos::pointer<IMDId *> mdid);

	// interface to a trigger from the MD cache
	gpos::pointer<const IMDTrigger *> RetrieveTrigger(
		gpos::pointer<IMDId *> mdid);

	// interface to an index from the MD cache
	gpos::pointer<const IMDIndex *> RetrieveIndex(gpos::pointer<IMDId *> mdid);

	// interface to a check constraint from the MD cache
	gpos::pointer<const IMDCheckConstraint *> RetrieveCheckConstraints(
		gpos::pointer<IMDId *> mdid);

	// retrieve a column stats object from the cache
	gpos::pointer<const IMDColStats *> Pmdcolstats(gpos::pointer<IMDId *> mdid);

	// retrieve a relation stats object from the cache
	gpos::pointer<const IMDRelStats *> Pmdrelstats(gpos::pointer<IMDId *> mdid);

	// retrieve a cast object from the cache
	gpos::pointer<const IMDCast *> Pmdcast(gpos::pointer<IMDId *> mdid_src,
										   gpos::pointer<IMDId *> mdid_dest);

	// retrieve a scalar comparison object from the cache
	gpos::pointer<const IMDScCmp *> Pmdsccmp(gpos::pointer<IMDId *> left_mdid,
											 gpos::pointer<IMDId *> right_mdid,
											 IMDType::ECmpType cmp_type);

	// construct a statistics object for the columns of the given relation
	gpos::owner<IStatistics *> Pstats(
		CMemoryPool *mp, IMDId *rel_mdid,
		gpos::pointer<CColRefSet *>
			pcrsHist,  // set of column references for which stats are needed
		gpos::pointer<CColRefSet *>
			pcrsWidth,	// set of column references for which the widths are needed
		gpos::pointer<CStatisticsConfig *> stats_config = nullptr);

	// serialize object to passed stream
	void Serialize(COstream &oos);

	// serialize system ids to passed stream
	void SerializeSysid(COstream &oos);
};
}  // namespace gpopt



#endif	// !GPOPT_CMDAccessor_H

// EOF
