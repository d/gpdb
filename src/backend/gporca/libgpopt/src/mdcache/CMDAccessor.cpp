//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CMDAccessor.cpp
//
//	@doc:
//		Implementation of the metadata accessor class handling accesses to
//		metadata objects in an optimization session
//---------------------------------------------------------------------------

#include "gpopt/mdcache/CMDAccessor.h"

#include "gpos/common/CAutoP.h"
#include "gpos/common/CAutoRef.h"
#include "gpos/common/CTimerUser.h"
#include "gpos/common/owner.h"
#include "gpos/error/CAutoTrace.h"
#include "gpos/io/COstreamString.h"
#include "gpos/task/CAutoSuspendAbort.h"

#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CColRefTable.h"
#include "gpopt/exception.h"
#include "gpopt/mdcache/CMDAccessorUtils.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/exception.h"
#include "naucrates/md/CMDIdCast.h"
#include "naucrates/md/CMDIdColStats.h"
#include "naucrates/md/CMDIdRelStats.h"
#include "naucrates/md/CMDIdScCmp.h"
#include "naucrates/md/CMDProviderGeneric.h"
#include "naucrates/md/IMDAggregate.h"
#include "naucrates/md/IMDCacheObject.h"
#include "naucrates/md/IMDCast.h"
#include "naucrates/md/IMDCheckConstraint.h"
#include "naucrates/md/IMDColStats.h"
#include "naucrates/md/IMDFunction.h"
#include "naucrates/md/IMDIndex.h"
#include "naucrates/md/IMDProvider.h"
#include "naucrates/md/IMDRelStats.h"
#include "naucrates/md/IMDRelation.h"
#include "naucrates/md/IMDRelationExternal.h"
#include "naucrates/md/IMDScCmp.h"
#include "naucrates/md/IMDScalarOp.h"
#include "naucrates/md/IMDTrigger.h"
#include "naucrates/md/IMDType.h"
#include "naucrates/traceflags/traceflags.h"

using namespace gpos;
using namespace gpmd;
using namespace gpopt;
using namespace gpdxl;

// no. of hashtable buckets
#define GPOPT_CACHEACC_HT_NUM_OF_BUCKETS 128

// static member initialization

// invalid mdid pointer
const MdidPtr CMDAccessor::SMDAccessorElem::m_pmdidInvalid = nullptr;

// invalid md provider element
const CMDAccessor::SMDProviderElem
	CMDAccessor::SMDProviderElem::m_mdpelemInvalid(
		CSystemId(IMDId::EmdidSentinel, nullptr, 0), nullptr);

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::SMDAccessorElem::SMDAccessorElem
//
//	@doc:
//		Constructs a metadata accessor element for the accessors hashtable
//
//---------------------------------------------------------------------------
CMDAccessor::SMDAccessorElem::SMDAccessorElem(
	gpos::owner<IMDCacheObject *> pimdobj, gpos::owner<IMDId *> mdid)
	: m_imd_obj(std::move(pimdobj)), m_mdid(std::move(mdid))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::SMDAccessorElem::~SMDAccessorElem
//
//	@doc:
//		Destructor for the metadata accessor element
//
//---------------------------------------------------------------------------
CMDAccessor::SMDAccessorElem::~SMDAccessorElem()
{
	// deleting the cache accessor will effectively unpin the cache entry for that object
	m_imd_obj->Release();
	m_mdid->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::SMDAccessorElem::MDId
//
//	@doc:
//		Return the key for this hashtable element
//
//---------------------------------------------------------------------------
gpos::pointer<IMDId *>
CMDAccessor::SMDAccessorElem::MDId() const
{
	return m_mdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::SMDAccessorElem::Equals
//
//	@doc:
//		Equality function for cache accessors hash table
//
//---------------------------------------------------------------------------
BOOL
CMDAccessor::SMDAccessorElem::Equals(const MdidPtr &left_mdid,
									 gpos::pointer<const MdidPtr &> right_mdid)
{
	if (left_mdid == m_pmdidInvalid || right_mdid == m_pmdidInvalid)
	{
		return left_mdid == m_pmdidInvalid && right_mdid == m_pmdidInvalid;
	}

	return left_mdid->Equals(right_mdid);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::SMDAccessorElem::HashValue
//
//	@doc:
//		Hash function for cache accessors hash table
//
//---------------------------------------------------------------------------
ULONG
CMDAccessor::SMDAccessorElem::HashValue(const MdidPtr &mdid)
{
	GPOS_ASSERT(m_pmdidInvalid != mdid);

	return mdid->HashValue();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::SMDProviderElem::SMDProviderElem
//
//	@doc:
//		Constructs an MD provider element
//
//---------------------------------------------------------------------------
CMDAccessor::SMDProviderElem::SMDProviderElem(CSystemId sysid,
											  gpos::owner<IMDProvider *> pmdp)
	: m_sysid(sysid), m_pmdp(std::move(pmdp))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::SMDProviderElem::~SMDProviderElem
//
//	@doc:
//		Destructor for the MD provider element
//
//---------------------------------------------------------------------------
CMDAccessor::SMDProviderElem::~SMDProviderElem()
{
	CRefCount::SafeRelease(m_pmdp);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::SMDProviderElem::Pmdp
//
//	@doc:
//		Returns the MD provider for this hash table element
//
//---------------------------------------------------------------------------
gpos::pointer<IMDProvider *>
CMDAccessor::SMDProviderElem::Pmdp()
{
	return m_pmdp;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::SMDProviderElem::Sysid
//
//	@doc:
//		Returns the system id for this hash table element
//
//---------------------------------------------------------------------------
CSystemId
CMDAccessor::SMDProviderElem::Sysid() const
{
	return m_sysid;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::SMDProviderElem::Equals
//
//	@doc:
//		Equality function for hash tables
//
//---------------------------------------------------------------------------
BOOL
CMDAccessor::SMDProviderElem::Equals(const SMDProviderElem &mdpelemLeft,
									 const SMDProviderElem &mdpelemRight)
{
	return mdpelemLeft.m_sysid.Equals(mdpelemRight.m_sysid);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::SMDProviderElem::HashValue
//
//	@doc:
//		Hash function for cost contexts hash table
//
//---------------------------------------------------------------------------
ULONG
CMDAccessor::SMDProviderElem::HashValue(const SMDProviderElem &mdpelem)
{
	GPOS_ASSERT(!Equals(mdpelem, m_mdpelemInvalid));

	return mdpelem.m_sysid.HashValue();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::CMDAccessor
//
//	@doc:
//		Constructs a metadata accessor
//
//---------------------------------------------------------------------------
CMDAccessor::CMDAccessor(CMemoryPool *mp, MDCache *pcache)
	: m_mp(mp), m_pcache(pcache), m_dLookupTime(0.0), m_dFetchTime(0.0)
{
	GPOS_ASSERT(nullptr != m_mp);
	GPOS_ASSERT(nullptr != m_pcache);

	m_pmdpGeneric = GPOS_NEW(mp) CMDProviderGeneric(mp);

	InitHashtables(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::CMDAccessor
//
//	@doc:
//		Constructs a metadata accessor and registers an MD provider
//
//---------------------------------------------------------------------------
CMDAccessor::CMDAccessor(CMemoryPool *mp, MDCache *pcache, CSystemId sysid,
						 gpos::owner<IMDProvider *> pmdp)
	: m_mp(mp), m_pcache(pcache), m_dLookupTime(0.0), m_dFetchTime(0.0)
{
	GPOS_ASSERT(nullptr != m_mp);
	GPOS_ASSERT(nullptr != m_pcache);

	m_pmdpGeneric = GPOS_NEW(mp) CMDProviderGeneric(mp);

	InitHashtables(mp);

	RegisterProvider(sysid, std::move(pmdp));
}


//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::CMDAccessor
//
//	@doc:
//		Constructs a metadata accessor and registers MD providers
//
//---------------------------------------------------------------------------
CMDAccessor::CMDAccessor(CMemoryPool *mp, MDCache *pcache,
						 gpos::pointer<const CSystemIdArray *> pdrgpsysid,
						 gpos::pointer<const CMDProviderArray *> pdrgpmdp)
	: m_mp(mp), m_pcache(pcache), m_dLookupTime(0.0), m_dFetchTime(0.0)
{
	GPOS_ASSERT(nullptr != m_mp);
	GPOS_ASSERT(nullptr != m_pcache);

	m_pmdpGeneric = GPOS_NEW(mp) CMDProviderGeneric(mp);

	InitHashtables(mp);

	RegisterProviders(pdrgpsysid, pdrgpmdp);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::DestroyAccessorElement
//
//	@doc:
//		Destroy accessor element;
//		called only at destruction time
//
//---------------------------------------------------------------------------
void
CMDAccessor::DestroyAccessorElement(SMDAccessorElem *pmdaccelem)
{
	GPOS_ASSERT(nullptr != pmdaccelem);

	// remove deletion lock for mdid
	pmdaccelem->MDId()->RemoveDeletionLock();
	;

	GPOS_DELETE(pmdaccelem);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::DestroyProviderElement
//
//	@doc:
//		Destroy provider element;
//		called only at destruction time
//
//---------------------------------------------------------------------------
void
CMDAccessor::DestroyProviderElement(SMDProviderElem *pmdpelem)
{
	GPOS_DELETE(pmdpelem);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::InitHashtables
//
//	@doc:
//		Initializes the hash tables
//
//---------------------------------------------------------------------------
void
CMDAccessor::InitHashtables(CMemoryPool *mp)
{
	// initialize Cache accessors hash table
	m_shtCacheAccessors.Init(mp, GPOPT_CACHEACC_HT_NUM_OF_BUCKETS,
							 GPOS_OFFSET(SMDAccessorElem, m_link),
							 GPOS_OFFSET(SMDAccessorElem, m_mdid),
							 &(SMDAccessorElem::m_pmdidInvalid),
							 SMDAccessorElem::HashValue,
							 SMDAccessorElem::Equals);

	// initialize MD providers hash table
	m_shtProviders.Init(mp, GPOPT_CACHEACC_HT_NUM_OF_BUCKETS,
						GPOS_OFFSET(SMDProviderElem, m_link),
						0,	// the HT element is used as key
						&(SMDProviderElem::m_mdpelemInvalid),
						SMDProviderElem::HashValue, SMDProviderElem::Equals);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::~CMDAccessor
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CMDAccessor::~CMDAccessor()
{
	// release cache accessors and MD providers in hashtables
	m_shtCacheAccessors.DestroyEntries(DestroyAccessorElement);
	m_shtProviders.DestroyEntries(DestroyProviderElement);
	GPOS_DELETE(m_pmdpGeneric);

	if (GPOS_FTRACE(EopttracePrintOptimizationStatistics))
	{
		// print fetch time and lookup time
		CAutoTrace at(m_mp);
		at.Os() << "[OPT]: Total metadata fetch time: " << m_dFetchTime << "ms"
				<< std::endl;
		at.Os() << "[OPT]: Total metadata lookup time (including fetch time): "
				<< m_dLookupTime << "ms" << std::endl;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::RegisterProvider
//
//	@doc:
//		Register a MD provider for the given source system id
//
//---------------------------------------------------------------------------
void
CMDAccessor::RegisterProvider(CSystemId sysid, gpos::owner<IMDProvider *> pmdp)
{
	CAutoP<SMDProviderElem> a_pmdpelem;
	a_pmdpelem = GPOS_NEW(m_mp) SMDProviderElem(sysid, std::move(pmdp));

	MDPHTAccessor mdhtacc(m_shtProviders, *(a_pmdpelem.Value()));

	// insert provider in the hash table
	mdhtacc.Insert(a_pmdpelem.Value());
	a_pmdpelem.Reset();
}


//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::RegisterProviders
//
//	@doc:
//		Register given MD providers
//
//---------------------------------------------------------------------------
void
CMDAccessor::RegisterProviders(gpos::pointer<const CSystemIdArray *> pdrgpsysid,
							   gpos::pointer<const CMDProviderArray *> pdrgpmdp)
{
	GPOS_ASSERT(nullptr != pdrgpmdp);
	GPOS_ASSERT(nullptr != pdrgpsysid);
	GPOS_ASSERT(pdrgpmdp->Size() == pdrgpsysid->Size());
	GPOS_ASSERT(0 < pdrgpmdp->Size());

	const ULONG ulProviders = pdrgpmdp->Size();
	for (ULONG ul = 0; ul < ulProviders; ul++)
	{
		gpos::owner<IMDProvider *> pmdp = (*pdrgpmdp)[ul];
		pmdp->AddRef();
		RegisterProvider(*((*pdrgpsysid)[ul]), pmdp);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::Pmdp
//
//	@doc:
//		Retrieve the MD provider for the given source system id
//
//---------------------------------------------------------------------------
IMDProvider *
CMDAccessor::Pmdp(CSystemId sysid)
{
	SMDProviderElem *pmdpelem = nullptr;

	{
		// scope for HT accessor

		SMDProviderElem mdpelem(sysid, nullptr /*pmdp*/);
		MDPHTAccessor mdhtacc(m_shtProviders, mdpelem);

		pmdpelem = mdhtacc.Find();
	}

	GPOS_ASSERT(nullptr != pmdpelem && "Could not find MD provider");

	return pmdpelem->Pmdp();
}



//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::GetImdObj
//
//	@doc:
//		Retrieves a metadata cache object from the md cache, possibly retrieving
//		it from the external metadata provider and storing it in the cache first.
//		Main workhorse for retrieving the different types of md cache objects.
//
//---------------------------------------------------------------------------
gpos::pointer<const IMDCacheObject *>
CMDAccessor::GetImdObj(gpos::pointer<IMDId *> mdid)
{
	BOOL fPrintOptStats = GPOS_FTRACE(EopttracePrintOptimizationStatistics);
	CTimerUser timerLookup;	 // timer to measure lookup time
	if (fPrintOptStats)
	{
		timerLookup.Restart();
	}

	gpos::pointer<const IMDCacheObject *> pimdobj = nullptr;

	// first, try to locate object in local hashtable
	{
		// scope for ht accessor
		MDHTAccessor mdhtacc(m_shtCacheAccessors, mdid);
		SMDAccessorElem *pmdaccelem = mdhtacc.Find();
		if (nullptr != pmdaccelem)
		{
			pimdobj = pmdaccelem->GetImdObj();
		}
	}

	if (nullptr == pimdobj)
	{
		// object not in local hashtable, try lookup in the MD cache

		// construct a key for cache lookup
		gpos::pointer<IMDProvider *> pmdp = Pmdp(mdid->Sysid());

		CMDKey mdkey(mdid);

		CAutoP<CacheAccessorMD> a_pmdcacc;
		a_pmdcacc = GPOS_NEW(m_mp) CacheAccessorMD(m_pcache);
		a_pmdcacc->Lookup(&mdkey);
		IMDCacheObject *pmdobjNew = a_pmdcacc->Val();
		if (nullptr == pmdobjNew)
		{
			// object not found in MD cache: retrieve it from MD provider
			CTimerUser timerFetch;
			if (fPrintOptStats)
			{
				timerFetch.Restart();
			}

			// Any object to be inserted into the MD cache must be allocated in the
			// different memory pool, so that it is not destroyed at the end of the
			// query. Since the mdid passed to GetMDObj() may be saved in the object,
			// make a copy of it here in the right memory pool.
			// An exception is made for CTAS (see below).
			CMemoryPool *mp = m_mp;
			gpos::owner<IMDId *> mdidCopy = mdid;
			if (IMDId::EmdidGPDBCtas != mdid->MdidType())
			{
				// create the accessor memory pool
				mp = a_pmdcacc->Pmp();
				mdidCopy = mdid->Copy(mp);
				GPOS_ASSERT(mdidCopy->Equals(mdid));
			}

			pmdobjNew = pmdp->GetMDObj(mp, this, mdidCopy);
			GPOS_ASSERT(nullptr != pmdobjNew);

			if (fPrintOptStats)
			{
				// add fetch time in msec
				CDouble dFetch(timerFetch.ElapsedUS() /
							   CDouble(GPOS_USEC_IN_MSEC));
				m_dFetchTime = CDouble(m_dFetchTime.Get() + dFetch.Get());
			}

			// For CTAS mdid, we avoid adding the corresponding object to the MD cache
			// since those objects have a fixed id, and if caching is enabled and those
			// objects are cached, then a subsequent CTAS query will attempt to use
			// the cached object, which has a different schema, resulting in a crash.
			// so for such objects, we bypass the MD cache, getting them from the
			// MD provider, directly to the local hash table

			if (IMDId::EmdidGPDBCtas != mdid->MdidType())
			{
				// add to MD cache
				CAutoP<CMDKey> a_pmdkeyCache;
				// ref count of the new object is set to one and optimizer becomes its owner
				a_pmdkeyCache = GPOS_NEW(mp) CMDKey(pmdobjNew->MDId());

				// object gets pinned independent of whether insertion succeeded or
				// failed because object was already in cache

				gpos::pointer<IMDCacheObject *> pmdobjInserted
					GPOS_ASSERTS_ONLY =
						a_pmdcacc->Insert(a_pmdkeyCache.Value(), pmdobjNew);

				GPOS_ASSERT(nullptr != pmdobjInserted);

				// safely inserted
				(void) a_pmdkeyCache.Reset();
			}
		}

		{
			// store in local hashtable
			GPOS_ASSERT(nullptr != pmdobjNew);
			gpos::owner<IMDId *> pmdidNew = pmdobjNew->MDId();
			pmdidNew->AddRef();

			CAutoP<SMDAccessorElem> a_pmdaccelem;
			a_pmdaccelem = GPOS_NEW(m_mp) SMDAccessorElem(pmdobjNew, pmdidNew);

			MDHTAccessor mdhtacc(m_shtCacheAccessors, pmdidNew);

			if (nullptr == mdhtacc.Find())
			{
				// object has not been inserted in the meantime
				mdhtacc.Insert(a_pmdaccelem.Value());

				// add deletion lock for mdid
				pmdidNew->AddDeletionLock();
				a_pmdaccelem.Reset();
			}
		}
	}

	// requested object must be in local hashtable already: retrieve it
	MDHTAccessor mdhtacc(m_shtCacheAccessors, mdid);
	SMDAccessorElem *pmdaccelem = mdhtacc.Find();

	GPOS_ASSERT(nullptr != pmdaccelem);

	pimdobj = pmdaccelem->GetImdObj();
	GPOS_ASSERT(nullptr != pimdobj);

	if (fPrintOptStats)
	{
		// add lookup time in msec
		CDouble dLookup(timerLookup.ElapsedUS() / CDouble(GPOS_USEC_IN_MSEC));
		m_dLookupTime = CDouble(m_dLookupTime.Get() + dLookup.Get());
	}

	return pimdobj;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::RetrieveRel
//
//	@doc:
//		Retrieves a metadata cache relation from the md cache, possibly retrieving
//		it from the external metadata provider and storing it in the cache first.
//
//---------------------------------------------------------------------------
gpos::pointer<const IMDRelation *>
CMDAccessor::RetrieveRel(gpos::pointer<IMDId *> mdid)
{
	gpos::pointer<const IMDCacheObject *> pmdobj = GetImdObj(mdid);
	if (IMDCacheObject::EmdtRel != pmdobj->MDType())
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound,
				   mdid->GetBuffer());
	}

	return dynamic_cast<const IMDRelation *>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::RetrieveType
//
//	@doc:
//		Retrieves the metadata description for a type from the md cache,
//		possibly retrieving it from the external metadata provider and storing
//		it in the cache first.
//
//---------------------------------------------------------------------------
gpos::pointer<const IMDType *>
CMDAccessor::RetrieveType(gpos::pointer<IMDId *> mdid)
{
	gpos::pointer<const IMDCacheObject *> pmdobj = GetImdObj(mdid);
	if (IMDCacheObject::EmdtType != pmdobj->MDType())
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound,
				   mdid->GetBuffer());
	}

	return dynamic_cast<const IMDType *>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::RetrieveType
//
//	@doc:
//		Retrieves the MD type from the md cache given the type info and source
//		system id,  possibly retrieving it from the external metadata provider
//		and storing it in the cache first.
//
//---------------------------------------------------------------------------
gpos::pointer<const IMDType *>
CMDAccessor::RetrieveType(CSystemId sysid, IMDType::ETypeInfo type_info)
{
	GPOS_ASSERT(IMDType::EtiGeneric != type_info);
	gpos::pointer<IMDProvider *> pmdp = Pmdp(sysid);
	CAutoRef<IMDId> a_pmdid;
	a_pmdid = pmdp->MDId(m_mp, sysid, type_info);
	gpos::pointer<const IMDCacheObject *> pmdobj = GetImdObj(a_pmdid.Value());
	if (IMDCacheObject::EmdtType != pmdobj->MDType())
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound,
				   a_pmdid.Value()->GetBuffer());
	}

	return dynamic_cast<const IMDType *>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::RetrieveType
//
//	@doc:
//		Retrieves the generic MD type from the md cache given the
//		type info,  possibly retrieving it from the external metadata provider
//		and storing it in the cache first.
//
//---------------------------------------------------------------------------
gpos::pointer<const IMDType *>
CMDAccessor::RetrieveType(IMDType::ETypeInfo type_info)
{
	GPOS_ASSERT(IMDType::EtiGeneric != type_info);

	gpos::pointer<IMDId *> mdid = m_pmdpGeneric->MDId(type_info);
	GPOS_ASSERT(nullptr != mdid);
	gpos::pointer<const IMDCacheObject *> pmdobj = GetImdObj(mdid);

	if (IMDCacheObject::EmdtType != pmdobj->MDType())
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound,
				   mdid->GetBuffer());
	}

	return dynamic_cast<const IMDType *>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::RetrieveScOp
//
//	@doc:
//		Retrieves the metadata description for a scalar operator from the md cache,
//		possibly retrieving it from the external metadata provider and storing
//		it in the cache first.
//
//---------------------------------------------------------------------------
gpos::pointer<const IMDScalarOp *>
CMDAccessor::RetrieveScOp(gpos::pointer<IMDId *> mdid)
{
	gpos::pointer<const IMDCacheObject *> pmdobj = GetImdObj(mdid);
	if (IMDCacheObject::EmdtOp != pmdobj->MDType())
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound,
				   mdid->GetBuffer());
	}

	return dynamic_cast<const IMDScalarOp *>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::RetrieveFunc
//
//	@doc:
//		Retrieves the metadata description for a function from the md cache,
//		possibly retrieving it from the external metadata provider and storing
//		it in the cache first.
//
//---------------------------------------------------------------------------
gpos::pointer<const IMDFunction *>
CMDAccessor::RetrieveFunc(gpos::pointer<IMDId *> mdid)
{
	gpos::pointer<const IMDCacheObject *> pmdobj = GetImdObj(mdid);
	if (IMDCacheObject::EmdtFunc != pmdobj->MDType())
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound,
				   mdid->GetBuffer());
	}

	return dynamic_cast<const IMDFunction *>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::FaggWindowFunc
//
//	@doc:
//		Check if the retrieved the window function metadata description from
//		the md cache is an aggregate window function. Internally this function
//		may retrieve it from the external metadata provider and storing
//		it in the cache.
//
//---------------------------------------------------------------------------
BOOL
CMDAccessor::FAggWindowFunc(gpos::pointer<IMDId *> mdid)
{
	gpos::pointer<const IMDCacheObject *> pmdobj = GetImdObj(mdid);

	return (IMDCacheObject::EmdtAgg == pmdobj->MDType());
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::RetrieveAgg
//
//	@doc:
//		Retrieves the metadata description for an aggregate from the md cache,
//		possibly retrieving it from the external metadata provider and storing
//		it in the cache first.
//
//---------------------------------------------------------------------------
gpos::pointer<const IMDAggregate *>
CMDAccessor::RetrieveAgg(gpos::pointer<IMDId *> mdid)
{
	gpos::pointer<const IMDCacheObject *> pmdobj = GetImdObj(mdid);
	if (IMDCacheObject::EmdtAgg != pmdobj->MDType())
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound,
				   mdid->GetBuffer());
	}

	return dynamic_cast<const IMDAggregate *>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::RetrieveTrigger
//
//	@doc:
//		Retrieves the metadata description for a trigger from the md cache,
//		possibly retrieving it from the external metadata provider and storing
//		it in the cache first.
//
//---------------------------------------------------------------------------
gpos::pointer<const IMDTrigger *>
CMDAccessor::RetrieveTrigger(gpos::pointer<IMDId *> mdid)
{
	gpos::pointer<const IMDCacheObject *> pmdobj = GetImdObj(mdid);
	if (IMDCacheObject::EmdtTrigger != pmdobj->MDType())
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound,
				   mdid->GetBuffer());
	}

	return dynamic_cast<const IMDTrigger *>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::RetrieveIndex
//
//	@doc:
//		Retrieves the metadata description for an index from the md cache,
//		possibly retrieving it from the external metadata provider and storing
//		it in the cache first.
//
//---------------------------------------------------------------------------
gpos::pointer<const IMDIndex *>
CMDAccessor::RetrieveIndex(gpos::pointer<IMDId *> mdid)
{
	gpos::pointer<const IMDCacheObject *> pmdobj = GetImdObj(mdid);
	if (IMDCacheObject::EmdtInd != pmdobj->MDType())
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound,
				   mdid->GetBuffer());
	}

	return dynamic_cast<const IMDIndex *>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::RetrieveCheckConstraints
//
//	@doc:
//		Retrieves the metadata description for a check constraint from the md cache,
//		possibly retrieving it from the external metadata provider and storing
//		it in the cache first.
//
//---------------------------------------------------------------------------
gpos::pointer<const IMDCheckConstraint *>
CMDAccessor::RetrieveCheckConstraints(gpos::pointer<IMDId *> mdid)
{
	gpos::pointer<const IMDCacheObject *> pmdobj = GetImdObj(mdid);
	if (IMDCacheObject::EmdtCheckConstraint != pmdobj->MDType())
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound,
				   mdid->GetBuffer());
	}

	return dynamic_cast<const IMDCheckConstraint *>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::Pmdcolstats
//
//	@doc:
//		Retrieves column statistics from the md cache, possibly retrieving it
//		from the external metadata provider and storing it in the cache first.
//
//---------------------------------------------------------------------------
gpos::pointer<const IMDColStats *>
CMDAccessor::Pmdcolstats(gpos::pointer<IMDId *> mdid)
{
	gpos::pointer<const IMDCacheObject *> pmdobj = GetImdObj(mdid);
	if (IMDCacheObject::EmdtColStats != pmdobj->MDType())
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound,
				   mdid->GetBuffer());
	}

	return dynamic_cast<const IMDColStats *>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::Pmdrelstats
//
//	@doc:
//		Retrieves relation statistics from the md cache, possibly retrieving it
//		from the external metadata provider and storing it in the cache first.
//
//---------------------------------------------------------------------------
gpos::pointer<const IMDRelStats *>
CMDAccessor::Pmdrelstats(gpos::pointer<IMDId *> mdid)
{
	gpos::pointer<const IMDCacheObject *> pmdobj = GetImdObj(mdid);
	if (IMDCacheObject::EmdtRelStats != pmdobj->MDType())
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound,
				   mdid->GetBuffer());
	}

	return dynamic_cast<const IMDRelStats *>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::Pmdcast
//
//	@doc:
//		Retrieve cast object between given source and destination types
//
//---------------------------------------------------------------------------
gpos::pointer<const IMDCast *>
CMDAccessor::Pmdcast(gpos::pointer<IMDId *> mdid_src,
					 gpos::pointer<IMDId *> mdid_dest)
{
	GPOS_ASSERT(nullptr != mdid_src);
	GPOS_ASSERT(nullptr != mdid_dest);

	mdid_src->AddRef();
	mdid_dest->AddRef();

	CAutoRef<IMDId> a_pmdidCast;
	a_pmdidCast =
		GPOS_NEW(m_mp) CMDIdCast(gpos::dyn_cast<CMDIdGPDB>(mdid_src),
								 gpos::dyn_cast<CMDIdGPDB>(mdid_dest));

	gpos::pointer<const IMDCacheObject *> pmdobj =
		GetImdObj(a_pmdidCast.Value());

	if (IMDCacheObject::EmdtCastFunc != pmdobj->MDType())
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound,
				   a_pmdidCast->GetBuffer());
	}

	return dynamic_cast<const IMDCast *>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::Pmdsccmp
//
//	@doc:
//		Retrieve scalar comparison object between given types
//
//---------------------------------------------------------------------------
gpos::pointer<const IMDScCmp *>
CMDAccessor::Pmdsccmp(gpos::pointer<IMDId *> left_mdid,
					  gpos::pointer<IMDId *> right_mdid,
					  IMDType::ECmpType cmp_type)
{
	GPOS_ASSERT(nullptr != left_mdid);
	GPOS_ASSERT(nullptr != left_mdid);
	GPOS_ASSERT(IMDType::EcmptOther > cmp_type);

	left_mdid->AddRef();
	right_mdid->AddRef();

	CAutoRef<IMDId> a_pmdidScCmp;
	a_pmdidScCmp = GPOS_NEW(m_mp)
		CMDIdScCmp(gpos::dyn_cast<CMDIdGPDB>(left_mdid),
				   gpos::dyn_cast<CMDIdGPDB>(right_mdid), cmp_type);

	gpos::pointer<const IMDCacheObject *> pmdobj =
		GetImdObj(a_pmdidScCmp.Value());

	if (IMDCacheObject::EmdtScCmp != pmdobj->MDType())
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound,
				   a_pmdidScCmp->GetBuffer());
	}

	return dynamic_cast<const IMDScCmp *>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::ExtractColumnHistWidth
//
//	@doc:
//		Record histogram and width information for a given column of a table
//
//---------------------------------------------------------------------------
void
CMDAccessor::RecordColumnStats(
	CMemoryPool *mp, IMDId *rel_mdid, ULONG colid, ULONG ulPos,
	BOOL isSystemCol, BOOL isEmptyTable,
	gpos::pointer<UlongToHistogramMap *> col_histogram_mapping,
	gpos::pointer<UlongToDoubleMap *> colid_width_mapping,
	gpos::pointer<CStatisticsConfig *> stats_config)
{
	GPOS_ASSERT(nullptr != rel_mdid);
	GPOS_ASSERT(nullptr != col_histogram_mapping);
	GPOS_ASSERT(nullptr != colid_width_mapping);

	// get the column statistics
	gpos::pointer<const IMDColStats *> pmdcolstats =
		Pmdcolstats(mp, rel_mdid, ulPos);
	GPOS_ASSERT(nullptr != pmdcolstats);

	// fetch the column width and insert it into the hashmap
	CDouble *width = GPOS_NEW(mp) CDouble(pmdcolstats->Width());
	colid_width_mapping->Insert(GPOS_NEW(mp) ULONG(colid), width);

	// extract the the histogram and insert it into the hashmap
	gpos::pointer<const IMDRelation *> pmdrel = RetrieveRel(rel_mdid);
	gpos::pointer<IMDId *> mdid_type = pmdrel->GetMdCol(ulPos)->MdidType();
	CHistogram *histogram = GetHistogram(mp, mdid_type, pmdcolstats);
	GPOS_ASSERT(nullptr != histogram);
	col_histogram_mapping->Insert(GPOS_NEW(mp) ULONG(colid), histogram);

	BOOL fGuc = GPOS_FTRACE(EopttracePrintColsWithMissingStats);
	BOOL fRecordMissingStats = !isEmptyTable && fGuc && !isSystemCol &&
							   (nullptr != stats_config) &&
							   histogram->IsColStatsMissing();
	if (fRecordMissingStats)
	{
		// record the columns with missing (dummy) statistics information
		rel_mdid->AddRef();
		gpos::owner<CMDIdColStats *> pmdidCol = GPOS_NEW(mp)
			CMDIdColStats(gpos::dyn_cast<CMDIdGPDB>(rel_mdid), ulPos);
		stats_config->AddMissingStatsColumn(pmdidCol);
		pmdidCol->Release();
	}
}


// Return the column statistics meta data object for a given column of a table
gpos::pointer<const IMDColStats *>
CMDAccessor::Pmdcolstats(CMemoryPool *mp, IMDId *rel_mdid, ULONG ulPos)
{
	rel_mdid->AddRef();
	gpos::owner<CMDIdColStats *> mdid_col_stats =
		GPOS_NEW(mp) CMDIdColStats(gpos::dyn_cast<CMDIdGPDB>(rel_mdid), ulPos);
	gpos::pointer<const IMDColStats *> pmdcolstats =
		Pmdcolstats(mdid_col_stats);
	mdid_col_stats->Release();

	return pmdcolstats;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::Pstats
//
//	@doc:
//		Construct a statistics object for the columns of the given relation
//
//---------------------------------------------------------------------------
gpos::owner<IStatistics *>
CMDAccessor::Pstats(CMemoryPool *mp, IMDId *rel_mdid,
					gpos::pointer<CColRefSet *> pcrsHist,
					gpos::pointer<CColRefSet *> pcrsWidth,
					gpos::pointer<CStatisticsConfig *> stats_config)
{
	GPOS_ASSERT(nullptr != rel_mdid);
	GPOS_ASSERT(nullptr != pcrsHist);
	GPOS_ASSERT(nullptr != pcrsWidth);

	// retrieve MD relation and MD relation stats objects
	rel_mdid->AddRef();
	gpos::owner<CMDIdRelStats *> rel_stats_mdid =
		GPOS_NEW(mp) CMDIdRelStats(gpos::dyn_cast<CMDIdGPDB>(rel_mdid));
	gpos::pointer<const IMDRelStats *> pmdRelStats =
		Pmdrelstats(rel_stats_mdid);
	rel_stats_mdid->Release();

	BOOL fEmptyTable = pmdRelStats->IsEmpty();
	gpos::pointer<const IMDRelation *> pmdrel = RetrieveRel(rel_mdid);

	gpos::owner<UlongToHistogramMap *> col_histogram_mapping =
		GPOS_NEW(mp) UlongToHistogramMap(mp);
	gpos::owner<UlongToDoubleMap *> colid_width_mapping =
		GPOS_NEW(mp) UlongToDoubleMap(mp);

	CColRefSetIter crsiHist(*pcrsHist);
	while (crsiHist.Advance())
	{
		CColRef *pcrHist = crsiHist.Pcr();

		// colref must be one of the base table
		CColRefTable *pcrtable = CColRefTable::PcrConvert(pcrHist);

		// extract the column identifier, position of the attribute in the system catalog
		ULONG colid = pcrtable->Id();
		INT attno = pcrtable->AttrNum();
		ULONG ulPos = pmdrel->GetPosFromAttno(attno);

		RecordColumnStats(mp, rel_mdid, colid, ulPos, pcrtable->IsSystemCol(),
						  fEmptyTable, col_histogram_mapping,
						  colid_width_mapping, stats_config);
	}

	// extract column widths
	CColRefSetIter crsiWidth(*pcrsWidth);

	while (crsiWidth.Advance())
	{
		CColRef *pcrWidth = crsiWidth.Pcr();

		// colref must be one of the base table
		CColRefTable *pcrtable = CColRefTable::PcrConvert(pcrWidth);

		// extract the column identifier, position of the attribute in the system catalog
		ULONG colid = pcrtable->Id();
		INT attno = pcrtable->AttrNum();
		ULONG ulPos = pmdrel->GetPosFromAttno(attno);

		CDouble *width = GPOS_NEW(mp) CDouble(pmdrel->ColWidth(ulPos));
		colid_width_mapping->Insert(GPOS_NEW(mp) ULONG(colid), width);
	}

	CDouble rows = std::max(DOUBLE(1.0), pmdRelStats->Rows().Get());

	return GPOS_NEW(mp)
		CStatistics(mp, std::move(col_histogram_mapping),
					std::move(colid_width_mapping), rows, fEmptyTable,
					pmdRelStats->RelPages(), pmdRelStats->RelAllVisible());
}


//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::GetHistogram
//
//	@doc:
//		Construct a histogram from the given MD column stats object
//
//---------------------------------------------------------------------------
CHistogram *
CMDAccessor::GetHistogram(CMemoryPool *mp, gpos::pointer<IMDId *> mdid_type,
						  gpos::pointer<const IMDColStats *> pmdcolstats)
{
	GPOS_ASSERT(nullptr != mdid_type);
	GPOS_ASSERT(nullptr != pmdcolstats);

	BOOL is_col_stats_missing = pmdcolstats->IsColStatsMissing();
	const ULONG num_of_buckets = pmdcolstats->Buckets();
	BOOL fBoolType = CMDAccessorUtils::FBoolType(this, mdid_type);
	if (is_col_stats_missing && fBoolType)
	{
		GPOS_ASSERT(0 == num_of_buckets);

		return CHistogram::MakeDefaultBoolHistogram(mp);
	}

	gpos::owner<CBucketArray *> buckets = GPOS_NEW(mp) CBucketArray(mp);
	for (ULONG ul = 0; ul < num_of_buckets; ul++)
	{
		gpos::pointer<const CDXLBucket *> dxl_bucket =
			pmdcolstats->GetDXLBucketAt(ul);
		CBucket *bucket = Pbucket(mp, mdid_type, dxl_bucket);
		buckets->Append(bucket);
	}

	CDouble null_freq = pmdcolstats->GetNullFreq();
	CDouble distinct_remaining = pmdcolstats->GetDistinctRemain();
	CDouble freq_remaining = pmdcolstats->GetFreqRemain();

	CHistogram *histogram = GPOS_NEW(mp)
		CHistogram(mp, std::move(buckets), true /*is_well_defined*/, null_freq,
				   distinct_remaining, freq_remaining, is_col_stats_missing);
	GPOS_ASSERT_IMP(fBoolType,
					3 >= histogram->GetNumDistinct() - CStatistics::Epsilon);

	return histogram;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::Pbucket
//
//	@doc:
//		Construct a typed bucket from a DXL bucket
//
//---------------------------------------------------------------------------
CBucket *
CMDAccessor::Pbucket(CMemoryPool *mp, gpos::pointer<IMDId *> mdid_type,
					 gpos::pointer<const CDXLBucket *> dxl_bucket)
{
	gpos::owner<IDatum *> pdatumLower =
		GetDatum(mp, mdid_type, dxl_bucket->GetDXLDatumLower());
	gpos::owner<IDatum *> pdatumUpper =
		GetDatum(mp, mdid_type, dxl_bucket->GetDXLDatumUpper());

	gpos::owner<CPoint *> bucket_lower_bound =
		GPOS_NEW(mp) CPoint(std::move(pdatumLower));
	gpos::owner<CPoint *> bucket_upper_bound =
		GPOS_NEW(mp) CPoint(std::move(pdatumUpper));

	return GPOS_NEW(mp)
		CBucket(std::move(bucket_lower_bound), std::move(bucket_upper_bound),
				dxl_bucket->IsLowerClosed(), dxl_bucket->IsUpperClosed(),
				dxl_bucket->GetFrequency(), dxl_bucket->GetNumDistinct());
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::GetDatum
//
//	@doc:
//		Construct a typed bucket from a DXL bucket
//
//---------------------------------------------------------------------------
gpos::owner<IDatum *>
CMDAccessor::GetDatum(CMemoryPool *mp, gpos::pointer<IMDId *> mdid_type,
					  gpos::pointer<const CDXLDatum *> dxl_datum)
{
	gpos::pointer<const IMDType *> pmdtype = RetrieveType(mdid_type);

	return pmdtype->GetDatumForDXLDatum(mp, dxl_datum);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::Serialize
//
//	@doc:
//		Serialize MD object into provided stream
//
//---------------------------------------------------------------------------
void
CMDAccessor::Serialize(COstream &oos)
{
	ULONG nentries = m_shtCacheAccessors.Size();
	IMDCacheObject **cacheEntries;
	CAutoRg<IMDCacheObject *> aCacheEntries;
	ULONG ul;

	// Iterate the hash table and insert all entries to the array.
	// The iterator holds a lock on the hash table, so we must not
	// do anything non-trivial that might e.g. allocate memory,
	// while iterating.
	cacheEntries = GPOS_NEW_ARRAY(m_mp, IMDCacheObject *, nentries);
	aCacheEntries = cacheEntries;
	{
		MDHTIter mdhtit(m_shtCacheAccessors);
		ul = 0;
		while (mdhtit.Advance())
		{
			MDHTIterAccessor mdhtitacc(mdhtit);
			SMDAccessorElem *pmdaccelem = mdhtitacc.Value();
			GPOS_ASSERT(nullptr != pmdaccelem);
			cacheEntries[ul++] = pmdaccelem->GetImdObj();
		}
		GPOS_ASSERT(ul == nentries);
	}

	// Now that we're done iterating and no longer hold the lock,
	// serialize the entries.
	for (ul = 0; ul < nentries; ul++)
		oos << cacheEntries[ul]->GetStrRepr()->GetBuffer();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::SerializeSysid
//
//	@doc:
//		Serialize the system ids into provided stream
//
//---------------------------------------------------------------------------
void
CMDAccessor::SerializeSysid(COstream &oos)
{
	ULONG ul = 0;
	MDPHTIter mdhtit(m_shtProviders);

	while (mdhtit.Advance())
	{
		MDPHTIterAccessor mdhtitacc(mdhtit);
		SMDProviderElem *pmdpelem = mdhtitacc.Value();
		CSystemId sysid = pmdpelem->Sysid();


		WCHAR wszSysId[GPDXL_MDID_LENGTH];
		CWStringStatic str(wszSysId, GPOS_ARRAY_SIZE(wszSysId));

		if (0 < ul)
		{
			str.AppendFormat(GPOS_WSZ_LIT("%s"), ",");
		}

		str.AppendFormat(GPOS_WSZ_LIT("%d.%ls"), sysid.MdidType(),
						 sysid.GetBuffer());

		oos << str.GetBuffer();
		ul++;
	}
}


// EOF
