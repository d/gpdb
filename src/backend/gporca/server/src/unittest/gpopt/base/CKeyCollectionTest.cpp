//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 - 2012 EMC Corp.
//
//	@filename:
//		CKeyCollectionTest.cpp
//
//	@doc:
//		Tests for CKeyCollectionTest
//---------------------------------------------------------------------------
#include "unittest/gpopt/base/CKeyCollectionTest.h"

#include "gpos/common/owner.h"

#include "gpopt/base/CKeyCollection.h"
#include "gpopt/eval/CConstExprEvaluatorDefault.h"
#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/mdcache/CMDCache.h"
#include "gpopt/metadata/CName.h"
#include "naucrates/md/CMDProviderMemory.h"
#include "naucrates/md/IMDTypeInt4.h"

#include "unittest/base.h"
#include "unittest/gpopt/CTestUtils.h"


using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CKeyCollectionTest::EresUnittest
//
//	@doc:
//		Unittest for key collections
//
//---------------------------------------------------------------------------
GPOS_RESULT
CKeyCollectionTest::EresUnittest()
{
	CUnittest rgut[] = {
		GPOS_UNITTEST_FUNC(CKeyCollectionTest::EresUnittest_Basics),
		GPOS_UNITTEST_FUNC(CKeyCollectionTest::EresUnittest_Subsumes)};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}

//---------------------------------------------------------------------------
//	@function:
//		CKeyCollectionTest::EresUnittest_Basics
//
//	@doc:
//		Basic test for key collections
//
//---------------------------------------------------------------------------
GPOS_RESULT
CKeyCollectionTest::EresUnittest_Basics()
{
	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();

	gpos::Ref<CColRefSet> pcrs = GPOS_NEW(mp) CColRefSet(mp);

	// Setup an MD cache with a file-based provider
	gpos::Ref<CMDProviderMemory> pmdp = CTestUtils::m_pmdpf;
	;

	CMDAccessor mda(mp, CMDCache::Pcache());
	mda.RegisterProvider(CTestUtils::m_sysidDefault, pmdp);

	// install opt context in TLS
	CAutoOptCtxt aoc(mp, &mda, nullptr, /* pceeval */
					 CTestUtils::GetCostModel(mp));

	// get column factory from optimizer context object
	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();

	// create test set
	CWStringConst strName(GPOS_WSZ_LIT("Test Column"));
	CName name(&strName);
	const IMDTypeInt4 *pmdtypeint4 = mda.PtMDType<IMDTypeInt4>();

	gpos::Ref<CKeyCollection> pkc = GPOS_NEW(mp) CKeyCollection(mp);

	const ULONG num_cols = 10;
	for (ULONG i = 0; i < num_cols; i++)
	{
		CColRef *colref =
			col_factory->PcrCreate(pmdtypeint4, default_type_modifier, name);
		pcrs->Include(colref);
	}

	pkc->Add(pcrs);
	GPOS_ASSERT(pkc->FKey(pcrs.get()));

	gpos::Ref<CColRefArray> colref_array = pkc->PdrgpcrKey(mp);
	GPOS_ASSERT(pkc->FKey(mp, colref_array.get()));

	pcrs->Include(colref_array.get());
	GPOS_ASSERT(pkc->FKey(pcrs.get()));

	;

	;

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CKeyCollectionTest::EresUnittest_Subsumes
//
//	@doc:
//		Basic test for triming key collections
//
//---------------------------------------------------------------------------
GPOS_RESULT
CKeyCollectionTest::EresUnittest_Subsumes()
{
	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();

	// Setup an MD cache with a file-based provider
	gpos::Ref<CMDProviderMemory> pmdp = CTestUtils::m_pmdpf;
	;

	CMDAccessor mda(mp, CMDCache::Pcache());
	mda.RegisterProvider(CTestUtils::m_sysidDefault, pmdp);

	// install opt context in TLS
	CAutoOptCtxt aoc(mp, &mda, nullptr, /* pceeval */
					 CTestUtils::GetCostModel(mp));

	// get column factory from optimizer context object
	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();

	// create test set
	CWStringConst strName(GPOS_WSZ_LIT("Test Column"));
	CName name(&strName);
	const IMDTypeInt4 *pmdtypeint4 = mda.PtMDType<IMDTypeInt4>();

	gpos::Ref<CKeyCollection> pkc = GPOS_NEW(mp) CKeyCollection(mp);

	gpos::Ref<CColRefSet> pcrs0 = GPOS_NEW(mp) CColRefSet(mp);
	gpos::Ref<CColRefSet> pcrs1 = GPOS_NEW(mp) CColRefSet(mp);
	gpos::Ref<CColRefSet> pcrs2 = GPOS_NEW(mp) CColRefSet(mp);

	const ULONG num_cols = 10;
	const ULONG ulLen1 = 3;
	for (ULONG ul = 0; ul < num_cols; ul++)
	{
		CColRef *colref =
			col_factory->PcrCreate(pmdtypeint4, default_type_modifier, name);
		pcrs0->Include(colref);

		if (ul < ulLen1)
		{
			pcrs1->Include(colref);
		}

		if (ul == 0)
		{
			pcrs2->Include(colref);
		}
	}

	pkc->Add(std::move(pcrs0));
	pkc->Add(std::move(pcrs1));
	pkc->Add(std::move(pcrs2));

	GPOS_ASSERT(pkc->FKey(pcrs2.get()));

	// get the second key
	gpos::Ref<CColRefArray> colref_array = pkc->PdrgpcrKey(mp, 1);
	GPOS_ASSERT(ulLen1 == colref_array->Size());
	GPOS_ASSERT(pkc->FKey(mp, colref_array.get()));

	// get the subsumed key
	gpos::Ref<CColRefArray> pdrgpcrSubsumed =
		pkc->PdrgpcrTrim(mp, colref_array.get());
	GPOS_ASSERT(colref_array->Size() >= pdrgpcrSubsumed->Size());

	gpos::Ref<CColRefSet> pcrsSubsumed = GPOS_NEW(mp) CColRefSet(mp);
	pcrsSubsumed->Include(colref_array.get());

#ifdef GPOS_DEBUG
	const ULONG ulLenSubsumed = pdrgpcrSubsumed->Size();
	for (ULONG ul = 0; ul < ulLenSubsumed; ul++)
	{
		CColRef *colref = (*pdrgpcrSubsumed)[ul];
		GPOS_ASSERT(pcrsSubsumed->FMember(colref));
	}
#endif	// GPOS_DEBUG

	;
	;
	;
	;

	return GPOS_OK;
}
// EOF
