//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CScalarAggFunc.cpp
//
//	@doc:
//		Implementation of scalar aggregate function call operators
//---------------------------------------------------------------------------

#include "gpopt/operators/CScalarAggFunc.h"

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "naucrates/md/CMDIdGPDB.h"
#include "naucrates/md/IMDAggregate.h"


using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CScalarAggFunc::CScalarAggFunc
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarAggFunc::CScalarAggFunc(CMemoryPool *mp,
							   gpos::owner<IMDId *> pmdidAggFunc,
							   gpos::owner<IMDId *> resolved_rettype,
							   const CWStringConst *pstrAggFunc,
							   BOOL is_distinct, EAggfuncStage eaggfuncstage,
							   BOOL fSplit)
	: CScalar(mp),
	  m_pmdidAggFunc(std::move(pmdidAggFunc)),
	  m_pmdidResolvedRetType(std::move(resolved_rettype)),
	  m_return_type_mdid(nullptr),
	  m_pstrAggFunc(pstrAggFunc),
	  m_is_distinct(is_distinct),
	  m_eaggfuncstage(eaggfuncstage),
	  m_fSplit(fSplit)
{
	GPOS_ASSERT(nullptr != m_pmdidAggFunc);
	GPOS_ASSERT(nullptr != pstrAggFunc);
	GPOS_ASSERT(m_pmdidAggFunc->IsValid());
	GPOS_ASSERT_IMP(nullptr != m_pmdidResolvedRetType,
					m_pmdidResolvedRetType->IsValid());
	GPOS_ASSERT(EaggfuncstageSentinel > eaggfuncstage);

	// store id of type obtained by looking up MD cache
	gpos::owner<IMDId *> mdid = PmdidLookupReturnType(
		m_pmdidAggFunc, (EaggfuncstageGlobal == m_eaggfuncstage));
	mdid->AddRef();
	m_return_type_mdid = mdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarAggFunc::PstrAggFunc
//
//	@doc:
//		Aggregate function name
//
//---------------------------------------------------------------------------
const CWStringConst *
CScalarAggFunc::PstrAggFunc() const
{
	return m_pstrAggFunc;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarAggFunc::MDId
//
//	@doc:
//		Aggregate function id
//
//---------------------------------------------------------------------------
gpos::pointer<IMDId *>
CScalarAggFunc::MDId() const
{
	return m_pmdidAggFunc;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarAggFunc::FCountStar
//
//	@doc:
//		Is function count(*)?
//
//---------------------------------------------------------------------------
BOOL
CScalarAggFunc::FCountStar() const
{
	// TODO,  04/26/2012, make this function system-independent
	// using MDAccessor
	return m_pmdidAggFunc->Equals(&CMDIdGPDB::m_mdid_count_star);
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarAggFunc::FCountAny
//
//	@doc:
//		Is function count(Any)?
//
//---------------------------------------------------------------------------
BOOL
CScalarAggFunc::FCountAny() const
{
	// TODO,  04/26/2012, make this function system-independent
	// using MDAccessor
	return m_pmdidAggFunc->Equals(&CMDIdGPDB::m_mdid_count_any);
}

// Is function either min() or max()?
BOOL
CScalarAggFunc::IsMinMax(gpos::pointer<const IMDType *> mdtype) const
{
	return m_pmdidAggFunc->Equals(
			   mdtype->GetMdidForAggType(IMDType::EaggMin)) ||
		   m_pmdidAggFunc->Equals(mdtype->GetMdidForAggType(IMDType::EaggMax));
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarAggFunc::HashValue
//
//	@doc:
//		Operator specific hash function
//
//---------------------------------------------------------------------------
ULONG
CScalarAggFunc::HashValue() const
{
	ULONG ulAggfuncstage = (ULONG) m_eaggfuncstage;
	return gpos::CombineHashes(
		CombineHashes(COperator::HashValue(), m_pmdidAggFunc->HashValue()),
		CombineHashes(gpos::HashValue<ULONG>(&ulAggfuncstage),
					  CombineHashes(gpos::HashValue<BOOL>(&m_is_distinct),
									gpos::HashValue<BOOL>(&m_fSplit))));
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarAggFunc::Matches
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarAggFunc::Matches(gpos::pointer<COperator *> pop) const
{
	if (pop->Eopid() == Eopid())
	{
		gpos::pointer<CScalarAggFunc *> popScAggFunc =
			gpos::dyn_cast<CScalarAggFunc>(pop);

		// match if func ids are identical
		return ((popScAggFunc->IsDistinct() == m_is_distinct) &&
				(popScAggFunc->Eaggfuncstage() == Eaggfuncstage()) &&
				(popScAggFunc->FSplit() == m_fSplit) &&
				m_pmdidAggFunc->Equals(popScAggFunc->MDId()));
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarAggFunc::PmdidLookupReturnType
//
//	@doc:
//		Lookup mdid of return type for given Agg function
//
//---------------------------------------------------------------------------
IMDId *
CScalarAggFunc::PmdidLookupReturnType(gpos::pointer<IMDId *> pmdidAggFunc,
									  BOOL fGlobal, CMDAccessor *pmdaInput)
{
	GPOS_ASSERT(nullptr != pmdidAggFunc);
	CMDAccessor *md_accessor = pmdaInput;

	if (nullptr == md_accessor)
	{
		md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	}
	GPOS_ASSERT(nullptr != md_accessor);

	// get aggregate function return type from the MD cache
	gpos::pointer<const IMDAggregate *> pmdagg =
		md_accessor->RetrieveAgg(pmdidAggFunc);
	if (fGlobal)
	{
		return pmdagg->GetResultTypeMdid();
	}

	return pmdagg->GetIntermediateResultTypeMdid();
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarAggFunc::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CScalarAggFunc::OsPrint(IOstream &os) const
{
	os << SzId() << " (";
	os << PstrAggFunc()->GetBuffer();
	os << " , Distinct: ";
	os << (m_is_distinct ? "true" : "false");
	os << " , Aggregate Stage: ";

	switch (m_eaggfuncstage)
	{
		case EaggfuncstageGlobal:
			os << "Global";
			break;

		case EaggfuncstageIntermediate:
			os << "Intermediate";
			break;

		case EaggfuncstageLocal:
			os << "Local";
			break;

		default:
			GPOS_ASSERT(!"Unsupported aggregate type");
	}

	os << ")";

	return os;
}


// EOF
