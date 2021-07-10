//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CUtils.cpp
//
//	@doc:
//		Implementation of general utility functions
//---------------------------------------------------------------------------
#include "gpopt/base/CUtils.h"

#include "gpos/common/clibwrapper.h"
#include "gpos/common/owner.h"
#include "gpos/common/syslibwrapper.h"
#include "gpos/io/CFileDescriptor.h"
#include "gpos/io/COstreamString.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/string/CWStringDynamic.h"
#include "gpos/task/CWorker.h"

#include "gpopt/base/CCastUtils.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CColRefTable.h"
#include "gpopt/base/CConstraintInterval.h"
#include "gpopt/base/CDistributionSpecRandom.h"
#include "gpopt/base/CKeyCollection.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/exception.h"
#include "gpopt/mdcache/CMDAccessorUtils.h"
#include "gpopt/operators/CExpressionPreprocessor.h"
#include "gpopt/operators/CLogicalApply.h"
#include "gpopt/operators/CLogicalCTEConsumer.h"
#include "gpopt/operators/CLogicalCTEProducer.h"
#include "gpopt/operators/CLogicalFullOuterJoin.h"
#include "gpopt/operators/CLogicalJoin.h"
#include "gpopt/operators/CLogicalLeftAntiSemiJoin.h"
#include "gpopt/operators/CLogicalLeftAntiSemiJoinNotIn.h"
#include "gpopt/operators/CLogicalLeftOuterJoin.h"
#include "gpopt/operators/CLogicalLimit.h"
#include "gpopt/operators/CLogicalNAryJoin.h"
#include "gpopt/operators/CLogicalProject.h"
#include "gpopt/operators/CLogicalSelect.h"
#include "gpopt/operators/CLogicalSequenceProject.h"
#include "gpopt/operators/CLogicalSetOp.h"
#include "gpopt/operators/CLogicalUnary.h"
#include "gpopt/operators/CPhysicalAgg.h"
#include "gpopt/operators/CPhysicalCTEConsumer.h"
#include "gpopt/operators/CPhysicalCTEProducer.h"
#include "gpopt/operators/CPhysicalMotionRandom.h"
#include "gpopt/operators/CPhysicalNLJoin.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/CScalarArray.h"
#include "gpopt/operators/CScalarArrayCoerceExpr.h"
#include "gpopt/operators/CScalarCast.h"
#include "gpopt/operators/CScalarCmp.h"
#include "gpopt/operators/CScalarCoerceViaIO.h"
#include "gpopt/operators/CScalarIdent.h"
#include "gpopt/operators/CScalarIsDistinctFrom.h"
#include "gpopt/operators/CScalarNullTest.h"
#include "gpopt/operators/CScalarOp.h"
#include "gpopt/operators/CScalarProjectElement.h"
#include "gpopt/operators/CScalarProjectList.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/search/CMemo.h"
#include "gpopt/translate/CTranslatorExprToDXLUtils.h"
#include "naucrates/base/IDatumBool.h"
#include "naucrates/base/IDatumInt2.h"
#include "naucrates/base/IDatumInt4.h"
#include "naucrates/base/IDatumInt8.h"
#include "naucrates/base/IDatumOid.h"
#include "naucrates/exception.h"
#include "naucrates/md/CMDArrayCoerceCastGPDB.h"
#include "naucrates/md/CMDIdGPDB.h"
#include "naucrates/md/CMDIdScCmp.h"
#include "naucrates/md/IMDAggregate.h"
#include "naucrates/md/IMDCast.h"
#include "naucrates/md/IMDScCmp.h"
#include "naucrates/md/IMDScalarOp.h"
#include "naucrates/md/IMDType.h"
#include "naucrates/md/IMDTypeBool.h"
#include "naucrates/md/IMDTypeGeneric.h"
#include "naucrates/md/IMDTypeInt2.h"
#include "naucrates/md/IMDTypeInt4.h"
#include "naucrates/md/IMDTypeInt8.h"
#include "naucrates/md/IMDTypeOid.h"
#include "naucrates/traceflags/traceflags.h"

using namespace gpopt;
using namespace gpmd;

#ifdef GPOS_DEBUG

// buffer of 16M characters for print wrapper
const ULONG ulBufferCapacity = 16 * 1024 * 1024;
static WCHAR wszBuffer[ulBufferCapacity];

// global wrapper for debug print of expression
void
PrintExpr(void *pv)
{
	gpopt::CUtils::PrintExpression((gpopt::CExpression *) pv);
}

// debug print expression
void
CUtils::PrintExpression(CExpression *pexpr)
{
	CWStringStatic str(wszBuffer, ulBufferCapacity);
	COstreamString oss(&str);

	if (nullptr == pexpr)
	{
		oss << std::endl << "(null)" << std::endl;
	}
	else
	{
		oss << std::endl << pexpr << std::endl << *pexpr << std::endl;
	}

	GPOS_TRACE(str.GetBuffer());
	str.Reset();
}

// global wrapper for debug print of memo
void
PrintMemo(void *pv)
{
	gpopt::CUtils::PrintMemo((gpopt::CMemo *) pv);
}

// debug print Memo structure
void
CUtils::PrintMemo(CMemo *pmemo)
{
	CWStringStatic str(wszBuffer, ulBufferCapacity);
	COstreamString oss(&str);

	if (nullptr == pmemo)
	{
		oss << std::endl << "(null)" << std::endl;
	}
	else
	{
		oss << std::endl << pmemo << std::endl;
		(void) pmemo->OsPrint(oss);
		oss << std::endl;
	}

	GPOS_TRACE(str.GetBuffer());
	str.Reset();
}

#endif	// GPOS_DEBUG

// helper function to print a column descriptor array
IOstream &
CUtils::OsPrintDrgPcoldesc(IOstream &os,
						   gpos::pointer<CColumnDescriptorArray *> pdrgpcoldesc,
						   ULONG ulLengthMax)
{
	ULONG length = std::min(pdrgpcoldesc->Size(), ulLengthMax);
	for (ULONG ul = 0; ul < length; ul++)
	{
		gpos::pointer<CColumnDescriptor *> pcoldesc = (*pdrgpcoldesc)[ul];
		pcoldesc->OsPrint(os);

		// separator
		os << (ul == length - 1 ? "" : ", ");
	}

	return os;
}


// generate a ScalarIdent expression
gpos::owner<CExpression *>
CUtils::PexprScalarIdent(CMemoryPool *mp, const CColRef *colref)
{
	GPOS_ASSERT(nullptr != colref);

	return GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarIdent(mp, colref));
}

// generate a ScalarProjectElement expression
gpos::owner<CExpression *>
CUtils::PexprScalarProjectElement(CMemoryPool *mp, CColRef *colref,
								  gpos::owner<CExpression *> pexpr)
{
	GPOS_ASSERT(nullptr != colref);
	GPOS_ASSERT(nullptr != pexpr);

	return GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CScalarProjectElement(mp, colref), std::move(pexpr));
}

// generate a comparison expression over two columns
gpos::owner<CExpression *>
CUtils::PexprScalarCmp(CMemoryPool *mp, const CColRef *pcrLeft,
					   const CColRef *pcrRight, CWStringConst strOp,
					   gpos::owner<IMDId *> mdid_op)
{
	GPOS_ASSERT(nullptr != pcrLeft);
	GPOS_ASSERT(nullptr != pcrRight);

	IMDType::ECmpType cmp_type = ParseCmpType(mdid_op);
	if (IMDType::EcmptOther != cmp_type)
	{
		gpos::pointer<IMDId *> left_mdid = pcrLeft->RetrieveType()->MDId();
		gpos::pointer<IMDId *> right_mdid = pcrRight->RetrieveType()->MDId();

		if (CMDAccessorUtils::FCmpOrCastedCmpExists(left_mdid, right_mdid,
													cmp_type))
		{
			gpos::owner<CExpression *> pexprScCmp =
				PexprScalarCmp(mp, pcrLeft, pcrRight, cmp_type);
			mdid_op->Release();

			return pexprScCmp;
		}
	}

	return GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CScalarCmp(
			mp, mdid_op, GPOS_NEW(mp) CWStringConst(mp, strOp.GetBuffer()),
			ParseCmpType(mdid_op)),
		PexprScalarIdent(mp, pcrLeft), PexprScalarIdent(mp, pcrRight));
}

// Generate a comparison expression between a column and an expression
gpos::owner<CExpression *>
CUtils::PexprScalarCmp(CMemoryPool *mp, const CColRef *pcrLeft,
					   gpos::owner<CExpression *> pexprRight,
					   CWStringConst strOp, gpos::owner<IMDId *> mdid_op)
{
	GPOS_ASSERT(nullptr != pcrLeft);
	GPOS_ASSERT(nullptr != pexprRight);

	IMDType::ECmpType cmp_type = ParseCmpType(mdid_op);
	if (IMDType::EcmptOther != cmp_type)
	{
		gpos::pointer<IMDId *> left_mdid = pcrLeft->RetrieveType()->MDId();
		gpos::pointer<IMDId *> right_mdid =
			gpos::dyn_cast<CScalar>(pexprRight->Pop())->MdidType();

		if (CMDAccessorUtils::FCmpOrCastedCmpExists(left_mdid, right_mdid,
													cmp_type))
		{
			gpos::owner<CExpression *> pexprScCmp =
				PexprScalarCmp(mp, pcrLeft, std::move(pexprRight), cmp_type);
			mdid_op->Release();

			return pexprScCmp;
		}
	}

	return GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CScalarCmp(
			mp, mdid_op, GPOS_NEW(mp) CWStringConst(mp, strOp.GetBuffer()),
			ParseCmpType(mdid_op)),
		PexprScalarIdent(mp, pcrLeft), std::move(pexprRight));
}

// Generate a comparison expression between two columns
gpos::owner<CExpression *>
CUtils::PexprScalarCmp(CMemoryPool *mp, const CColRef *pcrLeft,
					   const CColRef *pcrRight, IMDType::ECmpType cmp_type)
{
	GPOS_ASSERT(nullptr != pcrLeft);
	GPOS_ASSERT(nullptr != pcrRight);
	GPOS_ASSERT(IMDType::EcmptOther > cmp_type);

	gpos::owner<CExpression *> pexprLeft = PexprScalarIdent(mp, pcrLeft);
	gpos::owner<CExpression *> pexprRight = PexprScalarIdent(mp, pcrRight);

	return PexprScalarCmp(mp, std::move(pexprLeft), std::move(pexprRight),
						  cmp_type);
}

// Generate a comparison expression over a column and an expression
gpos::owner<CExpression *>
CUtils::PexprScalarCmp(CMemoryPool *mp, const CColRef *pcrLeft,
					   gpos::owner<CExpression *> pexprRight,
					   IMDType::ECmpType cmp_type)
{
	GPOS_ASSERT(nullptr != pcrLeft);
	GPOS_ASSERT(nullptr != pexprRight);
	GPOS_ASSERT(IMDType::EcmptOther > cmp_type);

	gpos::owner<CExpression *> pexprLeft = PexprScalarIdent(mp, pcrLeft);

	return PexprScalarCmp(mp, std::move(pexprLeft), std::move(pexprRight),
						  cmp_type);
}

// Generate a comparison expression between an expression and a column
gpos::owner<CExpression *>
CUtils::PexprScalarCmp(CMemoryPool *mp, gpos::owner<CExpression *> pexprLeft,
					   const CColRef *pcrRight, IMDType::ECmpType cmp_type)
{
	GPOS_ASSERT(nullptr != pexprLeft);
	GPOS_ASSERT(nullptr != pcrRight);
	GPOS_ASSERT(IMDType::EcmptOther > cmp_type);

	gpos::owner<CExpression *> pexprRight = PexprScalarIdent(mp, pcrRight);

	return PexprScalarCmp(mp, std::move(pexprLeft), std::move(pexprRight),
						  cmp_type);
}

// Generate a comparison expression over an expression and a column
gpos::owner<CExpression *>
CUtils::PexprScalarCmp(CMemoryPool *mp, gpos::owner<CExpression *> pexprLeft,
					   const CColRef *pcrRight, CWStringConst strOp,
					   gpos::owner<IMDId *> mdid_op)
{
	GPOS_ASSERT(nullptr != pexprLeft);
	GPOS_ASSERT(nullptr != pcrRight);

	IMDType::ECmpType cmp_type = ParseCmpType(mdid_op);
	if (IMDType::EcmptOther != cmp_type)
	{
		gpos::pointer<IMDId *> left_mdid =
			gpos::dyn_cast<CScalar>(pexprLeft->Pop())->MdidType();
		gpos::pointer<IMDId *> right_mdid = pcrRight->RetrieveType()->MDId();

		if (CMDAccessorUtils::FCmpOrCastedCmpExists(left_mdid, right_mdid,
													cmp_type))
		{
			gpos::owner<CExpression *> pexprScCmp =
				PexprScalarCmp(mp, std::move(pexprLeft), pcrRight, cmp_type);
			mdid_op->Release();

			return pexprScCmp;
		}
	}

	return GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CScalarCmp(
			mp, std::move(mdid_op),
			GPOS_NEW(mp) CWStringConst(mp, strOp.GetBuffer()), cmp_type),
		std::move(pexprLeft), PexprScalarIdent(mp, pcrRight));
}

// Generate a comparison expression over two expressions
gpos::owner<CExpression *>
CUtils::PexprScalarCmp(CMemoryPool *mp, gpos::owner<CExpression *> pexprLeft,
					   gpos::owner<CExpression *> pexprRight,
					   CWStringConst strOp, gpos::owner<IMDId *> mdid_op)
{
	GPOS_ASSERT(nullptr != pexprLeft);
	GPOS_ASSERT(nullptr != pexprRight);

	IMDType::ECmpType cmp_type = ParseCmpType(mdid_op);
	if (IMDType::EcmptOther != cmp_type)
	{
		gpos::pointer<IMDId *> left_mdid =
			gpos::dyn_cast<CScalar>(pexprLeft->Pop())->MdidType();
		gpos::pointer<IMDId *> right_mdid =
			gpos::dyn_cast<CScalar>(pexprRight->Pop())->MdidType();

		if (CMDAccessorUtils::FCmpOrCastedCmpExists(left_mdid, right_mdid,
													cmp_type))
		{
			gpos::owner<CExpression *> pexprScCmp = PexprScalarCmp(
				mp, std::move(pexprLeft), std::move(pexprRight), cmp_type);
			mdid_op->Release();

			return pexprScCmp;
		}
	}

	return GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CScalarCmp(
			mp, std::move(mdid_op),
			GPOS_NEW(mp) CWStringConst(mp, strOp.GetBuffer()), cmp_type),
		std::move(pexprLeft), std::move(pexprRight));
}

// Generate a comparison expression over two expressions
gpos::owner<CExpression *>
CUtils::PexprScalarCmp(CMemoryPool *mp, CExpression *pexprLeft,
					   CExpression *pexprRight, IMDId *mdid_scop)
{
	GPOS_ASSERT(nullptr != pexprLeft);
	GPOS_ASSERT(nullptr != pexprRight);
	GPOS_ASSERT(nullptr != mdid_scop);

	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	gpos::owner<CExpression *> pexprNewLeft = pexprLeft;
	gpos::owner<CExpression *> pexprNewRight = pexprRight;

	GPOS_ASSERT(pexprNewLeft != nullptr);
	GPOS_ASSERT(pexprNewRight != nullptr);

	CMDAccessorUtils::ApplyCastsForScCmp(mp, md_accessor, pexprNewLeft,
										 pexprNewRight, mdid_scop);

	mdid_scop->AddRef();
	gpos::pointer<const IMDScalarOp *> op =
		md_accessor->RetrieveScOp(mdid_scop);
	const CMDName mdname = op->Mdname();
	CWStringConst strCmpOpName(mdname.GetMDName()->GetBuffer());

	gpos::owner<CExpression *> pexprResult = GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp)
			CScalarCmp(mp, mdid_scop,
					   GPOS_NEW(mp) CWStringConst(mp, strCmpOpName.GetBuffer()),
					   op->ParseCmpType()),
		std::move(pexprNewLeft), std::move(pexprNewRight));

	return pexprResult;
}

// Generate a comparison expression over two expressions
gpos::owner<CExpression *>
CUtils::PexprScalarCmp(CMemoryPool *mp, gpos::owner<CExpression *> pexprLeft,
					   gpos::owner<CExpression *> pexprRight,
					   IMDType::ECmpType cmp_type)
{
	GPOS_ASSERT(nullptr != pexprLeft);
	GPOS_ASSERT(nullptr != pexprRight);
	GPOS_ASSERT(IMDType::EcmptOther > cmp_type);

	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	gpos::owner<CExpression *> pexprNewLeft = pexprLeft;
	gpos::owner<CExpression *> pexprNewRight = pexprRight;

	IMDId *op_mdid = CMDAccessorUtils::GetScCmpMdIdConsiderCasts(
		md_accessor, pexprNewLeft, pexprNewRight, cmp_type);
	CMDAccessorUtils::ApplyCastsForScCmp(mp, md_accessor, pexprNewLeft,
										 pexprNewRight, op_mdid);

	GPOS_ASSERT(pexprNewLeft != nullptr);
	GPOS_ASSERT(pexprNewRight != nullptr);

	op_mdid->AddRef();
	gpos::pointer<const IMDScalarOp *> op = md_accessor->RetrieveScOp(op_mdid);
	const CMDName mdname = op->Mdname();
	CWStringConst strCmpOpName(mdname.GetMDName()->GetBuffer());

	gpos::owner<CExpression *> pexprResult = GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CScalarCmp(
			mp, op_mdid,
			GPOS_NEW(mp) CWStringConst(mp, strCmpOpName.GetBuffer()), cmp_type),
		std::move(pexprNewLeft), std::move(pexprNewRight));

	return pexprResult;
}

// Generate an equality comparison expression over two columns
gpos::owner<CExpression *>
CUtils::PexprScalarEqCmp(CMemoryPool *mp, const CColRef *pcrLeft,
						 const CColRef *pcrRight)
{
	GPOS_ASSERT(nullptr != pcrLeft);
	GPOS_ASSERT(nullptr != pcrRight);

	return PexprScalarCmp(mp, pcrLeft, pcrRight, IMDType::EcmptEq);
}

// Generate an equality comparison expression over two expressions
gpos::owner<CExpression *>
CUtils::PexprScalarEqCmp(CMemoryPool *mp, gpos::owner<CExpression *> pexprLeft,
						 gpos::owner<CExpression *> pexprRight)
{
	GPOS_ASSERT(nullptr != pexprLeft);
	GPOS_ASSERT(nullptr != pexprRight);

	return PexprScalarCmp(mp, std::move(pexprLeft), std::move(pexprRight),
						  IMDType::EcmptEq);
}

// Generate an equality comparison expression over a column reference and an expression
gpos::owner<CExpression *>
CUtils::PexprScalarEqCmp(CMemoryPool *mp, const CColRef *pcrLeft,
						 gpos::owner<CExpression *> pexprRight)
{
	GPOS_ASSERT(nullptr != pcrLeft);
	GPOS_ASSERT(nullptr != pexprRight);

	return PexprScalarCmp(mp, pcrLeft, std::move(pexprRight), IMDType::EcmptEq);
}

// Generate an equality comparison expression over an expression and a column reference
gpos::owner<CExpression *>
CUtils::PexprScalarEqCmp(CMemoryPool *mp, gpos::owner<CExpression *> pexprLeft,
						 const CColRef *pcrRight)
{
	GPOS_ASSERT(nullptr != pexprLeft);
	GPOS_ASSERT(nullptr != pcrRight);

	return PexprScalarCmp(mp, std::move(pexprLeft), pcrRight, IMDType::EcmptEq);
}

// returns number of children or constants of it is all constants
ULONG
CUtils::UlScalarArrayArity(gpos::pointer<CExpression *> pexprArray)
{
	GPOS_ASSERT(FScalarArray(pexprArray));

	ULONG arity = pexprArray->Arity();
	if (0 == arity)
	{
		gpos::pointer<CScalarArray *> popScalarArray =
			gpos::dyn_cast<CScalarArray>(pexprArray->Pop());
		gpos::pointer<CScalarConstArray *> pdrgPconst =
			popScalarArray->PdrgPconst();
		arity = pdrgPconst->Size();
	}
	return arity;
}

// returns constant operator of a scalar array expression
CScalarConst *
CUtils::PScalarArrayConstChildAt(gpos::pointer<CExpression *> pexprArray,
								 ULONG ul)
{
	// if the CScalarArray is already collapsed and the consts are stored in the
	// operator itself, we return the constant from the const array.
	if (FScalarArrayCollapsed(pexprArray))
	{
		gpos::pointer<CScalarArray *> popScalarArray =
			gpos::dyn_cast<CScalarArray>(pexprArray->Pop());
		gpos::pointer<CScalarConstArray *> pdrgPconst =
			popScalarArray->PdrgPconst();
		CScalarConst *pScalarConst = (*pdrgPconst)[ul];
		return pScalarConst;
	}
	// otherwise, we convert the child expression's operator if that's possible.
	else
	{
		return gpos::dyn_cast<CScalarConst>((*pexprArray)[ul]->Pop());
	}
}

// returns constant expression of a scalar array expression
gpos::owner<CExpression *>
CUtils::PScalarArrayExprChildAt(CMemoryPool *mp,
								gpos::pointer<CExpression *> pexprArray,
								ULONG ul)
{
	ULONG arity = pexprArray->Arity();
	if (0 == arity)
	{
		gpos::pointer<CScalarArray *> popScalarArray =
			gpos::dyn_cast<CScalarArray>(pexprArray->Pop());
		gpos::pointer<CScalarConstArray *> pdrgPconst =
			popScalarArray->PdrgPconst();
		gpos::owner<CScalarConst *> pScalarConst = (*pdrgPconst)[ul];
		pScalarConst->AddRef();
		return GPOS_NEW(mp) CExpression(mp, std::move(pScalarConst));
	}
	else
	{
		gpos::owner<CExpression *> pexprConst = (*pexprArray)[ul];
		pexprConst->AddRef();
		return pexprConst;
	}
}

// returns the scalar array expression child of CScalarArrayComp
CExpression *
CUtils::PexprScalarArrayChild(gpos::pointer<CExpression *> pexprScalarArrayCmp)
{
	CExpression *pexprArray = (*pexprScalarArrayCmp)[1];
	if (FScalarArrayCoerce(pexprArray))
	{
		pexprArray = (*pexprArray)[0];
	}
	return pexprArray;
}

// returns if the scalar array has all constant elements or ScalarIdents
BOOL
CUtils::FScalarConstAndScalarIdentArray(gpos::pointer<CExpression *> pexprArray)
{
	for (ULONG i = 0; i < pexprArray->Arity(); ++i)
	{
		gpos::pointer<CExpression *> pexprChild = (*pexprArray)[i];

		if (!FScalarIdent(pexprChild) && !FScalarConst(pexprChild) &&
			!(CCastUtils::FScalarCast(pexprChild) &&
			  FScalarIdent((*pexprChild)[0])))
		{
			return false;
		}
	}

	return true;
}

// returns if the scalar array has all constant elements or children
BOOL
CUtils::FScalarConstArray(gpos::pointer<CExpression *> pexprArray)
{
	const ULONG arity = pexprArray->Arity();

	BOOL fAllConsts = FScalarArray(pexprArray);
	for (ULONG ul = 0; fAllConsts && ul < arity; ul++)
	{
		fAllConsts = CUtils::FScalarConst((*pexprArray)[ul]);
	}

	return fAllConsts;
}

// returns if the scalar constant array has already been collapased
BOOL
CUtils::FScalarArrayCollapsed(gpos::pointer<CExpression *> pexprArray)
{
	const ULONG ulExprArity = pexprArray->Arity();
	const ULONG ulConstArity = UlScalarArrayArity(pexprArray);

	return ulExprArity == 0 && ulConstArity > 0;
}

// If it's a scalar array of all CScalarConst, collapse it into a single
// expression but keep the constants in the operator.
gpos::owner<CExpression *>
CUtils::PexprCollapseConstArray(CMemoryPool *mp,
								gpos::pointer<CExpression *> pexprArray)
{
	GPOS_ASSERT(nullptr != pexprArray);

	const ULONG arity = pexprArray->Arity();

	// do not collapse already collapsed array, otherwise we lose the
	// collapsed constants.
	if (FScalarConstArray(pexprArray) && !FScalarArrayCollapsed(pexprArray))
	{
		gpos::owner<CScalarConstArray *> pdrgPconst =
			GPOS_NEW(mp) CScalarConstArray(mp);
		for (ULONG ul = 0; ul < arity; ul++)
		{
			gpos::owner<CScalarConst *> popConst =
				gpos::dyn_cast<CScalarConst>((*pexprArray)[ul]->Pop());
			popConst->AddRef();
			pdrgPconst->Append(popConst);
		}

		gpos::pointer<CScalarArray *> psArray =
			gpos::dyn_cast<CScalarArray>(pexprArray->Pop());
		IMDId *elem_type_mdid = psArray->PmdidElem();
		IMDId *array_type_mdid = psArray->PmdidArray();
		elem_type_mdid->AddRef();
		array_type_mdid->AddRef();

		gpos::owner<CScalarArray *> pConstArray = GPOS_NEW(mp)
			CScalarArray(mp, elem_type_mdid, array_type_mdid,
						 psArray->FMultiDimensional(), std::move(pdrgPconst));
		return GPOS_NEW(mp) CExpression(mp, std::move(pConstArray));
	}

	// process children
	gpos::owner<CExpressionArray *> pdrgpexpr =
		GPOS_NEW(mp) CExpressionArray(mp);

	for (ULONG ul = 0; ul < arity; ul++)
	{
		gpos::owner<CExpression *> pexprChild =
			PexprCollapseConstArray(mp, (*pexprArray)[ul]);
		pdrgpexpr->Append(pexprChild);
	}

	gpos::owner<COperator *> pop = pexprArray->Pop();
	pop->AddRef();
	return GPOS_NEW(mp) CExpression(mp, std::move(pop), std::move(pdrgpexpr));
}

// generate an ArrayCmp expression given an array of CScalarConst's
gpos::owner<CExpression *>
CUtils::PexprScalarArrayCmp(CMemoryPool *mp,
							CScalarArrayCmp::EArrCmpType earrcmptype,
							IMDType::ECmpType ecmptype,
							gpos::owner<CExpressionArray *> pexprScalarChildren,
							const CColRef *colref)
{
	GPOS_ASSERT(pexprScalarChildren != nullptr);
	GPOS_ASSERT(0 < pexprScalarChildren->Size());

	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	// get column type mdid and mdid of the array type corresponding to that type
	IMDId *pmdidColType = colref->RetrieveType()->MDId();
	IMDId *pmdidArrType = colref->RetrieveType()->GetArrayTypeMdid();
	IMDId *pmdidCmpOp = colref->RetrieveType()->GetMdidForCmpType(ecmptype);

	if (!IMDId::IsValid(pmdidColType) || !IMDId::IsValid(pmdidArrType) ||
		!IMDId::IsValid(pmdidCmpOp))
	{
		// cannot construct an ArrayCmp expression if any of these are invalid
		return nullptr;
	}

	pmdidColType->AddRef();
	pmdidArrType->AddRef();
	pmdidCmpOp->AddRef();

	const CMDName mdname = md_accessor->RetrieveScOp(pmdidCmpOp)->Mdname();
	CWStringConst strOp(mdname.GetMDName()->GetBuffer());

	gpos::owner<CExpression *> pexprArray = GPOS_NEW(mp)
		CExpression(mp,
					GPOS_NEW(mp) CScalarArray(mp, pmdidColType, pmdidArrType,
											  false /*is_multidimenstional*/),
					std::move(pexprScalarChildren));

	return GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CScalarArrayCmp(
			mp, pmdidCmpOp, GPOS_NEW(mp) CWStringConst(mp, strOp.GetBuffer()),
			earrcmptype),
		CUtils::PexprScalarIdent(mp, colref), std::move(pexprArray));
}

// generate a comparison against Zero
gpos::owner<CExpression *>
CUtils::PexprCmpWithZero(CMemoryPool *mp, gpos::owner<CExpression *> pexprLeft,
						 gpos::pointer<IMDId *> mdid_type_left,
						 IMDType::ECmpType ecmptype)
{
	GPOS_ASSERT(pexprLeft->Pop()->FScalar());

	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	gpos::pointer<const IMDType *> pmdtype =
		md_accessor->RetrieveType(mdid_type_left);
	GPOS_ASSERT(pmdtype->GetDatumType() == IMDType::EtiInt8 &&
				"left expression must be of type int8");

	gpos::owner<IMDId *> mdid_op = pmdtype->GetMdidForCmpType(ecmptype);
	mdid_op->AddRef();
	const CMDName mdname = md_accessor->RetrieveScOp(mdid_op)->Mdname();
	CWStringConst strOpName(mdname.GetMDName()->GetBuffer());

	return GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CScalarCmp(
			mp, std::move(mdid_op),
			GPOS_NEW(mp) CWStringConst(mp, strOpName.GetBuffer()), ecmptype),
		std::move(pexprLeft), CUtils::PexprScalarConstInt8(mp, 0 /*val*/));
}

// generate an Is Distinct From expression
gpos::owner<CExpression *>
CUtils::PexprIDF(CMemoryPool *mp, gpos::owner<CExpression *> pexprLeft,
				 gpos::owner<CExpression *> pexprRight)
{
	GPOS_ASSERT(nullptr != pexprLeft);
	GPOS_ASSERT(nullptr != pexprRight);

	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	gpos::owner<CExpression *> pexprNewLeft = pexprLeft;
	gpos::owner<CExpression *> pexprNewRight = pexprRight;

	IMDId *pmdidEqOp = CMDAccessorUtils::GetScCmpMdIdConsiderCasts(
		md_accessor, pexprNewLeft, pexprNewRight, IMDType::EcmptEq);
	CMDAccessorUtils::ApplyCastsForScCmp(mp, md_accessor, pexprNewLeft,
										 pexprNewRight, pmdidEqOp);
	pmdidEqOp->AddRef();
	const CMDName mdname = md_accessor->RetrieveScOp(pmdidEqOp)->Mdname();
	CWStringConst strEqOpName(mdname.GetMDName()->GetBuffer());

	return GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CScalarIsDistinctFrom(
			mp, pmdidEqOp,
			GPOS_NEW(mp) CWStringConst(mp, strEqOpName.GetBuffer())),
		std::move(pexprNewLeft), std::move(pexprNewRight));
}

// generate an Is Distinct From expression
gpos::owner<CExpression *>
CUtils::PexprIDF(CMemoryPool *mp, CExpression *pexprLeft,
				 CExpression *pexprRight, IMDId *mdid_scop)
{
	GPOS_ASSERT(nullptr != pexprLeft);
	GPOS_ASSERT(nullptr != pexprRight);

	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	gpos::owner<CExpression *> pexprNewLeft = pexprLeft;
	gpos::owner<CExpression *> pexprNewRight = pexprRight;

	CMDAccessorUtils::ApplyCastsForScCmp(mp, md_accessor, pexprNewLeft,
										 pexprNewRight, mdid_scop);
	mdid_scop->AddRef();
	const CMDName mdname = md_accessor->RetrieveScOp(mdid_scop)->Mdname();
	CWStringConst strEqOpName(mdname.GetMDName()->GetBuffer());

	return GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CScalarIsDistinctFrom(
			mp, mdid_scop,
			GPOS_NEW(mp) CWStringConst(mp, strEqOpName.GetBuffer())),
		std::move(pexprNewLeft), std::move(pexprNewRight));
}

// generate an Is NOT Distinct From expression for two columns; the two columns must have the same type
gpos::owner<CExpression *>
CUtils::PexprINDF(CMemoryPool *mp, const CColRef *pcrLeft,
				  const CColRef *pcrRight)
{
	GPOS_ASSERT(nullptr != pcrLeft);
	GPOS_ASSERT(nullptr != pcrRight);

	return PexprINDF(mp, PexprScalarIdent(mp, pcrLeft),
					 PexprScalarIdent(mp, pcrRight));
}

// Generate an Is NOT Distinct From expression for scalar expressions;
// the two expressions must have the same type
gpos::owner<CExpression *>
CUtils::PexprINDF(CMemoryPool *mp, gpos::owner<CExpression *> pexprLeft,
				  gpos::owner<CExpression *> pexprRight)
{
	GPOS_ASSERT(nullptr != pexprLeft);
	GPOS_ASSERT(nullptr != pexprRight);

	return PexprNegate(
		mp, PexprIDF(mp, std::move(pexprLeft), std::move(pexprRight)));
}

gpos::owner<CExpression *>
CUtils::PexprINDF(CMemoryPool *mp, CExpression *pexprLeft,
				  CExpression *pexprRight, IMDId *mdid_scop)
{
	GPOS_ASSERT(nullptr != pexprLeft);
	GPOS_ASSERT(nullptr != pexprRight);

	return PexprNegate(mp, PexprIDF(mp, pexprLeft, pexprRight, mdid_scop));
}

// Generate an Is Null expression
gpos::owner<CExpression *>
CUtils::PexprIsNull(CMemoryPool *mp, gpos::owner<CExpression *> pexpr)
{
	GPOS_ASSERT(nullptr != pexpr);

	return GPOS_NEW(mp)
		CExpression(mp, GPOS_NEW(mp) CScalarNullTest(mp), std::move(pexpr));
}

// Generate an Is Not Null expression
gpos::owner<CExpression *>
CUtils::PexprIsNotNull(CMemoryPool *mp, gpos::owner<CExpression *> pexpr)
{
	GPOS_ASSERT(nullptr != pexpr);

	return PexprNegate(mp, PexprIsNull(mp, std::move(pexpr)));
}

// Generate an Is Not False expression
gpos::owner<CExpression *>
CUtils::PexprIsNotFalse(CMemoryPool *mp, gpos::owner<CExpression *> pexpr)
{
	GPOS_ASSERT(nullptr != pexpr);

	return PexprIDF(mp, std::move(pexpr),
					PexprScalarConstBool(mp, false /*value*/));
}

// Find if a scalar expression uses a nullable column from the
// output of a logical expression
BOOL
CUtils::FUsesNullableCol(CMemoryPool *mp,
						 gpos::pointer<CExpression *> pexprScalar,
						 gpos::pointer<CExpression *> pexprLogical)
{
	GPOS_ASSERT(pexprScalar->Pop()->FScalar());
	GPOS_ASSERT(pexprLogical->Pop()->FLogical());

	gpos::pointer<CColRefSet *> pcrsNotNull =
		pexprLogical->DeriveNotNullColumns();
	gpos::owner<CColRefSet *> pcrsUsed = GPOS_NEW(mp) CColRefSet(mp);
	pcrsUsed->Include(pexprScalar->DeriveUsedColumns());
	pcrsUsed->Intersection(pexprLogical->DeriveOutputColumns());

	BOOL fUsesNullableCol = !pcrsNotNull->ContainsAll(pcrsUsed);
	pcrsUsed->Release();

	return fUsesNullableCol;
}

// Extract the partition key at the given level from the given array of partition keys
CColRef *
CUtils::PcrExtractPartKey(gpos::pointer<CColRef2dArray *> pdrgpdrgpcr,
						  ULONG ulLevel)
{
	GPOS_ASSERT(nullptr != pdrgpdrgpcr);
	GPOS_ASSERT(ulLevel < pdrgpdrgpcr->Size());

	gpos::pointer<CColRefArray *> pdrgpcrPartKey = (*pdrgpdrgpcr)[ulLevel];
	GPOS_ASSERT(1 == pdrgpcrPartKey->Size() &&
				"Composite keys not currently supported");

	return (*pdrgpcrPartKey)[0];
}

// check for existence of subqueries
BOOL
CUtils::FHasSubquery(gpos::pointer<CExpression *> pexpr)
{
	GPOS_ASSERT(nullptr != pexpr);
	GPOS_ASSERT(pexpr->Pop()->FScalar());

	return pexpr->DeriveHasSubquery();
}

// check for existence of CTE anchor
BOOL
CUtils::FHasCTEAnchor(gpos::pointer<CExpression *> pexpr)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(nullptr != pexpr);

	if (COperator::EopLogicalCTEAnchor == pexpr->Pop()->Eopid())
	{
		return true;
	}

	for (ULONG ul = 0; ul < pexpr->Arity(); ul++)
	{
		gpos::pointer<CExpression *> pexprChild = (*pexpr)[ul];
		if (FHasCTEAnchor(pexprChild))
		{
			return true;
		}
	}

	return false;
}

//---------------------------------------------------------------------------
//	@class:
//		CUtils::FHasSubqueryOrApply
//
//	@doc:
//		Check existence of subqueries or Apply operators in deep expression tree
//
//---------------------------------------------------------------------------
BOOL
CUtils::FHasSubqueryOrApply(gpos::pointer<CExpression *> pexpr, BOOL fCheckRoot)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(nullptr != pexpr);

	if (fCheckRoot)
	{
		gpos::pointer<COperator *> pop = pexpr->Pop();
		if (FApply(pop) && !FCorrelatedApply(pop))
		{
			return true;
		}

		if (pop->FScalar() && pexpr->DeriveHasSubquery())
		{
			return true;
		}
	}

	const ULONG arity = pexpr->Arity();
	BOOL fSubqueryOrApply = false;
	for (ULONG ul = 0; !fSubqueryOrApply && ul < arity; ul++)
	{
		fSubqueryOrApply = FHasSubqueryOrApply((*pexpr)[ul]);
	}

	return fSubqueryOrApply;
}

//---------------------------------------------------------------------------
//	@class:
//		CUtils::FHasCorrelatedApply
//
//	@doc:
//		Check existence of Correlated Apply operators in deep expression tree
//
//---------------------------------------------------------------------------
BOOL
CUtils::FHasCorrelatedApply(gpos::pointer<CExpression *> pexpr, BOOL fCheckRoot)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(nullptr != pexpr);

	if (fCheckRoot && FCorrelatedApply(pexpr->Pop()))
	{
		return true;
	}

	const ULONG arity = pexpr->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		if (FHasCorrelatedApply((*pexpr)[ul]))
		{
			return true;
		}
	}

	return false;
}

// check for existence of outer references
BOOL
CUtils::HasOuterRefs(gpos::pointer<CExpression *> pexpr)
{
	GPOS_ASSERT(nullptr != pexpr);
	GPOS_ASSERT(pexpr->Pop()->FLogical());

	return 0 < pexpr->DeriveOuterReferences()->Size();
}

// check if a given operator is a logical join
BOOL
CUtils::FLogicalJoin(gpos::pointer<COperator *> pop)
{
	GPOS_ASSERT(nullptr != pop);

	gpos::pointer<CLogicalJoin *> popJoin = nullptr;
	if (pop->FLogical())
	{
		// attempt casting to logical join,
		// dynamic cast returns NULL if operator is not join
		popJoin = dynamic_cast<CLogicalJoin *>(pop);
	}

	return (nullptr != popJoin);
}

// check if a given operator is a logical set operation
BOOL
CUtils::FLogicalSetOp(gpos::pointer<COperator *> pop)
{
	GPOS_ASSERT(nullptr != pop);

	gpos::pointer<CLogicalSetOp *> popSetOp = nullptr;
	if (pop->FLogical())
	{
		// attempt casting to logical SetOp,
		// dynamic cast returns NULL if operator is not SetOp
		popSetOp = dynamic_cast<CLogicalSetOp *>(pop);
	}

	return (nullptr != popSetOp);
}

// check if a given operator is a logical unary operator
BOOL
CUtils::FLogicalUnary(gpos::pointer<COperator *> pop)
{
	GPOS_ASSERT(nullptr != pop);

	gpos::pointer<CLogicalUnary *> popUnary = nullptr;
	if (pop->FLogical())
	{
		// attempt casting to logical unary,
		// dynamic cast returns NULL if operator is not unary
		popUnary = dynamic_cast<CLogicalUnary *>(pop);
	}

	return (nullptr != popUnary);
}

// check if a given operator is a hash join
BOOL
CUtils::FHashJoin(gpos::pointer<COperator *> pop)
{
	GPOS_ASSERT(nullptr != pop);

	gpos::pointer<CPhysicalHashJoin *> popHJN = nullptr;
	if (pop->FPhysical())
	{
		popHJN = dynamic_cast<CPhysicalHashJoin *>(pop);
	}

	return (nullptr != popHJN);
}

// check if a given operator is a correlated nested loops join
BOOL
CUtils::FCorrelatedNLJoin(gpos::pointer<COperator *> pop)
{
	GPOS_ASSERT(nullptr != pop);

	BOOL fCorrelatedNLJ = false;
	if (FNLJoin(pop))
	{
		fCorrelatedNLJ = dynamic_cast<CPhysicalNLJoin *>(pop)->FCorrelated();
	}

	return fCorrelatedNLJ;
}

// check if a given operator is a nested loops join
BOOL
CUtils::FNLJoin(gpos::pointer<COperator *> pop)
{
	GPOS_ASSERT(nullptr != pop);

	gpos::pointer<CPhysicalNLJoin *> popNLJ = nullptr;
	if (pop->FPhysical())
	{
		popNLJ = dynamic_cast<CPhysicalNLJoin *>(pop);
	}

	return (nullptr != popNLJ);
}

// check if a given operator is a logical join
BOOL
CUtils::FPhysicalJoin(gpos::pointer<COperator *> pop)
{
	GPOS_ASSERT(nullptr != pop);

	return FHashJoin(pop) || FNLJoin(pop);
}

// check if a given operator is a physical left outer join
BOOL
CUtils::FPhysicalLeftOuterJoin(gpos::pointer<COperator *> pop)
{
	GPOS_ASSERT(nullptr != pop);

	return COperator::EopPhysicalLeftOuterNLJoin == pop->Eopid() ||
		   COperator::EopPhysicalLeftOuterIndexNLJoin == pop->Eopid() ||
		   COperator::EopPhysicalLeftOuterHashJoin == pop->Eopid() ||
		   COperator::EopPhysicalCorrelatedLeftOuterNLJoin == pop->Eopid();
}

// check if a given operator is a physical agg
BOOL
CUtils::FPhysicalScan(gpos::pointer<COperator *> pop)
{
	GPOS_ASSERT(nullptr != pop);

	gpos::pointer<CPhysicalScan *> popScan = nullptr;
	if (pop->FPhysical())
	{
		// attempt casting to physical scan,
		// dynamic cast returns NULL if operator is not a scan
		popScan = dynamic_cast<CPhysicalScan *>(pop);
	}

	return (nullptr != popScan);
}

// check if a given operator is a physical agg
BOOL
CUtils::FPhysicalAgg(gpos::pointer<COperator *> pop)
{
	GPOS_ASSERT(nullptr != pop);

	gpos::pointer<CPhysicalAgg *> popAgg = nullptr;
	if (pop->FPhysical())
	{
		// attempt casting to physical agg,
		// dynamic cast returns NULL if operator is not an agg
		popAgg = dynamic_cast<CPhysicalAgg *>(pop);
	}

	return (nullptr != popAgg);
}

// check if a given operator is a physical motion
BOOL
CUtils::FPhysicalMotion(gpos::pointer<COperator *> pop)
{
	GPOS_ASSERT(nullptr != pop);

	gpos::pointer<CPhysicalMotion *> popMotion = nullptr;
	if (pop->FPhysical())
	{
		// attempt casting to physical motion,
		// dynamic cast returns NULL if operator is not a motion
		popMotion = dynamic_cast<CPhysicalMotion *>(pop);
	}

	return (nullptr != popMotion);
}

// check if a given operator is an FEnforcer
BOOL
CUtils::FEnforcer(gpos::pointer<COperator *> pop)
{
	GPOS_ASSERT(nullptr != pop);

	COperator::EOperatorId op_id = pop->Eopid();
	return COperator::EopPhysicalSort == op_id ||
		   COperator::EopPhysicalSpool == op_id ||
		   COperator::EopPhysicalPartitionSelector == op_id ||
		   FPhysicalMotion(pop);
}

// check if a given operator is an Apply
BOOL
CUtils::FApply(gpos::pointer<COperator *> pop)
{
	GPOS_ASSERT(nullptr != pop);

	gpos::pointer<CLogicalApply *> popApply = nullptr;
	if (pop->FLogical())
	{
		// attempt casting to logical apply,
		// dynamic cast returns NULL if operator is not an Apply operator
		popApply = dynamic_cast<CLogicalApply *>(pop);
	}

	return (nullptr != popApply);
}

// check if a given operator is a correlated Apply
BOOL
CUtils::FCorrelatedApply(gpos::pointer<COperator *> pop)
{
	GPOS_ASSERT(nullptr != pop);

	BOOL fCorrelatedApply = false;
	if (FApply(pop))
	{
		fCorrelatedApply = gpos::dyn_cast<CLogicalApply>(pop)->FCorrelated();
	}

	return fCorrelatedApply;
}

// check if a given operator is left semi apply
BOOL
CUtils::FLeftSemiApply(gpos::pointer<COperator *> pop)
{
	GPOS_ASSERT(nullptr != pop);

	BOOL fLeftSemiApply = false;
	if (FApply(pop))
	{
		fLeftSemiApply = gpos::dyn_cast<CLogicalApply>(pop)->FLeftSemiApply();
	}

	return fLeftSemiApply;
}

// check if a given operator is left anti semi apply
BOOL
CUtils::FLeftAntiSemiApply(gpos::pointer<COperator *> pop)
{
	GPOS_ASSERT(nullptr != pop);

	BOOL fLeftAntiSemiApply = false;
	if (FApply(pop))
	{
		fLeftAntiSemiApply =
			gpos::dyn_cast<CLogicalApply>(pop)->FLeftAntiSemiApply();
	}

	return fLeftAntiSemiApply;
}

// check if a given operator is a subquery
BOOL
CUtils::FSubquery(gpos::pointer<COperator *> pop)
{
	GPOS_ASSERT(nullptr != pop);

	COperator::EOperatorId op_id = pop->Eopid();
	return pop->FScalar() && (COperator::EopScalarSubquery == op_id ||
							  COperator::EopScalarSubqueryExists == op_id ||
							  COperator::EopScalarSubqueryNotExists == op_id ||
							  COperator::EopScalarSubqueryAll == op_id ||
							  COperator::EopScalarSubqueryAny == op_id);
}

// check if a given operator is existential subquery
BOOL
CUtils::FExistentialSubquery(gpos::pointer<COperator *> pop)
{
	GPOS_ASSERT(nullptr != pop);

	COperator::EOperatorId op_id = pop->Eopid();
	return pop->FScalar() && (COperator::EopScalarSubqueryExists == op_id ||
							  COperator::EopScalarSubqueryNotExists == op_id);
}

// check if a given operator is quantified subquery
BOOL
CUtils::FQuantifiedSubquery(gpos::pointer<COperator *> pop)
{
	GPOS_ASSERT(nullptr != pop);

	COperator::EOperatorId op_id = pop->Eopid();
	return pop->FScalar() && (COperator::EopScalarSubqueryAll == op_id ||
							  COperator::EopScalarSubqueryAny == op_id);
}

// check if given expression is a Project on ConstTable with one
// scalar subquery in Project List
BOOL
CUtils::FProjectConstTableWithOneScalarSubq(gpos::pointer<CExpression *> pexpr)
{
	GPOS_ASSERT(nullptr != pexpr);

	if (COperator::EopLogicalProject == pexpr->Pop()->Eopid() &&
		COperator::EopLogicalConstTableGet == (*pexpr)[0]->Pop()->Eopid())
	{
		gpos::pointer<CExpression *> pexprScalar = (*pexpr)[1];
		GPOS_ASSERT(COperator::EopScalarProjectList ==
					pexprScalar->Pop()->Eopid());

		if (1 == pexprScalar->Arity() &&
			FProjElemWithScalarSubq((*pexprScalar)[0]))
		{
			return true;
		}
	}

	return false;
}

// check if given expression is a Project Element with scalar subquery
BOOL
CUtils::FProjElemWithScalarSubq(gpos::pointer<CExpression *> pexpr)
{
	GPOS_ASSERT(nullptr != pexpr);

	return (COperator::EopScalarProjectElement == pexpr->Pop()->Eopid() &&
			COperator::EopScalarSubquery == (*pexpr)[0]->Pop()->Eopid());
}

// check if given expression is a scalar subquery with a ConstTableGet as the only child
BOOL
CUtils::FScalarSubqWithConstTblGet(gpos::pointer<CExpression *> pexpr)
{
	GPOS_ASSERT(nullptr != pexpr);

	if (COperator::EopScalarSubquery == pexpr->Pop()->Eopid() &&
		COperator::EopLogicalConstTableGet == (*pexpr)[0]->Pop()->Eopid() &&
		1 == pexpr->Arity())
	{
		return true;
	}

	return false;
}

// check if a limit expression has 0 offset
BOOL
CUtils::FHasZeroOffset(gpos::pointer<CExpression *> pexpr)
{
	GPOS_ASSERT(COperator::EopLogicalLimit == pexpr->Pop()->Eopid() ||
				COperator::EopPhysicalLimit == pexpr->Pop()->Eopid());

	return FScalarConstIntZero((*pexpr)[1]);
}

// check if an expression is a 0 integer
BOOL
CUtils::FScalarConstIntZero(gpos::pointer<CExpression *> pexprOffset)
{
	if (COperator::EopScalarConst != pexprOffset->Pop()->Eopid())
	{
		return false;
	}

	gpos::pointer<CScalarConst *> popScalarConst =
		gpos::dyn_cast<CScalarConst>(pexprOffset->Pop());
	gpos::pointer<IDatum *> datum = popScalarConst->GetDatum();

	switch (datum->GetDatumType())
	{
		case IMDType::EtiInt2:
			return (0 == dynamic_cast<IDatumInt2 *>(datum)->Value());
		case IMDType::EtiInt4:
			return (0 == dynamic_cast<IDatumInt4 *>(datum)->Value());
		case IMDType::EtiInt8:
			return (0 == dynamic_cast<IDatumInt8 *>(datum)->Value());
		default:
			return false;
	}
}

// deduplicate an array of expressions
gpos::owner<CExpressionArray *>
CUtils::PdrgpexprDedup(CMemoryPool *mp,
					   gpos::pointer<CExpressionArray *> pdrgpexpr)
{
	const ULONG size = pdrgpexpr->Size();
	gpos::owner<CExpressionArray *> pdrgpexprDedup =
		GPOS_NEW(mp) CExpressionArray(mp);
	gpos::owner<ExprHashSet *> phsexpr = GPOS_NEW(mp) ExprHashSet(mp);

	for (ULONG ul = 0; ul < size; ul++)
	{
		gpos::owner<CExpression *> pexpr = (*pdrgpexpr)[ul];
		pexpr->AddRef();
		if (phsexpr->Insert(pexpr))
		{
			pexpr->AddRef();
			pdrgpexprDedup->Append(pexpr);
		}
		else
		{
			pexpr->Release();
		}
	}

	phsexpr->Release();
	return pdrgpexprDedup;
}

// deep equality of expression arrays
BOOL
CUtils::Equals(gpos::pointer<const CExpressionArray *> pdrgpexprLeft,
			   gpos::pointer<const CExpressionArray *> pdrgpexprRight)
{
	GPOS_CHECK_STACK_SIZE;

	// NULL arrays are equal
	if (nullptr == pdrgpexprLeft || nullptr == pdrgpexprRight)
	{
		return nullptr == pdrgpexprLeft && nullptr == pdrgpexprRight;
	}

	// start with pointers comparison
	if (pdrgpexprLeft == pdrgpexprRight)
	{
		return true;
	}

	const ULONG length = pdrgpexprLeft->Size();
	BOOL fEqual = (length == pdrgpexprRight->Size());

	for (ULONG ul = 0; ul < length && fEqual; ul++)
	{
		gpos::pointer<const CExpression *> pexprLeft = (*pdrgpexprLeft)[ul];
		gpos::pointer<const CExpression *> pexprRight = (*pdrgpexprRight)[ul];
		fEqual = Equals(pexprLeft, pexprRight);
	}

	return fEqual;
}

// deep equality of expression trees
BOOL
CUtils::Equals(gpos::pointer<const CExpression *> pexprLeft,
			   gpos::pointer<const CExpression *> pexprRight)
{
	GPOS_CHECK_STACK_SIZE;

	// NULL expressions are equal
	if (nullptr == pexprLeft || nullptr == pexprRight)
	{
		return nullptr == pexprLeft && nullptr == pexprRight;
	}

	// start with pointers comparison
	if (pexprLeft == pexprRight)
	{
		return true;
	}

	// compare number of children and root operators
	if (pexprLeft->Arity() != pexprRight->Arity() ||
		!pexprLeft->Pop()->Matches(pexprRight->Pop()))
	{
		return false;
	}

	if (0 < pexprLeft->Arity() && pexprLeft->Pop()->FInputOrderSensitive())
	{
		return FMatchChildrenOrdered(pexprLeft, pexprRight);
	}

	return FMatchChildrenUnordered(pexprLeft, pexprRight);
}

// check if two expressions have the same children in any order
BOOL
CUtils::FMatchChildrenUnordered(gpos::pointer<const CExpression *> pexprLeft,
								gpos::pointer<const CExpression *> pexprRight)
{
	BOOL fEqual = true;
	const ULONG arity = pexprLeft->Arity();
	GPOS_ASSERT(pexprRight->Arity() == arity);

	for (ULONG ul = 0; fEqual && ul < arity; ul++)
	{
		gpos::pointer<CExpression *> pexpr = (*pexprLeft)[ul];
		fEqual = (UlOccurrences(pexpr, pexprLeft->PdrgPexpr()) ==
				  UlOccurrences(pexpr, pexprRight->PdrgPexpr()));
	}

	return fEqual;
}

// check if two expressions have the same children in the same order
BOOL
CUtils::FMatchChildrenOrdered(gpos::pointer<const CExpression *> pexprLeft,
							  gpos::pointer<const CExpression *> pexprRight)
{
	BOOL fEqual = true;
	const ULONG arity = pexprLeft->Arity();
	GPOS_ASSERT(pexprRight->Arity() == arity);

	for (ULONG ul = 0; fEqual && ul < arity; ul++)
	{
		// child must be at the same position in the other expression
		fEqual = Equals((*pexprLeft)[ul], (*pexprRight)[ul]);
	}

	return fEqual;
}

// return the number of occurrences of the given expression in the given array of expressions
ULONG
CUtils::UlOccurrences(gpos::pointer<const CExpression *> pexpr,
					  gpos::pointer<CExpressionArray *> pdrgpexpr)
{
	GPOS_ASSERT(nullptr != pexpr);
	ULONG count = 0;

	const ULONG size = pdrgpexpr->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		if (Equals(pexpr, (*pdrgpexpr)[ul]))
		{
			count++;
		}
	}

	return count;
}

// compare expression against an array of expressions
BOOL
CUtils::FEqualAny(gpos::pointer<const CExpression *> pexpr,
				  gpos::pointer<const CExpressionArray *> pdrgpexpr)
{
	GPOS_ASSERT(nullptr != pexpr);

	const ULONG size = pdrgpexpr->Size();
	BOOL fEqual = false;
	for (ULONG ul = 0; !fEqual && ul < size; ul++)
	{
		fEqual = Equals(pexpr, (*pdrgpexpr)[ul]);
	}

	return fEqual;
}

// check if first expression array contains all expressions in second array
BOOL
CUtils::Contains(gpos::pointer<const CExpressionArray *> pdrgpexprFst,
				 gpos::pointer<const CExpressionArray *> pdrgpexprSnd)
{
	GPOS_ASSERT(nullptr != pdrgpexprFst);
	GPOS_ASSERT(nullptr != pdrgpexprSnd);

	if (pdrgpexprFst == pdrgpexprSnd)
	{
		return true;
	}

	if (pdrgpexprFst->Size() < pdrgpexprSnd->Size())
	{
		return false;
	}

	const ULONG size = pdrgpexprSnd->Size();
	BOOL fContains = true;
	for (ULONG ul = 0; fContains && ul < size; ul++)
	{
		fContains = FEqualAny((*pdrgpexprSnd)[ul], pdrgpexprFst);
	}

	return fContains;
}

// generate a Not expression on top of the given expression
gpos::owner<CExpression *>
CUtils::PexprNegate(CMemoryPool *mp, gpos::owner<CExpression *> pexpr)
{
	gpos::owner<CExpressionArray *> pdrgpexpr =
		GPOS_NEW(mp) CExpressionArray(mp);
	pdrgpexpr->Append(std::move(pexpr));

	return PexprScalarBoolOp(mp, CScalarBoolOp::EboolopNot,
							 std::move(pdrgpexpr));
}

// generate a ScalarOp expression over a column and an expression
gpos::owner<CExpression *>
CUtils::PexprScalarOp(CMemoryPool *mp, const CColRef *pcrLeft,
					  gpos::owner<CExpression *> pexprRight,
					  const CWStringConst strOp, gpos::owner<IMDId *> mdid_op,
					  gpos::owner<IMDId *> return_type_mdid)
{
	GPOS_ASSERT(nullptr != pcrLeft);
	GPOS_ASSERT(nullptr != pexprRight);

	return GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp)
			CScalarOp(mp, std::move(mdid_op), std::move(return_type_mdid),
					  GPOS_NEW(mp) CWStringConst(mp, strOp.GetBuffer())),
		PexprScalarIdent(mp, pcrLeft), std::move(pexprRight));
}

// generate a ScalarBoolOp expression
gpos::owner<CExpression *>
CUtils::PexprScalarBoolOp(CMemoryPool *mp, CScalarBoolOp::EBoolOperator eboolop,
						  gpos::owner<CExpressionArray *> pdrgpexpr)
{
	GPOS_ASSERT(nullptr != pdrgpexpr);
	GPOS_ASSERT(0 < pdrgpexpr->Size());

	return GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarBoolOp(mp, eboolop),
									std::move(pdrgpexpr));
}

// generate a boolean scalar constant expression
gpos::owner<CExpression *>
CUtils::PexprScalarConstBool(CMemoryPool *mp, BOOL fval, BOOL is_null)
{
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	// create a BOOL datum
	gpos::pointer<const IMDTypeBool *> pmdtypebool =
		md_accessor->PtMDType<IMDTypeBool>();
	gpos::owner<IDatumBool *> datum =
		pmdtypebool->CreateBoolDatum(mp, fval, is_null);

	gpos::owner<CExpression *> pexpr = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CScalarConst(mp, (IDatum *) std::move(datum)));

	return pexpr;
}

// generate an int4 scalar constant expression
gpos::owner<CExpression *>
CUtils::PexprScalarConstInt4(CMemoryPool *mp, INT val)
{
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	// create a int4 datum
	gpos::pointer<const IMDTypeInt4 *> pmdtypeint4 =
		md_accessor->PtMDType<IMDTypeInt4>();
	gpos::owner<IDatumInt4 *> datum =
		pmdtypeint4->CreateInt4Datum(mp, val, false);

	gpos::owner<CExpression *> pexpr = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CScalarConst(mp, (IDatum *) std::move(datum)));

	return pexpr;
}

// generate an int8 scalar constant expression
gpos::owner<CExpression *>
CUtils::PexprScalarConstInt8(CMemoryPool *mp, LINT val, BOOL is_null)
{
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	// create a int8 datum
	gpos::pointer<const IMDTypeInt8 *> pmdtypeint8 =
		md_accessor->PtMDType<IMDTypeInt8>();
	gpos::owner<IDatumInt8 *> datum =
		pmdtypeint8->CreateInt8Datum(mp, val, is_null);

	gpos::owner<CExpression *> pexpr = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CScalarConst(mp, (IDatum *) std::move(datum)));

	return pexpr;
}

// generate an oid scalar constant expression
gpos::owner<CExpression *>
CUtils::PexprScalarConstOid(CMemoryPool *mp, OID oid_val)
{
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	// create a oid datum
	gpos::pointer<const IMDTypeOid *> pmdtype =
		md_accessor->PtMDType<IMDTypeOid>();
	gpos::owner<IDatumOid *> datum =
		pmdtype->CreateOidDatum(mp, oid_val, false /*is_null*/);

	gpos::owner<CExpression *> pexpr = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CScalarConst(mp, (IDatum *) std::move(datum)));

	return pexpr;
}

// generate a NULL constant of a given type
gpos::owner<CExpression *>
CUtils::PexprScalarConstNull(CMemoryPool *mp,
							 gpos::pointer<const IMDType *> typ,
							 INT type_modifier)
{
	gpos::owner<IDatum *> datum = nullptr;
	gpos::owner<IMDId *> mdid = typ->MDId();
	mdid->AddRef();

	switch (typ->GetDatumType())
	{
		case IMDType::EtiInt2:
		{
			gpos::pointer<const IMDTypeInt2 *> pmdtypeint2 =
				static_cast<const IMDTypeInt2 *>(typ);
			datum = pmdtypeint2->CreateInt2Datum(mp, 0, true);
		}
		break;

		case IMDType::EtiInt4:
		{
			gpos::pointer<const IMDTypeInt4 *> pmdtypeint4 =
				static_cast<const IMDTypeInt4 *>(typ);
			datum = pmdtypeint4->CreateInt4Datum(mp, 0, true);
		}
		break;

		case IMDType::EtiInt8:
		{
			gpos::pointer<const IMDTypeInt8 *> pmdtypeint8 =
				static_cast<const IMDTypeInt8 *>(typ);
			datum = pmdtypeint8->CreateInt8Datum(mp, 0, true);
		}
		break;

		case IMDType::EtiBool:
		{
			gpos::pointer<const IMDTypeBool *> pmdtypebool =
				static_cast<const IMDTypeBool *>(typ);
			datum = pmdtypebool->CreateBoolDatum(mp, false, true);
		}
		break;

		case IMDType::EtiOid:
		{
			gpos::pointer<const IMDTypeOid *> pmdtypeoid =
				static_cast<const IMDTypeOid *>(typ);
			datum = pmdtypeoid->CreateOidDatum(mp, 0, true);
		}
		break;

		case IMDType::EtiGeneric:
		{
			gpos::pointer<const IMDTypeGeneric *> pmdtypegeneric =
				static_cast<const IMDTypeGeneric *>(typ);
			datum = pmdtypegeneric->CreateGenericNullDatum(mp, type_modifier);
		}
		break;

		default:
			// shouldn't come here
			GPOS_RTL_ASSERT(!"Invalid operator type");
	}

	return GPOS_NEW(mp)
		CExpression(mp, GPOS_NEW(mp) CScalarConst(mp, std::move(datum)));
}

// get column reference defined by project element
CColRef *
CUtils::PcrFromProjElem(gpos::pointer<CExpression *> pexprPrEl)
{
	return (gpos::dyn_cast<CScalarProjectElement>(pexprPrEl->Pop()))->Pcr();
}

// generate an aggregate function operator
gpos::owner<CScalarAggFunc *>
CUtils::PopAggFunc(
	CMemoryPool *mp, gpos::owner<IMDId *> pmdidAggFunc,
	const CWStringConst *pstrAggFunc, BOOL is_distinct,
	EAggfuncStage eaggfuncstage, BOOL fSplit,
	gpos::owner<IMDId *>
		pmdidResolvedReturnType	 // return type to be used if original return type is ambiguous
)
{
	GPOS_ASSERT(nullptr != pmdidAggFunc);
	GPOS_ASSERT(nullptr != pstrAggFunc);
	GPOS_ASSERT_IMP(nullptr != pmdidResolvedReturnType,
					pmdidResolvedReturnType->IsValid());

	return GPOS_NEW(mp) CScalarAggFunc(
		mp, std::move(pmdidAggFunc), std::move(pmdidResolvedReturnType),
		pstrAggFunc, is_distinct, eaggfuncstage, fSplit);
}

// generate an aggregate function
gpos::owner<CExpression *>
CUtils::PexprAggFunc(CMemoryPool *mp, gpos::owner<IMDId *> pmdidAggFunc,
					 const CWStringConst *pstrAggFunc, const CColRef *colref,
					 BOOL is_distinct, EAggfuncStage eaggfuncstage, BOOL fSplit)
{
	GPOS_ASSERT(nullptr != pstrAggFunc);
	GPOS_ASSERT(nullptr != colref);

	// generate aggregate function
	gpos::owner<CScalarAggFunc *> popScAggFunc =
		PopAggFunc(mp, std::move(pmdidAggFunc), pstrAggFunc, is_distinct,
				   eaggfuncstage, fSplit);

	// generate function arguments
	gpos::owner<CExpression *> pexprScalarIdent = PexprScalarIdent(mp, colref);
	gpos::owner<CExpressionArray *> pdrgpexpr =
		GPOS_NEW(mp) CExpressionArray(mp);
	pdrgpexpr->Append(std::move(pexprScalarIdent));

	return GPOS_NEW(mp)
		CExpression(mp, std::move(popScAggFunc), std::move(pdrgpexpr));
}


// generate a count(*) expression
gpos::owner<CExpression *>
CUtils::PexprCountStar(CMemoryPool *mp)
{
	// TODO,  04/26/2012, create count(*) expressions in a system-independent
	// way using MDAccessor

	gpos::owner<CExpressionArray *> pdrgpexpr =
		GPOS_NEW(mp) CExpressionArray(mp);
	gpos::owner<CMDIdGPDB *> mdid = GPOS_NEW(mp) CMDIdGPDB(GPDB_COUNT_STAR);
	CWStringConst *str = GPOS_NEW(mp) CWStringConst(GPOS_WSZ_LIT("count"));

	gpos::owner<CScalarAggFunc *> popScAggFunc =
		PopAggFunc(mp, std::move(mdid), str, false /*is_distinct*/,
				   EaggfuncstageGlobal /*eaggfuncstage*/, false /*fSplit*/);

	gpos::owner<CExpression *> pexprCountStar = GPOS_NEW(mp)
		CExpression(mp, std::move(popScAggFunc), std::move(pdrgpexpr));

	return pexprCountStar;
}

// generate a GbAgg with count(*) function over the given expression
gpos::owner<CExpression *>
CUtils::PexprCountStar(CMemoryPool *mp, gpos::owner<CExpression *> pexprLogical)
{
	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	// generate a count(*) expression
	gpos::owner<CExpression *> pexprCountStar = PexprCountStar(mp);

	// generate a computed column with count(*) type
	gpos::pointer<CScalarAggFunc *> popScalarAggFunc =
		gpos::dyn_cast<CScalarAggFunc>(pexprCountStar->Pop());
	gpos::pointer<IMDId *> mdid_type = popScalarAggFunc->MdidType();
	INT type_modifier = popScalarAggFunc->TypeModifier();
	gpos::pointer<const IMDType *> pmdtype =
		md_accessor->RetrieveType(mdid_type);
	CColRef *pcrComputed = col_factory->PcrCreate(pmdtype, type_modifier);
	gpos::owner<CExpression *> pexprPrjElem =
		PexprScalarProjectElement(mp, pcrComputed, std::move(pexprCountStar));
	gpos::owner<CExpression *> pexprPrjList = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CScalarProjectList(mp), std::move(pexprPrjElem));

	// generate a Gb expression
	gpos::owner<CColRefArray *> colref_array = GPOS_NEW(mp) CColRefArray(mp);
	return PexprLogicalGbAggGlobal(mp, std::move(colref_array),
								   std::move(pexprLogical),
								   std::move(pexprPrjList));
}

// generate a GbAgg with count(*) and sum(col) over the given expression
gpos::owner<CExpression *>
CUtils::PexprCountStarAndSum(CMemoryPool *mp, const CColRef *colref,
							 gpos::owner<CExpression *> pexprLogical)
{
	GPOS_ASSERT(pexprLogical->DeriveOutputColumns()->FMember(colref));

	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	// generate a count(*) expression
	gpos::owner<CExpression *> pexprCountStar = PexprCountStar(mp);

	// generate a computed column with count(*) type
	gpos::pointer<CScalarAggFunc *> popScalarAggFunc =
		gpos::dyn_cast<CScalarAggFunc>(pexprCountStar->Pop());
	gpos::pointer<IMDId *> mdid_type = popScalarAggFunc->MdidType();
	INT type_modifier = popScalarAggFunc->TypeModifier();
	gpos::pointer<const IMDType *> pmdtype =
		md_accessor->RetrieveType(mdid_type);
	CColRef *pcrComputed = col_factory->PcrCreate(pmdtype, type_modifier);
	gpos::owner<CExpression *> pexprPrjElemCount =
		PexprScalarProjectElement(mp, pcrComputed, std::move(pexprCountStar));

	// generate sum(col) expression
	gpos::owner<CExpression *> pexprSum = PexprSum(mp, colref);
	gpos::pointer<CScalarAggFunc *> popScalarSumFunc =
		gpos::dyn_cast<CScalarAggFunc>(pexprSum->Pop());
	gpos::pointer<const IMDType *> pmdtypeSum =
		md_accessor->RetrieveType(popScalarSumFunc->MdidType());
	CColRef *pcrSum =
		col_factory->PcrCreate(pmdtypeSum, popScalarSumFunc->TypeModifier());
	gpos::owner<CExpression *> pexprPrjElemSum =
		PexprScalarProjectElement(mp, pcrSum, std::move(pexprSum));
	gpos::owner<CExpression *> pexprPrjList = GPOS_NEW(mp)
		CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp),
					std::move(pexprPrjElemCount), std::move(pexprPrjElemSum));

	// generate a Gb expression
	gpos::owner<CColRefArray *> colref_array = GPOS_NEW(mp) CColRefArray(mp);
	return PexprLogicalGbAggGlobal(mp, std::move(colref_array),
								   std::move(pexprLogical),
								   std::move(pexprPrjList));
}

// return True if passed expression is a Project Element defined on count(*)/count(Any) agg
BOOL
CUtils::FCountAggProjElem(
	gpos::pointer<CExpression *> pexprPrjElem,
	CColRef **ppcrCount	 // output: count(*)/count(Any) column
)
{
	GPOS_ASSERT(nullptr != pexprPrjElem);
	GPOS_ASSERT(nullptr != ppcrCount);

	gpos::pointer<COperator *> pop = pexprPrjElem->Pop();
	if (COperator::EopScalarProjectElement != pop->Eopid())
	{
		return false;
	}

	if (COperator::EopScalarAggFunc == (*pexprPrjElem)[0]->Pop()->Eopid())
	{
		gpos::pointer<CScalarAggFunc *> popAggFunc =
			gpos::dyn_cast<CScalarAggFunc>((*pexprPrjElem)[0]->Pop());
		if (popAggFunc->FCountStar() || popAggFunc->FCountAny())
		{
			*ppcrCount = gpos::dyn_cast<CScalarProjectElement>(pop)->Pcr();
			return true;
		}
	}

	return FHasCountAgg((*pexprPrjElem)[0], ppcrCount);
}

// check if the given expression has a count(*)/count(Any) agg, return the top-most found count column
BOOL
CUtils::FHasCountAgg(gpos::pointer<CExpression *> pexpr,
					 CColRef **ppcrCount  // output: count(*)/count(Any) column
)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(nullptr != ppcrCount);

	if (COperator::EopScalarProjectElement == pexpr->Pop()->Eopid())
	{
		// base case, count(*)/count(Any) must appear below a project element
		return FCountAggProjElem(pexpr, ppcrCount);
	}

	// recursively process children
	BOOL fHasCountAgg = false;
	const ULONG arity = pexpr->Arity();
	for (ULONG ul = 0; !fHasCountAgg && ul < arity; ul++)
	{
		fHasCountAgg = FHasCountAgg((*pexpr)[ul], ppcrCount);
	}

	return fHasCountAgg;
}


static BOOL
FCountAggMatchingColumn(gpos::pointer<CExpression *> pexprPrjElem,
						const CColRef *colref)
{
	CColRef *pcrCount = nullptr;
	return CUtils::FCountAggProjElem(pexprPrjElem, &pcrCount) &&
		   colref == pcrCount;
}


BOOL
CUtils::FHasCountAggMatchingColumn(
	gpos::pointer<const CExpression *> pexpr, const CColRef *colref,
	gpos::pointer<const CLogicalGbAgg *> *ppgbAgg)
{
	gpos::pointer<COperator *> pop = pexpr->Pop();
	// base case, we have a logical agg operator
	if (COperator::EopLogicalGbAgg == pop->Eopid())
	{
		gpos::pointer<const CExpression *> const pexprProjectList = (*pexpr)[1];
		GPOS_ASSERT(COperator::EopScalarProjectList ==
					pexprProjectList->Pop()->Eopid());
		const ULONG arity = pexprProjectList->Arity();
		for (ULONG ul = 0; ul < arity; ul++)
		{
			gpos::pointer<CExpression *> const pexprPrjElem =
				(*pexprProjectList)[ul];
			if (FCountAggMatchingColumn(pexprPrjElem, colref))
			{
				gpos::pointer<const CLogicalGbAgg *> pgbAgg =
					gpos::dyn_cast<CLogicalGbAgg>(pop);
				*ppgbAgg = pgbAgg;
				return true;
			}
		}
	}
	// recurse
	else
	{
		const ULONG arity = pexpr->Arity();
		for (ULONG ul = 0; ul < arity; ul++)
		{
			gpos::pointer<const CExpression *> pexprChild = (*pexpr)[ul];
			if (FHasCountAggMatchingColumn(pexprChild, colref, ppgbAgg))
			{
				return true;
			}
		}
	}
	return false;
}

// generate a sum(col) expression
gpos::owner<CExpression *>
CUtils::PexprSum(CMemoryPool *mp, const CColRef *colref)
{
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	return PexprAgg(mp, md_accessor, IMDType::EaggSum, colref,
					false /*is_distinct*/);
}

// generate a GbAgg with sum(col) expressions for all columns in the passed array
gpos::owner<CExpression *>
CUtils::PexprGbAggSum(CMemoryPool *mp, gpos::owner<CExpression *> pexprLogical,
					  gpos::pointer<CColRefArray *> pdrgpcrSum)
{
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();

	gpos::owner<CExpressionArray *> pdrgpexpr =
		GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG size = pdrgpcrSum->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		CColRef *colref = (*pdrgpcrSum)[ul];
		gpos::owner<CExpression *> pexprSum = PexprSum(mp, colref);
		gpos::pointer<CScalarAggFunc *> popScalarAggFunc =
			gpos::dyn_cast<CScalarAggFunc>(pexprSum->Pop());
		gpos::pointer<const IMDType *> pmdtypeSum =
			md_accessor->RetrieveType(popScalarAggFunc->MdidType());
		CColRef *pcrSum = col_factory->PcrCreate(
			pmdtypeSum, popScalarAggFunc->TypeModifier());
		gpos::owner<CExpression *> pexprPrjElemSum =
			PexprScalarProjectElement(mp, pcrSum, pexprSum);
		pdrgpexpr->Append(pexprPrjElemSum);
	}

	gpos::owner<CExpression *> pexprPrjList = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CScalarProjectList(mp), std::move(pdrgpexpr));

	// generate a Gb expression
	gpos::owner<CColRefArray *> colref_array = GPOS_NEW(mp) CColRefArray(mp);
	return PexprLogicalGbAggGlobal(mp, std::move(colref_array),
								   std::move(pexprLogical),
								   std::move(pexprPrjList));
}

// generate a count(<distinct> col) expression
gpos::owner<CExpression *>
CUtils::PexprCount(CMemoryPool *mp, const CColRef *colref, BOOL is_distinct)
{
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	return PexprAgg(mp, md_accessor, IMDType::EaggCount, colref, is_distinct);
}

// generate a min(col) expression
gpos::owner<CExpression *>
CUtils::PexprMin(CMemoryPool *mp, CMDAccessor *md_accessor,
				 const CColRef *colref)
{
	return PexprAgg(mp, md_accessor, IMDType::EaggMin, colref,
					false /*is_distinct*/);
}

// generate an aggregate expression of the specified type
gpos::owner<CExpression *>
CUtils::PexprAgg(CMemoryPool *mp, CMDAccessor *md_accessor,
				 IMDType::EAggType agg_type, const CColRef *colref,
				 BOOL is_distinct)
{
	GPOS_ASSERT(IMDType::EaggGeneric > agg_type);
	GPOS_ASSERT(colref->RetrieveType()->GetMdidForAggType(agg_type)->IsValid());

	gpos::pointer<const IMDAggregate *> pmdagg = md_accessor->RetrieveAgg(
		colref->RetrieveType()->GetMdidForAggType(agg_type));

	gpos::owner<IMDId *> agg_mdid = pmdagg->MDId();
	agg_mdid->AddRef();
	CWStringConst *str = GPOS_NEW(mp)
		CWStringConst(mp, pmdagg->Mdname().GetMDName()->GetBuffer());

	return PexprAggFunc(mp, std::move(agg_mdid), str, colref, is_distinct,
						EaggfuncstageGlobal /*fGlobal*/, false /*fSplit*/);
}

// generate a select expression
gpos::owner<CExpression *>
CUtils::PexprLogicalSelect(CMemoryPool *mp, gpos::owner<CExpression *> pexpr,
						   gpos::owner<CExpression *> pexprPredicate)
{
	GPOS_ASSERT(nullptr != pexpr);
	GPOS_ASSERT(nullptr != pexprPredicate);

	gpos::pointer<CTableDescriptor *> ptabdesc = nullptr;
	if (pexpr->Pop()->Eopid() == CLogical::EopLogicalSelect ||
		pexpr->Pop()->Eopid() == CLogical::EopLogicalGet ||
		pexpr->Pop()->Eopid() == CLogical::EopLogicalDynamicGet)
	{
		ptabdesc = pexpr->DeriveTableDescriptor();
		// there are some cases where we don't populate LogicalSelect currently
		GPOS_ASSERT_IMP(pexpr->Pop()->Eopid() != CLogical::EopLogicalSelect,
						nullptr != ptabdesc);
	}
	return GPOS_NEW(mp)
		CExpression(mp, GPOS_NEW(mp) CLogicalSelect(mp, ptabdesc),
					std::move(pexpr), std::move(pexprPredicate));
}

// if predicate is True return logical expression, otherwise return a new select node
gpos::owner<CExpression *>
CUtils::PexprSafeSelect(CMemoryPool *mp,
						gpos::owner<CExpression *> pexprLogical,
						gpos::owner<CExpression *> pexprPred)
{
	GPOS_ASSERT(nullptr != pexprLogical);
	GPOS_ASSERT(nullptr != pexprPred);

	if (FScalarConstTrue(pexprPred))
	{
		// caller must have add-refed the predicate before coming here
		pexprPred->Release();
		return pexprLogical;
	}

	return PexprLogicalSelect(mp, std::move(pexprLogical),
							  std::move(pexprPred));
}

// generate a select expression, if child is another Select expression
// collapse both Selects into one expression
gpos::owner<CExpression *>
CUtils::PexprCollapseSelect(CMemoryPool *mp, CExpression *pexpr,
							CExpression *pexprPredicate)
{
	GPOS_ASSERT(nullptr != pexpr);
	GPOS_ASSERT(nullptr != pexprPredicate);

	if (COperator::EopLogicalSelect == pexpr->Pop()->Eopid() &&
		2 ==
			pexpr
				->Arity()  // perform collapsing only when we have a full Select tree
	)
	{
		// collapse Selects into one expression
		(*pexpr)[0]->AddRef();
		gpos::owner<CExpression *> pexprChild = (*pexpr)[0];
		gpos::owner<CExpression *> pexprScalar =
			CPredicateUtils::PexprConjunction(mp, pexprPredicate, (*pexpr)[1]);

		return GPOS_NEW(mp)
			CExpression(mp, GPOS_NEW(mp) CLogicalSelect(mp),
						std::move(pexprChild), std::move(pexprScalar));
	}

	pexpr->AddRef();
	pexprPredicate->AddRef();

	return GPOS_NEW(mp)
		CExpression(mp, GPOS_NEW(mp) CLogicalSelect(mp), pexpr, pexprPredicate);
}

// generate a project expression
gpos::owner<CExpression *>
CUtils::PexprLogicalProject(CMemoryPool *mp, gpos::owner<CExpression *> pexpr,
							gpos::owner<CExpression *> pexprPrjList,
							BOOL fNewComputedCol)
{
	GPOS_ASSERT(nullptr != pexpr);
	GPOS_ASSERT(nullptr != pexprPrjList);
	GPOS_ASSERT(COperator::EopScalarProjectList ==
				pexprPrjList->Pop()->Eopid());

	if (fNewComputedCol)
	{
		CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
		const ULONG arity = pexprPrjList->Arity();
		for (ULONG ul = 0; ul < arity; ul++)
		{
			gpos::pointer<CExpression *> pexprPrEl = (*pexprPrjList)[ul];
			col_factory->AddComputedToUsedColsMap(pexprPrEl);
		}
	}
	return GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalProject(mp),
									std::move(pexpr), std::move(pexprPrjList));
}

// generate a sequence project expression
gpos::owner<CExpression *>
CUtils::PexprLogicalSequenceProject(CMemoryPool *mp,
									gpos::owner<CDistributionSpec *> pds,
									gpos::owner<COrderSpecArray *> pdrgpos,
									gpos::owner<CWindowFrameArray *> pdrgpwf,
									gpos::owner<CExpression *> pexpr,
									gpos::owner<CExpression *> pexprPrjList)
{
	GPOS_ASSERT(nullptr != pds);
	GPOS_ASSERT(nullptr != pdrgpos);
	GPOS_ASSERT(nullptr != pdrgpwf);
	GPOS_ASSERT(pdrgpwf->Size() == pdrgpos->Size());
	GPOS_ASSERT(nullptr != pexpr);
	GPOS_ASSERT(nullptr != pexprPrjList);
	GPOS_ASSERT(COperator::EopScalarProjectList ==
				pexprPrjList->Pop()->Eopid());

	return GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CLogicalSequenceProject(
			mp, std::move(pds), std::move(pdrgpos), std::move(pdrgpwf)),
		std::move(pexpr), std::move(pexprPrjList));
}

// construct a projection of NULL constants using the given column
// names and types on top of the given expression
gpos::owner<CExpression *>
CUtils::PexprLogicalProjectNulls(
	CMemoryPool *mp, gpos::pointer<CColRefArray *> colref_array,
	gpos::owner<CExpression *> pexpr,
	gpos::pointer<UlongToColRefMap *> colref_mapping)
{
	gpos::owner<IDatumArray *> pdrgpdatum =
		CTranslatorExprToDXLUtils::PdrgpdatumNulls(mp, colref_array);
	gpos::owner<CExpression *> pexprProjList =
		PexprScalarProjListConst(mp, colref_array, pdrgpdatum, colref_mapping);
	pdrgpdatum->Release();

	return PexprLogicalProject(mp, std::move(pexpr), std::move(pexprProjList),
							   false /*fNewComputedCol*/);
}

// construct a project list using the column names and types of the given
// array of column references, and the given datums
gpos::owner<CExpression *>
CUtils::PexprScalarProjListConst(
	CMemoryPool *mp, gpos::pointer<CColRefArray *> colref_array,
	gpos::pointer<IDatumArray *> pdrgpdatum,
	gpos::pointer<UlongToColRefMap *> colref_mapping)
{
	GPOS_ASSERT(colref_array->Size() == pdrgpdatum->Size());

	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
	gpos::owner<CScalarProjectList *> pscprl =
		GPOS_NEW(mp) CScalarProjectList(mp);

	const ULONG arity = colref_array->Size();
	if (0 == arity)
	{
		return GPOS_NEW(mp) CExpression(mp, std::move(pscprl));
	}

	gpos::owner<CExpressionArray *> pdrgpexprProjElems =
		GPOS_NEW(mp) CExpressionArray(mp);
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CColRef *colref = (*colref_array)[ul];

		gpos::owner<IDatum *> datum = (*pdrgpdatum)[ul];
		datum->AddRef();
		gpos::owner<CScalarConst *> popScConst =
			GPOS_NEW(mp) CScalarConst(mp, datum);
		gpos::owner<CExpression *> pexprScConst =
			GPOS_NEW(mp) CExpression(mp, popScConst);

		CColRef *new_colref = col_factory->PcrCreate(
			colref->RetrieveType(), colref->TypeModifier(), colref->Name());
		if (nullptr != colref_mapping)
		{
			BOOL fInserted GPOS_ASSERTS_ONLY = colref_mapping->Insert(
				GPOS_NEW(mp) ULONG(colref->Id()), new_colref);
			GPOS_ASSERT(fInserted);
		}

		gpos::owner<CScalarProjectElement *> popScPrEl =
			GPOS_NEW(mp) CScalarProjectElement(mp, new_colref);
		gpos::owner<CExpression *> pexprScPrEl =
			GPOS_NEW(mp) CExpression(mp, popScPrEl, pexprScConst);

		pdrgpexprProjElems->Append(pexprScPrEl);
	}

	return GPOS_NEW(mp)
		CExpression(mp, std::move(pscprl), std::move(pdrgpexprProjElems));
}

// generate a project expression with one additional project element
gpos::owner<CExpression *>
CUtils::PexprAddProjection(CMemoryPool *mp, gpos::owner<CExpression *> pexpr,
						   gpos::owner<CExpression *> pexprProjected)
{
	GPOS_ASSERT(pexpr->Pop()->FLogical());
	GPOS_ASSERT(pexprProjected->Pop()->FScalar());

	gpos::owner<CExpressionArray *> pdrgpexprProjected =
		GPOS_NEW(mp) CExpressionArray(mp);
	pdrgpexprProjected->Append(std::move(pexprProjected));

	gpos::owner<CExpression *> pexprProjection =
		PexprAddProjection(mp, std::move(pexpr), pdrgpexprProjected);
	pdrgpexprProjected->Release();

	return pexprProjection;
}

// generate a project expression with one or more additional project elements
gpos::owner<CExpression *>
CUtils::PexprAddProjection(CMemoryPool *mp, gpos::owner<CExpression *> pexpr,
						   gpos::pointer<CExpressionArray *> pdrgpexprProjected,
						   BOOL fNewComputedCol)
{
	GPOS_ASSERT(pexpr->Pop()->FLogical());
	GPOS_ASSERT(nullptr != pdrgpexprProjected);

	if (0 == pdrgpexprProjected->Size())
	{
		return pexpr;
	}

	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	gpos::owner<CExpressionArray *> pdrgpexprPrjElem =
		GPOS_NEW(mp) CExpressionArray(mp);

	const ULONG ulProjected = pdrgpexprProjected->Size();
	for (ULONG ul = 0; ul < ulProjected; ul++)
	{
		CExpression *pexprProjected = (*pdrgpexprProjected)[ul];
		GPOS_ASSERT(pexprProjected->Pop()->FScalar());

		// generate a computed column with scalar expression type
		gpos::pointer<CScalar *> popScalar =
			gpos::dyn_cast<CScalar>(pexprProjected->Pop());
		gpos::pointer<const IMDType *> pmdtype =
			md_accessor->RetrieveType(popScalar->MdidType());
		CColRef *colref =
			col_factory->PcrCreate(pmdtype, popScalar->TypeModifier());

		pexprProjected->AddRef();
		pdrgpexprPrjElem->Append(
			PexprScalarProjectElement(mp, colref, pexprProjected));
	}

	gpos::owner<CExpression *> pexprPrjList = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CScalarProjectList(mp), std::move(pdrgpexprPrjElem));

	return PexprLogicalProject(mp, std::move(pexpr), std::move(pexprPrjList),
							   fNewComputedCol);
}

// generate an aggregate expression
gpos::owner<CExpression *>
CUtils::PexprLogicalGbAgg(CMemoryPool *mp,
						  gpos::owner<CColRefArray *> colref_array,
						  gpos::owner<CExpression *> pexprRelational,
						  gpos::owner<CExpression *> pexprPrL,
						  COperator::EGbAggType egbaggtype)
{
	GPOS_ASSERT(nullptr != colref_array);
	GPOS_ASSERT(nullptr != pexprRelational);
	GPOS_ASSERT(nullptr != pexprPrL);

	// create a new logical group by operator
	gpos::owner<CLogicalGbAgg *> pop =
		GPOS_NEW(mp) CLogicalGbAgg(mp, std::move(colref_array), egbaggtype);

	return GPOS_NEW(mp) CExpression(
		mp, std::move(pop), std::move(pexprRelational), std::move(pexprPrL));
}

// generate a global aggregate expression
gpos::owner<CExpression *>
CUtils::PexprLogicalGbAggGlobal(CMemoryPool *mp,
								gpos::owner<CColRefArray *> colref_array,
								gpos::owner<CExpression *> pexprRelational,
								gpos::owner<CExpression *> pexprProjList)
{
	return PexprLogicalGbAgg(
		mp, std::move(colref_array), std::move(pexprRelational),
		std::move(pexprProjList), COperator::EgbaggtypeGlobal);
}

// check if given project list has a global aggregate function
BOOL
CUtils::FHasGlobalAggFunc(gpos::pointer<const CExpression *> pexprAggProjList)
{
	GPOS_ASSERT(COperator::EopScalarProjectList ==
				pexprAggProjList->Pop()->Eopid());
	BOOL fGlobal = false;

	const ULONG arity = pexprAggProjList->Arity();

	for (ULONG ul = 0; ul < arity && !fGlobal; ul++)
	{
		gpos::pointer<CExpression *> pexprPrEl = (*pexprAggProjList)[ul];
		GPOS_ASSERT(COperator::EopScalarProjectElement ==
					pexprPrEl->Pop()->Eopid());
		// get the scalar child of the project element
		gpos::pointer<CExpression *> pexprAggFunc = (*pexprPrEl)[0];
		gpos::pointer<CScalarAggFunc *> popScAggFunc =
			gpos::dyn_cast<CScalarAggFunc>(pexprAggFunc->Pop());
		fGlobal = popScAggFunc->FGlobal();
	}

	return fGlobal;
}

// return the comparison type for the given mdid
IMDType::ECmpType
CUtils::ParseCmpType(gpos::pointer<IMDId *> mdid)
{
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	gpos::pointer<const IMDScalarOp *> md_scalar_op =
		md_accessor->RetrieveScOp(mdid);
	return md_scalar_op->ParseCmpType();
}

// return the comparison type for the given mdid
IMDType::ECmpType
CUtils::ParseCmpType(CMDAccessor *md_accessor, gpos::pointer<IMDId *> mdid)
{
	gpos::pointer<const IMDScalarOp *> md_scalar_op =
		md_accessor->RetrieveScOp(mdid);
	return md_scalar_op->ParseCmpType();
}

// check if the expression is a scalar boolean const
BOOL
CUtils::FScalarConstBool(gpos::pointer<CExpression *> pexpr, BOOL value)
{
	GPOS_ASSERT(nullptr != pexpr);

	gpos::pointer<COperator *> pop = pexpr->Pop();
	if (COperator::EopScalarConst == pop->Eopid())
	{
		gpos::pointer<CScalarConst *> popScalarConst =
			gpos::dyn_cast<CScalarConst>(pop);
		if (IMDType::EtiBool == popScalarConst->GetDatum()->GetDatumType())
		{
			gpos::pointer<IDatumBool *> datum =
				dynamic_cast<IDatumBool *>(popScalarConst->GetDatum());
			return !datum->IsNull() && datum->GetValue() == value;
		}
	}

	return false;
}

BOOL
CUtils::FScalarConstBoolNull(gpos::pointer<CExpression *> pexpr)
{
	GPOS_ASSERT(nullptr != pexpr);

	gpos::pointer<COperator *> pop = pexpr->Pop();
	if (COperator::EopScalarConst == pop->Eopid())
	{
		gpos::pointer<CScalarConst *> popScalarConst =
			gpos::dyn_cast<CScalarConst>(pop);
		if (IMDType::EtiBool == popScalarConst->GetDatum()->GetDatumType())
		{
			return popScalarConst->GetDatum()->IsNull();
		}
	}

	return false;
}

// checks to see if the expression is a scalar const TRUE
BOOL
CUtils::FScalarConstTrue(gpos::pointer<CExpression *> pexpr)
{
	return FScalarConstBool(pexpr, true /*value*/);
}

// checks to see if the expression is a scalar const FALSE
BOOL
CUtils::FScalarConstFalse(gpos::pointer<CExpression *> pexpr)
{
	return FScalarConstBool(pexpr, false /*value*/);
}

//	create an array of expression's output columns including a key for grouping
gpos::owner<CColRefArray *>
CUtils::PdrgpcrGroupingKey(
	CMemoryPool *mp, gpos::pointer<CExpression *> pexpr,
	gpos::owner<CColRefArray *> *
		ppdrgpcrKey	 // output: array of key columns contained in the returned grouping columns
)
{
	GPOS_ASSERT(nullptr != pexpr);
	GPOS_ASSERT(nullptr != ppdrgpcrKey);

	gpos::pointer<CKeyCollection *> pkc = pexpr->DeriveKeyCollection();
	GPOS_ASSERT(nullptr != pkc);

	gpos::pointer<CColRefSet *> pcrsOutput = pexpr->DeriveOutputColumns();
	gpos::owner<CColRefSet *> pcrsUsedOuter = GPOS_NEW(mp) CColRefSet(mp);

	// remove any columns that are not referenced in the query from pcrsOuterOutput
	// filter out system columns since they may introduce columns with undefined sort/hash operators
	CColRefSetIter it(*pcrsOutput);
	while (it.Advance())
	{
		CColRef *pcr = it.Pcr();

		if (CColRef::EUsed == pcr->GetUsage() && !pcr->IsSystemCol())
		{
			pcrsUsedOuter->Include(pcr);
		}
	}

	// prefer extracting a hashable key since Agg operator may redistribute child on grouping columns
	gpos::owner<CColRefArray *> pdrgpcrKey = pkc->PdrgpcrHashableKey(mp);
	if (nullptr == pdrgpcrKey)
	{
		// if no hashable key, extract any key
		pdrgpcrKey = pkc->PdrgpcrKey(mp);
	}
	GPOS_ASSERT(nullptr != pdrgpcrKey);

	gpos::owner<CColRefSet *> pcrsKey = GPOS_NEW(mp) CColRefSet(mp, pdrgpcrKey);
	pcrsUsedOuter->Union(pcrsKey);

	pcrsKey->Release();
	gpos::owner<CColRefArray *> colref_array = pcrsUsedOuter->Pdrgpcr(mp);
	pcrsUsedOuter->Release();

	// set output key array
	*ppdrgpcrKey = pdrgpcrKey;

	return colref_array;
}

// add an equivalence class to the array. If the new equiv class contains
// columns from separate equiv classes, then these are merged. Returns a new
// array of equivalence classes
gpos::owner<CColRefSetArray *>
CUtils::AddEquivClassToArray(CMemoryPool *mp,
							 gpos::pointer<const CColRefSet *> pcrsNew,
							 gpos::pointer<const CColRefSetArray *> pdrgpcrs)
{
	gpos::owner<CColRefSetArray *> pdrgpcrsNew =
		GPOS_NEW(mp) CColRefSetArray(mp);
	gpos::owner<CColRefSet *> pcrsCopy = GPOS_NEW(mp) CColRefSet(mp, *pcrsNew);

	const ULONG length = pdrgpcrs->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		CColRefSet *pcrs = (*pdrgpcrs)[ul];
		if (pcrsCopy->IsDisjoint(pcrs))
		{
			pcrs->AddRef();
			pdrgpcrsNew->Append(pcrs);
		}
		else
		{
			pcrsCopy->Include(pcrs);
		}
	}

	pdrgpcrsNew->Append(std::move(pcrsCopy));

	return pdrgpcrsNew;
}

// merge 2 arrays of equivalence classes
gpos::owner<CColRefSetArray *>
CUtils::PdrgpcrsMergeEquivClasses(CMemoryPool *mp, CColRefSetArray *pdrgpcrsFst,
								  gpos::pointer<CColRefSetArray *> pdrgpcrsSnd)
{
	pdrgpcrsFst->AddRef();
	gpos::owner<CColRefSetArray *> pdrgpcrsMerged = pdrgpcrsFst;

	const ULONG length = pdrgpcrsSnd->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		gpos::pointer<CColRefSet *> pcrs = (*pdrgpcrsSnd)[ul];

		gpos::owner<CColRefSetArray *> pdrgpcrs =
			AddEquivClassToArray(mp, pcrs, pdrgpcrsMerged);
		pdrgpcrsMerged->Release();
		pdrgpcrsMerged = pdrgpcrs;
	}

	return pdrgpcrsMerged;
}

// intersect 2 arrays of equivalence classes. This is accomplished by using
// a hashmap to find intersects between two arrays of colomun referance sets
gpos::owner<CColRefSetArray *>
CUtils::PdrgpcrsIntersectEquivClasses(
	CMemoryPool *mp, gpos::pointer<CColRefSetArray *> pdrgpcrsFst,
	gpos::pointer<CColRefSetArray *> pdrgpcrsSnd)
{
	GPOS_ASSERT(CUtils::FEquivalanceClassesDisjoint(mp, pdrgpcrsFst));
	GPOS_ASSERT(CUtils::FEquivalanceClassesDisjoint(mp, pdrgpcrsSnd));

	gpos::owner<CColRefSetArray *> pdrgpcrs = GPOS_NEW(mp) CColRefSetArray(mp);

	const ULONG ulLenFst = pdrgpcrsFst->Size();
	const ULONG ulLenSnd = pdrgpcrsSnd->Size();

	// nothing to intersect, so return empty array
	if (ulLenSnd == 0 || ulLenFst == 0)
	{
		return pdrgpcrs;
	}

	gpos::owner<ColRefToColRefSetMap *> phmcscrs =
		GPOS_NEW(mp) ColRefToColRefSetMap(mp);
	gpos::owner<ColRefToColRefMap *> phmcscrDone =
		GPOS_NEW(mp) ColRefToColRefMap(mp);

	// populate a hashmap in this loop
	for (ULONG ulFst = 0; ulFst < ulLenFst; ulFst++)
	{
		CColRefSet *pcrsFst = (*pdrgpcrsFst)[ulFst];

		// because the equivalence classes are disjoint, a single column will only be a member
		// of one equivalence class. therefore, we create a hash map keyed on one column
		CColRefSetIter crsi(*pcrsFst);
		while (crsi.Advance())
		{
			CColRef *colref = crsi.Pcr();
			pcrsFst->AddRef();
			phmcscrs->Insert(colref, pcrsFst);
		}
	}

	// probe the hashmap with the equivalence classes
	for (ULONG ulSnd = 0; ulSnd < ulLenSnd; ulSnd++)
	{
		gpos::pointer<CColRefSet *> pcrsSnd = (*pdrgpcrsSnd)[ulSnd];

		// iterate on column references in the equivalence class
		CColRefSetIter crsi(*pcrsSnd);
		while (crsi.Advance())
		{
			// lookup a column in the hashmap
			CColRef *colref = crsi.Pcr();
			gpos::pointer<CColRefSet *> pcrs = phmcscrs->Find(colref);
			CColRef *pcrDone = phmcscrDone->Find(colref);

			// continue if we don't find this column or if that column
			// is already processed and outputed as an intersection of two
			// column referance sets
			if (nullptr != pcrs && pcrDone == nullptr)
			{
				gpos::owner<CColRefSet *> pcrsNew = GPOS_NEW(mp) CColRefSet(mp);
				pcrsNew->Include(pcrsSnd);
				pcrsNew->Intersection(pcrs);
				pdrgpcrs->Append(pcrsNew);

				CColRefSetIter hmpcrs(*pcrsNew);
				// now that we have created an intersection, any additional matches to these
				// columns would create a duplicate intersect, so we add those columns to
				// the DONE hash map
				while (hmpcrs.Advance())
				{
					CColRef *pcrProcessed = hmpcrs.Pcr();
					phmcscrDone->Insert(pcrProcessed, pcrProcessed);
				}
			}
		}
	}
	phmcscrDone->Release();
	phmcscrs->Release();

	return pdrgpcrs;
}

// return a copy of equivalence classes from all children
gpos::owner<CColRefSetArray *>
CUtils::PdrgpcrsCopyChildEquivClasses(CMemoryPool *mp,
									  CExpressionHandle &exprhdl)
{
	gpos::owner<CColRefSetArray *> pdrgpcrs = GPOS_NEW(mp) CColRefSetArray(mp);
	const ULONG arity = exprhdl.Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		if (!exprhdl.FScalarChild(ul))
		{
			gpos::pointer<CColRefSetArray *> pdrgpcrsChild =
				exprhdl.DerivePropertyConstraint(ul)->PdrgpcrsEquivClasses();

			gpos::owner<CColRefSetArray *> pdrgpcrsChildCopy =
				GPOS_NEW(mp) CColRefSetArray(mp);
			const ULONG size = pdrgpcrsChild->Size();
			for (ULONG ulInner = 0; ulInner < size; ulInner++)
			{
				gpos::owner<CColRefSet *> pcrs =
					GPOS_NEW(mp) CColRefSet(mp, *(*pdrgpcrsChild)[ulInner]);
				pdrgpcrsChildCopy->Append(pcrs);
			}

			gpos::owner<CColRefSetArray *> pdrgpcrsMerged =
				PdrgpcrsMergeEquivClasses(mp, pdrgpcrs, pdrgpcrsChildCopy);
			pdrgpcrsChildCopy->Release();
			pdrgpcrs->Release();
			pdrgpcrs = pdrgpcrsMerged;
		}
	}

	return pdrgpcrs;
}

// return a copy of the given array of columns, excluding the columns in the given colrefset
gpos::owner<CColRefArray *>
CUtils::PdrgpcrExcludeColumns(CMemoryPool *mp,
							  gpos::pointer<CColRefArray *> pdrgpcrOriginal,
							  gpos::pointer<CColRefSet *> pcrsExcluded)
{
	GPOS_ASSERT(nullptr != pdrgpcrOriginal);
	GPOS_ASSERT(nullptr != pcrsExcluded);

	gpos::owner<CColRefArray *> colref_array = GPOS_NEW(mp) CColRefArray(mp);
	const ULONG num_cols = pdrgpcrOriginal->Size();
	for (ULONG ul = 0; ul < num_cols; ul++)
	{
		CColRef *colref = (*pdrgpcrOriginal)[ul];
		if (!pcrsExcluded->FMember(colref))
		{
			colref_array->Append(colref);
		}
	}

	return colref_array;
}

// helper function to print a colref array
IOstream &
CUtils::OsPrintDrgPcr(IOstream &os,
					  gpos::pointer<const CColRefArray *> colref_array,
					  ULONG ulLenMax)
{
	ULONG length = colref_array->Size();
	for (ULONG ul = 0; ul < std::min(length, ulLenMax); ul++)
	{
		(*colref_array)[ul]->OsPrint(os);
		if (ul < length - 1)
		{
			os << ", ";
		}
	}

	if (ulLenMax < length)
	{
		os << "...";
	}

	return os;
}

// check if given expression is a scalar comparison
BOOL
CUtils::FScalarCmp(gpos::pointer<CExpression *> pexpr)
{
	return (COperator::EopScalarCmp == pexpr->Pop()->Eopid());
}

// check if given expression is a scalar array comparison
BOOL
CUtils::FScalarArrayCmp(gpos::pointer<CExpression *> pexpr)
{
	return (COperator::EopScalarArrayCmp == pexpr->Pop()->Eopid());
}

// check if given expression has any one stage agg nodes
BOOL
CUtils::FHasOneStagePhysicalAgg(gpos::pointer<const CExpression *> pexpr)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(nullptr != pexpr);

	if (FPhysicalAgg(pexpr->Pop()) &&
		!gpos::dyn_cast<CPhysicalAgg>(pexpr->Pop())->FMultiStage())
	{
		return true;
	}

	// recursively check children
	const ULONG arity = pexpr->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		if (FHasOneStagePhysicalAgg((*pexpr)[ul]))
		{
			return true;
		}
	}

	return false;
}

// check if given operator exists in the given list
BOOL
CUtils::FOpExists(gpos::pointer<const COperator *> pop,
				  const COperator::EOperatorId *peopid, ULONG ulOps)
{
	GPOS_ASSERT(nullptr != pop);
	GPOS_ASSERT(nullptr != peopid);

	COperator::EOperatorId op_id = pop->Eopid();
	for (ULONG ul = 0; ul < ulOps; ul++)
	{
		if (op_id == peopid[ul])
		{
			return true;
		}
	}

	return false;
}

// check if given expression has any operator in the given list
BOOL
CUtils::FHasOp(gpos::pointer<const CExpression *> pexpr,
			   const COperator::EOperatorId *peopid, ULONG ulOps)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(nullptr != pexpr);
	GPOS_ASSERT(nullptr != peopid);

	if (FOpExists(pexpr->Pop(), peopid, ulOps))
	{
		return true;
	}

	// recursively check children
	const ULONG arity = pexpr->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		if (FHasOp((*pexpr)[ul], peopid, ulOps))
		{
			return true;
		}
	}

	return false;
}

// return number of inlinable CTEs in the given expression
ULONG
CUtils::UlInlinableCTEs(gpos::pointer<CExpression *> pexpr, ULONG ulDepth)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(nullptr != pexpr);

	gpos::pointer<COperator *> pop = pexpr->Pop();

	if (COperator::EopLogicalCTEConsumer == pop->Eopid())
	{
		gpos::pointer<CLogicalCTEConsumer *> popConsumer =
			gpos::dyn_cast<CLogicalCTEConsumer>(pop);
		gpos::pointer<CExpression *> pexprProducer =
			COptCtxt::PoctxtFromTLS()->Pcteinfo()->PexprCTEProducer(
				popConsumer->UlCTEId());
		GPOS_ASSERT(nullptr != pexprProducer);
		return ulDepth + UlInlinableCTEs(pexprProducer, ulDepth + 1);
	}

	// recursively process children
	const ULONG arity = pexpr->Arity();
	ULONG ulChildCTEs = 0;
	for (ULONG ul = 0; ul < arity; ul++)
	{
		ulChildCTEs += UlInlinableCTEs((*pexpr)[ul], ulDepth);
	}

	return ulChildCTEs;
}

// return number of joins in the given expression
ULONG
CUtils::UlJoins(gpos::pointer<CExpression *> pexpr)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(nullptr != pexpr);

	ULONG ulJoins = 0;
	gpos::pointer<COperator *> pop = pexpr->Pop();

	if (COperator::EopLogicalCTEConsumer == pop->Eopid())
	{
		gpos::pointer<CLogicalCTEConsumer *> popConsumer =
			gpos::dyn_cast<CLogicalCTEConsumer>(pop);
		gpos::pointer<CExpression *> pexprProducer =
			COptCtxt::PoctxtFromTLS()->Pcteinfo()->PexprCTEProducer(
				popConsumer->UlCTEId());
		return UlJoins(pexprProducer);
	}

	if (FLogicalJoin(pop) || FPhysicalJoin(pop))
	{
		ulJoins = 1;
		if (COperator::EopLogicalNAryJoin == pop->Eopid())
		{
			// N-Ary join is equivalent to a cascade of (Arity - 2) binary joins
			ulJoins = pexpr->Arity() - 2;
		}
	}

	// recursively process children
	const ULONG arity = pexpr->Arity();
	ULONG ulChildJoins = 0;
	for (ULONG ul = 0; ul < arity; ul++)
	{
		ulChildJoins += UlJoins((*pexpr)[ul]);
	}

	return ulJoins + ulChildJoins;
}

// return number of subqueries in the given expression
ULONG
CUtils::UlSubqueries(gpos::pointer<CExpression *> pexpr)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(nullptr != pexpr);

	ULONG ulSubqueries = 0;
	gpos::pointer<COperator *> pop = pexpr->Pop();

	if (COperator::EopLogicalCTEConsumer == pop->Eopid())
	{
		gpos::pointer<CLogicalCTEConsumer *> popConsumer =
			gpos::dyn_cast<CLogicalCTEConsumer>(pop);
		gpos::pointer<CExpression *> pexprProducer =
			COptCtxt::PoctxtFromTLS()->Pcteinfo()->PexprCTEProducer(
				popConsumer->UlCTEId());
		return UlSubqueries(pexprProducer);
	}

	if (FSubquery(pop))
	{
		ulSubqueries = 1;
	}

	// recursively process children
	const ULONG arity = pexpr->Arity();
	ULONG ulChildSubqueries = 0;
	for (ULONG ul = 0; ul < arity; ul++)
	{
		ulChildSubqueries += UlSubqueries((*pexpr)[ul]);
	}

	return ulSubqueries + ulChildSubqueries;
}

// check if given expression is a scalar boolean operator
BOOL
CUtils::FScalarBoolOp(gpos::pointer<CExpression *> pexpr)
{
	return (COperator::EopScalarBoolOp == pexpr->Pop()->Eopid());
}

// is the given expression a scalar bool op of the passed type?
BOOL
CUtils::FScalarBoolOp(gpos::pointer<CExpression *> pexpr,
					  CScalarBoolOp::EBoolOperator eboolop)
{
	GPOS_ASSERT(nullptr != pexpr);

	gpos::pointer<COperator *> pop = pexpr->Pop();
	return pop->FScalar() && COperator::EopScalarBoolOp == pop->Eopid() &&
		   eboolop == gpos::dyn_cast<CScalarBoolOp>(pop)->Eboolop();
}

// check if given expression is a scalar null test
BOOL
CUtils::FScalarNullTest(gpos::pointer<CExpression *> pexpr)
{
	return (COperator::EopScalarNullTest == pexpr->Pop()->Eopid());
}

// check if given expression is a NOT NULL predicate
BOOL
CUtils::FScalarNotNull(gpos::pointer<CExpression *> pexpr)
{
	return FScalarBoolOp(pexpr, CScalarBoolOp::EboolopNot) &&
		   FScalarNullTest((*pexpr)[0]);
}

// check if given expression is a scalar identifier
BOOL
CUtils::FScalarIdent(gpos::pointer<CExpression *> pexpr)
{
	return (COperator::EopScalarIdent == pexpr->Pop()->Eopid());
}

BOOL
CUtils::FScalarIdentIgnoreCast(gpos::pointer<CExpression *> pexpr)
{
	return (COperator::EopScalarIdent == pexpr->Pop()->Eopid() ||
			CScalarIdent::FCastedScId(pexpr));
}

// check if given expression is a scalar boolean identifier
BOOL
CUtils::FScalarIdentBoolType(gpos::pointer<CExpression *> pexpr)
{
	return FScalarIdent(pexpr) &&
		   IMDType::EtiBool == gpos::dyn_cast<CScalarIdent>(pexpr->Pop())
								   ->Pcr()
								   ->RetrieveType()
								   ->GetDatumType();
}

// check if given expression is a scalar array
BOOL
CUtils::FScalarArray(gpos::pointer<CExpression *> pexpr)
{
	return (COperator::EopScalarArray == pexpr->Pop()->Eopid());
}

// check if given expression is a scalar array coerce
BOOL
CUtils::FScalarArrayCoerce(gpos::pointer<CExpression *> pexpr)
{
	return (COperator::EopScalarArrayCoerceExpr == pexpr->Pop()->Eopid());
}

// is the given expression a scalar identifier with the given column reference
BOOL
CUtils::FScalarIdent(gpos::pointer<CExpression *> pexpr, CColRef *colref)
{
	GPOS_ASSERT(nullptr != pexpr);
	GPOS_ASSERT(nullptr != colref);

	return COperator::EopScalarIdent == pexpr->Pop()->Eopid() &&
		   gpos::dyn_cast<CScalarIdent>(pexpr->Pop())->Pcr() == colref;
}

// check if the expression is a scalar const
BOOL
CUtils::FScalarConst(gpos::pointer<CExpression *> pexpr)
{
	return (COperator::EopScalarConst == pexpr->Pop()->Eopid());
}

// check if the expression is variable-free
BOOL
CUtils::FVarFreeExpr(gpos::pointer<CExpression *> pexpr)
{
	GPOS_ASSERT(pexpr->Pop()->FScalar());

	if (FScalarConst(pexpr))
	{
		return true;
	}

	if (pexpr->DeriveHasSubquery())
	{
		return false;
	}

	GPOS_ASSERT(0 == pexpr->DeriveDefinedColumns()->Size());
	gpos::pointer<CColRefSet *> pcrsUsed = pexpr->DeriveUsedColumns();

	// no variables in expression
	return 0 == pcrsUsed->Size();
}

// check if the expression is a scalar predicate, i.e. bool op, comparison, or null test
BOOL
CUtils::FPredicate(gpos::pointer<CExpression *> pexpr)
{
	gpos::pointer<COperator *> pop = pexpr->Pop();
	return pop->FScalar() &&
		   (FScalarCmp(pexpr) || CPredicateUtils::FIDF(pexpr) ||
			FScalarArrayCmp(pexpr) || FScalarBoolOp(pexpr) ||
			FScalarNullTest(pexpr) ||
			CLogical::EopScalarNAryJoinPredList == pop->Eopid());
}

// checks that the given type has all the comparisons: Eq, NEq, L, LEq, G, GEq.
BOOL
CUtils::FHasAllDefaultComparisons(gpos::pointer<const IMDType *> pmdtype)
{
	GPOS_ASSERT(nullptr != pmdtype);

	return IMDId::IsValid(pmdtype->GetMdidForCmpType(IMDType::EcmptEq)) &&
		   IMDId::IsValid(pmdtype->GetMdidForCmpType(IMDType::EcmptNEq)) &&
		   IMDId::IsValid(pmdtype->GetMdidForCmpType(IMDType::EcmptL)) &&
		   IMDId::IsValid(pmdtype->GetMdidForCmpType(IMDType::EcmptLEq)) &&
		   IMDId::IsValid(pmdtype->GetMdidForCmpType(IMDType::EcmptG)) &&
		   IMDId::IsValid(pmdtype->GetMdidForCmpType(IMDType::EcmptGEq));
}

// determine whether a type is supported for use in contradiction detection.
// The assumption is that we only compare data of the same type.
BOOL
CUtils::FConstrainableType(gpos::pointer<IMDId *> mdid_type)
{
	if (FIntType(mdid_type))
	{
		return true;
	}
	if (GPOS_FTRACE(EopttraceEnableConstantExpressionEvaluation))
	{
		CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
		gpos::pointer<const IMDType *> pmdtype =
			md_accessor->RetrieveType(mdid_type);

		return FHasAllDefaultComparisons(pmdtype);
	}
	else
	{
		// also allow date/time/timestamp/float4/float8
		return (CMDIdGPDB::m_mdid_date.Equals(mdid_type) ||
				CMDIdGPDB::m_mdid_time.Equals(mdid_type) ||
				CMDIdGPDB::m_mdid_timestamp.Equals(mdid_type) ||
				CMDIdGPDB::m_mdid_timeTz.Equals(mdid_type) ||
				CMDIdGPDB::m_mdid_timestampTz.Equals(mdid_type) ||
				CMDIdGPDB::m_mdid_float4.Equals(mdid_type) ||
				CMDIdGPDB::m_mdid_float8.Equals(mdid_type) ||
				CMDIdGPDB::m_mdid_numeric.Equals(mdid_type));
	}
}

// determine whether a type is an integer type
BOOL
CUtils::FIntType(gpos::pointer<IMDId *> mdid_type)
{
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	IMDType::ETypeInfo type_info =
		md_accessor->RetrieveType(mdid_type)->GetDatumType();

	return (IMDType::EtiInt2 == type_info || IMDType::EtiInt4 == type_info ||
			IMDType::EtiInt8 == type_info);
}

// check if a binary operator uses only columns produced by its children
BOOL
CUtils::FUsesChildColsOnly(CExpressionHandle &exprhdl)
{
	GPOS_ASSERT(3 == exprhdl.Arity());

	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();
	gpos::pointer<CColRefSet *> pcrsUsed = exprhdl.DeriveUsedColumns(2);
	gpos::owner<CColRefSet *> pcrs = GPOS_NEW(mp) CColRefSet(mp);
	pcrs->Include(exprhdl.DeriveOutputColumns(0 /*child_index*/));
	pcrs->Include(exprhdl.DeriveOutputColumns(1 /*child_index*/));
	BOOL fUsesChildCols = pcrs->ContainsAll(pcrsUsed);
	pcrs->Release();

	return fUsesChildCols;
}

// check if inner child of a binary operator uses columns not produced by outer child
BOOL
CUtils::FInnerUsesExternalCols(CExpressionHandle &exprhdl)
{
	GPOS_ASSERT(3 == exprhdl.Arity());

	gpos::pointer<CColRefSet *> outer_refs =
		exprhdl.DeriveOuterReferences(1 /*child_index*/);
	if (0 == outer_refs->Size())
	{
		return false;
	}
	gpos::pointer<CColRefSet *> pcrsOutput =
		exprhdl.DeriveOutputColumns(0 /*child_index*/);

	return !pcrsOutput->ContainsAll(outer_refs);
}

// check if inner child of a binary operator uses only columns not produced by outer child
BOOL
CUtils::FInnerUsesExternalColsOnly(CExpressionHandle &exprhdl)
{
	return FInnerUsesExternalCols(exprhdl) &&
		   exprhdl.DeriveOuterReferences(1)->IsDisjoint(
			   exprhdl.DeriveOutputColumns(0));
}

// check if given columns have available comparison operators
BOOL
CUtils::FComparisonPossible(gpos::pointer<CColRefArray *> colref_array,
							IMDType::ECmpType cmp_type)
{
	GPOS_ASSERT(nullptr != colref_array);
	GPOS_ASSERT(0 < colref_array->Size());

	const ULONG size = colref_array->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		CColRef *colref = (*colref_array)[ul];
		gpos::pointer<const IMDType *> pmdtype = colref->RetrieveType();
		if (!IMDId::IsValid(pmdtype->GetMdidForCmpType(cmp_type)))
		{
			return false;
		}
	}

	return true;
}

// counts the number of times a certain operator appears
ULONG
CUtils::UlCountOperator(gpos::pointer<const CExpression *> pexpr,
						COperator::EOperatorId op_id)
{
	ULONG ulOpCnt = 0;
	if (op_id == pexpr->Pop()->Eopid())
	{
		ulOpCnt += 1;
	}

	const ULONG arity = pexpr->Arity();
	for (ULONG ulChild = 0; ulChild < arity; ulChild++)
	{
		ulOpCnt += UlCountOperator((*pexpr)[ulChild], op_id);
	}
	return ulOpCnt;
}

// return the max subset of redistributable columns for the given columns
gpos::owner<CColRefArray *>
CUtils::PdrgpcrRedistributableSubset(CMemoryPool *mp,
									 gpos::pointer<CColRefArray *> colref_array)
{
	GPOS_ASSERT(nullptr != colref_array);
	GPOS_ASSERT(0 < colref_array->Size());

	gpos::owner<CColRefArray *> pdrgpcrRedist = GPOS_NEW(mp) CColRefArray(mp);
	const ULONG size = colref_array->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		CColRef *colref = (*colref_array)[ul];
		gpos::pointer<const IMDType *> pmdtype = colref->RetrieveType();
		if (pmdtype->IsRedistributable())
		{
			pdrgpcrRedist->Append(colref);
		}
	}

	return pdrgpcrRedist;
}

// check if hashing is possible for the given columns
BOOL
CUtils::IsHashable(gpos::pointer<CColRefArray *> colref_array)
{
	GPOS_ASSERT(nullptr != colref_array);
	GPOS_ASSERT(0 < colref_array->Size());

	const ULONG size = colref_array->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		CColRef *colref = (*colref_array)[ul];
		gpos::pointer<const IMDType *> pmdtype = colref->RetrieveType();
		if (!pmdtype->IsHashable())
		{
			return false;
		}
	}

	return true;
}

// check if given operator is a logical DML operator
BOOL
CUtils::FLogicalDML(gpos::pointer<COperator *> pop)
{
	GPOS_ASSERT(nullptr != pop);

	COperator::EOperatorId op_id = pop->Eopid();
	return COperator::EopLogicalDML == op_id ||
		   COperator::EopLogicalInsert == op_id ||
		   COperator::EopLogicalDelete == op_id ||
		   COperator::EopLogicalUpdate == op_id;
}

// return regular string from wide-character string
CHAR *
CUtils::CreateMultiByteCharStringFromWCString(CMemoryPool *mp, WCHAR *wsz)
{
	ULONG ulMaxLength = GPOS_WSZ_LENGTH(wsz) * GPOS_SIZEOF(WCHAR) + 1;
	CHAR *sz = GPOS_NEW_ARRAY(mp, CHAR, ulMaxLength);
	clib::Wcstombs(sz, wsz, ulMaxLength);
	sz[ulMaxLength - 1] = '\0';

	return sz;
}

// construct an array of colids from the given array of column references
gpos::owner<ULongPtrArray *>
CUtils::Pdrgpul(CMemoryPool *mp,
				gpos::pointer<const CColRefArray *> colref_array)
{
	gpos::owner<ULongPtrArray *> pdrgpul = GPOS_NEW(mp) ULongPtrArray(mp);

	const ULONG length = colref_array->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		CColRef *colref = (*colref_array)[ul];
		ULONG *pul = GPOS_NEW(mp) ULONG(colref->Id());
		pdrgpul->Append(pul);
	}

	return pdrgpul;
}

// generate a timestamp-based filename in the provided buffer.
void
CUtils::GenerateFileName(CHAR *buf, const CHAR *szPrefix, const CHAR *szExt,
						 ULONG length, ULONG ulSessionId, ULONG ulCmdId)
{
	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();

	CWStringDynamic *filename_template = GPOS_NEW(mp) CWStringDynamic(mp);
	COstreamString oss(filename_template);
	oss << szPrefix << "_%04d%02d%02d_%02d%02d%02d_%d_%d." << szExt;

	const WCHAR *wszFileNameTemplate = filename_template->GetBuffer();
	GPOS_ASSERT(length >= GPOS_FILE_NAME_BUF_SIZE);

	TIMEVAL tv;
	TIME tm;

	// get local time
	syslib::GetTimeOfDay(&tv, nullptr /*timezone*/);
	TIME *ptm GPOS_ASSERTS_ONLY = clib::Localtime_r(&tv.tv_sec, &tm);

	GPOS_ASSERT(nullptr != ptm && "Failed to get local time");

	WCHAR wszBuf[GPOS_FILE_NAME_BUF_SIZE];
	CWStringStatic str(wszBuf, GPOS_ARRAY_SIZE(wszBuf));

	str.AppendFormat(wszFileNameTemplate, tm.tm_year + 1900, tm.tm_mon + 1,
					 tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, ulSessionId,
					 ulCmdId);

	INT iResult = (INT) clib::Wcstombs(buf, wszBuf, length);

	GPOS_RTL_ASSERT((INT) 0 < iResult && iResult <= (INT) str.Length());

	buf[length - 1] = '\0';

	GPOS_DELETE(filename_template);
}

// return the mapping of the given colref based on the given hashmap
CColRef *
CUtils::PcrRemap(const CColRef *colref,
				 gpos::pointer<UlongToColRefMap *> colref_mapping,
				 BOOL
#ifdef GPOS_DEBUG
					 must_exist
#endif	//GPOS_DEBUG
)
{
	GPOS_ASSERT(nullptr != colref);
	GPOS_ASSERT(nullptr != colref_mapping);

	ULONG id = colref->Id();
	CColRef *pcrMapped = colref_mapping->Find(&id);

	if (nullptr != pcrMapped)
	{
		GPOS_ASSERT(colref != pcrMapped);
		return pcrMapped;
	}

	GPOS_ASSERT(!must_exist && "Column does not exist in hashmap");
	return const_cast<CColRef *>(colref);
}

// create a new colrefset corresponding to the given colrefset and based on the given mapping
gpos::owner<CColRefSet *>
CUtils::PcrsRemap(CMemoryPool *mp, gpos::pointer<CColRefSet *> pcrs,
				  gpos::pointer<UlongToColRefMap *> colref_mapping,
				  BOOL must_exist)
{
	GPOS_ASSERT(nullptr != pcrs);
	GPOS_ASSERT(nullptr != colref_mapping);

	gpos::owner<CColRefSet *> pcrsMapped = GPOS_NEW(mp) CColRefSet(mp);

	CColRefSetIter crsi(*pcrs);
	while (crsi.Advance())
	{
		CColRef *colref = crsi.Pcr();
		CColRef *pcrMapped = PcrRemap(colref, colref_mapping, must_exist);
		pcrsMapped->Include(pcrMapped);
	}

	return pcrsMapped;
}

// create an array of column references corresponding to the given array
// and based on the given mapping
gpos::owner<CColRefArray *>
CUtils::PdrgpcrRemap(CMemoryPool *mp,
					 gpos::pointer<CColRefArray *> colref_array,
					 gpos::pointer<UlongToColRefMap *> colref_mapping,
					 BOOL must_exist)
{
	GPOS_ASSERT(nullptr != colref_array);
	GPOS_ASSERT(nullptr != colref_mapping);

	gpos::owner<CColRefArray *> pdrgpcrNew = GPOS_NEW(mp) CColRefArray(mp);

	const ULONG length = colref_array->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		CColRef *colref = (*colref_array)[ul];
		CColRef *pcrMapped = PcrRemap(colref, colref_mapping, must_exist);
		pdrgpcrNew->Append(pcrMapped);
	}

	return pdrgpcrNew;
}

// ceate an array of column references corresponding to the given array
// and based on the given mapping. Create new colrefs if necessary
gpos::owner<CColRefArray *>
CUtils::PdrgpcrRemapAndCreate(CMemoryPool *mp,
							  gpos::pointer<CColRefArray *> colref_array,
							  gpos::pointer<UlongToColRefMap *> colref_mapping)
{
	GPOS_ASSERT(nullptr != colref_array);
	GPOS_ASSERT(nullptr != colref_mapping);

	// get column factory from optimizer context object
	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();

	gpos::owner<CColRefArray *> pdrgpcrNew = GPOS_NEW(mp) CColRefArray(mp);

	const ULONG length = colref_array->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		CColRef *colref = (*colref_array)[ul];
		ULONG id = colref->Id();
		CColRef *pcrMapped = colref_mapping->Find(&id);
		if (nullptr == pcrMapped)
		{
			// not found in hashmap, so create a new colref and add to hashmap
			pcrMapped = col_factory->PcrCopy(colref);

			BOOL result GPOS_ASSERTS_ONLY =
				colref_mapping->Insert(GPOS_NEW(mp) ULONG(id), pcrMapped);
			GPOS_ASSERT(result);
		}

		pdrgpcrNew->Append(pcrMapped);
	}

	return pdrgpcrNew;
}

// create an array of column arrays corresponding to the given array
// and based on the given mapping
gpos::owner<CColRef2dArray *>
CUtils::PdrgpdrgpcrRemap(CMemoryPool *mp,
						 gpos::pointer<CColRef2dArray *> pdrgpdrgpcr,
						 gpos::pointer<UlongToColRefMap *> colref_mapping,
						 BOOL must_exist)
{
	GPOS_ASSERT(nullptr != pdrgpdrgpcr);
	GPOS_ASSERT(nullptr != colref_mapping);

	gpos::owner<CColRef2dArray *> pdrgpdrgpcrNew =
		GPOS_NEW(mp) CColRef2dArray(mp);

	const ULONG arity = pdrgpdrgpcr->Size();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		gpos::owner<CColRefArray *> colref_array =
			PdrgpcrRemap(mp, (*pdrgpdrgpcr)[ul], colref_mapping, must_exist);
		pdrgpdrgpcrNew->Append(colref_array);
	}

	return pdrgpdrgpcrNew;
}

// remap given array of expressions with provided column mappings
gpos::owner<CExpressionArray *>
CUtils::PdrgpexprRemap(CMemoryPool *mp,
					   gpos::pointer<CExpressionArray *> pdrgpexpr,
					   gpos::pointer<UlongToColRefMap *> colref_mapping)
{
	GPOS_ASSERT(nullptr != pdrgpexpr);
	GPOS_ASSERT(nullptr != colref_mapping);

	gpos::owner<CExpressionArray *> pdrgpexprNew =
		GPOS_NEW(mp) CExpressionArray(mp);

	const ULONG arity = pdrgpexpr->Size();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		gpos::pointer<CExpression *> pexpr = (*pdrgpexpr)[ul];
		pdrgpexprNew->Append(pexpr->PexprCopyWithRemappedColumns(
			mp, colref_mapping, true /*must_exist*/));
	}

	return pdrgpexprNew;
}

// create col ID->ColRef mapping using the given ColRef arrays
gpos::owner<UlongToColRefMap *>
CUtils::PhmulcrMapping(CMemoryPool *mp,
					   gpos::pointer<CColRefArray *> pdrgpcrFrom,
					   gpos::pointer<CColRefArray *> pdrgpcrTo)
{
	GPOS_ASSERT(nullptr != pdrgpcrFrom);
	GPOS_ASSERT(nullptr != pdrgpcrTo);

	gpos::owner<UlongToColRefMap *> colref_mapping =
		GPOS_NEW(mp) UlongToColRefMap(mp);
	AddColumnMapping(mp, colref_mapping, pdrgpcrFrom, pdrgpcrTo);

	return colref_mapping;
}

// add col ID->ColRef mappings to the given hashmap based on the given ColRef arrays
void
CUtils::AddColumnMapping(CMemoryPool *mp,
						 gpos::pointer<UlongToColRefMap *> colref_mapping,
						 gpos::pointer<CColRefArray *> pdrgpcrFrom,
						 gpos::pointer<CColRefArray *> pdrgpcrTo)
{
	GPOS_ASSERT(nullptr != colref_mapping);
	GPOS_ASSERT(nullptr != pdrgpcrFrom);
	GPOS_ASSERT(nullptr != pdrgpcrTo);

	const ULONG ulColumns = pdrgpcrFrom->Size();
	GPOS_ASSERT(ulColumns == pdrgpcrTo->Size());

	for (ULONG ul = 0; ul < ulColumns; ul++)
	{
		CColRef *pcrFrom = (*pdrgpcrFrom)[ul];
		ULONG ulFromId = pcrFrom->Id();
		CColRef *pcrTo = (*pdrgpcrTo)[ul];
		GPOS_ASSERT(pcrFrom != pcrTo);

#ifdef GPOS_DEBUG
		BOOL result = false;
#endif	// GPOS_DEBUG
		CColRef *pcrExist = colref_mapping->Find(&ulFromId);
		if (nullptr == pcrExist)
		{
#ifdef GPOS_DEBUG
			result =
#endif	// GPOS_DEBUG
				colref_mapping->Insert(GPOS_NEW(mp) ULONG(ulFromId), pcrTo);
			GPOS_ASSERT(result);
		}
		else
		{
#ifdef GPOS_DEBUG
			result =
#endif	// GPOS_DEBUG
				colref_mapping->Replace(&ulFromId, pcrTo);
		}
		GPOS_ASSERT(result);
	}
}

// create a copy of the array of column references
gpos::owner<CColRefArray *>
CUtils::PdrgpcrExactCopy(CMemoryPool *mp,
						 gpos::pointer<CColRefArray *> colref_array)
{
	gpos::owner<CColRefArray *> pdrgpcrNew = GPOS_NEW(mp) CColRefArray(mp);

	const ULONG num_cols = colref_array->Size();
	for (ULONG ul = 0; ul < num_cols; ul++)
	{
		CColRef *colref = (*colref_array)[ul];
		pdrgpcrNew->Append(colref);
	}

	return pdrgpcrNew;
}

// Create an array of new column references with the same names and types
// as the given column references. If the passed map is not null, mappings
// from old to copied variables are added to it.
gpos::owner<CColRefArray *>
CUtils::PdrgpcrCopy(CMemoryPool *mp, gpos::pointer<CColRefArray *> colref_array,
					BOOL fAllComputed,
					gpos::pointer<UlongToColRefMap *> colref_mapping)
{
	// get column factory from optimizer context object
	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();

	gpos::owner<CColRefArray *> pdrgpcrNew = GPOS_NEW(mp) CColRefArray(mp);

	const ULONG num_cols = colref_array->Size();
	for (ULONG ul = 0; ul < num_cols; ul++)
	{
		CColRef *colref = (*colref_array)[ul];
		CColRef *new_colref = nullptr;
		if (fAllComputed)
		{
			new_colref = col_factory->PcrCreate(colref);
		}
		else
		{
			new_colref = col_factory->PcrCopy(colref);
		}

		pdrgpcrNew->Append(new_colref);
		if (nullptr != colref_mapping)
		{
			BOOL fInserted GPOS_ASSERTS_ONLY = colref_mapping->Insert(
				GPOS_NEW(mp) ULONG(colref->Id()), new_colref);
			GPOS_ASSERT(fInserted);
		}
	}

	return pdrgpcrNew;
}

// equality check between two arrays of column refs. Inputs can be NULL
BOOL
CUtils::Equals(gpos::pointer<CColRefArray *> pdrgpcrFst,
			   gpos::pointer<CColRefArray *> pdrgpcrSnd)
{
	if (nullptr == pdrgpcrFst || nullptr == pdrgpcrSnd)
	{
		return (nullptr == pdrgpcrFst && nullptr == pdrgpcrSnd);
	}

	return pdrgpcrFst->Equals(pdrgpcrSnd);
}

// compute hash value for an array of column references
ULONG
CUtils::UlHashColArray(gpos::pointer<const CColRefArray *> colref_array,
					   const ULONG ulMaxCols)
{
	ULONG ulHash = 0;

	const ULONG length = colref_array->Size();
	for (ULONG ul = 0; ul < length && ul < ulMaxCols; ul++)
	{
		CColRef *colref = (*colref_array)[ul];
		ulHash = gpos::CombineHashes(ulHash, gpos::HashPtr<CColRef>(colref));
	}

	return ulHash;
}

// Return the set of column reference from the CTE Producer corresponding to the
// subset of input columns from the CTE Consumer
gpos::owner<CColRefSet *>
CUtils::PcrsCTEProducerColumns(
	CMemoryPool *mp, gpos::pointer<CColRefSet *> pcrsInput,
	gpos::pointer<CLogicalCTEConsumer *> popCTEConsumer)
{
	gpos::pointer<CExpression *> pexprProducer =
		COptCtxt::PoctxtFromTLS()->Pcteinfo()->PexprCTEProducer(
			popCTEConsumer->UlCTEId());
	GPOS_ASSERT(nullptr != pexprProducer);
	gpos::pointer<CLogicalCTEProducer *> popProducer =
		gpos::dyn_cast<CLogicalCTEProducer>(pexprProducer->Pop());

	gpos::owner<CColRefArray *> pdrgpcrInput = pcrsInput->Pdrgpcr(mp);
	gpos::owner<UlongToColRefMap *> colref_mapping =
		PhmulcrMapping(mp, popCTEConsumer->Pdrgpcr(), popProducer->Pdrgpcr());
	gpos::owner<CColRefArray *> pdrgpcrOutput =
		PdrgpcrRemap(mp, pdrgpcrInput, colref_mapping, true /*must_exist*/);

	gpos::owner<CColRefSet *> pcrsCTEProducer = GPOS_NEW(mp) CColRefSet(mp);
	pcrsCTEProducer->Include(pdrgpcrOutput);

	colref_mapping->Release();
	pdrgpcrInput->Release();
	pdrgpcrOutput->Release();

	return pcrsCTEProducer;
}

// Construct the join condition (AND-tree) of INDF condition
// from the array of input column reference arrays
gpos::owner<CExpression *>
CUtils::PexprConjINDFCond(CMemoryPool *mp,
						  gpos::pointer<CColRef2dArray *> pdrgpdrgpcrInput)
{
	GPOS_ASSERT(nullptr != pdrgpdrgpcrInput);
	GPOS_ASSERT(2 == pdrgpdrgpcrInput->Size());

	// assemble the new scalar condition
	gpos::owner<CExpression *> pexprScCond = nullptr;
	const ULONG length = (*pdrgpdrgpcrInput)[0]->Size();
	GPOS_ASSERT(0 != length);
	GPOS_ASSERT(length == (*pdrgpdrgpcrInput)[1]->Size());

	gpos::owner<CExpressionArray *> pdrgpexprInput =
		GPOS_NEW(mp) CExpressionArray(mp, length);
	for (ULONG ul = 0; ul < length; ul++)
	{
		CColRef *pcrLeft = (*(*pdrgpdrgpcrInput)[0])[ul];
		CColRef *pcrRight = (*(*pdrgpdrgpcrInput)[1])[ul];
		GPOS_ASSERT(pcrLeft != pcrRight);

		pdrgpexprInput->Append(CUtils::PexprNegate(
			mp, CUtils::PexprIDF(mp, CUtils::PexprScalarIdent(mp, pcrLeft),
								 CUtils::PexprScalarIdent(mp, pcrRight))));
	}

	pexprScCond =
		CPredicateUtils::PexprConjunction(mp, std::move(pdrgpexprInput));

	return pexprScCond;
}

// return index of the set containing given column; if column is not found, return gpos::ulong_max
ULONG
CUtils::UlPcrIndexContainingSet(gpos::pointer<CColRefSetArray *> pdrgpcrs,
								const CColRef *colref)
{
	GPOS_ASSERT(nullptr != pdrgpcrs);

	const ULONG size = pdrgpcrs->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		gpos::pointer<CColRefSet *> pcrs = (*pdrgpcrs)[ul];
		if (pcrs->FMember(colref))
		{
			return ul;
		}
	}

	return gpos::ulong_max;
}

// cast the input expression to the destination mdid
gpos::owner<CExpression *>
CUtils::PexprCast(CMemoryPool *mp, CMDAccessor *md_accessor, CExpression *pexpr,
				  IMDId *mdid_dest)
{
	GPOS_ASSERT(nullptr != mdid_dest);
	gpos::pointer<IMDId *> mdid_src =
		gpos::dyn_cast<CScalar>(pexpr->Pop())->MdidType();
	GPOS_ASSERT(
		CMDAccessorUtils::FCastExists(md_accessor, mdid_src, mdid_dest));

	gpos::pointer<const IMDCast *> pmdcast =
		md_accessor->Pmdcast(mdid_src, mdid_dest);

	mdid_dest->AddRef();
	pmdcast->GetCastFuncMdId()->AddRef();
	gpos::owner<CExpression *> pexprCast;

	if (pmdcast->GetMDPathType() == IMDCast::EmdtArrayCoerce)
	{
		gpos::pointer<CMDArrayCoerceCastGPDB *> parrayCoerceCast =
			const_cast<CMDArrayCoerceCastGPDB *>(
				gpos::cast<CMDArrayCoerceCastGPDB>(pmdcast));
		pexprCast = GPOS_NEW(mp) CExpression(
			mp,
			GPOS_NEW(mp) CScalarArrayCoerceExpr(
				mp, parrayCoerceCast->GetCastFuncMdId(), mdid_dest,
				parrayCoerceCast->TypeModifier(),
				parrayCoerceCast->IsExplicit(),
				(COperator::ECoercionForm) parrayCoerceCast->GetCoercionForm(),
				parrayCoerceCast->Location()),
			pexpr);
	}
	else if (pmdcast->GetMDPathType() == IMDCast::EmdtCoerceViaIO)
	{
		gpos::owner<CScalarCoerceViaIO *> op = GPOS_NEW(mp)
			CScalarCoerceViaIO(mp, mdid_dest, default_type_modifier,
							   COperator::EcfImplicitCast, -1 /* location */);
		pexprCast = GPOS_NEW(mp) CExpression(mp, op, pexpr);
	}
	else
	{
		gpos::owner<CScalarCast *> popCast =
			GPOS_NEW(mp) CScalarCast(mp, mdid_dest, pmdcast->GetCastFuncMdId(),
									 pmdcast->IsBinaryCoercible());
		pexprCast = GPOS_NEW(mp) CExpression(mp, popCast, pexpr);
	}

	return pexprCast;
}

// check whether a colref array contains repeated items
BOOL
CUtils::FHasDuplicates(gpos::pointer<const CColRefArray *> colref_array)
{
	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();
	gpos::owner<CColRefSet *> pcrs = GPOS_NEW(mp) CColRefSet(mp);

	const ULONG length = colref_array->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		CColRef *colref = (*colref_array)[ul];
		if (pcrs->FMember(colref))
		{
			pcrs->Release();
			return true;
		}

		pcrs->Include(colref);
	}

	pcrs->Release();
	return false;
}

// construct a logical join expression operator of the given type, with the given children
gpos::owner<CExpression *>
CUtils::PexprLogicalJoin(CMemoryPool *mp, EdxlJoinType edxljointype,
						 gpos::owner<CExpressionArray *> pdrgpexpr)
{
	gpos::owner<COperator *> pop = nullptr;
	switch (edxljointype)
	{
		case EdxljtInner:
			pop = GPOS_NEW(mp) CLogicalNAryJoin(mp);
			break;

		case EdxljtLeft:
			pop = GPOS_NEW(mp) CLogicalLeftOuterJoin(mp);
			break;

		case EdxljtLeftAntiSemijoin:
			pop = GPOS_NEW(mp) CLogicalLeftAntiSemiJoin(mp);
			break;

		case EdxljtLeftAntiSemijoinNotIn:
			pop = GPOS_NEW(mp) CLogicalLeftAntiSemiJoinNotIn(mp);
			break;

		case EdxljtFull:
			pop = GPOS_NEW(mp) CLogicalFullOuterJoin(mp);
			break;

		default:
			GPOS_ASSERT(!"Unsupported join type");
	}

	return GPOS_NEW(mp) CExpression(mp, std::move(pop), std::move(pdrgpexpr));
}

// construct an array of scalar ident expressions from the given array of column references
gpos::owner<CExpressionArray *>
CUtils::PdrgpexprScalarIdents(CMemoryPool *mp,
							  gpos::pointer<CColRefArray *> colref_array)
{
	GPOS_ASSERT(nullptr != colref_array);

	gpos::owner<CExpressionArray *> pdrgpexpr =
		GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG length = colref_array->Size();

	for (ULONG ul = 0; ul < length; ul++)
	{
		CColRef *colref = (*colref_array)[ul];
		gpos::owner<CExpression *> pexpr = PexprScalarIdent(mp, colref);
		pdrgpexpr->Append(pexpr);
	}

	return pdrgpexpr;
}

// return used columns by expressions in the given array
gpos::owner<CColRefSet *>
CUtils::PcrsExtractColumns(CMemoryPool *mp,
						   gpos::pointer<const CExpressionArray *> pdrgpexpr)
{
	GPOS_ASSERT(nullptr != pdrgpexpr);
	gpos::owner<CColRefSet *> pcrs = GPOS_NEW(mp) CColRefSet(mp);

	const ULONG length = pdrgpexpr->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		gpos::pointer<CExpression *> pexpr = (*pdrgpexpr)[ul];
		pcrs->Include(pexpr->DeriveUsedColumns());
	}

	return pcrs;
}

// returns a new bitset of the given length, where all the bits are set
gpos::owner<CBitSet *>
CUtils::PbsAllSet(CMemoryPool *mp, ULONG size)
{
	gpos::owner<CBitSet *> pbs = GPOS_NEW(mp) CBitSet(mp, size);
	for (ULONG ul = 0; ul < size; ul++)
	{
		pbs->ExchangeSet(ul);
	}

	return pbs;
}

// returns a new bitset, setting the bits in the given array
gpos::owner<CBitSet *>
CUtils::Pbs(CMemoryPool *mp, gpos::pointer<ULongPtrArray *> pdrgpul)
{
	GPOS_ASSERT(nullptr != pdrgpul);
	gpos::owner<CBitSet *> pbs = GPOS_NEW(mp) CBitSet(mp);

	const ULONG length = pdrgpul->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		ULONG *pul = (*pdrgpul)[ul];
		pbs->ExchangeSet(*pul);
	}

	return pbs;
}

// Helper to create a dummy constant table expression;
// the table has one boolean column with value True and one row
gpos::owner<CExpression *>
CUtils::PexprLogicalCTGDummy(CMemoryPool *mp)
{
	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	// generate a bool column
	gpos::pointer<const IMDTypeBool *> pmdtypebool =
		md_accessor->PtMDType<IMDTypeBool>();
	CColRef *colref =
		col_factory->PcrCreate(pmdtypebool, default_type_modifier);
	gpos::owner<CColRefArray *> pdrgpcrCTG = GPOS_NEW(mp) CColRefArray(mp);
	pdrgpcrCTG->Append(colref);

	// generate a bool datum
	gpos::owner<IDatumArray *> pdrgpdatum = GPOS_NEW(mp) IDatumArray(mp);
	gpos::owner<IDatumBool *> datum =
		pmdtypebool->CreateBoolDatum(mp, false /*value*/, false /*is_null*/);
	pdrgpdatum->Append(std::move(datum));
	gpos::owner<IDatum2dArray *> pdrgpdrgpdatum =
		GPOS_NEW(mp) IDatum2dArray(mp);
	pdrgpdrgpdatum->Append(std::move(pdrgpdatum));

	return GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalConstTableGet(mp, std::move(pdrgpcrCTG),
											   std::move(pdrgpdrgpdatum)));
}

// map a column from source array to destination array based on position
CColRef *
CUtils::PcrMap(CColRef *pcrSource, gpos::pointer<CColRefArray *> pdrgpcrSource,
			   gpos::pointer<CColRefArray *> pdrgpcrTarget)
{
	const ULONG num_cols = pdrgpcrSource->Size();
	GPOS_ASSERT(num_cols == pdrgpcrTarget->Size());

	CColRef *pcrTarget = nullptr;
	for (ULONG ul = 0; nullptr == pcrTarget && ul < num_cols; ul++)
	{
		if ((*pdrgpcrSource)[ul] == pcrSource)
		{
			pcrTarget = (*pdrgpcrTarget)[ul];
		}
	}

	return pcrTarget;
}

// Check if duplicate values can be generated when executing the given Motion expression,
// duplicates occur if Motion's input has replicated/universal distribution,
// which means that we have exactly the same copy of input on each host,
BOOL
CUtils::FDuplicateHazardMotion(gpos::pointer<CExpression *> pexprMotion)
{
	GPOS_ASSERT(nullptr != pexprMotion);
	GPOS_ASSERT(CUtils::FPhysicalMotion(pexprMotion->Pop()));

	gpos::pointer<CExpression *> pexprChild = (*pexprMotion)[0];
	gpos::pointer<CDrvdPropPlan *> pdpplanChild =
		gpos::dyn_cast<CDrvdPropPlan>(pexprChild->PdpDerive());
	gpos::pointer<CDistributionSpec *> pdsChild = pdpplanChild->Pds();
	CDistributionSpec::EDistributionType edtChild = pdsChild->Edt();

	BOOL fReplicatedInput =
		CDistributionSpec::EdtStrictReplicated == edtChild ||
		CDistributionSpec::EdtUniversal == edtChild ||
		CDistributionSpec::EdtTaintedReplicated == edtChild;

	return fReplicatedInput;
}

// Collapse the top two project nodes like this, if unable return NULL;
//
// clang-format off
//	+--CLogicalProject                                            <-- pexpr
//		|--CLogicalProject                                        <-- pexprRel
//		|  |--CLogicalGet "t" ("t"), Columns: ["a" (0), "b" (1)]  <-- pexprChildRel
//		|  +--CScalarProjectList                                  <-- pexprChildScalar
//		|     +--CScalarProjectElement "c" (9)
//		|        +--CScalarFunc ()
//		|           |--CScalarIdent "a" (0)
//		|           +--CScalarConst ()
//		+--CScalarProjectList                                     <-- pexprScalar
//		   +--CScalarProjectElement "d" (10)                      <-- pexprPrE
//			  +--CScalarFunc ()
//				 |--CScalarIdent "b" (1)
//				 +--CScalarConst ()
// clang-format on
gpos::owner<CExpression *>
CUtils::PexprCollapseProjects(CMemoryPool *mp,
							  gpos::pointer<CExpression *> pexpr)
{
	GPOS_ASSERT(nullptr != pexpr);

	if (pexpr->Pop()->Eopid() != COperator::EopLogicalProject)
	{
		// not a project node
		return nullptr;
	}

	gpos::pointer<CExpression *> pexprRel = (*pexpr)[0];
	gpos::pointer<CExpression *> pexprScalar = (*pexpr)[1];

	if (pexprRel->Pop()->Eopid() != COperator::EopLogicalProject)
	{
		// not a project node
		return nullptr;
	}

	CExpression *pexprChildRel = (*pexprRel)[0];
	gpos::pointer<CExpression *> pexprChildScalar = (*pexprRel)[1];

	gpos::owner<CColRefSet *> pcrsDefinedChild =
		GPOS_NEW(mp) CColRefSet(mp, *pexprChildScalar->DeriveDefinedColumns());

	// array of project elements for the new child project node
	gpos::owner<CExpressionArray *> pdrgpexprPrElChild =
		GPOS_NEW(mp) CExpressionArray(mp);

	// array of project elements that have set returning scalar functions that can be collapsed
	gpos::owner<CExpressionArray *> pdrgpexprSetReturnFunc =
		GPOS_NEW(mp) CExpressionArray(mp);
	ULONG ulCollapsableSetReturnFunc = 0;

	BOOL fChildProjElHasSetReturn =
		pexprChildScalar->DeriveHasNonScalarFunction();

	// iterate over the parent project elements and see if we can add it to the child's project node
	gpos::owner<CExpressionArray *> pdrgpexprPrEl =
		GPOS_NEW(mp) CExpressionArray(mp);
	ULONG ulLenPr = pexprScalar->Arity();
	for (ULONG ul1 = 0; ul1 < ulLenPr; ul1++)
	{
		CExpression *pexprPrE = (*pexprScalar)[ul1];

		gpos::owner<CColRefSet *> pcrsUsed =
			GPOS_NEW(mp) CColRefSet(mp, *pexprPrE->DeriveUsedColumns());

		pexprPrE->AddRef();

		BOOL fHasSetReturn = pexprPrE->DeriveHasNonScalarFunction();

		pcrsUsed->Intersection(pcrsDefinedChild);
		ULONG ulIntersect = pcrsUsed->Size();

		if (fHasSetReturn)
		{
			pdrgpexprSetReturnFunc->Append(pexprPrE);

			if (0 == ulIntersect)
			{
				// there are no columns from the relational child that this project element uses
				ulCollapsableSetReturnFunc++;
			}
		}
		else if (0 == ulIntersect)
		{
			pdrgpexprPrElChild->Append(pexprPrE);
		}
		else
		{
			pdrgpexprPrEl->Append(pexprPrE);
		}

		pcrsUsed->Release();
	}

	const ULONG ulTotalSetRetFunc = pdrgpexprSetReturnFunc->Size();

	if (!fChildProjElHasSetReturn &&
		ulCollapsableSetReturnFunc == ulTotalSetRetFunc)
	{
		// there are set returning functions and
		// all of the set returning functions are collapsabile
		AppendArrayExpr(pdrgpexprSetReturnFunc, pdrgpexprPrElChild);
	}
	else
	{
		// We come here when either
		// 1. None of the parent's project element use a set retuning function
		// 2. Both parent's and relation child's project list has atleast one project element using set
		// returning functions. In this case we should not collapsed into one project to ensure for correctness.
		// 3. In the parent's project list there exists a project element with set returning functions that
		// cannot be collapsed. If the parent's project list has more than one project element with
		// set returning functions we should either collapse all of them or none of them for correctness.

		AppendArrayExpr(pdrgpexprSetReturnFunc, pdrgpexprPrEl);
	}

	// clean up
	pcrsDefinedChild->Release();
	pdrgpexprSetReturnFunc->Release();

	// add all project elements of the origin child project node
	ULONG ulLenChild = pexprChildScalar->Arity();
	for (ULONG ul = 0; ul < ulLenChild; ul++)
	{
		gpos::owner<CExpression *> pexprPrE = (*pexprChildScalar)[ul];
		pexprPrE->AddRef();
		pdrgpexprPrElChild->Append(pexprPrE);
	}

	if (ulLenPr == pdrgpexprPrEl->Size())
	{
		// no candidate project element found for collapsing
		pdrgpexprPrElChild->Release();
		pdrgpexprPrEl->Release();

		return nullptr;
	}

	pexprChildRel->AddRef();
	gpos::owner<CExpression *> pexprProject = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalProject(mp), pexprChildRel,
		GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp),
								 pdrgpexprPrElChild));

	if (0 == pdrgpexprPrEl->Size())
	{
		// no residual project elements
		pdrgpexprPrEl->Release();

		return pexprProject;
	}

	return GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalProject(mp), std::move(pexprProject),
		GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp),
								 std::move(pdrgpexprPrEl)));
}

// append expressions in the source array to destination array
void
CUtils::AppendArrayExpr(gpos::pointer<CExpressionArray *> pdrgpexprSrc,
						gpos::pointer<CExpressionArray *> pdrgpexprDest)
{
	GPOS_ASSERT(nullptr != pdrgpexprSrc);
	GPOS_ASSERT(nullptr != pdrgpexprDest);

	ULONG length = pdrgpexprSrc->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		gpos::owner<CExpression *> pexprPrE = (*pdrgpexprSrc)[ul];
		pexprPrE->AddRef();
		pdrgpexprDest->Append(pexprPrE);
	}
}

// compares two datums. Takes pointer pointer to a datums.
INT
CUtils::IDatumCmp(const void *val1, const void *val2)
{
	gpos::pointer<const IDatum *> dat1 = *(IDatum **) (val1);
	gpos::pointer<const IDatum *> dat2 = *(IDatum **) (val2);

	const IComparator *pcomp = COptCtxt::PoctxtFromTLS()->Pcomp();

	if (pcomp->Equals(dat1, dat2))
	{
		return 0;
	}
	else if (pcomp->IsLessThan(dat1, dat2))
	{
		return -1;
	}

	return 1;
}

// compares two points. Takes pointer pointer to a CPoint.
INT
CUtils::CPointCmp(const void *val1, const void *val2)
{
	gpos::pointer<const CPoint *> p1 = *(CPoint **) (val1);
	gpos::pointer<const CPoint *> p2 = *(CPoint **) (val2);

	if (p1->Equals(p2))
	{
		return 0;
	}
	else if (p1->IsLessThan(p2))
	{
		return -1;
	}

	GPOS_ASSERT(p1->IsGreaterThan(p2));
	return 1;
}

// check if the equivalance classes are disjoint
BOOL
CUtils::FEquivalanceClassesDisjoint(
	CMemoryPool *mp, gpos::pointer<const CColRefSetArray *> pdrgpcrs)
{
	const ULONG length = pdrgpcrs->Size();

	// nothing to check
	if (length == 0)
	{
		return true;
	}

	gpos::owner<ColRefToColRefSetMap *> phmcscrs =
		GPOS_NEW(mp) ColRefToColRefSetMap(mp);

	// populate a hashmap in this loop
	for (ULONG ulFst = 0; ulFst < length; ulFst++)
	{
		gpos::owner<CColRefSet *> pcrsFst = (*pdrgpcrs)[ulFst];

		CColRefSetIter crsi(*pcrsFst);
		while (crsi.Advance())
		{
			CColRef *colref = crsi.Pcr();
			pcrsFst->AddRef();

			// if there is already an entry for the column referance it means the column is
			// part of another set which shows the equivalance classes are not disjoint
			if (!phmcscrs->Insert(colref, pcrsFst))
			{
				phmcscrs->Release();
				pcrsFst->Release();
				return false;
			}
		}
	}

	phmcscrs->Release();
	return true;
}

// check if the equivalance classes are same
BOOL
CUtils::FEquivalanceClassesEqual(CMemoryPool *mp,
								 gpos::pointer<CColRefSetArray *> pdrgpcrsFst,
								 gpos::pointer<CColRefSetArray *> pdrgpcrsSnd)
{
	const ULONG ulLenFrst = pdrgpcrsFst->Size();
	const ULONG ulLenSecond = pdrgpcrsSnd->Size();

	if (ulLenFrst != ulLenSecond)
		return false;

	gpos::owner<ColRefToColRefSetMap *> phmcscrs =
		GPOS_NEW(mp) ColRefToColRefSetMap(mp);
	for (ULONG ulFst = 0; ulFst < ulLenFrst; ulFst++)
	{
		CColRefSet *pcrsFst = (*pdrgpcrsFst)[ulFst];
		CColRefSetIter crsi(*pcrsFst);
		while (crsi.Advance())
		{
			CColRef *colref = crsi.Pcr();
			pcrsFst->AddRef();
			phmcscrs->Insert(colref, pcrsFst);
		}
	}

	for (ULONG ulSnd = 0; ulSnd < ulLenSecond; ulSnd++)
	{
		gpos::pointer<CColRefSet *> pcrsSnd = (*pdrgpcrsSnd)[ulSnd];
		CColRef *colref = pcrsSnd->PcrAny();
		gpos::pointer<CColRefSet *> pcrs = phmcscrs->Find(colref);
		if (!pcrsSnd->Equals(pcrs))
		{
			phmcscrs->Release();
			return false;
		}
	}
	phmcscrs->Release();
	return true;
}

// This function provides a check for a plan with CTE, if both
// CTEProducer and CTEConsumer are executed on the same locality.
// If it is not the case, the plan is bogus and cannot be executed
// by the executor, therefore it throws an exception causing fallback
// to planner.
//
// The overall algorithm for detecting CTE producer and consumer
// inconsistency employs a HashMap while preorder traversing the tree.
// Preorder traversal will guarantee that we visit the producer before
// we visit the consumer. In this regard, when we see a CTE producer,
// we add its CTE id as a key and its execution locality as a value to
// the HashMap.
// And when we encounter the matching CTE consumer while we traverse the
// tree, we check if the locality matches by looking up the CTE id from
// the HashMap. If we see a non-matching locality, we report the
// anamoly.
//
// We change the locality and push it down the tree whenever we detect
// a motion and the motion type enforces a locality change. We pass the
// locality type by value instead of referance to avoid locality changes
// affect parent and sibling localities.
void
CUtils::ValidateCTEProducerConsumerLocality(
	CMemoryPool *mp, gpos::pointer<CExpression *> pexpr, EExecLocalityType eelt,
	gpos::owner<UlongToUlongMap *>
		phmulul	 // Hash Map containing the CTE Producer id and its execution locality
)
{
	gpos::pointer<COperator *> pop = pexpr->Pop();
	if (COperator::EopPhysicalCTEProducer == pop->Eopid())
	{
		// record the location (either master or segment or singleton)
		// where the CTE producer is being executed
		ULONG ulCTEID = gpos::dyn_cast<CPhysicalCTEProducer>(pop)->UlCTEId();
		phmulul->Insert(GPOS_NEW(mp) ULONG(ulCTEID), GPOS_NEW(mp) ULONG(eelt));
	}
	else if (COperator::EopPhysicalCTEConsumer == pop->Eopid())
	{
		ULONG ulCTEID = gpos::dyn_cast<CPhysicalCTEConsumer>(pop)->UlCTEId();
		ULONG *pulLocProducer = phmulul->Find(&ulCTEID);

		// check if the CTEConsumer is being executed in the same location
		// as the CTE Producer
		if (nullptr == pulLocProducer || *pulLocProducer != (ULONG) eelt)
		{
			phmulul->Release();
			GPOS_RAISE(gpopt::ExmaGPOPT,
					   gpopt::ExmiCTEProducerConsumerMisAligned, ulCTEID);
		}
	}
	// In case of a Gather motion, the execution locality is set to segments
	// since the child of Gather motion executes on segments
	else if (COperator::EopPhysicalMotionGather == pop->Eopid())
	{
		eelt = EeltSegments;
	}
	else if (COperator::EopPhysicalMotionHashDistribute == pop->Eopid() ||
			 COperator::EopPhysicalMotionRandom == pop->Eopid() ||
			 COperator::EopPhysicalMotionBroadcast == pop->Eopid())
	{
		// For any of these physical motions, the outer child's execution needs to be
		// tracked for depending upon the distribution spec
		gpos::pointer<CDrvdPropPlan *> pdpplanChild =
			gpos::dyn_cast<CDrvdPropPlan>((*pexpr)[0]->PdpDerive());
		gpos::pointer<CDistributionSpec *> pdsChild = pdpplanChild->Pds();

		eelt = CUtils::ExecLocalityType(pdsChild);
	}

	const ULONG length = pexpr->Arity();
	for (ULONG ul = 0; ul < length; ul++)
	{
		gpos::pointer<CExpression *> pexprChild = (*pexpr)[ul];

		if (!pexprChild->Pop()->FScalar())
		{
			ValidateCTEProducerConsumerLocality(mp, pexprChild, eelt, phmulul);
		}
	}
}

// get execution locality type
CUtils::EExecLocalityType
CUtils::ExecLocalityType(gpos::pointer<CDistributionSpec *> pds)
{
	EExecLocalityType eelt;
	if (CDistributionSpec::EdtSingleton == pds->Edt() ||
		CDistributionSpec::EdtStrictSingleton == pds->Edt())
	{
		gpos::pointer<CDistributionSpecSingleton *> pdss =
			gpos::dyn_cast<CDistributionSpecSingleton>(pds);
		if (pdss->FOnMaster())
		{
			eelt = EeltMaster;
		}
		else
		{
			eelt = EeltSingleton;
		}
	}
	else
	{
		eelt = EeltSegments;
	}
	return eelt;
}

// generate a limit expression on top of the given relational child with the given offset and limit count
gpos::owner<CExpression *>
CUtils::PexprLimit(CMemoryPool *mp, gpos::owner<CExpression *> pexpr,
				   ULONG ulOffSet, ULONG count)
{
	GPOS_ASSERT(pexpr);

	gpos::owner<COrderSpec *> pos = GPOS_NEW(mp) COrderSpec(mp);
	gpos::owner<CLogicalLimit *> popLimit = GPOS_NEW(mp)
		CLogicalLimit(mp, std::move(pos), true /* fGlobal */,
					  true /* fHasCount */, false /*fTopLimitUnderDML*/);
	gpos::owner<CExpression *> pexprLimitOffset =
		CUtils::PexprScalarConstInt8(mp, ulOffSet);
	gpos::owner<CExpression *> pexprLimitCount =
		CUtils::PexprScalarConstInt8(mp, count);

	return GPOS_NEW(mp)
		CExpression(mp, std::move(popLimit), std::move(pexpr),
					std::move(pexprLimitOffset), std::move(pexprLimitCount));
}

// generate part oid
BOOL
CUtils::FGeneratePartOid(gpos::pointer<IMDId *> mdid)
{
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	gpos::pointer<const IMDRelation *> pmdrel = md_accessor->RetrieveRel(mdid);

	gpos::pointer<COptimizerConfig *> optimizer_config =
		COptCtxt::PoctxtFromTLS()->GetOptimizerConfig();
	BOOL fInsertSortOnRows =
		(pmdrel->RetrieveRelStorageType() ==
		 IMDRelation::ErelstorageAppendOnlyRows) &&
		(optimizer_config->GetHint()->UlMinNumOfPartsToRequireSortOnInsert() <=
		 pmdrel->PartitionCount());

	return fInsertSortOnRows;
}

// check if a given operator is a ANY subquery
BOOL
CUtils::FAnySubquery(gpos::pointer<COperator *> pop)
{
	GPOS_ASSERT(nullptr != pop);

	BOOL fInSubquery = false;
	if (COperator::EopScalarSubqueryAny == pop->Eopid())
	{
		fInSubquery = true;
	}


	return fInSubquery;
}

// returns the expression under the Nth project element of a CLogicalProject
CExpression *
CUtils::PNthProjectElementExpr(gpos::pointer<CExpression *> pexpr, ULONG ul)
{
	GPOS_ASSERT(pexpr->Pop()->Eopid() == COperator::EopLogicalProject);

	// Logical Project's first child is relational child and the second
	// child is the project list. We initially get the project list and then
	// the nth element in the project list and finally expression under that
	// element.
	return (*(*(*pexpr)[1])[ul])[0];
}

// check if the Project list has an inner reference assuming project list has one projecet element
BOOL
CUtils::FInnerRefInProjectList(gpos::pointer<CExpression *> pexpr)
{
	GPOS_ASSERT(nullptr != pexpr);
	GPOS_ASSERT(COperator::EopLogicalProject == pexpr->Pop()->Eopid());

	// extract output columns of the relational child
	gpos::pointer<CColRefSet *> pcrsOuterOutput =
		(*pexpr)[0]->DeriveOutputColumns();

	// Project List with one project element
	gpos::pointer<CExpression *> pexprInner = (*pexpr)[1];
	GPOS_ASSERT(1 == pexprInner->Arity());
	BOOL fExprHasAnyCrFromCrs =
		CUtils::FExprHasAnyCrFromCrs(pexprInner, pcrsOuterOutput);

	return fExprHasAnyCrFromCrs;
}

// Check if expression tree has a col being referenced in the CColRefSet passed as input
BOOL
CUtils::FExprHasAnyCrFromCrs(gpos::pointer<CExpression *> pexpr,
							 gpos::pointer<CColRefSet *> pcrs)
{
	GPOS_ASSERT(nullptr != pexpr);
	GPOS_ASSERT(nullptr != pcrs);
	CColRef *colref = nullptr;

	COperator::EOperatorId op_id = pexpr->Pop()->Eopid();
	switch (op_id)
	{
		case COperator::EopScalarProjectElement:
		{
			// check project elements
			gpos::pointer<CScalarProjectElement *> popScalarProjectElement =
				gpos::dyn_cast<CScalarProjectElement>(pexpr->Pop());
			colref = (CColRef *) popScalarProjectElement->Pcr();
			if (pcrs->FMember(colref))
				return true;
			break;
		}
		case COperator::EopScalarIdent:
		{
			// Check scalarIdents
			gpos::pointer<CScalarIdent *> popScalarOp =
				gpos::dyn_cast<CScalarIdent>(pexpr->Pop());
			colref = (CColRef *) popScalarOp->Pcr();
			if (pcrs->FMember(colref))
				return true;
			break;
		}
		default:
			break;
	}

	// recursively process children
	const ULONG arity = pexpr->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		if (FExprHasAnyCrFromCrs((*pexpr)[ul], pcrs))
			return true;
	}

	return false;
}

// returns true if expression contains aggregate window function
BOOL
CUtils::FHasAggWindowFunc(gpos::pointer<CExpression *> pexpr)
{
	GPOS_ASSERT(nullptr != pexpr);

	if (COperator::EopScalarWindowFunc == pexpr->Pop()->Eopid())
	{
		gpos::pointer<CScalarWindowFunc *> popScWindowFunc =
			gpos::dyn_cast<CScalarWindowFunc>(pexpr->Pop());
		return popScWindowFunc->FAgg();
	}

	// process children
	BOOL fHasAggWindowFunc = false;
	for (ULONG ul = 0; !fHasAggWindowFunc && ul < pexpr->Arity(); ul++)
	{
		fHasAggWindowFunc = FHasAggWindowFunc((*pexpr)[ul]);
	}

	return fHasAggWindowFunc;
}

BOOL
CUtils::FCrossJoin(gpos::pointer<CExpression *> pexpr)
{
	GPOS_ASSERT(nullptr != pexpr);

	BOOL fCrossJoin = false;
	if (pexpr->Pop()->Eopid() == COperator::EopLogicalInnerJoin)
	{
		GPOS_ASSERT(3 == pexpr->Arity());
		gpos::pointer<CExpression *> pexprPred = (*pexpr)[2];
		if (CUtils::FScalarConstTrue(pexprPred))
			fCrossJoin = true;
	}
	return fCrossJoin;
}

// Determine whether a scalar expression consists only of a scalar id and NDV-preserving
// functions plus casts. If so, return the corresponding CColRef.
BOOL
CUtils::IsExprNDVPreserving(gpos::pointer<CExpression *> pexpr,
							const CColRef **underlying_colref)
{
	gpos::pointer<CExpression *> curr_expr = pexpr;

	*underlying_colref = nullptr;

	// go down the expression tree, visiting the child containing a scalar ident until
	// we found the ident or until we found a non-NDV-preserving function (at which point there
	// is no more need to check)
	while (1)
	{
		gpos::pointer<COperator *> pop = curr_expr->Pop();
		ULONG child_with_scalar_ident = 0;

		switch (pop->Eopid())
		{
			case COperator::EopScalarIdent:
			{
				// we reached the bottom of the expression, return the ColRef
				gpos::pointer<CScalarIdent *> cr =
					gpos::dyn_cast<CScalarIdent>(pop);

				*underlying_colref = cr->Pcr();
				GPOS_ASSERT(1 == pexpr->DeriveUsedColumns()->Size());
				return true;
			}

			case COperator::EopScalarCast:
				// skip over casts
				// Note: We might in the future investigate whether there are some casts
				// that reduce NDVs by too much. Most, if not all, casts that have that potential are
				// converted to functions, though. Examples: timestamp -> date, double precision -> int.
				break;

			case COperator::EopScalarCoalesce:
			{
				// coalesce(col, const1, ... constn) is treated as an NDV-preserving function
				for (ULONG c = 1; c < curr_expr->Arity(); c++)
				{
					if (0 < (*curr_expr)[c]->DeriveUsedColumns()->Size())
					{
						// this coalesce has a ColRef in the second or later arguments, assume for
						// now that this doesn't preserve NDVs (we could add logic to support this case later)
						return false;
					}
				}
				break;
			}
			case COperator::EopScalarFunc:
			{
				// check whether the function is NDV-preserving
				CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
				gpos::pointer<CScalarFunc *> sf =
					gpos::dyn_cast<CScalarFunc>(pop);
				gpos::pointer<const IMDFunction *> pmdfunc =
					md_accessor->RetrieveFunc(sf->FuncMdId());

				if (!pmdfunc->IsNDVPreserving() || 1 != curr_expr->Arity())
				{
					return false;
				}
				break;
			}

			case COperator::EopScalarOp:
			{
				CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
				gpos::pointer<CScalarOp *> so = gpos::dyn_cast<CScalarOp>(pop);
				gpos::pointer<const IMDScalarOp *> pmdscop =
					md_accessor->RetrieveScOp(so->MdIdOp());

				if (!pmdscop->IsNDVPreserving() || 2 != curr_expr->Arity())
				{
					return false;
				}

				// col <op> const is NDV-preserving, and so is const <op> col
				if (0 == (*curr_expr)[1]->DeriveUsedColumns()->Size())
				{
					// col <op> const
					child_with_scalar_ident = 0;
				}
				else if (0 == (*curr_expr)[0]->DeriveUsedColumns()->Size())
				{
					// const <op> col
					child_with_scalar_ident = 1;
				}
				else
				{
					// give up for now, both children reference a column,
					// e.g. col1 <op> col2
					return false;
				}
				break;
			}

			default:
				// anything else we see is considered non-NDV-preserving
				return false;
		}

		curr_expr = (*curr_expr)[child_with_scalar_ident];
	}
}


// search the given array of predicates for predicates with equality or IS NOT
// DISTINCT FROM operators that has one side equal to the given expression, if
// found, return the other side of equality, otherwise return NULL
CExpression *
CUtils::PexprMatchEqualityOrINDF(
	CExpression *pexprToMatch,
	gpos::pointer<CExpressionArray *>
		pdrgpexpr  // array of predicates to inspect
)
{
	GPOS_ASSERT(nullptr != pexprToMatch);
	GPOS_ASSERT(nullptr != pdrgpexpr);

	CExpression *pexprMatching = nullptr;
	const ULONG ulSize = pdrgpexpr->Size();
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		gpos::pointer<CExpression *> pexprPred = (*pdrgpexpr)[ul];
		CExpression *pexprPredOuter, *pexprPredInner;


		if (CPredicateUtils::IsEqualityOp(pexprPred))
		{
			pexprPredOuter = (*pexprPred)[0];
			pexprPredInner = (*pexprPred)[1];
		}
		else if (CPredicateUtils::FINDF(pexprPred))
		{
			pexprPredOuter = (*(*pexprPred)[0])[0];
			pexprPredInner = (*(*pexprPred)[0])[1];
		}
		else
		{
			continue;
		}

		gpos::pointer<IMDId *> pmdidTypeOuter =
			gpos::dyn_cast<CScalar>(pexprPredOuter->Pop())->MdidType();
		gpos::pointer<IMDId *> pmdidTypeInner =
			gpos::dyn_cast<CScalar>(pexprPredInner->Pop())->MdidType();
		if (!pmdidTypeOuter->Equals(pmdidTypeInner))
		{
			// only consider equality of identical types
			continue;
		}

		pexprToMatch =
			CCastUtils::PexprWithoutBinaryCoercibleCasts(pexprToMatch);
		if (CUtils::Equals(
				CCastUtils::PexprWithoutBinaryCoercibleCasts(pexprPredOuter),
				pexprToMatch))
		{
			pexprMatching = pexprPredInner;
			break;
		}

		if (CUtils::Equals(
				CCastUtils::PexprWithoutBinaryCoercibleCasts(pexprPredInner),
				pexprToMatch))
		{
			pexprMatching = pexprPredOuter;
			break;
		}
	}

	if (nullptr != pexprMatching)
		return CCastUtils::PexprWithoutBinaryCoercibleCasts(pexprMatching);
	return pexprMatching;
}

// from the input join expression, remove the inferred predicates
// and return the new join expression without inferred predicate
gpos::owner<CExpression *>
CUtils::MakeJoinWithoutInferredPreds(CMemoryPool *mp,
									 gpos::pointer<CExpression *> join_expr)
{
	GPOS_ASSERT(COperator::EopLogicalInnerJoin == join_expr->Pop()->Eopid());

	CExpressionHandle expression_handle(mp);
	expression_handle.Attach(join_expr);
	gpos::pointer<CExpression *> scalar_expr =
		expression_handle.PexprScalarExactChild(join_expr->Arity() - 1,
												true /*error_on_null_return*/);
	gpos::owner<CExpression *> scalar_expr_without_inferred_pred =
		CPredicateUtils::PexprRemoveImpliedConjuncts(mp, scalar_expr,
													 expression_handle);

	// create a new join expression using the scalar expr without inferred predicate
	CExpression *left_child_expr = (*join_expr)[0];
	CExpression *right_child_expr = (*join_expr)[1];
	left_child_expr->AddRef();
	right_child_expr->AddRef();
	gpos::owner<COperator *> join_op = join_expr->Pop();
	join_op->AddRef();
	return GPOS_NEW(mp)
		CExpression(mp, std::move(join_op), left_child_expr, right_child_expr,
					std::move(scalar_expr_without_inferred_pred));
}

// check if the input expr array contains the expr
BOOL
CUtils::Contains(gpos::pointer<const CExpressionArray *> exprs,
				 gpos::pointer<CExpression *> expr_to_match)
{
	if (nullptr == exprs)
	{
		return false;
	}

	BOOL contains = false;
	for (ULONG ul = 0; ul < exprs->Size() && !contains; ul++)
	{
		gpos::pointer<CExpression *> expr = (*exprs)[ul];
		contains = CUtils::Equals(expr, expr_to_match);
	}
	return contains;
}

BOOL
CUtils::Equals(gpos::pointer<const CExpressionArrays *> exprs_arr,
			   gpos::pointer<const CExpressionArrays *> other_exprs_arr)
{
	GPOS_CHECK_STACK_SIZE;

	// NULL arrays are equal
	if (nullptr == exprs_arr || nullptr == other_exprs_arr)
	{
		return nullptr == exprs_arr && nullptr == other_exprs_arr;
	}

	// do pointer comparision
	if (exprs_arr == other_exprs_arr)
	{
		return true;
	}

	// if the size is not equal, the two arrays are not equal
	if (exprs_arr->Size() != other_exprs_arr->Size())
	{
		return false;
	}

	// if all the elements are equal, then both the arrays are equal
	BOOL equal = true;
	for (ULONG id = 0; id < exprs_arr->Size() && equal; id++)
	{
		equal = CUtils::Equals((*exprs_arr)[id], (*other_exprs_arr)[id]);
	}
	return equal;
}

BOOL
CUtils::Equals(gpos::pointer<const IMdIdArray *> mdids,
			   gpos::pointer<const IMdIdArray *> other_mdids)
{
	GPOS_CHECK_STACK_SIZE;

	// NULL arrays are equal
	if (nullptr == mdids || nullptr == other_mdids)
	{
		return nullptr == mdids && nullptr == other_mdids;
	}

	// do pointer comparision
	if (mdids == other_mdids)
	{
		return true;
	}

	// if the size is not equal, the two arrays are not equal
	if (mdids->Size() != other_mdids->Size())
	{
		return false;
	}

	// if all the elements are equal, then both the arrays are equal
	for (ULONG id = 0; id < mdids->Size(); id++)
	{
		if (!CUtils::Equals((*mdids)[id], (*other_mdids)[id]))
		{
			return false;
		}
	}
	return true;
}

BOOL
CUtils::Equals(gpos::pointer<const IMDId *> mdid,
			   gpos::pointer<const IMDId *> other_mdid)
{
	if ((mdid == nullptr) ^ (other_mdid == nullptr))
	{
		return false;
	}

	return (mdid == other_mdid) || mdid->Equals(other_mdid);
}

// operators from which the inferred predicates can be removed
// NB: currently, only inner join is included, but we can add more later.
BOOL
CUtils::CanRemoveInferredPredicates(COperator::EOperatorId op_id)
{
	return op_id == COperator::EopLogicalInnerJoin;
}

gpos::owner<CExpressionArrays *>
CUtils::GetCombinedExpressionArrays(
	CMemoryPool *mp, gpos::pointer<CExpressionArrays *> exprs_array,
	gpos::pointer<CExpressionArrays *> exprs_array_other)
{
	gpos::owner<CExpressionArrays *> result_exprs =
		GPOS_NEW(mp) CExpressionArrays(mp);
	AddExprs(result_exprs, exprs_array);
	AddExprs(result_exprs, exprs_array_other);

	return result_exprs;
}

void
CUtils::AddExprs(gpos::pointer<CExpressionArrays *> results_exprs,
				 gpos::pointer<CExpressionArrays *> input_exprs)
{
	GPOS_ASSERT(nullptr != results_exprs);
	GPOS_ASSERT(nullptr != input_exprs);
	for (ULONG ul = 0; ul < input_exprs->Size(); ul++)
	{
		gpos::owner<CExpressionArray *> exprs = (*input_exprs)[ul];
		exprs->AddRef();
		results_exprs->Append(exprs);
	}
	GPOS_ASSERT(results_exprs->Size() >= input_exprs->Size());
}
// EOF
