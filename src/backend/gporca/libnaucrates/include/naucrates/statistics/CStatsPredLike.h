//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CStatsPredLike.h
//
//	@doc:
//		LIKE filter for statistics
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CStatsPredLike_H
#define GPNAUCRATES_CStatsPredLike_H

#include "gpos/base.h"
#include "gpos/common/owner.h"

#include "gpopt/operators/CExpression.h"
#include "naucrates/md/IMDType.h"
#include "naucrates/statistics/CStatsPred.h"

namespace gpnaucrates
{
using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		CStatsPredLike
//
//	@doc:
//		LIKE filter for statistics
//---------------------------------------------------------------------------
class CStatsPredLike : public CStatsPred
{
private:
	// left hand side of the LIKE expression
	gpos::owner<CExpression *> m_expr_left;

	// right hand side of the LIKE expression
	gpos::owner<CExpression *> m_expr_right;

	// default scale factor
	CDouble m_default_scale_factor;

public:
	CStatsPredLike &operator=(CStatsPredLike &) = delete;

	CStatsPredLike(const CStatsPredLike &) = delete;

	// ctor
	CStatsPredLike(ULONG colid, gpos::owner<CExpression *> expr_left,
				   gpos::owner<CExpression *> expr_right,
				   CDouble default_scale_factor);

	// dtor
	~CStatsPredLike() override;

	// the column identifier on which the predicates are on
	ULONG GetColId() const override;

	// filter type id
	EStatsPredType
	GetPredStatsType() const override
	{
		return CStatsPred::EsptLike;
	}

	// left hand side of the LIKE expression
	virtual gpos::pointer<CExpression *>
	GetExprOnLeft() const
	{
		return m_expr_left;
	}

	// right hand side of the LIKE expression
	virtual gpos::pointer<CExpression *>
	GetExprOnRight() const
	{
		return m_expr_right;
	}

	// default scale factor
	virtual CDouble DefaultScaleFactor() const;

	// conversion function
	static gpos::cast_func<CStatsPredLike *>
	ConvertPredStats(CStatsPred *pred_stats)
	{
		GPOS_ASSERT(nullptr != pred_stats);
		GPOS_ASSERT(CStatsPred::EsptLike == pred_stats->GetPredStatsType());

		return dynamic_cast<CStatsPredLike *>(pred_stats);
	}

};	// class CStatsPredLike
}  // namespace gpnaucrates

#endif	// !GPNAUCRATES_CStatsPredLike_H

// EOF
