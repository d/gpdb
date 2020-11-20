//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CDrvdPropCtxtPlan.h
//
//	@doc:
//		Container of information passed among expression nodes during
//		derivation of plan properties
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CDrvdPropCtxtPlan_H
#define GPOPT_CDrvdPropCtxtPlan_H

#include "gpos/base.h"

#include "gpopt/base/CDrvdPropCtxt.h"
#include "gpopt/base/CCTEMap.h"


namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CDrvdPropCtxtPlan
//
//	@doc:
//		Container of information passed among expression nodes during
//		derivation of plan properties
//
//---------------------------------------------------------------------------
class CDrvdPropCtxtPlan : public CDrvdPropCtxt
{
private:
	// map of CTE id to producer plan properties
	UlongToDrvdPropPlanMap *m_phmulpdpCTEs;

	// the number of expected partition selectors
	ULONG m_ulExpectedPartitionSelectors;

	// if true, a call to AddProps updates the CTE.
	BOOL m_fUpdateCTEMap;

protected:
	// copy function
	CDrvdPropCtxt *PdpctxtCopy(CMemoryPool *mp) const override;

	// add props to context
	void AddProps(CDrvdProp *pdp) override;

public:
	CDrvdPropCtxtPlan(const CDrvdPropCtxtPlan &) = delete;

	// ctor
	CDrvdPropCtxtPlan(CMemoryPool *mp, BOOL fUpdateCTEMap = true);

	// dtor
	~CDrvdPropCtxtPlan() override;

	ULONG
	UlExpectedPartitionSelectors() const
	{
		return m_ulExpectedPartitionSelectors;
	}

	// set the number of expected partition selectors based on the given
	// operator and the given cost context
	void SetExpectedPartitionSelectors(COperator *pop, CCostContext *pcc);

	// print
	IOstream &OsPrint(IOstream &os) const override;

	// return the plan properties of CTE producer with given id
	CDrvdPropPlan *PdpplanCTEProducer(ULONG ulCTEId) const;

	// copy plan properties of given CTE prdoucer
	void CopyCTEProducerProps(CDrvdPropPlan *pdpplan, ULONG ulCTEId);

#ifdef GPOS_DEBUG

	// is it a plan property context?
	BOOL
	FPlan() const override
	{
		return true;
	}

#endif	// GPOS_DEBUG

	// conversion function
	static CDrvdPropCtxtPlan *
	PdpctxtplanConvert(CDrvdPropCtxt *pdpctxt)
	{
		GPOS_ASSERT(NULL != pdpctxt);

		return reinterpret_cast<CDrvdPropCtxtPlan *>(pdpctxt);
	}

};	// class CDrvdPropCtxtPlan

}  // namespace gpopt


#endif	// !GPOPT_CDrvdPropCtxtPlan_H
