//	Greenplum Database
//	Copyright (C) 2016 VMware, Inc. or its affiliates.

#ifndef GPOPT_CColConstraintsArrayMapper_H
#define GPOPT_CColConstraintsArrayMapper_H

#include "gpos/common/owner.h"
#include "gpos/memory/CMemoryPool.h"

#include "gpopt/base/CConstraint.h"
#include "gpopt/base/IColConstraintsMapper.h"

namespace gpopt
{
class CColConstraintsArrayMapper : public IColConstraintsMapper
{
public:
	CColConstraintsArrayMapper(gpos::CMemoryPool *mp,
							   gpos::Ref<CConstraintArray> pdrgpcnstr);
	gpos::Ref<CConstraintArray> PdrgPcnstrLookup(CColRef *colref) override;

	~CColConstraintsArrayMapper() override;

private:
	gpos::CMemoryPool *m_mp;
	gpos::Ref<CConstraintArray> m_pdrgpcnstr;
};
}  // namespace gpopt

#endif	//GPOPT_CColConstraintsArrayMapper_H
