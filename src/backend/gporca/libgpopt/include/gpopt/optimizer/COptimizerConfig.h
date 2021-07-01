//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 VMware, Inc. or its affiliates.
//
//	@filename:
//		COptimizerConfig.h
//
//	@doc:
//		Configurations used by the optimizer
//---------------------------------------------------------------------------

#ifndef GPOPT_COptimizerConfig_H
#define GPOPT_COptimizerConfig_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/common/CRefCount.h"
#include "gpos/common/owner.h"

#include "gpopt/base/CWindowOids.h"
#include "gpopt/cost/ICostModel.h"
#include "gpopt/engine/CCTEConfig.h"
#include "gpopt/engine/CEnumeratorConfig.h"
#include "gpopt/engine/CHint.h"
#include "gpopt/engine/CStatisticsConfig.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		COptimizerConfig
//
//	@doc:
//		Configuration parameters of the optimizer including damping factors used
//		during statistics derivation, CTE inlining cut-off threshold, Id of plan to
//		be extracted (if plan enumeration is enabled), number of plans to be sampled
//		from the space (if plan sampling is enabled) etc.
//
//		Most of these configurations can be changed from outside ORCA through
//		GUCs. They are also included in optimizer’s minidumps under
//		<dxl:OptimizerConfig> element
//
//---------------------------------------------------------------------------
class COptimizerConfig : public CRefCount
{
private:
	// plan enumeration configuration
	gpos::owner<CEnumeratorConfig *> m_enumerator_cfg;

	// statistics configuration
	gpos::owner<CStatisticsConfig *> m_stats_conf;

	// CTE configuration
	gpos::owner<CCTEConfig *> m_cte_conf;

	// cost model configuration
	gpos::owner<ICostModel *> m_cost_model;

	// hint configuration
	gpos::owner<CHint *> m_hint;

	// default window oids
	gpos::owner<CWindowOids *> m_window_oids;

public:
	// ctor
	COptimizerConfig(CEnumeratorConfig *pec, CStatisticsConfig *stats_config,
					 CCTEConfig *pcteconf, ICostModel *pcm, CHint *phint,
					 CWindowOids *pdefoidsGPDB);

	// dtor
	~COptimizerConfig() override;


	// plan enumeration configuration
	gpos::pointer<CEnumeratorConfig *>
	GetEnumeratorCfg() const
	{
		return m_enumerator_cfg;
	}

	// statistics configuration
	gpos::pointer<CStatisticsConfig *>
	GetStatsConf() const
	{
		return m_stats_conf;
	}

	// CTE configuration
	gpos::pointer<CCTEConfig *>
	GetCteConf() const
	{
		return m_cte_conf;
	}

	// cost model configuration
	gpos::pointer<ICostModel *>
	GetCostModel() const
	{
		return m_cost_model;
	}

	// default window oids
	gpos::pointer<CWindowOids *>
	GetWindowOids() const
	{
		return m_window_oids;
	}

	// hint configuration
	gpos::pointer<CHint *>
	GetHint() const
	{
		return m_hint;
	}

	// generate default optimizer configurations
	static gpos::owner<COptimizerConfig *> PoconfDefault(CMemoryPool *mp);

	// generate default optimizer configurations with the given cost model
	static gpos::owner<COptimizerConfig *> PoconfDefault(CMemoryPool *mp,
														 ICostModel *pcm);

	void Serialize(CMemoryPool *mp, CXMLSerializer *xml_serializer,
				   CBitSet *pbsTrace) const;

};	// class COptimizerConfig

}  // namespace gpopt

#endif	// !GPOPT_COptimizerConfig_H

// EOF
