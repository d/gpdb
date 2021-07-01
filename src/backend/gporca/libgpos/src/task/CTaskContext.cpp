//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CTaskContext.cpp
//
//	@doc:
//		Task context implementation
//---------------------------------------------------------------------------

#include "gpos/task/CTaskContext.h"

#include "gpos/common/CAutoRef.h"
#include "gpos/error/CLoggerStream.h"


using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CTaskContext::CTaskContext
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CTaskContext::CTaskContext(CMemoryPool *mp)
	: m_bitset(nullptr),
	  m_log_out(&CLoggerStream::m_stdout_stream_logger),
	  m_log_err(&CLoggerStream::m_stderr_stream_logger),
	  m_locale(ElocEnUS_Utf8)
{
	m_bitset = GPOS_NEW(mp) CBitSet(mp, EtraceSentinel);
}


//---------------------------------------------------------------------------
//	@function:
//		CTaskContext::CTaskContext
//
//	@doc:
//		used to inherit parent task's context
//
//---------------------------------------------------------------------------
CTaskContext::CTaskContext(CMemoryPool *mp, const CTaskContext &task_ctxt)
	: m_bitset(nullptr),
	  m_log_out(task_ctxt.GetOutputLogger()),
	  m_log_err(task_ctxt.GetErrorLogger()),
	  m_locale(task_ctxt.Locale())
{
	m_bitset = GPOS_NEW(mp) CBitSet(mp);
	m_bitset->Union(task_ctxt.m_bitset.get());
}


//---------------------------------------------------------------------------
//	@function:
//		CTaskContext::~CTaskContext
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CTaskContext::~CTaskContext()
{
	;
}


//---------------------------------------------------------------------------
//	@function:
//		CTaskContext::Trace
//
//	@doc:
//		Set trace flag; return original setting
//
//---------------------------------------------------------------------------
BOOL
CTaskContext::SetTrace(ULONG trace, BOOL val)
{
	if (val)
	{
		return m_bitset->ExchangeSet(trace);
	}
	else
	{
		return m_bitset->ExchangeClear(trace);
	}
}

// EOF
