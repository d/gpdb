//---------------------------------------------------------------------------
//	Greenplum Database
//  Copyright (c) 2020 VMware, Inc.
//
//	@filename:
//		CRefCount.cpp
//
//	@doc:
//		Implementation of class for ref-counted objects
//---------------------------------------------------------------------------

#include "gpos/common/CRefCount.h"
#include "gpos/error/CAutoTrace.h"
#include "gpos/task/CTask.h"

using namespace gpos;

#ifdef GPOS_DEBUG
// debug print for interactive debugging sessions only
void CRefCount::DbgPrint() const
{
	CAutoTrace at(CTask::Self()->Pmp());

	OsPrint(at.Os());
}
#endif // GPOS_DEBUG

void
CRefCount::AddRef()
{
#ifdef GPOS_DEBUG
	Check();
#endif // GPOS_DEBUG
	m_refs++;
}

void
CRefCount::Release()
{
#ifdef GPOS_DEBUG
	Check();
#endif // GPOS_DEBUG
	m_refs--;

	if (0 == m_refs)
	{
		if (!Deletable())
		{
			// restore ref-count
			AddRef();

			// deletion is not allowed
			GPOS_RAISE(CException::ExmaSystem, CException::ExmiInvalidDeletion);
		}

		GPOS_DELETE(this);
	}
}

CRefCount::CRefCount()
	:
	m_refs(1)
{}

CRefCount::~CRefCount() noexcept(false)
{
	// enforce strict ref-counting unless we're in a pending exception,
	// e.g., a ctor has thrown
	GPOS_ASSERT(NULL == ITask::Self() ||
		            ITask::Self()->HasPendingExceptions() ||
		            0 == m_refs);
}

// EOF

