#include "postgres.h"
#include "c.h"
#include "utils/elog.h"


// PGDLLIMPORT sigjmp_buf *PG_exception_stack;
// PGDLLIMPORT ErrorContextCallback *error_context_stack;
//#define PG_RE_THROW()  \
//	pg_re_throw()
//
// void
// elog_start(const char *filename, int lineno, const char *funcname)
//{
//
//}
//
// void
// elog_finish(int elevel, const char * fmt, ...)
//{
//
//}
//
// bool
// errstart(int elevel, const char * filename, int lineno, const char *
// funcname, const char * domain) { 	return true;
//}
// void errfinish(int dummy,...) {
//
//}
//
// int	errcode(int sqlerrcode) {
//	return 0;
//}
//
// int errmsg(const char *fmt,...) {
//	return 0;
//}
// void pg_re_throw(void) {}
// int errdetail(const char *fmt,...) {}
// void pre_format_elog_string(int errnumber, const char *domain) {}
// char *format_elog_string(const char *fmt,...){}
// void elog_internalerror(const char *filename, int lineno, const char
// *funcname){} int	errcode_for_file_access(void){} int errhint(const char
// *fmt,...){} int errcontext_msg(const char *fmt,...){} void write_stderr(const
// char *fmt,...){} void FlushErrorState(void){} void EmitErrorReport(void){}

#include <setjmp.h>
#include <cgreen/cgreen.h>
#include <cgreen/mocks.h>
#include <cdb/cdbvars.h>
#include <utils/relcache.h>
#include <access/genam.h>
#include <access/relscan.h>
#include <access/htup.h>

#include "cdb/cdbutil.h"

#define PG_RE_THROW() siglongjmp(*PG_exception_stack, 1)

/*
 * This method will emulate the real ExceptionalCondition
 * function by re-throwing the exception, essentially falling
 * back to the next available PG_CATCH();
 */
void
_ExceptionalCondition(void *data)
{
	(void *) data;
	PG_RE_THROW();
}

static int32 saved_segment_index;

Describe(cdbutil);
BeforeEach(cdbutil)
{
	saved_segment_index = GpIdentity.segindex;
}
AfterEach(cdbutil)
{
	GpIdentity.segindex = saved_segment_index;
}

Ensure(cdbutil, dbid_exists_raises_exception_when_not_run_in_qd)
{
	always_expect(elog_start);
	always_expect(elog_finish, with_sideeffect(&_ExceptionalCondition, NULL));
	GpIdentity.segindex = 1;
	PG_TRY();
	{
		dbid_exists(1);
		fail_test("Should not have reached this point");
	}
	PG_CATCH();
	{
	}
	PG_END_TRY();
}

#include <unistd.h>

Ensure(cdbutil,
	   dbid_exists_when_run_in_qd_throw_exception_if_segment_does_not_exist)
{
//	sleep(5);

	GpIdentity.segindex = MASTER_CONTENT_ID;
	RelationData relation = {};
	ScanKeyData scanKeyData = {};

	struct SysScanDescData sysScanDesc = {};
	
	expect(heap_open, will_return(&relation));
	expect(
		ScanKeyInit,
		
		will_set_contents_of_parameter(entry, &scanKeyData, sizeof(ScanKeyData)));
	
	expect(systable_beginscan, 
		when(key, is_equal_to_contents_of(&scanKeyData, sizeof(ScanKeyData))),
		will_return(&sysScanDesc));
	expect(systable_getnext, will_return(NULL));

	always_expect(elog_start);
	expect(elog_finish, with_sideeffect(&_ExceptionalCondition, NULL));

	PG_TRY();
	{
		dbid_exists(1);
		fail_test("Should not have reached this point");
	}
	PG_CATCH();
	{
	}
	PG_END_TRY();
}


int
main(int argc, char **argv)
{
	TestSuite *suite = create_test_suite();
	add_test_with_context(
		suite, cdbutil, dbid_exists_raises_exception_when_not_run_in_qd);
	add_test_with_context(
		suite,
		cdbutil,
		dbid_exists_when_run_in_qd_throw_exception_if_segment_does_not_exist);
	TestReporter *reporter = create_text_reporter();
	TextReporterOptions options = {.use_colours = true, .quiet_mode = false};
	set_reporter_options(reporter, &options);
	return run_test_suite(suite, reporter);
}
