#ifndef GPOS_attributes_H
#define GPOS_attributes_H

#ifdef USE_CMAKE
#define GPOS_FORMAT_ARCHETYPE printf
#else
#include "pg_config.h"
#define GPOS_FORMAT_ARCHETYPE PG_PRINTF_ATTRIBUTE
#endif

#define GPOS_ATTRIBUTE_PRINTF(f, a) \
	__attribute__((format(GPOS_FORMAT_ARCHETYPE, f, a)))

#ifdef __GNUC__
#define GPOS_UNUSED __attribute__((unused))
#else
#define GPOS_UNUSED
#endif

#ifndef GPOS_DEBUG
#define GPOS_ASSERTS_ONLY GPOS_UNUSED
#else
#define GPOS_ASSERTS_ONLY
#endif

#ifdef GPOS_DEBUG
#define GPOS_UNREACHABLE(msg) GPOS_ASSERT(!msg)
#else
/*
 * Technically the __builtin_unreachable intrinsic was first introduced in GCC
 * 4.5.0, but given that C++14 support wasn't complete until 5.1, we're good
 * with just detecting GCC here.
 */
#ifdef __GNUC__
#define GPOS_UNREACHABLE(msg) __builtin_unreachable()
#else
#define GPOS_UNREACHABLE(msg) std::terminate()
#endif
#endif

#endif	// !GPOS_attributes_H
