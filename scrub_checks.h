/*-------------------------------------------------------------------------
 *
 * scrub_checks.h
 *	  header file for scrub consistency check methods
 *
 * IDENTIFICATION
 *	  scrub_checks.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SCRUB_CHECKS_H
#define SCRUB_CHECKS_H

#include "postgres.h"

#include "access/heapam.h"
#include "common/relpath.h"
#include "utils/rel.h"

/*
 * A fat structure to track all kinds of statistics - different levels
 * of checks (checksum, page structure, tuple structure, ...) and types
 * of objects (heap, indexes, ...).
 *
 * We might split this into multiple parts in the future, but this way
 * we can pass it to all the methods pretty easily.
 */
typedef struct ScrubCounters
{
	/* page stats */
	uint64		pages_total;	/* pages checked */
	uint64		pages_failed;	/* pages with any failure */

	/* checksum stats */
	uint64		checksums_total;	/* checksums checked */
	uint64		checksums_failed;	/* checksum failures (provable) */

	/* header stats */
	uint64		headers_total;	/* page headers checked */
	uint64		headers_failed; /* page header failures */

	/* heap content checks */
	uint64		heap_pages_total;
	uint64		heap_pages_failed;
	uint64		heap_tuples_total;
	uint64		heap_tuples_failed;

	/* toast values */
	uint64		heap_attr_toast_external_invalid;
	uint64		heap_attr_compression_broken;
	uint64		heap_attr_toast_bytes_total;
	uint64		heap_attr_toast_bytes_failed;
	uint64		heap_attr_toast_values_total;
	uint64		heap_attr_toast_values_failed;
	uint64		heap_attr_toast_chunks_total;
	uint64		heap_attr_toast_chunks_failed;

	/* btree content checks */
	uint64		btree_pages_total;
	uint64		btree_pages_failed;
	uint64		btree_tuples_total;
	uint64		btree_tuples_failed;

}			ScrubCounters;

bool		check_page_checksum(Relation rel, ForkNumber forkNum,
								BlockNumber block,
								ScrubCounters * counters);

bool		check_page_header(Relation rel, ForkNumber forkNum,
							  Page page, BlockNumber block,
							  ScrubCounters * counters);

bool		check_page_contents(Relation rel, ForkNumber forkNum,
								Page page, BlockNumber block,
								ScrubCounters * counters);

void		merge_counters(ScrubCounters * dest, ScrubCounters * src);

#endif							/* SCRUB_CHECKS_H */
