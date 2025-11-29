/*-------------------------------------------------------------------------
 *
 * scrub.h
 *	  header file for scrub helper deamon
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  scrub.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SCRUB_H
#define SCRUB_H

#include "postgres.h"
#include "funcapi.h"

/* Shared memory */
extern Size ScrubShmemSize(void);
extern void ScrubShmemInit(void);

/* Start the background processes for scrubbig */
bool		StartScrubLauncher(int cost_delay, int cost_limit, Oid dboid,
							   bool reset);

/* Shutdown the background processes, if any */
void		ShutdownScrubLauncherIfRunning(void);

/* Background worker entrypoints */
void		ScrubLauncherMain(Datum arg);
void		ScrubWorkerMain(Datum arg);

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

bool check_page_checksum(Relation rel, ForkNumber forkNum,
						 BlockNumber block,
						 ScrubCounters * counters);

bool check_page_header(Relation rel, ForkNumber forkNum,
					   Page page, BlockNumber block,
					   ScrubCounters * counters);

bool check_page_contents(Relation rel, ForkNumber forkNum,
						 Page page, BlockNumber block,
						 ScrubCounters * counters);

void merge_counters(ScrubCounters * dest, ScrubCounters * src);

bool check_page_heap(Relation rel, Page page, BlockNumber block,
					 ScrubCounters * counters);

bool check_page_btree(Relation rel, Page page, BlockNumber block,
					  ScrubCounters * counters);

bool check_toasted_attribute(Snapshot snapshot, struct varlena *attr,
							 ScrubCounters * counters);

#endif							/* SCRUB_H */
