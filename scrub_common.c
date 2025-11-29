/*-------------------------------------------------------------------------
 *
 * scrub_checks.c
 *	  implementation of methods checking consistency of various objects
 *
 * IDENTIFICATION
 *	  scrub_checks.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup.h"
#include "access/htup_details.h"
#include "access/nbtree.h"
#include "access/detoast.h"
#include "access/heaptoast.h"
#include "access/xact.h"
#include "access/table.h"

#include "catalog/pg_am.h"
#include "funcapi.h"
#include "storage/checksum.h"
#include "storage/smgr.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
/* #include "utils/tqual.h" */

#include "scrub.h"


/*
 * check_page_checksum
 *	  Verify page checksum, if enabled.
 *
 * We do not read the page using BufferRead, because that would verify
 * the checksum internally and interrupt the check in case of failure
 * (which we don't want - we want to continue with the other pages).
 *
 * Instead we call smgrread() directly, which does all the relfilenode
 * translation etc. and read() but without the checksum verification.
 * That however means we're not protected against concurrent writes,
 * and so may observe torn pages.
 *
 * We use the same defense as checksum verification in pg_basebackup,
 * i.e. if the page checksum fails, we remember the page LSN and read
 * the page again - if the LSN did not change, we consider this to be
 * a checksum failure. Otherwise we assume the failure was due to a
 * torn page and ignore it (effectively considering the checksum OK).
 */
bool
check_page_checksum(Relation reln, ForkNumber forkNum, BlockNumber block,
					ScrubCounters * counters)
{
	uint16		checksum;
	PageHeader	pagehdr;
	XLogRecPtr	page_lsn;

	/* XXX the buffer needs to be properly aligned */
	char		tmp[BLCKSZ + PG_IO_ALIGN_SIZE];
	char	   *buffer = (char *) TYPEALIGN(PG_IO_ALIGN_SIZE, tmp);

	/*
	 * When data checksums are not enabled, act as if the checksum was correct
	 * (but do not update any counters, because that would be rather
	 * confusing).
	 */
	if (!DataChecksumsEnabled())
		return true;

	/* Everything beyond here counts as a checksum verification. */
	counters->checksums_total += 1;

	/*
	 * read a copy of the page into a local buffer
	 *
	 * XXX Using smgrread means we're subject to zero_damaged_pages and so on.
	 * Not sure that's really desirable.
	 */
	smgrread(reln->rd_smgr, forkNum, block, buffer);

	/*
	 * Do not verify checksums on new pages - we don't set them.
	 *
	 * XXX Calling PageIsNew() is not sufficient here, because the header may
	 * get corrupted, setting pd_upper=0 incorrectly. Which we'd fail to
	 * detect. So we should probably verify that new pages are indeed filled
	 * with zeroes.
	 */
	if (PageIsNew((Page) buffer))
		return true;

	/*
	 * Verify checksum on the non-empty page. It's however possible that we
	 * read a torn page due to a concurrent write. The write() is not atomic,
	 * so we may read 4kB of old and 4kB of new data (or possibly smaller
	 * chunks, depending on OS, file system page size etc.).
	 *
	 * That is, the 8kB page originally looks like [A1,A2] and while we do the
	 * read() there's a concurrent write [B1,B2]. In that case we may read
	 * [A1,B2] - a torn page.
	 *
	 * We do however assume we can't see effects of the write() in random
	 * order - we expect a byte to get visible only after all preceding bytes.
	 * So when the second part of the page is "new" we assume the reread will
	 * see the first part as new too (including the LSN). That is, we may not
	 * observe [B1,A2].
	 *
	 * So if the checksum check fails, we reread() the page and see if the
	 * page LSN changed. If the LSN did not change, the page was not torn and
	 * the checksum really is incorrect.
	 *
	 * If the LSN did change, we consider the original page torn, ignore the
	 * checksum failure and skip the page (effectively considering the
	 * checksum correct).
	 *
	 * We might try verifying checksum on the new page version, but it would
	 * not tell us much more - a failure might be due to the page being torn
	 * again (we might repeat the whole dance but that poses risk of an
	 * infinite loop). So we simply skip this page.
	 *
	 * This assumes torn pages are not very frequent, which seems like a
	 * reasonable assumption.
	 *
	 * XXX This is pretty much what we do in basebackup.c, except that
	 * basebackup reads the pages directly in batches, and we read it through
	 * smgr.
	 *
	 * XXX Does this correctly account the re-reads against vacuum_cost? If
	 * not, it'll affect the throttling.
	 */

	pagehdr = (PageHeader) buffer;
	checksum = pg_checksum_page(buffer, block);

	/* if the checksum matches, we're done */
	if (checksum == pagehdr->pd_checksum)
		return true;

	/* otherwise remember the page LSN and reread the buffer */
	page_lsn = PageGetLSN((Page) buffer);

	/* read a copy of the page into a local buffer */
	smgrread(reln->rd_smgr, forkNum, block, buffer);

	/* XXX do we need to recheck this? */
	if (PageIsNew((Page) buffer))
		return true;

	/* if the LSN did not change, it's a checksum failure */
	if (page_lsn == PageGetLSN((Page) buffer))
	{
		counters->checksums_failed += 1;

		ereport(WARNING,
				(errmsg("[%d] checksum failure - header %u computed %u",
						block, pagehdr->pd_checksum, checksum)));
		return false;
	}

	/* otherwise it's likely due to a concurrent write, so ignore */
	return true;
}


/*
 * check_page_header_generic
 *		Perform global generic page checks (mostly info from the PageHeader).
 *
 * These apply to pages in all types of relations - tables, indexes, ...
 *
 * Check all following values in a page header.
 *
 *	pd_lsn		- identifies xlog record for last change to this page.
 *	pd_tli		- ditto. (up to 9.2)
 *	pd_checksum - page checksum, if set (since 9.3)
 *	pd_flags	- flag bits.
 *	pd_lower	- offset to start of free space.
 *	pd_upper	- offset to end of free space.
 *	pd_special	- offset to start of special space.
 *	pd_pagesize_version - size in bytes and page layout version number.
 *	pd_prune_xid - oldest XID among potentially prunable tuples on page.
 *
 * We do bail out after finding the first issue. Chances are that when there
 * is one issue, there will be more. Counting each of them would not be a
 * useful measure of data corruption.
 *
 */
static bool
check_page_header_generic(Page page, BlockNumber block)
{
	PageHeader	header = (PageHeader) page;

	ereport(DEBUG1,
			(errmsg("[%d] header [lower=%d, upper=%d, special=%d free=%d]",
					block, header->pd_lower, header->pd_upper,
					header->pd_special, header->pd_upper - header->pd_lower)));

	/* check the page size (should be BLCKSZ) */
	if (PageGetPageSize(page) != BLCKSZ)
	{
		ereport(WARNING,
				(errmsg("[%d] invalid page size %d (%d)", block,
						(int) PageGetPageSize(page), BLCKSZ)));
		return false;
	}

	/*
	 * This checks that the layout version is between 0 and 4, which are page
	 * versions supported by PostgreSQL. But the following checks depend on
	 * the format, so we only do them for the current version, which is
	 * PG_PAGE_LAYOUT_VERSION (4).
	 */
	if ((PageGetPageLayoutVersion(page) < 0) ||
		(PageGetPageLayoutVersion(page) > 4))
	{
		ereport(WARNING,
				(errmsg("[%d] invalid page layout version %d",
						block, PageGetPageLayoutVersion(page))));
		return false;
	}

	/*
	 * The page LSN must be in the past, it can't possibly be in the future.
	 */
	if (PageGetLSN(page) > GetXLogInsertRecPtr())
	{
		ereport(WARNING,
				(errmsg("[%d] page LSN %X/%X is ahead of current insert position",
						block, (uint32) (PageGetLSN(page) >> 32),
						(uint32) PageGetLSN(page))));
		return false;
	}

	/*
	 * All page versions between 0 and 4 are correct, but we only know how to
	 * do more checks on the most recent format, so just bail out. For now we
	 * consider pages with obsolete page format to be OK.
	 */
	if (PageGetPageLayoutVersion(page) != 4)
	{
		ereport(WARNING,
				(errmsg("[%d] obsolete page layout version %d, skipping",
						block, PageGetPageLayoutVersion(page))));
		return true;
	}

	/*
	 * If the page is new, we don't really need to do any further checks. New
	 * pages are perfectly valid and expected to be found in relations, and
	 * the code should treat them as empty (i.e. sanely).
	 */
	if (PageIsNew(page))
		return true;

	/*
	 * All header pointers (lower, upper, special) should be greater than
	 * PageHeaderData and less than BLCKSZ.
	 */
	if ((header->pd_lower < offsetof(PageHeaderData, pd_linp)) ||
		(header->pd_lower > BLCKSZ))
	{
		ereport(WARNING,
				(errmsg("[%d] pd_lower %d not between %d and %d", block,
						header->pd_lower,
						(int) offsetof(PageHeaderData, pd_linp), BLCKSZ)));
		return false;
	}

	if ((header->pd_upper < offsetof(PageHeaderData, pd_linp)) ||
		(header->pd_upper > BLCKSZ))
	{
		ereport(WARNING,
				(errmsg("[%d] pd_upper %d not between %d and %d", block,
						header->pd_upper,
						(int) offsetof(PageHeaderData, pd_linp), BLCKSZ)));
		return false;
	}

	if ((header->pd_special < offsetof(PageHeaderData, pd_linp)) ||
		(header->pd_special > BLCKSZ))
	{
		ereport(WARNING,
				(errmsg("[%d] pd_special %d not between %d and %d", block,
						header->pd_special,
						(int) offsetof(PageHeaderData, pd_linp), BLCKSZ)));
		return false;
	}

	/*
	 * There is a simple relationship between the pointers:
	 *
	 * pd_lower <= pg_upper <= pd_special
	 *
	 * Which we will validate in two steps.
	 */
	if (header->pd_lower > header->pd_upper)
	{
		ereport(WARNING,
				(errmsg("[%d] pd_lower > pd_upper (%d > %d)",
						block, header->pd_lower, header->pd_upper)));
		return false;
	}

	/* special should be >= upper */
	if (header->pd_upper > header->pd_special)
	{
		ereport(WARNING,
				(errmsg("[%d] pd_upper > pd_special (%d > %d)",
						block, header->pd_upper, header->pd_special)));
		return false;
	}

	/*
	 * Older PostgreSQL versions (up to 9.2) also had pd_tli, tracking page
	 * timeline. PostgreSQL 9.3 replaced that with pd_checksum, so we need to
	 * be careful when reading and interpreting this value.
	 *
	 * We only check the timeline here - checksums are verified elsewhere, and
	 * we only check the header structure here.
	 */
#if (PG_VERSION_NUM < 90300)
	/* The timeline must not be greater than the current one. */
	if (header->pd_tli > ThisTimeLineID)
	{
		ereport(WARNING,
				(errmsg("[%d] invalid timeline %u (current %u)",
						block, header->pd_tli, ThisTimeLineID)));
		return false;
	}
#endif

	/*
	 * Check that we only have valid flags set.
	 */
	if ((header->pd_flags & PD_VALID_FLAG_BITS) != header->pd_flags)
	{
		ereport(WARNING,
				(errmsg("[%d] page has invalid flags set %u",
						block, header->pd_flags)));
		return false;
	}

	return true;
}

bool
check_page_header(Relation rel, ForkNumber forkNum,
				  Page page, BlockNumber block, ScrubCounters * counters)
{
	counters->headers_total += 1;

	/* do generic checks first, continue with AM-specific tests if OK */
	if (!check_page_header_generic(page, block))
	{
		counters->headers_failed += 1;
		return false;
	}

	return true;
}

bool
check_page_contents(Relation rel, ForkNumber forkNum,
					Page page, BlockNumber block, ScrubCounters * counters)
{
	switch (rel->rd_rel->relkind)
	{
		case RELKIND_RELATION:
		case RELKIND_MATVIEW:
		case RELKIND_TOASTVALUE:

			if (forkNum == MAIN_FORKNUM)
			{
				counters->heap_pages_total += 1;

				if (!check_page_heap(rel, page, block, counters))
					counters->heap_pages_failed += 1;
			}

			/* FIXME implement checks of the other fork types */
			return true;

		case RELKIND_INDEX:

			Assert(forkNum == MAIN_FORKNUM);

			/* btree */
			if (rel->rd_rel->relam == BTREE_AM_OID)
			{
				counters->btree_pages_total += 1;

				if (!check_page_btree(rel, page, block, counters))
					counters->btree_pages_failed += 1;
			}

			/* FIXME implement checks of the other fork types */
			return true;

		default:
			return true;
	}
}

void
merge_counters(ScrubCounters * dst, ScrubCounters * src)
{
	dst->pages_total += src->pages_total;
	dst->pages_failed += src->pages_failed;

	dst->checksums_total += src->checksums_total;
	dst->checksums_failed += src->checksums_failed;

	dst->headers_total += src->headers_total;
	dst->headers_failed += src->headers_failed;

	dst->heap_pages_total += src->heap_pages_total;
	dst->heap_pages_failed += src->heap_pages_failed;
	dst->heap_tuples_total += src->heap_tuples_total;
	dst->heap_tuples_failed += src->heap_tuples_failed;

	dst->heap_attr_toast_external_invalid += src->heap_attr_toast_external_invalid;
	dst->heap_attr_compression_broken += src->heap_attr_compression_broken;
	dst->heap_attr_toast_bytes_total += src->heap_attr_toast_bytes_total;
	dst->heap_attr_toast_bytes_failed += src->heap_attr_toast_bytes_failed;
	dst->heap_attr_toast_values_total += src->heap_attr_toast_values_total;
	dst->heap_attr_toast_values_failed += src->heap_attr_toast_values_failed;
	dst->heap_attr_toast_chunks_total += src->heap_attr_toast_chunks_total;
	dst->heap_attr_toast_chunks_failed += src->heap_attr_toast_chunks_failed;

	dst->btree_pages_total += src->btree_pages_total;
	dst->btree_pages_failed += src->btree_pages_failed;
	dst->btree_tuples_total += src->btree_tuples_total;
	dst->btree_tuples_failed += src->btree_tuples_failed;
}
