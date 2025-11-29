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
#include "catalog/pg_am.h"
#include "funcapi.h"
#include "storage/checksum.h"
#include "storage/smgr.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
//#include "utils/tqual.h"

#include "scrub_checks.h"


static int toast_open_indexes(Relation toastrel,
				   LOCKMODE lock,
				   Relation **toastidxs,
				   int *num_indexes);
static void toast_close_indexes(Relation *toastidxs, int num_indexes,
					LOCKMODE lock);

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
					ScrubCounters *counters)
{
	uint16			checksum;
	PageHeader		pagehdr;
	XLogRecPtr		page_lsn;

	/* XXX the buffer needs to be properly aligned */
	char			tmp[BLCKSZ + PG_IO_ALIGN_SIZE];
	char		   *buffer = (char *) TYPEALIGN(PG_IO_ALIGN_SIZE, tmp);

	/*
	 * When data checksums are not enabled, act as if the checksum was
	 * correct (but do not update any counters, because that would be
	 * rather confusing).
	 */
	if (!DataChecksumsEnabled())
		return true;

	/* Everything beyond here counts as a checksum verification. */
	counters->checksums_total += 1;

	/*
	 * read a copy of the page into a local buffer
	 *
	 * XXX Using smgrread means we're subject to zero_damaged_pages and so
	 * on. Not sure that's really desirable.
	 */
	smgrread(reln->rd_smgr, forkNum, block, buffer);

	/*
	 * Do not verify checksums on new pages - we don't set them.
	 *
	 * XXX Calling PageIsNew() is not sufficient here, because the header
	 * may get corrupted, setting pd_upper=0 incorrectly. Which we'd fail
	 * to detect. So we should probably verify that new pages are indeed
	 * filled with zeroes.
	 */
	if (PageIsNew((Page) buffer))
		return true;

	/*
	 * Verify checksum on the non-empty page. It's however possible that
	 * we read a torn page due to a concurrent write. The write() is not
	 * atomic, so we may read 4kB of old and 4kB of new data (or possibly
	 * smaller chunks, depending on OS, file system page size etc.).
	 *
	 * That is, the 8kB page originally looks like [A1,A2] and while we
	 * do the read() there's a concurrent write [B1,B2]. In that case
	 * we may read [A1,B2] - a torn page.
	 *
	 * We do however assume we can't see effects of the write() in random
	 * order - we expect a byte to get visible only after all preceding
	 * bytes. So when the second part of the page is "new" we assume the
	 * reread will see the first part as new too (including the LSN).
	 * That is, we may not observe [B1,A2].
	 *
	 * So if the checksum check fails, we reread() the page and see if
	 * the page LSN changed. If the LSN did not change, the page was not
	 * torn and the checksum really is incorrect.
	 *
	 * If the LSN did change, we consider the original page torn, ignore
	 * the checksum failure and skip the page (effectively considering
	 * the checksum correct).
	 *
	 * We might try verifying checksum on the new page version, but it
	 * would not tell us much more - a failure might be due to the page
	 * being torn again (we might repeat the whole dance but that poses
	 * risk of an infinite loop). So we simply skip this page.
	 *
	 * This assumes torn pages are not very frequent, which seems like
	 * a reasonable assumption.
	 *
	 * XXX This is pretty much what we do in basebackup.c, except that
	 * basebackup reads the pages directly in batches, and we read it
	 * through smgr.
	 *
	 * XXX Does this correctly account the re-reads against vacuum_cost?
	 * If not, it'll affect the throttling.
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
						block, (uint32)(PageGetLSN(page) >> 32),
						(uint32) PageGetLSN(page))));
		return false;
	}

	/*
	 * All page versions between 0 and 4 are correct, but we only know how
	 * to do more checks on the most recent format, so just bail out. For
	 * now we consider pages with obsolete page format to be OK.
	 */
	if (PageGetPageLayoutVersion(page) != 4)
	{
		ereport(WARNING,
				(errmsg("[%d] obsolete page layout version %d, skipping",
						block, PageGetPageLayoutVersion(page))));
		return true;
	}

	/*
	 * If the page is new, we don't really need to do any further checks.
	 * New pages are perfectly valid and expected to be found in relations,
	 * and the code should treat them as empty (i.e. sanely).
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
	 *     pd_lower <= pg_upper <= pd_special
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
	 * timeline. PostgreSQL 9.3 replaced that with pd_checksum, so we need
	 * to be careful when reading and interpreting this value.
	 *
	 * We only check the timeline here - checksums are verified elsewhere,
	 * and we only check the header structure here.
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
				  Page page, BlockNumber block, ScrubCounters *counters)
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

/*
 * HeapTupleIsVisibleMVCC
 *		True iff heap tuple is valid for the given MVCC snapshot.
 *
 * This is pretty much HeapTupleSatisfiesMVCC, just without setting the
 * hint bits. See tqual.c for details.
 */
static bool
HeapTupleIsVisibleMVCC(HeapTuple htup, Snapshot snapshot)
{
	HeapTupleHeader tuple = htup->t_data;

	Assert(ItemPointerIsValid(&htup->t_self));

	if (!HeapTupleHeaderXminCommitted(tuple))
	{
		if (HeapTupleHeaderXminInvalid(tuple))
			return false;

		/* Used by pre-9.0 binary upgrades */
		if (tuple->t_infomask & HEAP_MOVED_OFF)
		{
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (TransactionIdIsCurrentTransactionId(xvac))
				return false;

			if (!XidInMVCCSnapshot(xvac, snapshot))
			{
				if (TransactionIdDidCommit(xvac))
					return false;
			}
		}
		/* Used by pre-9.0 binary upgrades */
		else if (tuple->t_infomask & HEAP_MOVED_IN)
		{
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (!TransactionIdIsCurrentTransactionId(xvac))
			{
				if (XidInMVCCSnapshot(xvac, snapshot))
					return false;

				if (!TransactionIdDidCommit(xvac))
					return false;
			}
		}
		else if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)))
		{
			if (HeapTupleHeaderGetCmin(tuple) >= snapshot->curcid)
				return false;	/* inserted after scan started */

			if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid */
				return true;

			if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))	/* not deleter */
				return true;

			if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
			{
				TransactionId xmax;

				xmax = HeapTupleGetUpdateXid(tuple);

				/* not LOCKED_ONLY, so it has to have an xmax */
				Assert(TransactionIdIsValid(xmax));

				/* updating subtransaction must have aborted */
				if (!TransactionIdIsCurrentTransactionId(xmax))
					return true;
				else if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
					return true;	/* updated after scan started */
				else
					return false;	/* updated before scan started */
			}

			if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
			{
				/* deleting subtransaction must have aborted */
				return true;
			}

			if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
				return true;	/* deleted after scan started */
			else
				return false;	/* deleted before scan started */
		}
		else if (XidInMVCCSnapshot(HeapTupleHeaderGetRawXmin(tuple), snapshot))
			return false;
		else if (!TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple)))
		{
			/* it must have aborted or crashed */
			return false;
		}
	}
	else
	{
		/* xmin is committed, but maybe not according to our snapshot */
		if (!HeapTupleHeaderXminFrozen(tuple) &&
			XidInMVCCSnapshot(HeapTupleHeaderGetRawXmin(tuple), snapshot))
			return false;		/* treat as still in progress */
	}

	/* by here, the inserting transaction has committed */

	if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid or aborted */
		return true;

	if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
		return true;

	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
	{
		TransactionId xmax;

		/* already checked above */
		Assert(!HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask));

		xmax = HeapTupleGetUpdateXid(tuple);

		/* not LOCKED_ONLY, so it has to have an xmax */
		Assert(TransactionIdIsValid(xmax));

		if (TransactionIdIsCurrentTransactionId(xmax))
		{
			if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
				return true;	/* deleted after scan started */
			else
				return false;	/* deleted before scan started */
		}
		if (XidInMVCCSnapshot(xmax, snapshot))
			return true;
		if (TransactionIdDidCommit(xmax))
			return false;		/* updating transaction committed */
		/* it must have aborted or crashed */
		return true;
	}

	if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED))
	{
		if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
		{
			if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
				return true;	/* deleted after scan started */
			else
				return false;	/* deleted before scan started */
		}

		if (XidInMVCCSnapshot(HeapTupleHeaderGetRawXmax(tuple), snapshot))
			return true;

		if (!TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple)))
		{
			/* it must have aborted or crashed */
			return true;
		}

		/* xmax transaction committed */
	}
	else
	{
		/* xmax is committed, but maybe not according to our snapshot */
		if (XidInMVCCSnapshot(HeapTupleHeaderGetRawXmax(tuple), snapshot))
			return true;		/* treat as still in progress */
	}

	/* xmax transaction committed */

	return false;
}

static bool
check_toasted_attribute(Snapshot snapshot, struct varlena *attr, ScrubCounters *counters)
{
	Relation	toastrel;
	Relation   *toastidxs;
	ScanKeyData toastkey;
	SysScanDesc toastscan;
	HeapTuple	ttup;
	TupleDesc	toasttupDesc;
	struct varatt_external toast_pointer;
	int32		ressize;
	int32		residx,
				nextidx;
	int32		numchunks;
	Pointer		chunk;
	bool		isnull;
	int32		chunksize;
	int			num_indexes;
	int			validIndex;
	bool		success = true;

	if (!VARATT_IS_EXTERNAL_ONDISK(attr))
		elog(ERROR, "toast_fetch_datum shouldn't be called for non-ondisk datums");

	/* Must copy to access aligned fields */
	VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr);

	ressize = VARATT_EXTERNAL_GET_EXTSIZE(toast_pointer);
	numchunks = ((ressize - 1) / TOAST_MAX_CHUNK_SIZE) + 1;

	/* track total expected values */
	counters->heap_attr_toast_values_total += 1;
	counters->heap_attr_toast_chunks_total += numchunks;
	counters->heap_attr_toast_bytes_total += ressize;

	/*
	 * Open the toast relation and its indexes
	 */
	toastrel = table_open(toast_pointer.va_toastrelid, AccessShareLock);
	toasttupDesc = toastrel->rd_att;

	/* Look for the valid index of the toast relation */
	validIndex = toast_open_indexes(toastrel,
									AccessShareLock,
									&toastidxs,
									&num_indexes);

	/*
	 * Setup a scan key to fetch from the index by va_valueid
	 */
	ScanKeyInit(&toastkey,
				(AttrNumber) 1,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(toast_pointer.va_valueid));

	/*
	 * Read the chunks by index
	 *
	 * Note that because the index is actually on (valueid, chunkidx) we will
	 * see the chunks in chunkidx order, even though we didn't explicitly ask
	 * for it.
	 */
	nextidx = 0;

	toastscan = systable_beginscan_ordered(toastrel, toastidxs[validIndex],
										   snapshot, 1, &toastkey);
	while ((ttup = systable_getnext_ordered(toastscan, ForwardScanDirection)) != NULL)
	{
		/*
		 * Have a chunk, extract the sequence number and the data
		 */
		residx = DatumGetInt32(fastgetattr(ttup, 2, toasttupDesc, &isnull));
		Assert(!isnull);
		chunk = DatumGetPointer(fastgetattr(ttup, 3, toasttupDesc, &isnull));
		Assert(!isnull);
		if (!VARATT_IS_EXTENDED(chunk))
		{
			chunksize = VARSIZE(chunk) - VARHDRSZ;
		}
		else if (VARATT_IS_SHORT(chunk))
		{
			/* could happen due to heap_form_tuple doing its thing */
			chunksize = VARSIZE_SHORT(chunk) - VARHDRSZ_SHORT;
		}
		else
		{
			/* should never happen */
			elog(WARNING, "found toasted toast chunk for toast value %u in %s",
				 toast_pointer.va_valueid,
				 RelationGetRelationName(toastrel));
			success = false;
			goto cleanup;
		}

		/*
		 * Some checks on the data we've found
		 */
		if (residx != nextidx)
		{
			elog(WARNING, "unexpected chunk number %d (expected %d) for toast value %u in %s",
				 residx, nextidx,
				 toast_pointer.va_valueid,
				 RelationGetRelationName(toastrel));
			success = false;
			goto cleanup;
		}

		if (residx < numchunks - 1)
		{
			if (chunksize != TOAST_MAX_CHUNK_SIZE)
			{
				elog(WARNING, "unexpected chunk size %d (expected %d) in chunk %d of %d for toast value %u in %s",
					 chunksize, (int) TOAST_MAX_CHUNK_SIZE,
					 residx, numchunks,
					 toast_pointer.va_valueid,
					 RelationGetRelationName(toastrel));
				success = false;
				goto cleanup;
			}
		}
		else if (residx == numchunks - 1)
		{
			if ((residx * TOAST_MAX_CHUNK_SIZE + chunksize) != ressize)
			{
				elog(WARNING, "unexpected chunk size %d (expected %d) in final chunk %d for toast value %u in %s",
					 chunksize,
					 (int) (ressize - residx * TOAST_MAX_CHUNK_SIZE),
					 residx,
					 toast_pointer.va_valueid,
					 RelationGetRelationName(toastrel));
				success = false;
				goto cleanup;
			}
		}
		else
		{
			elog(WARNING, "unexpected chunk number %d (out of range %d..%d) for toast value %u in %s",
				 residx,
				 0, numchunks - 1,
				 toast_pointer.va_valueid,
				 RelationGetRelationName(toastrel));
			success = false;
			goto cleanup;
		}

		nextidx++;
	}

	/*
	 * Final checks that we successfully fetched the datum
	 */
	if (nextidx != numchunks)
	{
		elog(WARNING, "missing chunk number %d for toast value %u in %s",
			 nextidx,
			 toast_pointer.va_valueid,
			 RelationGetRelationName(toastrel));
		success = false;
		goto cleanup;
	}

cleanup:

	/*
	 * End scan and close relations
	 */
	systable_endscan_ordered(toastscan);
	toast_close_indexes(toastidxs, num_indexes, AccessShareLock);
	table_close(toastrel, AccessShareLock);

	if (!success)
	{
		counters->heap_attr_toast_values_failed += 1;

		/* all remaining chunks */
		counters->heap_attr_toast_chunks_failed += (numchunks - nextidx);

		/* count the whole toast value as failed */
		counters->heap_attr_toast_bytes_failed += ressize;
	}

	return success;
}

/* checks the individual attributes of the tuple */
static bool
check_heap_tuple_attributes(Relation rel, Page page, BlockNumber block,
							OffsetNumber off, ScrubCounters *counters)
{
	HeapTupleData	tuple;
	HeapTupleHeader	tupheader;
	uint16		offset,
				endoffset;
	int			i,
				tuplenatts;
	bool		has_nulls = false;
	Snapshot	snapshot = GetActiveSnapshot();
	bool		success = true;

	/* page as a simple array of bytes */
	char	   *buffer = (char *) page;

	/* check_heap_tuple did checks on item pointers, so trust it */
	ItemId		lp = PageGetItemId(page, off);

	ereport(DEBUG2,
			(errmsg("[%d:%d] checking attributes for the tuple", block, off)));

	/*
	 * Get the header of the tuple (it starts at the 'lp_off' offset and it's
	 * t_hoff long (incl. bitmap)).
	 */
	tupheader = (HeapTupleHeader) PageGetItem(page, lp);

	/*
	 * Assemble a tuple, so that we can do a simple visibility check.
	 *
	 * A visibility check has to be done before accessing attributes, as
	 * the heap may easily contain tuples that do not match the descriptor.
	 * For example BEGIN + ALTER TABLE ADD COLUMN + INSERT + ROLLBACK will
	 * produce such tuples, and the extra attribute will not be part of
	 * a descriptor (thanks to the rollback).
	 *
	 * A visibility fixes this - the visible tuples are guaranteed to match
	 * the current descriptor, which prevents these issues.
	 */
	tuple.t_data = tupheader;
	tuple.t_len = ItemIdGetLength(lp);
	ItemPointerSet(&(tuple.t_self), block, off);

	/*
	 * Inspect attributes only if the tuple is visible to our snapshot.
	 */
	if (!HeapTupleIsVisibleMVCC(&tuple, snapshot))
		return true;

	/* attribute offset - always starts at (buffer + off) */
	offset = lp->lp_off + tupheader->t_hoff;

	tuplenatts = HeapTupleHeaderGetNatts(tupheader);

	/*
	 * It's possible that the tuple descriptor has more attributes than the
	 * on-disk tuple. That can happen e.g. after a new attribute is added to
	 * the table in a way that does not require table rewrite (and possibly
	 * even after a rewrite, not sure).
	 *
	 * However, the opposite should never happen - visible on-disk tuples
	 * must not have more attributes than the descriptor.
	 */
	if (tuplenatts > rel->rd_att->natts)
	{
		ereport(WARNING,
				(errmsg("[%d:%d] tuple has too many attributes, %d found, %d expected",
						block, off,
						HeapTupleHeaderGetNatts(tupheader),
						RelationGetNumberOfAttributes(rel))));

		return false;
	}

	ereport(DEBUG3,
			(errmsg("[%d:%d] tuple has %d attributes (%d in relation)",
					block, off, tuplenatts, rel->rd_att->natts)));

	/* check all the attributes */
	for (i = 0; i < tuplenatts; i++)
	{
		CompactAttribute *attr = TupleDescCompactAttr(rel->rd_att, i);
		char *attname = get_attname(RelationGetRelid(rel), (i + 1), false);

		/* actual length of the attribute value */
		int			len;

		/* copied from src/backend/commands/analyze.c */
		bool		is_varlena = (!attr->attbyval && attr->attlen == -1);
		bool		is_varwidth = (!attr->attbyval && attr->attlen < 0);

		/*
		 * If the attribute is marked as NULL (in the tuple header), skip to
		 * the next attribute. The bitmap is only present when the tuple has
		 * HEAP_HASNULL flag.
		 */
		if ((tupheader->t_infomask & HEAP_HASNULL) &&
			att_isnull(i, tupheader->t_bits))
		{
			ereport(DEBUG3,
					(errmsg("[%d:%d] attribute '%s' is NULL (skipping)",
							block, off, attname)));
			has_nulls = true;	/* remember we've seen NULL value */
			continue;
		}

		/* track offset, fix the alignment */
		offset = att_pointer_alignby(offset, attr->attalignby,
									 attr->attlen,
								     buffer + offset);

		if (is_varlena)
		{
			/*
			 * OK, so it's a varlena. We need to determine and check length
			 * occupied in the tuple, so that we can continue with attributes
			 * after it. Large values may be TOAST-ed, but VARSIZE_ANY should
			 * simply give us the right number (toast pointer).
			 *
			 * XXX Only check negative value here, we'll check that it does
			 * not overflow tuple end later (for all attribute types).
			 */

			len = VARSIZE_ANY(buffer + offset);
			if (len < 0)
			{
				ereport(WARNING,
						(errmsg("[%d:%d] attribute '%s' has negative length < 0 (%d)",
								block, off, attname, len)));
				return false;
			}

			/*
			 * If the value is EXTERNAL (i.e. TOAST-ed), then it has to be
			 * ONDISK (we're reading it right from the page, so it can't be
			 * indirect nor expanded.
			 */
			if (VARATT_IS_EXTERNAL(buffer + offset))
			{
				varatt_external	toast_pointer;
				int32	extsize = VARATT_EXTERNAL_GET_EXTSIZE(toast_pointer);

				if (!VARATT_IS_EXTERNAL_ONDISK(buffer + offset))
				{
					ereport(WARNING,
							(errmsg("[%d:%d] attribute '%s' is EXTERNAL but not ONDISK",
									block, off, attname)));
					counters->heap_attr_toast_external_invalid += 1;
					return false;
				}

				/* Must copy to access aligned fields */
				VARATT_EXTERNAL_GET_POINTER(toast_pointer, buffer + offset);

				if ((toast_pointer.va_rawsize < 0) ||
					(toast_pointer.va_rawsize > 1024 * 1024 * 1024L))
				{
					/*
					 * This does not break the page structure (it's just a
					 * corrupted varlena header, probably), so we simply
					 * remember the failure and continue with the next
					 * attribute.
					 */
					ereport(WARNING,
							(errmsg("[%d:%d] external attribute '%s' has invalid raw length %d",
									block, off, attname, toast_pointer.va_rawsize)));

					counters->heap_attr_toast_external_invalid += 1;
					success = false;
					offset += len;	/* move to the next attribute */
					continue;
				}


				if ((extsize < 0) ||
					(extsize > 1024 * 1024 * 1024L))
				{
					/*
					 * This does not break the page structure (it's just a
					 * corrupted varlena header, probably), so we simply
					 * remember the failure and continue with the next
					 * attribute.
					 */
					ereport(WARNING,
							(errmsg("[%d:%d] external attribute '%s' has invalid external length %d",
									block, off, attname, extsize)));

					counters->heap_attr_toast_external_invalid += 1;
					success = false;
					offset += len;	/* move to the next attribute */
					continue;
				}

				/*
				 * Try detoasting the value, just like toast_fetch_datum does,
				 * but without the failures (i.e. we just need to report an
				 * issue and continue, without interrupting the whole scrub).
				 *
				 * Again, this does not break the page structure, so we can
				 * continue with more attributes.
				 */
				if (!check_toasted_attribute(snapshot, (struct varlena *) (buffer + offset), counters))
				{
					ereport(WARNING,
							(errmsg("[%d:%d] failed to detoast attribute '%s'",
									block, off, attname)));

					success = false;
					offset += len;	/* move to the next attribute */
					continue;
				}
			}

			if (VARATT_IS_COMPRESSED(buffer + offset))
			{
				/* the raw length should be less than 1G (and positive) */
				if ((VARSIZE_ANY_EXHDR(buffer + offset) < 0) ||
					(VARSIZE_ANY_EXHDR(buffer + offset) > 1024 * 1024 * 1024L))
				{
					/*
					 * This does not break the page structure (it's just a
					 * corrupted varlena header, probably), so we simply
					 * remember the failure and continue with the next
					 * attribute.
					 */
					ereport(WARNING,
							(errmsg("[%d:%d] attribute '%s' has invalid length %zd",
									block, off, attname,
									VARSIZE_ANY_EXHDR(buffer + offset))));

					counters->heap_attr_compression_broken += 1;
					success = false;
					offset += len;	/* move to the next attribute */
					continue;
				}

				/*
				 * TODO consider checking that:
				 *
				 * ((tp).va_extsize < (tp).va_rawsize - VARHDRSZ)
				 */
			}

			/*
			 * TODO  Check if the varlena value can be detoasted - see
			 * heap_tuple_untoast_attr in backend/access/heap/tuptoaster.c.
			 */
		}
		else if (is_varwidth)
		{
			int remaining_bytes;

			/*
			 * Get the C-string length (at most to the end of the tuple).
			 * To protect against unterminated strings, we use remaining
			 * bytes of the tuple (which ends at lp_off+lp_len) as end
			 * for the strnlen call.
			 *
			 * We do know that the \0 terminator is stored in the tuple
			 * (but not counted by strnlen), so we expect to get
			 *
			 * (len + 1) <= remaining_bytes
			 *
			 * We do the check a bit further down, for all types at once.
			 */
			remaining_bytes
				= (buffer + lp->lp_off + lp->lp_len) - (buffer + offset);

			len = strnlen(buffer + offset, remaining_bytes) + 1;
		}
		else
			/* attributes with fixed length */
			len = attr->attlen;

		Assert(len >= 0);

		/*
		 * Check if the length makes sense (is not negative and does not
		 * overflow the tuple end, stop validating the other rows (we don't
		 * know where to continue anyway).
		 */
		endoffset = lp->lp_off + lp->lp_len;
		if (offset + len > endoffset)
		{
			/* the attribute overflows end of the tuple (on disk) */
			ereport(WARNING,
					(errmsg("[%d:%d] attribute '%s' (off=%d len=%d) overflows tuple end (off=%d, len=%d)",
							block, off, attname,
							offset, len, lp->lp_off, lp->lp_len)));
			return false;
		}

		/* skip to the next attribute */
		offset += len;

		ereport(DEBUG3,
				(errmsg("[%d:%d] attribute '%s' length=%d",
						block, off, attname, len)));
	}

	ereport(DEBUG3,
			(errmsg("[%d:%d] last attribute ends at %d, tuple ends at %d",
					block, off, offset, lp->lp_off + lp->lp_len)));

	/*
	 * Check if tuples with HEAP_HASNULL actually have NULL attribute.
	 */
	if ((tupheader->t_infomask & HEAP_HASNULL) && !has_nulls)
	{
		ereport(WARNING,
				(errmsg("[%d:%d] has HEAP_HASNULL flag but no NULLs",
						block, off)));
		return false;
	}

	/*
	 * The end of last attribute should fall within the length given in the
	 * line pointer.
	 */
	endoffset = lp->lp_off + lp->lp_len;
	if (offset > endoffset)
	{
		ereport(WARNING,
				(errmsg("[%d:%d] the last attribute ends at %d but the tuple ends at %d",
						block, off, offset, endoffset)));
		return false;
	}

	/* OK, attributes for this tuple seem fine */
	return success;
}

/* checks that the tuples do not overlap and then the individual attributes */
static bool
check_heap_tuple(Relation rel, Page page, BlockNumber block,
				 OffsetNumber off, ScrubCounters *counters)
{
	OffsetNumber	off2;
	uint16		a_start,
				a_end;
	ItemId		lp;
	PageHeader	header = (PageHeader) page;

	/* get pointer to the line pointer */
	lp = &header->pd_linp[off - 1];

	/* check length with respect to lp_flags (unused, normal, redirect, dead) */
	if (lp->lp_flags == LP_REDIRECT)
	{
		uint16	offset,
				maxoffset;

		ereport(DEBUG2,
				(errmsg("[%d:%d] tuple is LP_REDIRECT", block, off)));

		/* redirected line pointers must not have any storage associated */
		if (lp->lp_len != 0)
		{
			ereport(WARNING,
					(errmsg("[%d:%d] tuple with LP_REDIRECT and len != 0 (%d)",
							block, off, lp->lp_len)));

			return false;
		}

		/*
		 * Check that the LP_REDIRECT target is OK (exists and is either
		 * LP_NORMAL or LP_DEAD), as expected by HOT. Also make sure the
		 * offset is valid (below pd_lower).
		 */
		offset = lp->lp_off;
		maxoffset = (header->pd_lower - SizeOfPageHeaderData) / sizeof(ItemIdData);

		/* the target offset is bogus */
		if (offset > maxoffset)
		{
			ereport(WARNING,
					(errmsg("[%d:%d] LP_REDIRECT item points to invalid offset %u (max %u)",
							block, off, offset, maxoffset)));

			return false;
		}

		/* the target item is neither NORMAL nor DEAD */
		if ((header->pd_linp[offset- 1].lp_flags != LP_NORMAL) &&
			(header->pd_linp[offset - 1].lp_flags != LP_DEAD))
		{
			ereport(WARNING,
					(errmsg("[%d:%d] LP_REDIRECT item points to item that is not LP_NORMAL/LP_DEAD (flag %u)",
							block, off, header->pd_linp[offset - 1].lp_flags)));

			return false;
		}

		/* OK, this REDIRECT item seems to be fine */
		return true;
	}
	else if (lp->lp_flags == LP_UNUSED)
	{
		ereport(DEBUG2,
				(errmsg("[%d:%d] tuple is LP_UNUSED", block, off)));

		/* LP_UNUSED => (len = 0) */
		if (lp->lp_len != 0)
		{
			ereport(WARNING,
					(errmsg("[%d:%d] tuple with LP_UNUSED and len != 0 (%d)",
							block, off, lp->lp_len)));

			return false;
		}

		/* OK, this UNUSED items seems to be fine (zero length) */
		return true;
	}
	else if (lp->lp_flags == LP_DEAD)
	{
		/*
		 * Dead tuples may or may not have storage, depending on if vacuum did
		 * the first part of heap cleanup. If there is no storage, we don't
		 * have anything to check. If there is storage, we do the same check
		 * as for LP_NORMAL.
		 */
		ereport(DEBUG2,
				(errmsg("[%d:%d] tuple is LP_DEAD", block, off)));

		/*
		 * No storage, so we're done with this item pointer.
		 */
		if (lp->lp_len == 0)
			return true;

		/*
		 * We intentionally don't return here, because for DEAD tuples with
		 * storage we want to do the cross-check with other tuples below.
		 * More thorough checks on offset/length are part of the cross-check.
		 */
	}
	else if (lp->lp_flags == LP_NORMAL)
	{
		ereport(DEBUG2,
				(errmsg("[%d:%d] tuple is LP_NORMAL", block, off)));

		/*
		 * Intentionally don't return, to do cross-check below. More thorough
		 * checks on offset/length are part of the cross-check.
		 */
	}
	else
	{
		ereport(WARNING,
				(errmsg("[%d:%d] item has unknown lp_flag %u",
						block, off, lp->lp_flags)));

		return false;
	}

	/*
	 * So the item is either LP_NORMAL or LP_DEAD (with storage). Check that
	 * the values (length and offset) are within reasonable boundaries (that
	 * is, the offset has to be between pd_upper and pd_special, and length
	 * has less than (pd_special - pd_upper).
	 *
	 * XXX We have already checked that pd_upper/pd_special are reasonable.
	 */
	if (lp->lp_off < header->pd_upper)
	{
		ereport(WARNING,
				(errmsg("[%d:%d] tuple with offset < pd_upper (%d < %d)",
						block, off, lp->lp_off, header->pd_upper)));

		return false;
	}
	else if (lp->lp_off + lp->lp_len > header->pd_special)
	{
		ereport(WARNING,
				(errmsg("[%d:%d] tuple with offset + length > special (%d + %d > %d)",
						block, off, lp->lp_off, lp->lp_len, header->pd_special)));

		return false;
	}

	/*
	 * Check if the tuple overlaps with any preceding tuple - subsequent ones
	 * will be cross-checked when check_heap_tuple gets called for them.
	 */

	a_start = lp->lp_off;
	a_end = lp->lp_off + lp->lp_len;

	for (off2 = 1; off2 < off; off2++)
	{
		uint16	b_start,
				b_end;

		ItemId	lp2 = PageGetItemId(page, off2);

		/*
		 * We only care about items with storage here, so we can skip
		 * LP_UNUSED and LP_REDIRECT right away, and LP_DEAD if they
		 * have no storage.
		 */
		if ((lp2->lp_flags == LP_UNUSED) ||
			(lp2->lp_flags == LP_REDIRECT) ||
			(lp2->lp_flags == LP_DEAD && lp2->lp_len == 0))
			continue;

		b_start = lp2->lp_off;
		b_end = lp2->lp_off + lp2->lp_len;

		/*
		 * When two intervals do not overlap, then one has to end before
		 * the other.
		 */
		if (!((a_end <= b_start) || (b_end <= a_start)))
		{
			ereport(WARNING,
					(errmsg("[%d:%d] intersects with [%d:%d] (%d,%d) vs. (%d,%d)",
							block, off, block, off2, a_start, a_end, b_start, b_end)));

			return false;
		}
	}

	/* check attributes of the tuple */
	return check_heap_tuple_attributes(rel, page, block, off, counters);
}

/* checks heap tuples (table) on the page, one by one */
static bool
check_page_heap(Relation rel, Page page, BlockNumber block,
				ScrubCounters *counters)
{
	/* tuple checks */
	int		maxoff = PageGetMaxOffsetNumber(page);
	int		off;
	bool	success = true;

	/* XXX Can we get maxoff > MaxHeapTuplesPerPage here? */

	for (off = 1; off <= maxoff; off++)
	{
		/* XXX we're checking items and not tuples */
		counters->heap_tuples_total += 1;

		/* check all tuples, but return false on any failure */
		if (!check_heap_tuple(rel, page, block, off, counters))
		{
			counters->heap_tuples_failed += 1;
			success = false;
		}
	}

	return success;
}



/* checks the individual attributes of the tuple */
static bool
check_btree_attributes(Relation rel, Page page, BlockNumber block,
					   OffsetNumber off, int dlen,
					   ScrubCounters *counters)
{
	IndexTuple	tuple;
	int			i,
				offset;
	BTPageOpaque opaque;
	ItemId		linp;
	bool		has_nulls = false;
	bits8	   *nulls;

	/* page as a simple array of bytes */
	char	   *buffer = (char *) page;

	/* get the index tuple and info about the page */
	linp = PageGetItemId(page, off);
	tuple = (IndexTuple) PageGetItem(page, linp);
	opaque = (BTPageOpaque) PageGetSpecialPointer(page);

	/* current attribute offset - always starts at (raw_page + off) */
	offset = linp->lp_off + IndexInfoFindDataOffset(tuple->t_info);

	/*
	 * For non-leaf pages, the first data tuple may or may not actually have
	 * any data. See src/backend/access/nbtree/README, "Notes About Data
	 * Representation".
	 *
	 * FIXME This may not be quite right. Maybe we should only check this on
	 * (non-)leaf pages? But then it causes assert failures. However, why
	 * shouldn't we be checking the hikey attributes too?
	 */
	if (off < P_FIRSTDATAKEY(opaque))
	{
		ereport(DEBUG3,
				(errmsg("[%d:%d] first data key tuple on non-leaf block => no data, skipping",
						block, off)));
		return true;
	}

	/* XXX: MAXALIGN? */
	nulls = (bits8 *) (buffer + linp->lp_off + sizeof(IndexTupleData));

	/*
	 * check all the index attributes
	 *
	 * TODO This is mostly copy'n'paste from check_heap_tuple_attributes, so
	 * maybe it could be refactored to share the code.
	 */
	for (i = 0; i < rel->rd_att->natts; i++)
	{
		CompactAttribute *attr = TupleDescCompactAttr(rel->rd_att, i);
		char *attname = get_attname(RelationGetRelid(rel), (i + 1), false);

		/* actual length of the attribute value */
		int			len;

		/* copy from src/backend/commands/analyze.c */
		bool		is_varlena = (!attr->attbyval && attr->attlen == -1);
		bool		is_varwidth = (!attr->attbyval && attr->attlen < 0);

		/*
		 * Skip attributes marked as NULL (in the tuple header).
		 */
		if (IndexTupleHasNulls(tuple) && att_isnull(i, nulls))
		{
			has_nulls = true;
			continue;
		}

		/* fix the alignment (see src/include/access/tupmacs.h) */
		offset = att_pointer_alignby(offset, attr->attalignby,
									 attr->attlen, buffer + offset);

		if (is_varlena)
		{
			/*
			 * We don't support toasted values in indexes, so this should not
			 * have the same issue as check_heap_tuple_attributes.
			 */

			len = VARSIZE_ANY(buffer + offset);

			if (len < 0)
			{
				ereport(WARNING,
						(errmsg("[%d:%d] attribute '%s' has negative length < 0 (%d)",
								block, off, attname, len)));
				return false;
			}

			/* indexes must not contain toasted values */
			if (VARATT_IS_EXTERNAL(buffer + offset))
			{
				ereport(WARNING,
						(errmsg("[%d:%d] attribute '%s' is marked as EXTERNAL",
								block, off, attname)));
				return false;
			}

			if (VARATT_IS_COMPRESSED(buffer + offset))
			{
				/* the raw length should be less than 1G (and positive) */
				if ((VARSIZE_ANY_EXHDR(buffer + offset) < 0) ||
					(VARSIZE_ANY_EXHDR(buffer + offset) > 1024 * 1024 * 1024L))
				{
					ereport(WARNING,
							(errmsg("[%d:%d]  attribute '%s' has invalid length %zd (should be between 0 and 1G)",
									block, off, attname,
									VARSIZE_ANY_EXHDR(buffer + offset))));
					return false;
				}
			}
		}
		else if (is_varwidth)
		{
			int remaining_bytes;

			/*
			 * Get the C-string length (at most to the end of the tuple).
			 * To protect against unterminated strings, we use remaining
			 * bytes of the tuple (which ends at lp_off+lp_len) as end
			 * for the strnlen call.
			 *
			 * We do know that the \0 terminator is stored in the tuple
			 * (but not counted by strnlen), so we expect to get
			 *
			 * (len + 1) <= remaining_bytes
			 *
			 * We do the check a bit further down, for all types at once.
			 */
			remaining_bytes
				= (buffer + linp->lp_off + linp->lp_len) - (buffer + offset);

			len = strnlen(buffer + offset, remaining_bytes) + 1;
		}
		else
			/* attributes with fixed length */
			len = attr->attlen;

		Assert(len >= 0);

		/*
		 * Check if the length makes sense (is not negative and does not
		 * overflow the tuple end, stop validating the other rows (we don't
		 * know where to continue anyway).
		 */
		if ((dlen > 0) && (offset + len > (linp->lp_off + linp->lp_len)))
		{
			ereport(WARNING,
					(errmsg("[%d:%d] attribute '%s' (offset=%d length=%d) overflows tuple end (off=%d, len=%d)",
							block, off, attname,
							offset, len, linp->lp_off, linp->lp_len)));
			return false;
		}

		/* skip to the next attribute */
		offset += (dlen > 0) ? len : 0;

		ereport(DEBUG3,
				(errmsg("[%d:%d] attribute '%s' len=%d",
						block, off, attname, len)));
	}

	ereport(DEBUG3,
			(errmsg("[%d:%d] last attribute ends at %d, tuple ends at %d",
					block, off, offset, linp->lp_off + linp->lp_len)));

	/*
	 * Check if tuples with nulls (INDEX_NULL_MASK) actually have NULLs.
	 */
	if (IndexTupleHasNulls(tuple) && !has_nulls)
	{
		ereport(WARNING,
				(errmsg("[%d:%d] tuple has INDEX_NULL_MASKL flag but no NULLs",
						block, off)));
		return false;
	}

	/*
	 * after the last attribute, the offset should be less than the end of the
	 * tuple
	 */
	if (MAXALIGN(offset) > linp->lp_off + linp->lp_len)
	{
		ereport(WARNING,
				(errmsg("[%d:%d] the last attribute ends at %d but the tuple ends at %d",
						block, off, offset, linp->lp_off + linp->lp_len)));
		return false;
	}

	return true;
}

/* checks that the tuples do not overlap and then the individual attributes */
/* FIXME This should do exactly the same checks of lp_flags as in heap.c */
static bool
btree_check_tuple(Relation rel, Page page, BlockNumber block, OffsetNumber off,
				  ScrubCounters *counters)
{
	int			dlen;
	IndexTuple	itup;
	OffsetNumber	off2,
					a_start,
					a_end;

	ItemId		lp = PageGetItemId(page, off);

	/*
	 * we can ignore unused items
	 *
	 * XXX Should we check some other fields of the ItemId (length?)
	 */
	if (lp->lp_flags == LP_UNUSED)
	{
		ereport(DEBUG2,
				(errmsg("[%d:%d] index item is unused", block, off)));
		return true;
	}

	/*
	 * We only expect LP_NORMAL, LP_UNUSED and LP_DEAD items in indexes.
	 * LP_DEAD is used by index scans to "kill" dead tuples that became
	 * invisible to all transactions. So report any items with
	 * unexpected status.
	 *
	 * XXX LP_UNUSED items were already handled by a preceding check.
	 */
	if ((lp->lp_flags != LP_NORMAL) &&
		(lp->lp_flags != LP_DEAD))
	{
		ereport(WARNING,
				(errmsg("[%d:%d] index item has unexpected lp_flags (%u)",
						block, off, lp->lp_flags)));
		return false;
	}

	/* OK, so this is a LP_NORMAL/LP_DEAD item, and we can inspect it. */
	itup = (IndexTuple) PageGetItem(page, lp);

	/* check intersection with other tuples */

	/* [A,B] vs [C,D] */
	a_start = lp->lp_off;
	a_end   = lp->lp_off + lp->lp_len;

	for (off2 = 1; off2 < off; off2++)
	{
		OffsetNumber	b_start,
						b_end;
		ItemId		lp2 = PageGetItemId(page, off2);

		/*
		 * We only expect LP_NORMAL and LP_UNUSED items in (btree) indexes,
		 * and we can skip the unused ones.
		 */
		if (lp2->lp_flags == LP_UNUSED)
			continue;

		/*
		 * This was already detected while processing the tuple, so this time
		 * we simply skip it (otherwise all subsequent tuples would fail).
		 */
		if ((lp2->lp_flags != LP_NORMAL) &&
			(lp2->lp_flags != LP_DEAD))
			continue;

		b_start = lp2->lp_off;
		b_end = lp2->lp_off + lp2->lp_len;

		/*
		 * When two intervals do not overlap, then one has to end before
		 * the other.
		 */
		if (!((a_end <= b_start) || (b_end <= a_start)))
		{
			ereport(WARNING,
					(errmsg("[%d:%d] intersects with [%d:%d] (%d,%d) vs. (%d,%d)",
							block, off, block, off2,
							a_start, a_end, b_start, b_end)));
			return false;
		}
	}

	/* compute size of the data stored in the index tuple */
	dlen = IndexTupleSize(itup) - IndexInfoFindDataOffset(itup->t_info);

	/* check attributes only for tuples with (lp_flags==LP_NORMAL) */
	return check_btree_attributes(rel, page, block, off, dlen, counters);
}


/* checks index tuples on the page, one by one */
static bool
check_page_tuples_btree(Relation rel, Page page, BlockNumber block,
						ScrubCounters *counters)
{
	/* tuple checks */
	OffsetNumber	maxoffset = PageGetMaxOffsetNumber(page);
	OffsetNumber	off;
	bool			success = true;

	ereport(DEBUG1,
			(errmsg("[%d] max number of tuples = %d", block, maxoffset)));

	/* FIXME this should check lp_flags, just as the heap check */
	for (off = 1; off <= maxoffset; off++)
	{
		counters->btree_tuples_total += 1;

		if (!btree_check_tuple(rel, page, block, off, counters))
		{
			counters->btree_tuples_failed += 1;
			success = false;
		}
	}

	return success;
}

static bool
check_page_btree(Relation rel, Page page, BlockNumber block,
				 ScrubCounters *counters)
{
	PageHeader		header = (PageHeader) page;
	BTPageOpaque	opaque;

	/*
	 * Block 0 is a meta-page, otherwise it's a regular index-page.
	 */
	if (block == BTREE_METAPAGE)
	{
		BTMetaPageData *mpdata = BTPageGetMeta(page);

		if (mpdata->btm_magic != BTREE_MAGIC)
		{
			ereport(WARNING,
					(errmsg("[%d] metapage contains invalid magic number %d (should be %d)",
							block, mpdata->btm_magic, BTREE_MAGIC)));
			return false;
		}

		if (mpdata->btm_version != BTREE_VERSION)
		{
			ereport(WARNING,
					(errmsg("[%d] metapage contains invalid version %d (should be %d)",
							block, mpdata->btm_version, BTREE_VERSION)));
			return false;
		}

		/*
		 * FIXME Check that the btm_root/btm_fastroot is between 1 and
		 * number of index blocks.
		 *
		 * FIXME Check that the btm_level/btm_fastlevel is equal to the
		 * level fo the root block.
		 */

		return true;
	}

	/*
	 * Non-metapage, so it's supposed to have special area at the end.
	 * Check that there's just sufficient amount of space for index data.
	 */
	if (header->pd_special > BLCKSZ - sizeof(BTPageOpaque))
	{
		ereport(WARNING,
				(errmsg("[%d] there's not enough special space for index data (%d > %d)",
						block,
						(int) sizeof(BTPageOpaque),
						BLCKSZ - header->pd_special)));
		return false;
	}

	/*
	 * Get the opaque pointer. At this point we can just use the macro,
	 * as pd_special was thoroughly tested.
	 */
	opaque = (BTPageOpaque) PageGetSpecialPointer(page);

	/*
	 * If the page is a leaf page, then level needs to be 0. Otherwise, it
	 * should be greater than 0. Deleted pages don't have a level though,
	 * the level field is replaced with an XID.
	 */
	if (!P_ISDELETED(opaque))
	{
		if (P_ISLEAF(opaque) && (opaque->btpo_level > 0))
		{
			ereport(WARNING,
					(errmsg("[%d] is leaf page, but level %d is not zero",
							block, opaque->btpo_level)));
			return false;
		}
		else if (!P_ISLEAF(opaque) && (opaque->btpo_level == 0))
		{
			ereport(WARNING,
					(errmsg("[%d] is a non-leaf page, but level is zero",
							block)));
			return false;
		}
	}

	/*
	 * FIXME check btpo_flags (BTP_LEAF, BTP_ROOT, BTP_DELETED, BTP_META,
	 * BTP_HALF_DEAD, BTP_SPLIT_END and BTP_HAS_GARBAGE) and act accordingly.
	 */

	return check_page_tuples_btree(rel, page, block, counters);
}

bool
check_page_contents(Relation rel, ForkNumber forkNum,
					Page page, BlockNumber block, ScrubCounters *counters)
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



/* ----------
 * toast_open_indexes
 *
 *	Get an array of the indexes associated to the given toast relation
 *	and return as well the position of the valid index used by the toast
 *	relation in this array. It is the responsibility of the caller of this
 *	function to close the indexes as well as free them.
 */
static int
toast_open_indexes(Relation toastrel,
				   LOCKMODE lock,
				   Relation **toastidxs,
				   int *num_indexes)
{
	int			i = 0;
	int			res = 0;
	bool		found = false;
	List	   *indexlist;
	ListCell   *lc;

	/* Get index list of the toast relation */
	indexlist = RelationGetIndexList(toastrel);
	Assert(indexlist != NIL);

	*num_indexes = list_length(indexlist);

	/* Open all the index relations */
	*toastidxs = (Relation *) palloc(*num_indexes * sizeof(Relation));
	foreach(lc, indexlist)
		(*toastidxs)[i++] = index_open(lfirst_oid(lc), lock);

	/* Fetch the first valid index in list */
	for (i = 0; i < *num_indexes; i++)
	{
		Relation	toastidx = (*toastidxs)[i];

		if (toastidx->rd_index->indisvalid)
		{
			res = i;
			found = true;
			break;
		}
	}

	/*
	 * Free index list, not necessary anymore as relations are opened and a
	 * valid index has been found.
	 */
	list_free(indexlist);

	/*
	 * The toast relation should have one valid index, so something is going
	 * wrong if there is nothing.
	 */
	if (!found)
		elog(ERROR, "no valid index found for toast relation with Oid %u",
			 RelationGetRelid(toastrel));

	return res;
}

/* ----------
 * toast_close_indexes
 *
 *	Close an array of indexes for a toast relation and free it. This should
 *	be called for a set of indexes opened previously with toast_open_indexes.
 */
static void
toast_close_indexes(Relation *toastidxs, int num_indexes, LOCKMODE lock)
{
	int			i;

	/* Close relations and clean up things */
	for (i = 0; i < num_indexes; i++)
		index_close(toastidxs[i], lock);
	pfree(toastidxs);
}


void
merge_counters(ScrubCounters *dst, ScrubCounters *src)
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
