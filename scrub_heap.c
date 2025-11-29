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

#include "access/detoast.h"
#include "access/heaptoast.h"
#include "access/xact.h"
#include "funcapi.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "scrub.h"

static bool HeapTupleIsVisibleMVCC(HeapTuple htup, Snapshot snapshot);


/* checks the individual attributes of the tuple */
static bool
check_heap_tuple_attributes(Relation rel, Page page, BlockNumber block,
							OffsetNumber off, ScrubCounters * counters)
{
	HeapTupleData tuple;
	HeapTupleHeader tupheader;
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
	 * A visibility check has to be done before accessing attributes, as the
	 * heap may easily contain tuples that do not match the descriptor. For
	 * example BEGIN + ALTER TABLE ADD COLUMN + INSERT + ROLLBACK will produce
	 * such tuples, and the extra attribute will not be part of a descriptor
	 * (thanks to the rollback).
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
	 * However, the opposite should never happen - visible on-disk tuples must
	 * not have more attributes than the descriptor.
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
		char	   *attname = get_attname(RelationGetRelid(rel), (i + 1), false);

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
				varatt_external toast_pointer;
				int32		extsize = VARATT_EXTERNAL_GET_EXTSIZE(toast_pointer);

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
			int			remaining_bytes;

			/*
			 * Get the C-string length (at most to the end of the tuple). To
			 * protect against unterminated strings, we use remaining bytes of
			 * the tuple (which ends at lp_off+lp_len) as end for the strnlen
			 * call.
			 *
			 * We do know that the \0 terminator is stored in the tuple (but
			 * not counted by strnlen), so we expect to get
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
				 OffsetNumber off, ScrubCounters * counters)
{
	OffsetNumber off2;
	uint16		a_start,
				a_end;
	ItemId		lp;
	PageHeader	header = (PageHeader) page;

	/* get pointer to the line pointer */
	lp = &header->pd_linp[off - 1];

	/* check length with respect to lp_flags (unused, normal, redirect, dead) */
	if (lp->lp_flags == LP_REDIRECT)
	{
		uint16		offset,
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
		if ((header->pd_linp[offset - 1].lp_flags != LP_NORMAL) &&
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
		 * storage we want to do the cross-check with other tuples below. More
		 * thorough checks on offset/length are part of the cross-check.
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
		uint16		b_start,
					b_end;

		ItemId		lp2 = PageGetItemId(page, off2);

		/*
		 * We only care about items with storage here, so we can skip
		 * LP_UNUSED and LP_REDIRECT right away, and LP_DEAD if they have no
		 * storage.
		 */
		if ((lp2->lp_flags == LP_UNUSED) ||
			(lp2->lp_flags == LP_REDIRECT) ||
			(lp2->lp_flags == LP_DEAD && lp2->lp_len == 0))
			continue;

		b_start = lp2->lp_off;
		b_end = lp2->lp_off + lp2->lp_len;

		/*
		 * When two intervals do not overlap, then one has to end before the
		 * other.
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

	/*
	 * Assert that the caller has registered the snapshot.  This function
	 * doesn't care about the registration as such, but in general you
	 * shouldn't try to use a snapshot without registration because it might
	 * get invalidated while it's still in use, and this is a convenient place
	 * to check for that.
	 */
	Assert(snapshot->regd_count > 0 || snapshot->active_count > 0);

	Assert(ItemPointerIsValid(&htup->t_self));
	Assert(htup->t_tableOid != InvalidOid);

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

/* checks heap tuples (table) on the page, one by one */
bool
check_page_heap(Relation rel, Page page, BlockNumber block,
				ScrubCounters * counters)
{
	/* tuple checks */
	int			maxoff = PageGetMaxOffsetNumber(page);
	int			off;
	bool		success = true;

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
