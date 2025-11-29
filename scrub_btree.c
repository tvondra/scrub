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
/* #include "utils/tqual.h" */

#include "scrub.h"


/* checks the individual attributes of the tuple */
static bool
check_btree_attributes(Relation rel, Page page, BlockNumber block,
					   OffsetNumber off, int dlen,
					   ScrubCounters * counters)
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
		char	   *attname = get_attname(RelationGetRelid(rel), (i + 1), false);

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

			/*
			 * XXX We can get here for internal pages, because we include only
			 * values needed to decide where to go next. In that case we
			 * should not print the warning at all. We should still do some
			 * checks, e.g. that we matched the expected end of data, and so
			 * on.
			 */
			Assert(!P_ISLEAF(opaque));

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
				  ScrubCounters * counters)
{
	int			dlen;
	IndexTuple	itup;
	OffsetNumber off2,
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
	 * invisible to all transactions. So report any items with unexpected
	 * status.
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
	a_end = lp->lp_off + lp->lp_len;

	for (off2 = 1; off2 < off; off2++)
	{
		OffsetNumber b_start,
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
		 * When two intervals do not overlap, then one has to end before the
		 * other.
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
						ScrubCounters * counters)
{
	/* tuple checks */
	OffsetNumber maxoffset = PageGetMaxOffsetNumber(page);
	OffsetNumber off;
	bool		success = true;

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

bool
check_page_btree(Relation rel, Page page, BlockNumber block,
				 ScrubCounters * counters)
{
	PageHeader	header = (PageHeader) page;
	BTPageOpaque opaque;

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
		 * FIXME Check that the btm_root/btm_fastroot is between 1 and number
		 * of index blocks.
		 *
		 * FIXME Check that the btm_level/btm_fastlevel is equal to the level
		 * fo the root block.
		 */

		return true;
	}

	/*
	 * Non-metapage, so it's supposed to have special area at the end. Check
	 * that there's just sufficient amount of space for index data.
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
	 * Get the opaque pointer. At this point we can just use the macro, as
	 * pd_special was thoroughly tested.
	 */
	opaque = (BTPageOpaque) PageGetSpecialPointer(page);

	/*
	 * If the page is a leaf page, then level needs to be 0. Otherwise, it
	 * should be greater than 0. Deleted pages don't have a level though, the
	 * level field is replaced with an XID.
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
