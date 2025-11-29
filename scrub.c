/*-------------------------------------------------------------------------
 *
 * scrub.c
 *	  Backend worker to walk the database and check consistency of data.
 *
 * 
 *
 * IDENTIFICATION
 *	  scrub.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "scrub.h"
#include "scrub_checks.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_database.h"
#include "commands/dbcommands.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "storage/bufmgr.h"
#include "storage/buf_internals.h"
#include "storage/checksum.h"
#include "storage/ipc.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/ps_status.h"
#include "utils/lsyscache.h"

PG_MODULE_MAGIC;

void		_PG_init(void);

PG_FUNCTION_INFO_V1(scrub_start);
PG_FUNCTION_INFO_V1(scrub_stop);
PG_FUNCTION_INFO_V1(scrub_status);
PG_FUNCTION_INFO_V1(scrub_reset);
PG_FUNCTION_INFO_V1(scrub_is_running);

PGDLLEXPORT void ScrubLauncherMain(Datum main_arg);
PGDLLEXPORT void ScrubWorkerMain(Datum main_arg);

typedef struct ScrubShmemStruct
{
	pg_atomic_flag launcher_started;
	bool		stop_requested;
	bool		success;
	bool		process_shared_catalogs;

	/* parameter values (set on start) */
	int			cost_delay;
	int			cost_limit;
	int			dboid;

	/* current progress/status */
	slock_t		mutex;
	ScrubCounters	counters;

}			ScrubShmemStruct;

/* Shared memory segment for scrub helper */
static ScrubShmemStruct * ScrubShmem;

/* Bookkeeping for work to do */
typedef struct DatabaseEntry
{
	Oid			dboid;
	char	   *dbname;
	int			attempts;
}			DatabaseEntry;

typedef struct RelationEntry
{
	Oid			reloid;
	char		relkind;
	uint32		page;
}			RelationEntry;

/* Prototypes */
static List *BuildDatabaseList(void);
static List *BuildRelationList(bool include_shared);
static bool ScrubDatabase(DatabaseEntry * db);

static void scrub_shmem_init(void);

/* needed for disabling failures after checksum errors */
extern bool ignore_checksum_failure;

/*
 * Module load callback.
 */
void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	EmitWarningsOnPlaceholders("scrub");

	RequestAddinShmemSpace(ScrubShmemSize());
}


static void
scrub_shmem_init(void)
{
	bool		found;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	ScrubShmem = ShmemInitStruct("scrub",
								ScrubShmemSize(),
								&found);
	if (!found)
	{
		memset(ScrubShmem, 0, ScrubShmemSize());
	}

	LWLockRelease(AddinShmemInitLock);
}

/*
 * Main entry point for scrub helper launcher process
 */
bool
StartScrubLauncher(int cost_delay, int cost_limit, Oid dboid,
						 bool reset)
{
	BackgroundWorker bgw;
	BackgroundWorkerHandle *bgw_handle;

	scrub_shmem_init();

	if (!pg_atomic_test_set_flag(&ScrubShmem->launcher_started))
	{
		/*
		 * Failed to set means somebody else started
		 */
		ereport(ERROR,
				(errmsg("could not start scrub helper: already running")));
	}

	SpinLockAcquire(&ScrubShmem->mutex);

	ScrubShmem->cost_delay = cost_delay;
	ScrubShmem->cost_limit = cost_limit;
	ScrubShmem->dboid = dboid;
	ScrubShmem->stop_requested = false;

	if (reset)
		memset(&ScrubShmem->counters, 0, sizeof(ScrubCounters));

	SpinLockRelease(&ScrubShmem->mutex);

	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	snprintf(bgw.bgw_library_name, BGW_MAXLEN, "scrub");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "ScrubLauncherMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN, "scrub helper launcher");
	snprintf(bgw.bgw_type, BGW_MAXLEN, "scrub helper launcher");
	bgw.bgw_restart_time = BGW_NEVER_RESTART;
	bgw.bgw_notify_pid = MyProcPid;
	bgw.bgw_main_arg = (Datum) 0;

	elog(LOG, "launching scrub worker on DB %d", dboid);

	if (!RegisterDynamicBackgroundWorker(&bgw, &bgw_handle))
	{
		pg_atomic_clear_flag(&ScrubShmem->launcher_started);
		return false;
	}

	return true;
}

static bool
ScrubLauncherIsRunning(void)
{
	scrub_shmem_init();

	return (!pg_atomic_unlocked_test_flag(&ScrubShmem->launcher_started));
}

void
ShutdownScrubLauncherIfRunning(void)
{
	scrub_shmem_init();

	/* If launcher not started, nothing to shut down. */
	if (pg_atomic_unlocked_test_flag(&ScrubShmem->launcher_started))
		return;

	/* Set flag in shared memory, so that bgworkers stop. */
	SpinLockAcquire(&ScrubShmem->mutex);
	ScrubShmem->stop_requested = true;
	SpinLockRelease(&ScrubShmem->mutex);
}

static bool
should_terminate(void)
{
	bool terminate;

	/* Set flag in shared memory, so that bgworkers stop. */
	SpinLockAcquire(&ScrubShmem->mutex);
	terminate = ScrubShmem->stop_requested;
	SpinLockRelease(&ScrubShmem->mutex);

	return terminate;
}

/*
 * Scrub a single relation/fork.
 *
 * XXX What if the relation gets dropped before we get to it?
 */
static bool
ScrubSingleRelationFork(Relation reln, ForkNumber forkNum, BufferAccessStrategy strategy)
{
	BlockNumber numblocks = RelationGetNumberOfBlocksInFork(reln, forkNum);
	BlockNumber b;

	char	buffer[1024];
	char   *forks[] = {"MAIN", "FSM", "VISIBILITYMAP", "INIT"};

	/*
	 * Make sure we ignore checksum errors, so that the scrubbing is
	 * not interrupted on the first checksum error (we want to scan the
	 * whole relation).
	 */
	ignore_checksum_failure = true;

	/* Check if the user requested to stop the scrub. */
	if (should_terminate())
		return false;

	for (b = 0; b < numblocks; b++)
	{
		Buffer		buf = ReadBufferExtended(reln, forkNum, b, RBM_NORMAL, strategy);
		Page		page;

		ScrubCounters	counters;
		bool		failure = true;	/* assume failure */

		sprintf(buffer, "scrubbing (\"%s\".\"%s\") : %s %u/%u",
						get_namespace_name(RelationGetNamespace(reln)),
						RelationGetRelationName(reln),
						forks[forkNum], b, numblocks);

		set_ps_display(buffer);

		memset(&counters, 0, sizeof(ScrubCounters));

		/* Need to get a share lock before accessing it */
		LockBuffer(buf, BUFFER_LOCK_SHARE);

		/* Do we already have a valid checksum? */
		page = BufferGetPage(buf);

		/*
		 * verify page checksum
		 *
		 * XXX This is a bit pointless, because we check the checksums when
		 * reading the page into shared buffers, and that already happened
		 * in ReadBufferExtended above. So this can't find a failure, IMO.
		 *
		 * XXX We should probably lock the buffer for I/O (so that others
		 * can't write it out), and read the page ourselves into a small
		 * private buffer (not into shared buffers). And check that.
		 */
		if (!check_page_checksum(reln, forkNum, b, &counters))
			goto update_stats;

		/* check page header */
		if (!check_page_header(reln, forkNum, page, b, &counters))
			goto update_stats;

		/* check page contents (updates counters directly) */
		if (!check_page_contents(reln, forkNum, page, b, &counters))
			goto update_stats;

		/* if we got here, the page is fine */
		failure = false;

update_stats:

		/* if anything interrupted the checks, count it as failure */
		counters.pages_total += 1;
		counters.pages_failed += (failure) ? 1 : 0;

		/* Update the counters placed in shared memory. */
		SpinLockAcquire(&ScrubShmem->mutex);

		merge_counters(&ScrubShmem->counters, &counters);

		SpinLockRelease(&ScrubShmem->mutex);

		/*
		 * XXX maybe we could create a copy of the page, unlock it and
		 * then do the checks on the copy?
		 */
		UnlockReleaseBuffer(buf);

		/* Check if the user requested to stop the scrub. */
		if (should_terminate())
			return false;

		/* If not, do the throttling. */
		vacuum_delay_point(false);
	}

	return true;
}

static bool
ScrubSingleRelationByOid(Oid relationId, BufferAccessStrategy strategy)
{
	bool		success = true;
	Relation	rel;
	ForkNumber	fnum;
	char		buffer[1024];
	Snapshot	snapshot = SnapshotAny;

	StartTransactionCommand();

	/*
	 * properly install a valid snapshot, make sure it's active
	 *
	 * XXX This creates a snapshot for the whole scrub the current relation.
	 * Maybe that's too long, and could cause issues with holding xmin back,
	 * and so on? Then maybe create shorter snapshots for blocks or a couple
	 * of blocks?
	 */
	snapshot = RegisterSnapshot(GetTransactionSnapshot());
	PushActiveSnapshot(snapshot);

	rel = relation_open(relationId, AccessShareLock);

	/* skip relations without a storage */
	if (!RELKIND_HAS_STORAGE(rel->rd_rel->relkind))
		goto cleanup;

	elog(LOG, "scrubbing relation %d (\"%s\".\"%s\")", relationId,
			  get_namespace_name(RelationGetNamespace(rel)),
			  RelationGetRelationName(rel));

	/* update process title */
	sprintf(buffer, "scrubbing (\"%s\".\"%s\")",
					get_namespace_name(RelationGetNamespace(rel)),
					RelationGetRelationName(rel));
	set_ps_display(buffer);

	RelationGetSmgr(rel);

	/* process all forks existing for the relation */
	for (fnum = 0; fnum <= MAX_FORKNUM; fnum++)
	{
		/* Check if the user requested to stop the scrub. */
		if (should_terminate())
		{
			success = false;
			break;
		}

		if (smgrexists(rel->rd_smgr, fnum))
		{
			if (!ScrubSingleRelationFork(rel, fnum, strategy))
			{
				success = false;
				break;
			}
		}
	}

cleanup:
	relation_close(rel, AccessShareLock);

	/* cleanup the snapshot */
	PopActiveSnapshot();
	UnregisterSnapshot(snapshot);

	CommitTransactionCommand();

	return success;
}

/*
 * ScrubDatabase
 *		scrub a single database
 *
 * We do this by launching a dynamic background worker into this database,
 * and waiting for it to finish.  We have to do this in a separate worker,
 * since each process can only be connected to one database during it's
 * lifetime.
 */
static bool
ScrubDatabase(DatabaseEntry * db)
{
	BackgroundWorker bgw;
	BackgroundWorkerHandle *bgw_handle;
	BgwHandleStatus status;
	pid_t		pid;

	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	snprintf(bgw.bgw_library_name, BGW_MAXLEN, "scrub");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "ScrubWorkerMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN, "scrub helper worker");
	snprintf(bgw.bgw_type, BGW_MAXLEN, "scrub helper worker");
	bgw.bgw_restart_time = BGW_NEVER_RESTART;
	bgw.bgw_notify_pid = MyProcPid;
	bgw.bgw_main_arg = ObjectIdGetDatum(db->dboid);

	elog(LOG, "scrubbing database %d (%s)", db->dboid, db->dbname);

	if (!RegisterDynamicBackgroundWorker(&bgw, &bgw_handle))
	{
		ereport(LOG,
				(errmsg("failed to start worker for scrub helper in %s", db->dbname)));
		return false;
	}

	status = WaitForBackgroundWorkerStartup(bgw_handle, &pid);
	if (status != BGWH_STARTED)
	{
		ereport(LOG,
				(errmsg("failed to wait for worker startup for scrub helper in %s", db->dbname)));
		return false;
	}

	ereport(DEBUG1,
			(errmsg("started background worker for scrub in %s", db->dbname)));

	status = WaitForBackgroundWorkerShutdown(bgw_handle);
	if (status != BGWH_STOPPED)
	{
		ereport(LOG,
				(errmsg("failed to wait for worker shutdown for scrub helper in %s", db->dbname)));
		return false;
	}

	ereport(DEBUG1,
			(errmsg("background worker for scrub in %s completed", db->dbname)));

	return ScrubShmem->success;
}

static void
launcher_exit(int code, Datum arg)
{
	pg_atomic_clear_flag(&ScrubShmem->launcher_started);
}

/*
 * Main function for the scrub launcher process, which starts a bgworker
 * for each database.
 */
void
ScrubLauncherMain(Datum arg)
{
	ListCell   *lc;
	List	   *DatabaseList;

	on_shmem_exit(launcher_exit, 0);

	ereport(LOG,
			(errmsg("scrub helper launcher started")));

	pqsignal(SIGTERM, die);

	BackgroundWorkerUnblockSignals();

	init_ps_display("scrub launcher");

	scrub_shmem_init();

	/*
	 * Initialize a connection to shared catalogs only.
	 */
	BackgroundWorkerInitializeConnection(NULL, NULL,
										 BGWORKER_BYPASS_ALLOWCONN);

	/*
	 * Set up so first run processes shared catalogs, but not once in every db
	 */
	ScrubShmem->process_shared_catalogs = true;

	/*
	 * Create a database list.
	 */
	DatabaseList = BuildDatabaseList();

	/*
	 * If there are no databases at all to scrub, we can exit immediately
	 * as there is no work to do.
	 */
	if (DatabaseList == NIL || list_length(DatabaseList) == 0)
		return;

	foreach(lc, DatabaseList)
	{
		DatabaseEntry *db = (DatabaseEntry *) lfirst(lc);

		/* Check if the user requested to stop the scrub. */
		if (should_terminate())
			break;

		/*
		 * The database may have disappeared, in which case the scrub
		 * will fail. But we want to continue with the other items.
		 */
		if (!ScrubDatabase(db))
			ereport(WARNING,
					(errmsg("failed to scrub db \"%s\"", db->dbname)));

		/*
		 * Now that one database has completed shared catalogs, we
		 * don't have to process them again .
		 */
		ScrubShmem->process_shared_catalogs = false;
	}

	ereport(LOG,
			(errmsg("scrub complete, scrub helper launcher shutting down")));
}


/*
 * ScrubShmemSize
 *		Compute required space for scrublauncher-related shared memory
 */
Size
ScrubShmemSize(void)
{
	Size		size;

	size = sizeof(ScrubShmemStruct);
	size = MAXALIGN(size);

	return size;
}

/*
 * ScrubShmemInit
 *		Allocate and initialize scrublauncher-related shared memory
 */
void
ScrubShmemInit(void)
{
	bool		found;

	ScrubShmem = (ScrubShmemStruct *) ShmemInitStruct("Scrub Data",
													  ScrubShmemSize(),
													  &found);

	pg_atomic_init_flag(&ScrubShmem->launcher_started);

	/* initialize the mutex, guarding the shared state */
	SpinLockInit(&ScrubShmem->mutex);
}


/*
 * BuildDatabaseList
 *		Compile a list of all currently available databases in the cluster
 *
 * This is intended to create the worklist for the workers to go through, and
 * as we are only concerned with already existing databases we need to ever
 * rebuild this list, which simplifies the coding.
 */
static List *
BuildDatabaseList(void)
{
	List	   *DatabaseList = NIL;
	Relation	rel;
	TableScanDesc scan;
	HeapTuple	tup;
	MemoryContext ctx = CurrentMemoryContext;
	MemoryContext oldctx;

	StartTransactionCommand();

	rel = table_open(DatabaseRelationId, AccessShareLock);
	scan = table_beginscan_catalog(rel, 0, NULL);

	while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
	{
		Form_pg_database pgdb = (Form_pg_database) GETSTRUCT(tup);
		DatabaseEntry *db;

		if ((ScrubShmem->dboid != InvalidOid) &&
			(ScrubShmem->dboid != pgdb->oid))
			continue;

		if (!pgdb->datallowconn)
			ereport(WARNING,
					(errmsg("Database %s does not allow connections.", NameStr(pgdb->datname)),
					 errhint("Allow connections using ALTER DATABASE and try again.")));

		oldctx = MemoryContextSwitchTo(ctx);

		db = (DatabaseEntry *) palloc(sizeof(DatabaseEntry));

		db->dboid = pgdb->oid;
		db->dbname = pstrdup(NameStr(pgdb->datname));

		DatabaseList = lappend(DatabaseList, db);

		MemoryContextSwitchTo(oldctx);
	}

	table_endscan(scan);
	table_close(rel, AccessShareLock);

	CommitTransactionCommand();

	return DatabaseList;
}

/*
 * BuildRelationList
 *		Compile a list of all relations existing in the database
 *
 * Build list of relations to scrub. If shared is true, both shared
 * relations and local ones are returned, else all non-shared relations
 * are returned.
 */
static List *
BuildRelationList(bool include_shared)
{
	List	   *RelationList = NIL;
	Relation	rel;
	TableScanDesc scan;
	HeapTuple	tup;
	MemoryContext ctx = CurrentMemoryContext;
	MemoryContext oldctx;

	StartTransactionCommand();

	rel = table_open(RelationRelationId, AccessShareLock);
	scan = table_beginscan_catalog(rel, 0, NULL);

	while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
	{
		Form_pg_class pgc = (Form_pg_class) GETSTRUCT(tup);
		RelationEntry *relentry;

		if (pgc->relisshared && !include_shared)
			continue;

		/*
		 * Foreign tables have by definition no local storage that can be
		 * scrubbed, so just skip them.
		 */
		if (pgc->relkind == RELKIND_FOREIGN_TABLE)
			continue;

		oldctx = MemoryContextSwitchTo(ctx);
		relentry = (RelationEntry *) palloc(sizeof(RelationEntry));

		relentry->reloid = pgc->oid;
		relentry->relkind = pgc->relkind;

		RelationList = lappend(RelationList, relentry);

		MemoryContextSwitchTo(oldctx);
	}

	table_endscan(scan);
	table_close(rel, AccessShareLock);

	CommitTransactionCommand();

	return RelationList;
}

/*
 * Main function for scrubbing a single database, executed in a bgworker.
 */
void
ScrubWorkerMain(Datum arg)
{
	Oid			dboid = DatumGetObjectId(arg);
	List	   *RelationList = NIL;
	ListCell   *lc;
	BufferAccessStrategy strategy;

	pqsignal(SIGTERM, die);

	BackgroundWorkerUnblockSignals();

	init_ps_display("scrub worker");

	BackgroundWorkerInitializeConnectionByOid(dboid, InvalidOid,
											  BGWORKER_BYPASS_ALLOWCONN);

	scrub_shmem_init();

	/*
	 * Enable vacuum cost delay, if any.
	 */
	VacuumCostDelay = ScrubShmem->cost_delay;
	VacuumCostLimit = ScrubShmem->cost_limit;
	VacuumCostActive = (VacuumCostDelay > 0);
	VacuumCostBalance = 0;
	VacuumCostPageHit = 0;
	VacuumCostPageMiss = 0;
	VacuumCostPageDirty = 0;

	/*
	 * Create and set the vacuum strategy as our buffer strategy
	 */
	strategy = GetAccessStrategy(BAS_VACUUM);

	RelationList = BuildRelationList(ScrubShmem->process_shared_catalogs);
	foreach(lc, RelationList)
	{
		RelationEntry *rel = (RelationEntry *) lfirst(lc);

		/* Check if the user requested to stop the scrub. */
		if (should_terminate())
			break;

		/* */
		if (!ScrubSingleRelationByOid(rel->reloid, strategy))
			ereport(WARNING,
					(errmsg("failed to process table with oid %d", rel->reloid)));
	}
	list_free_deep(RelationList);

	ScrubShmem->success = true;

	ereport(DEBUG1,
			(errmsg("scrub worker completed in database oid %d", dboid)));
}

/*
 * Start scrub - either on a single database, on all databases.
 */
Datum
scrub_start(PG_FUNCTION_ARGS)
{
	Oid		dboid = InvalidOid;
	int		cost_delay = PG_GETARG_INT32(1);
	int		cost_limit = PG_GETARG_INT32(2);
	bool	reset = PG_GETARG_BOOL(3);

	/* Is dbname supplied? */
	if (!PG_ARGISNULL(0))
	{
		Name	dbName = PG_GETARG_NAME(0);
		dboid = get_database_oid(NameStr(*dbName), false);
	}

	if (cost_delay < 0)
		ereport(ERROR,
				(errmsg("cost delay cannot be less than zero")));

	if (cost_limit <= 0)
		ereport(ERROR,
				(errmsg("cost limit must be a positive value")));

	if (!StartScrubLauncher(cost_delay, cost_limit, dboid, reset))
		ereport(ERROR,
				(errmsg("failed to start scrub launcher process")));

	PG_RETURN_BOOL(true);
}

/*
 * Request the scrub processes to stop.
 */
Datum
scrub_stop(PG_FUNCTION_ARGS)
{
	ShutdownScrubLauncherIfRunning();

	PG_RETURN_BOOL(true);
}

/*
 * Reset statistics counters stored in shared memory.
 */
Datum
scrub_reset(PG_FUNCTION_ARGS)
{
	scrub_shmem_init();

	SpinLockAcquire(&ScrubShmem->mutex);

	memset(&ScrubShmem->counters, 0, sizeof(ScrubCounters));

	SpinLockRelease(&ScrubShmem->mutex);

	PG_RETURN_BOOL(true);
}

/*
 * Check if the scrub process is running.
 */
Datum
scrub_is_running(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(ScrubLauncherIsRunning());
}

/*
 * Return current values of statistic counters.
 */
Datum
scrub_status(PG_FUNCTION_ARGS)
{
	HeapTuple		tuple;
	TupleDesc		tupdesc;
	bool			running;

	Datum			values[23];
	bool			isnull[23];

	/* attach to shared memory (does not matter if done repeatedly) */
	scrub_shmem_init();

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	running = !pg_atomic_unlocked_test_flag(&ScrubShmem->launcher_started);

	SpinLockAcquire(&ScrubShmem->mutex);

#define COUNTER(field) ScrubShmem->counters.field

	values[0] = BoolGetDatum(running);
	values[1] = Int64GetDatum(COUNTER(pages_total));
	values[2] = Int64GetDatum(COUNTER(pages_failed));
	values[3] = Int64GetDatum(COUNTER(checksums_total));
	values[4] = Int64GetDatum(COUNTER(checksums_failed));
	values[5] = Int64GetDatum(COUNTER(headers_total));
	values[6] = Int64GetDatum(COUNTER(headers_failed));
	values[7] = Int64GetDatum(COUNTER(heap_pages_total));
	values[8] = Int64GetDatum(COUNTER(heap_pages_failed));
	values[9] = Int64GetDatum(COUNTER(heap_tuples_total));
	values[10] = Int64GetDatum(COUNTER(heap_tuples_failed));
	values[11] = Int64GetDatum(COUNTER(heap_attr_toast_external_invalid));
	values[12] = Int64GetDatum(COUNTER(heap_attr_compression_broken));
	values[13] = Int64GetDatum(COUNTER(heap_attr_toast_bytes_total));
	values[14] = Int64GetDatum(COUNTER(heap_attr_toast_bytes_failed));
	values[15] = Int64GetDatum(COUNTER(heap_attr_toast_values_total));
	values[16] = Int64GetDatum(COUNTER(heap_attr_toast_values_failed));
	values[17] = Int64GetDatum(COUNTER(heap_attr_toast_chunks_total));
	values[18] = Int64GetDatum(COUNTER(heap_attr_toast_chunks_failed));
	values[19] = Int64GetDatum(COUNTER(btree_pages_total));
	values[20] = Int64GetDatum(COUNTER(btree_pages_failed));
	values[21] = Int64GetDatum(COUNTER(btree_tuples_total));
	values[22] = Int64GetDatum(COUNTER(btree_tuples_failed));

	SpinLockRelease(&ScrubShmem->mutex);

	memset(isnull, 0, sizeof(isnull));

	/* build a tuple */
	tuple = heap_form_tuple(tupdesc, values, isnull);

	/* make the tuple into a datum */
	return HeapTupleGetDatum(tuple);
}
