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

#endif							/* SCRUB_H */
