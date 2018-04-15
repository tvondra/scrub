scrub
=====

An extension to perform scrubbing - regular background data consistency
checks, with minimal impact on user queries. This is a similar idea to
scrubbing in ZFS, for example (hence the extension name).

The sad truth is that most databases of non-trivial size are affected by
some sort of data corruption, with many possible causes - storage issues,
filesystem/OS/PostgreSQL bugs, memory bit flips, etc. The larger/older
the database system is, the more likely there already is data corruption
but no one noticed it yet.

The best way to identify data corruption is by actually reading all the
data. That is why in the past data corruption was often noticed while
performing backups using `pg_dump`. Unfortunately `pg_dump` is not very
suitable for large databases, and is usually replaced by `pg_basebackup`
which works at the filesystem level (and so is oblivious to many types
of internal data corruption).

It is possible to read all data by executing `SELECT` statements, but
that has many disadvantages (some of them shared with `pg_dump`) - long
running transactions, not checking indexes, no throttling, etc.

Furthermore, the code executed during regular query execution is not
written to expect data corruption. In the better case it reports the
first issue it runs into and terminates the query (making it difficult
to assess the extent of data corruption), but it may also cause further
damage by accessing the corrupted data (possibly even crashing the
database processes).

The purpose of this extension is to allow such data consistency checks
without such issues and limitations of `pg_dump` or regular queries.


Checks
------

A fairly wide range of consistency checks is already implemented:

1. page checksum

   Only when data checksums are enabled (`data_checksums = on`).

2. generic page structure

   Checks of basic page structure shared by all objects (described by
   `pd_lower`, `pd_upper` and `pd_special` offsets).

3. page structure specific to heap and b-tree indexes

   Checks of page structure specific to relation type, and structure of
   page items (including detoasting/decompression of varlena values).

Additional checks may be implemented in the future, for example:

* checks of XID values stored in tuple header (wraparound etc.)

* cross-checks between relation forks (heap vs. visibility map)

* cross-checks between tables and indexes


Install
-------

The extension requires additional space in shared memory, and so has to
be loaded using `shared_preload_libraries`:

    shared_preload_libraries = 'scrub'

After restarting the database to apply the change, the extension may be
installed in the usual way:

    CREATE EXTENSION scrub;

At that point the extension is ready for usage.


Usage
-----

To start scrubbing of a particular database, use `scrub_start` function:

    SELECT scrub_start(dbname := 'testdb',
                       cost_delay := 10,
                       cost_limit := 1000,
                       reset_status := true);

The `cost_delay` and `cost_limit` parameters allow throttling the work
the same way `vacuum_cost_delay` and `vacuum_cost_limit` do (note that
scrubbing never causes writes). `reset_status` resets statistics about
progress of the current scrub.

`scrub_start` only initiates a background worker that does the actual
work, and returns. Use `scrub_stop` to stop the scrub (terminate the
background worker).

    SELECT scrub_stop();

`scrub_is_running` just provides a simple way to see if there is a scrub
(on any database) in progress.

    SELECT scrub_is_running();

Details about identified issues are written to server log, but overall
progress (number of checked pages, failed tests etc.) can be obtained
from `scrub_status` function.

    SELECT * FROM scrub_status();

This returns a single row with accumulated counters, possibly covering
multiple scrub runs. Use `scrub_reset` to reset the statistics.

    SELECT scrub_reset();
