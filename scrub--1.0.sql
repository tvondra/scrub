/* scrub--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION scrub" to load this file. \quit

CREATE FUNCTION scrub_start(dbname name, cost_delay int, cost_limit int, reset_status bool)
RETURNS bool
AS 'MODULE_PATHNAME', 'scrub_start'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION scrub_stop()
RETURNS bool
AS 'MODULE_PATHNAME', 'scrub_stop'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION scrub_reset()
RETURNS bool
AS 'MODULE_PATHNAME', 'scrub_reset'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION scrub_is_running()
RETURNS bool
AS 'MODULE_PATHNAME', 'scrub_is_running'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION scrub_status(
		OUT is_running BOOL,
		OUT pages_total BIGINT,
		OUT pages_failed BIGINT,
		OUT checksums_total BIGINT,
		OUT checksums_failed BIGINT,
		OUT headers_total BIGINT,
		OUT headers_failed BIGINT,
		OUT heap_pages_total BIGINT,
		OUT heap_pages_failed BIGINT,
		OUT heap_tuples_total BIGINT,
		OUT heap_tuples_failed BIGINT,
		OUT heap_attr_toast_external_invalid BIGINT,
		OUT heap_attr_compression_broken BIGINT,
		OUT heap_attr_toast_bytes_total BIGINT,
		OUT heap_attr_toast_bytes_failed BIGINT,
		OUT heap_attr_toast_values_total BIGINT,
		OUT heap_attr_toast_values_failed BIGINT,
		OUT heap_attr_toast_chunks_total BIGINT,
		OUT heap_attr_toast_chunks_failed BIGINT,
		OUT btree_pages_total BIGINT,
		OUT btree_pages_failed BIGINT,
		OUT btree_tuples_total BIGINT,
		OUT btree_tuples_failed BIGINT)
AS 'MODULE_PATHNAME', 'scrub_status'
LANGUAGE C STRICT PARALLEL SAFE;
