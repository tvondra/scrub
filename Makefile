# scrub/Makefile
#

MODULE_big = scrub

OBJS = scrub.o scrub_common.o scrub_btree.o scrub_heap.o scrub_toast.o

EXTENSION = scrub
DATA = scrub--1.0.sql
MODULES = scrub

CFLAGS=`pg_config --includedir-server`

TESTS        = $(wildcard test/sql/*.sql)
REGRESS      = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
