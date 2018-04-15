# scrub/Makefile
#

MODULE_big = scrub

OBJS = scrub.o scrub_checks.o

EXTENSION = scrub
DATA = scrub--1.0.sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

ifndef MAJORVERSION
    MAJORVERSION := $(basename $(VERSION))
endif
