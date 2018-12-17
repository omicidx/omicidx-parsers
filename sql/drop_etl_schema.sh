#!/bin/bash
psql -h $PG_HOST -U $PG_USER $PG_DB -c "drop schema etl cascade;"
