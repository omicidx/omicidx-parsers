#!/bin/bash
ENTITY=$1
echo "copying ${ENTITY}.json.gz to db"
gunzip -c ${ENTITY}.json.gz \
    | sed 's/\\/\\\\/g' \
    | psql -h $PG_HOST -p $PG_PORT -U $PG_USER $PG_DB \
	   -c "COPY etl.${ENTITY}_jsonb (doc) FROM STDIN;" 2>&1 | tee copy.${ENTITY}.out
