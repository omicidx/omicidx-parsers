#!/bin/bash
ENTITY='biosample'
echo "copying ${ENTITY}.json.gz to db"
cat ${ENTITY}.json \
    | sed 's/\\/\\\\/g' \
    | psql -h $PG_HOST -U $PG_USER $PG_DB \
	   -c "COPY etl.${ENTITY}_jsonb (doc) FROM STDIN;" 2>&1 | tee copy.${ENTITY}.out
