#!/bin/bash
echo "copying fileinfo_runs to db"
gunzip -c fileinfo_runs.csv.gz \
    | sed 's/"//g' \
    | psql \
	  -h $PG_HOST \
	  -U $PG_USER \
          $PG_DB \
	  -c "COPY etl.fileinfo_runs \
	  FROM STDIN CSV HEADER NULL '-';" 2>&1 | tee copy.fileinfo_runs.out

echo "copying fileinfo_addons to db"
gunzip -c fileinfo_addons.csv.gz \
    | sed 's/"//g' \
    | psql \
	  -h $PG_HOST \
	  -U $PG_USER \
          $PG_DB \
	  -c "COPY etl.fileinfo_addons \
	  FROM STDIN CSV HEADER NULL '-';" 2>&1 | tee copy.fileinfo_addons.out
