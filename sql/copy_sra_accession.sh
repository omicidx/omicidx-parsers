#!/bin/bash
gunzip -c SRA_Accessions.tab.gz \
    | sed 's/"//g' \
    | psql \
	  -h $PG_HOST \
	  -U $PG_USER \
          $PG_DB \
	  -c "COPY etl.sra_accession (Accession,Submission,Status\
	  ,Updated,Published,Received,Type,Center,Visibility,Alias,Experiment\
	  ,Sample,Study,Loaded,Spots,Bases,Md5sum,Bio_Sample\
	  ,Bio_Project,Replaced_By) \
	  FROM STDIN CSV DELIMITER E'\t' HEADER NULL '-';" 2>&1 | tee copy.sra_accession.out
