#!/bin/bash
psql -h $PG_HOST -U $PG_USER $PG_DB <<EOF
\t on
\pset format unaligned
with x as (select 
    r.*, 
	row_to_json(e) as experiment, 
	row_to_json(s) as sample, 
	row_to_json(t) as study 
	from sra_study t 
	join sra_experiment e on e.study_accession = t.accession 
	join sra_sample s on e.sample_accession = s.accession 
	join sra_run r on r.experiment_accession = e.accession) 
select row_to_json(x) from x
\g my_data_dump.json 
EOF

2>&1 | tee dump_joined_run_to_json.out
