#!/bin/bash
psql -h $PG_HOST -U $PG_USER $PG_DB <<EOF

alter table etl.sra_run 
add constraint fk_sra_run_sra_experiment 
foreign key (experiment_accession) 
references etl.sra_experiment(accession);

alter table etl.sra_experiment 
add constraint fk_sra_experiment_sra_sample 
foreign key (sample_accession) 
references etl.sra_sample(accession);

alter table etl.sra_experiment 
add constraint fk_sra_experiment_sra_study 
foreign key (study_accession) 
references etl.sra_study(accession);

EOF
2>&1 | tee add_foreign_keys.out
