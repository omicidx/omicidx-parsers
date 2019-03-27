#!/bin/bash
psql -h $PG_HOST -U $PG_USER $PG_DB <<EOF

--
-- Nasty delete hacks due to referential integrity on ORIGINAL
-- data from SRA failing.
--


create index expt_study_accession on sra_experiment(study_accession);
create index run_experiment_accession on sra_run(experiment_accession);
create index expt_sample_accession on sra_experiment(sample_accession);

insert into etl.sra_experiment (accession, status)
  (select distinct(r.experiment_accession),'[added by omicidx]' 
      from etl.sra_run r 
      left join etl.sra_experiment e on e.accession=r.experiment_accession 
      where e.accession is NULL and r.experiment_accession is not NULL);

alter table etl.sra_run 
add constraint fk_sra_run_sra_experiment 
foreign key (experiment_accession) 
references etl.sra_experiment(accession);

insert into etl.sra_sample (accession, status)
  (select distinct(e.sample_accession),'[added by omicidx]' 
      from etl.sra_experiment e 
      left join etl.sra_sample s on s.accession=e.sample_accession 
      where s.accession is NULL and e.sample_accession is not NULL);


alter table etl.sra_experiment 
add constraint fk_sra_experiment_sra_sample 
foreign key (sample_accession) 
references etl.sra_sample(accession);

insert into etl.sra_study (accession, status)
  (select distinct(e.study_accession), '[added by omicidx]' 
      from etl.sra_experiment e 
      left join etl.sra_study s on s.accession=e.study_accession 
      where s.accession is NULL and e.study_accession is not null);

alter table etl.sra_experiment 
add constraint fk_sra_experiment_sra_study 
foreign key (study_accession) 
references etl.sra_study(accession);

drop table public.sra_study cascade;
drop table public.sra_experiment cascade;
drop table public.sra_run cascade;
drop table public.sra_sample cascade;
alter table etl.sra_study set schema public;
alter table etl.sra_sample set schema public;
alter table etl.sra_run set schema public;
alter table etl.sra_experiment set schema public;


EOF
2>&1 | tee add_foreign_keys.out
