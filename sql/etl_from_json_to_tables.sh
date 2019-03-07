#!/bin/bash
psql -h $PG_HOST -U $PG_USER $PG_DB <<EOF

truncate etl.sra_study;
truncate etl.sra_sample;
truncate etl.sra_run;
truncate etl.sra_experiment;

-----------------------------------------
--
-- SRA_STUDY
--

with tbl as 
(select
	doc->>'BioProject' as bioproject,
	doc->>'GEO' as gse,
	doc->>'abstract' as abstract,
	doc->>'accession' as accession,
	doc->>'alias' as alias,
	doc->'attributes' as attributes,
	doc->>'broker_name' as broker_name,
	doc->>'center_name' as center_name,
	doc->>'description' as description,
	doc->'identifiers' as identifiers,
	doc->>'study_type' as study_type,
	doc->>'title' as title,
	doc->'xrefs' as xrefs
from
	etl.study_jsonb
), b as 
(select accession,
       status,
       updated,
       published,
       received,
       visibility,
       bio_project,
       replaced_by
from etl.sra_accession
where type='STUDY' and accession is not NULL
)
insert into etl.sra_study
select bioproject,
       gse,
       abstract,
       b.accession,
       alias,
       attributes,
       broker_name,
       center_name,
       description,
       identifiers,
       study_type,
       title,
       xrefs,
       b.status,
       b.updated,
       b.published,
       b.received,
       b.visibility,
       b.bio_project,
       b.replaced_by
from b
left join tbl
on tbl.accession = b.accession where b.accession is not NULL;

-- SRA_STUDY
-- 
----------------------------------------------



----------------------------------------------
--
-- SRA_RUN
--

with tbl as
(select
	doc->>'accession' as accession,
	doc->>'broker_name' as broker_name,
	doc->'identifiers' as identifiers,
	doc->>'run_center' as run_center,
	doc->>'center_name' as center_name,
	(doc->>'run_date')::timestamp as run_date,
	doc->'attributes' as attributes,
	(doc->>'nreads')::int as nreads,
	doc->>'alias' as alias,
	(doc->>'spot_length')::int as spot_length,
	doc->>'experiment_accession' as experiment_accession,
	doc->'reads' as reads
from
	etl.run_jsonb)
insert into etl.sra_run 
select tbl.*,
       b.status,
       b.updated,
       b.published,
       b.received,
       b.visibility,
       b.bio_project,
       b.replaced_by,
       b.loaded,
       b.spots,
       b.bases
from tbl
join etl.sra_accession b
on tbl.accession = b.accession;

-- SRA_RUN
--
----------------------------------------------

----------------------------------------------
-- 
-- SRA_EXPERIMENT
--

with tbl as
(select
	doc->>'accession' as accession,
	doc->>'alias' as alias,
	doc->'attributes' as attributes,
    	doc->>'broker_name' as broker_name,
	doc->>'center_name' as center_name,
	doc->>'description' as description,
	doc->>'design' as design,
	doc->'identifiers' as identifiers,
	doc->>'instrument_model' as instrument_model,
	doc->>'library_construction_protocol' as library_construction_protocol,
	doc->>'library_layout' as library_layout,
	doc->>'library_layout_length' as library_layout_length,
	doc->>'library_layout_orientation' as library_layout_orientation,
	doc->>'library_layout_sdev' as library_layout_sdev,
	doc->>'library_name' as library_name,
	doc->>'library_selection' as library_selection,
	doc->>'library_source' as library_source,
	doc->>'library_strategy' as library_strategy,
	doc->>'platform' as platform,
	doc->>'sample_accession' as sample_accession,
	doc->>'study_accession' as study_accession,
	doc->>'title' as title,
	doc->'xrefs' as xrefs
from etl.experiment_jsonb
),
b as 
(select accession,
       status,
       updated,
       published,
       received,
       visibility,
       replaced_by
from etl.sra_accession
where type='EXPERIMENT' and accession is not NULL
)
insert into etl.sra_experiment 
select b.accession,
       alias,
       attributes,
       broker_name,
       center_name,
       description,
       design,
       identifiers,
       instrument_model,
       library_construction_protocol,
       library_layout,
       library_layout_length,
       library_layout_orientation,
       library_layout_sdev,
       library_name,
       library_selection,
       library_source,
       library_strategy,
       platform,
       sample_accession,
       study_accession,
       title,
       xrefs,
       b.status,
       b.updated,
       b.published,
       b.received,
       b.visibility,
       b.replaced_by
from tbl 
right join b
on tbl.accession = b.accession;

-- SRA_EXPERIMENT
--
----------------------------------------------


----------------------------------------------
--
-- SRA_SAMPLE
--

with tbl as
(select
	doc->>'accession' as accession,
	doc->>'alias' as alias,
	doc->'attributes' as attributes,
    	doc->>'BioSample' as bio_sample,	
    	doc->>'broker_name' as broker_name,
	doc->>'center_name' as center_name,
	doc->>'description' as description,
	doc->>'GEO' as gsm,
	doc->'identifiers' as identifiers,
	doc->>'organism' as organism,
	doc->>'title' as title,
	(doc->>'taxon_id')::int as taxon_id,
	doc->'xrefs' as xrefs
from etl.sample_jsonb
),
b as 
(select accession,
       status,
       updated,
       published,
       received,
       visibility,
       replaced_by
from etl.sra_accession
where type='SAMPLE' and accession is not NULL
)
insert into etl.sra_sample
select b.accession,
       alias,
       attributes,
       bio_sample,
       broker_name,
       center_name,
       description,
       gsm,
       identifiers,
       organism,
       title,
       taxon_id,
       xrefs,
       b.status,
       b.updated,
       b.published,
       b.received,
       b.visibility,
       b.replaced_by
from tbl 
right join b
on tbl.accession = b.accession;

-- SRA_SAMPLE
--
----------------------------------------------



EOF
2>&1 | tee etl_from_json_to_tables.out
