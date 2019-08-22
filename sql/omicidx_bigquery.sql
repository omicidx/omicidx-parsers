--------------------------------------------------
-- omicidx_etl.run to omicidx.sra_run           --
--------------------------------------------------

CREATE OR REPLACE TABLE `isb-cgc-01-0006.omicidx.sra_run` AS
SELECT 
  run.* EXCEPT (published, lastupdate, received, total_spots, total_bases, avg_length),
  acc.Updated as lastupdate,
  acc.Published as published,
  acc.Received as received,
  acc.Spots as total_spots,
  acc.Bases as total_bases,
  CAST(acc.Bases AS NUMERIC)/CAST(acc.Spots AS NUMERIC) as avg_length,
  acc.Sample as sample_accession,
  acc.Study as study_accession
FROM 
    `isb-cgc-01-0006.omicidx_etl.sra_run` run
  JOIN 
    `isb-cgc-01-0006.omicidx_etl.sra_accessions` acc
  ON acc.Accession = run.accession;


----------------------------------------------------------
-- omicidx_etl.sra_experiment to omicidx.sra_experiment --
----------------------------------------------------------

CREATE OR REPLACE TABLE `isb-cgc-01-0006.omicidx.sra_experiment` AS
WITH run_stat_agg AS (
SELECT
  SUM(CAST(a.Spots AS INT64)) as total_spots,
  SUM(CAST(a.Bases AS INT64)) as total_bases,
  COUNT(a.Accession) as run_count,
  ARRAY_AGG(a.Accession) as run_accessions,
  a.Experiment as experiment_accession
FROM 
  `isb-cgc-01-0006.omicidx_etl.sra_accessions` a
WHERE
  a.Type='RUN'
GROUP BY 
  experiment_accession)
SELECT 
  expt.* EXCEPT (published, lastupdate, received),
  acc.Updated as lastupdate,
  acc.Published as published,
  acc.Received as received,
  CAST(acc.Bases AS NUMERIC)/CAST(acc.Spots AS NUMERIC) as avg_length
FROM 
    `isb-cgc-01-0006.omicidx_etl.sra_experiment` expt
  JOIN 
    `isb-cgc-01-0006.omicidx_etl.sra_accessions` acc
  ON acc.Accession = expt.accession
  LEFT OUTER JOIN
    run_stat_agg on run_stat_agg.experiment_accession=expt.accession;



----------------------------------------------
-- omicidx_etl.sample to omicidx.sra_sample --
----------------------------------------------

CREATE OR REPLACE TABLE `isb-cgc-01-0006.omicidx.sra_sample` AS
WITH run_stat_agg AS (
SELECT
  SUM(CAST(a.Spots AS INT64)) as total_spots,
  SUM(CAST(a.Bases AS INT64)) as total_bases,
  COUNT(a.Accession) as run_count,
  ARRAY_AGG(a.Accession) as run_accessions,
  a.Experiment as experiment_accession
FROM 
  `isb-cgc-01-0006.omicidx_etl.sra_accessions` a
WHERE
  a.Type='RUN'
GROUP BY 
  experiment_accession)
SELECT 
  expt.* EXCEPT (published, lastupdate, received),
  acc.Updated as lastupdate,
  acc.Published as published,
  acc.Received as received,
  CAST(acc.Bases AS NUMERIC)/CAST(acc.Spots AS NUMERIC) as avg_length
FROM 
    `isb-cgc-01-0006.omicidx_etl.sra_experiment` expt
  JOIN 
    `isb-cgc-01-0006.omicidx_etl.sra_accessions` acc
  ON acc.Accession = expt.accession
  LEFT OUTER JOIN
    run_stat_agg on run_stat_agg.experiment_accession=expt.accession;




--------------------------------------------
-- omicidx_etl.study to omicidx.sra_study --
--------------------------------------------

CREATE OR REPLACE TABLE `isb-cgc-01-0006.omicidx.sra_study` AS
WITH run_stat_agg AS (
SELECT
  COUNT(a.Accession) as sample_count,
  ARRAY_AGG(a.Accession) as sample_accessions,
  a.Study as study_accession
FROM 
  `isb-cgc-01-0006.omicidx_etl.sra_accessions` a
WHERE
  a.Type='SAMPLE'
GROUP BY 
  study_accession)
SELECT 
  study.* EXCEPT (published, lastupdate, received),
  acc.Updated as lastupdate,
  acc.Published as published,
  acc.Received as received
FROM 
    `isb-cgc-01-0006.omicidx_etl.study` study
  JOIN 
    `isb-cgc-01-0006.omicidx_etl.sra_accessions` acc
  ON acc.Accession = study.accession
  LEFT OUTER JOIN
    run_stat_agg on run_stat_agg.study_accession=study.accession;


-------------------------------
-- sra_experiment index dump --
-------------------------------

SELECT
  expt.*,
  STRUCT(samp).samp as sample,
  STRUCT(study).study as study
FROM 
  `isb-cgc-01-0006.omicidx.sra_experiment` expt 
  JOIN 
  `isb-cgc-01-0006.omicidx.sra_sample` samp
  ON samp.accession = expt.sample_accession
  JOIN 
  `isb-cgc-01-0006.omicidx.sra_study` study
  ON study.accession = expt.study_accession


-------------------------------
-- sra_run for elasticsearch --
-------------------------------

create or replace table omicidx_etl.sra_run_for_es as
SELECT
  run.*,
  STRUCT(expt).expt as experiment,
  STRUCT(samp).samp as sample,
  STRUCT(study).study as study
FROM 
  `isb-cgc-01-0006.omicidx.sra_run` run
  LEFT OUTER JOIN
  `isb-cgc-01-0006.omicidx.sra_experiment` expt
  ON run.experiment_accession = expt.accession
  JOIN 
  `isb-cgc-01-0006.omicidx.sra_sample` samp
  ON samp.accession = expt.sample_accession
  JOIN 
  `isb-cgc-01-0006.omicidx.sra_study` study
  ON study.accession = expt.study_accession


---------------------------------
-- sra study for elasticsearch --
---------------------------------

CREATE TABLE omicidx_etl.sra_study_for_es as
WITH agg_counts as
(SELECT
  study.accession,
  COUNT(DISTINCT expt.sample_accession) as sample_count,
  COUNT(DISTINCT expt.accession) as experiment_count,
  COUNT(DISTINCT run.accession) as run_count,
  SUM(CAST(run.total_bases as INT64)) as total_bases,
  SUM(CAST(run.total_spots as INT64)) as total_spots,
  AVG(CAST(run.total_bases as INT64)) as mean_bases_per_run,
  ARRAY_AGG(DISTINCT sample.taxon_id) as taxon_ids
FROM `isb-cgc-01-0006.omicidx.sra_study` study 
JOIN `isb-cgc-01-0006.omicidx.sra_experiment` expt 
  ON expt.study_accession = study.accession
JOIN `isb-cgc-01-0006.omicidx.sra_run` run
  ON run.experiment_accession = expt.accession
JOIN `isb-cgc-01-0006.omicidx.sra_sample` sample
  ON expt.sample_accession = sample.accession
GROUP BY study.accession
) 
SELECT 
  study.*,
  agg_counts.* EXCEPT(accession) 
FROM agg_counts 
JOIN `isb-cgc-01-0006.omicidx.sra_study` study
  ON study.accession=agg_counts.accession;
  

----------------------------------
-- sra sample for elasticsearch --
----------------------------------

CREATE OR REPLACE TABLE omicidx_etl.sra_sample_for_es as
WITH agg_counts as
(SELECT
  sample.accession,
  COUNT(DISTINCT expt.accession) as experiment_count,
  COUNT(DISTINCT run.accession) as run_count,
  SUM(CAST(run.total_bases as INT64)) as total_bases,
  SUM(CAST(run.total_spots as INT64)) as total_spots,
  AVG(CAST(run.total_bases as INT64)) as mean_bases_per_run
FROM `isb-cgc-01-0006.omicidx.sra_study` study 
JOIN `isb-cgc-01-0006.omicidx.sra_experiment` expt 
  ON expt.study_accession = study.accession
JOIN `isb-cgc-01-0006.omicidx.sra_run` run
  ON run.experiment_accession = expt.accession
JOIN `isb-cgc-01-0006.omicidx.sra_sample` sample
  ON expt.sample_accession = sample.accession
GROUP BY sample.accession
) 
SELECT 
  sample.*,
  STRUCT(study).study,
  agg_counts.* EXCEPT(accession) 
FROM `isb-cgc-01-0006.omicidx.sra_sample` sample
JOIN agg_counts 
  ON agg_counts.accession = sample.accession
JOIN `isb-cgc-01-0006.omicidx.sra_experiment` expt 
  ON expt.sample_accession = sample.accession
JOIN `isb-cgc-01-0006.omicidx.sra_study` study
  ON study.accession=expt.study_accession;
