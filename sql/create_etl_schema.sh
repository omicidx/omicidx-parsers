#!/bin/bash
psql -h $PG_HOST -U $PG_USER $PG_DB <<EOF
create schema etl;

create table etl.run_jsonb(doc jsonb);

create table etl.experiment_jsonb(doc jsonb);

create table etl.study_jsonb(doc jsonb);

create table etl.sample_jsonb(doc jsonb);

create table etl.sra_accession (
       accession text primary key,
       submission text,
       status text,
       updated timestamp,
       published timestamp,
       received  timestamp,
       type text,
       center text,
       visibility text,
       alias text,
       experiment text,
       sample  text,
       study   text,
       loaded  text,
       spots   bigint,
       bases   bigint,
       md5sum  text,
       bio_sample text,
       bio_project text,
       replaced_by text
);
create index etl_sra_accession_accession_idx on etl.sra_accession(accession);

create table etl.fileinfo_addons (
       accession text,
       file_size bigint,
       file_md5 text,
       file_date timestamp,
       filename text,
       url text
);
create index etl_fileinfo_addons_accession_idx on etl.fileinfo_addons(accession);


create table etl.fileinfo_runs (
       accession text,
       file_size bigint,
       file_md5 text,
       file_date timestamp
);
create index etl_fileinfo_runs_accession_idx on etl.fileinfo_runs(accession);


CREATE TABLE etl.sra_experiment (
    accession text primary key,
    alias text,
    attributes jsonb,
    broker_name text,
    center_name text,
    description text,
    design text,
    identifiers jsonb,
    instrument_model text,
    library_construction_protocol text,
    library_layout text,
    library_layout_length text,
    library_layout_orientation text,
    library_layout_sdev text,
    library_name text,
    library_selection text,
    library_source text,
    library_strategy text,
    platform text,
    sample_accession text,
    study_accession text,
    title text,
    xrefs text,
    status text,
    updated timestamp without time zone,
    published timestamp without time zone,
    received timestamp without time zone,
    visibility text,
    replaced_by text
);


CREATE TABLE etl.sra_run (
    accession text primary key,
    broker_name text,
    identifiers jsonb,
    run_center text,
    center_name text,
    run_date timestamp without time zone,
    attributes jsonb,
    nreads integer,
    alias text,
    spot_length integer,
    experiment_accession text,
    reads jsonb,
    status text,
    updated timestamp without time zone,
    published timestamp without time zone,
    received timestamp without time zone,
    visibility text,
    bio_project text,
    replaced_by text,
    loaded text,
    spots bigint,
    bases bigint
);


CREATE TABLE etl.sra_sample (
    accession text primary key,
    alias text,
    attributes jsonb,
    bio_sample text,
    broker_name text,
    center_name text,
    description text,
    gsm text,
    identifiers jsonb,
    organism text,
    title text,
    taxon_id integer,
    xrefs jsonb,
    status text,
    updated timestamp without time zone,
    published timestamp without time zone,
    received timestamp without time zone,
    visibility text,
    replaced_by text
);

CREATE TABLE etl.sra_study (
    bioproject text,
    gse text,
    abstract text,
    accession text primary key,
    alias text,
    attributes jsonb,
    broker_name text,
    center_name text,
    description text,
    identifiers jsonb,
    study_type text,
    title text,
    xrefs jsonb,
    status text,
    updated timestamp without time zone,
    published timestamp without time zone,
    received timestamp without time zone,
    visibility text,
    bio_project text,
    replaced_by text
);

EOF
2>&1 | tee create_schema.out
