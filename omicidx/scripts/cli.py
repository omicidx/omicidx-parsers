#!/usr/bin/env python
import click
import subprocess
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
logger = logging.getLogger(__name__)

@click.group(help="""Do things in this order:

\b
  1. download-mirror-files
  2. process-xml-to-json
  3. create on GCS

And this will be wrapped.
""")
def cli():
    pass


@cli.group()
def sra():
    pass

@sra.command("download",
             help = """Downloads the files necessary to build
             the SRA json conversions of the XML files.

             Files will be placed in the <mirrordir> directory. Mirrordirs
             have the format `NCBI_SRA_Mirroring_20190801_Full`.
             """)
@click.argument('mirrordir')
def download_mirror_files(mirrordir):
    logger.info('getting xml files')
    subprocess.run("wget -nH -np --cut-dirs=3 -r -e robots=off {}/{}/".format(
        "http://ftp.ncbi.nlm.nih.gov/sra/reports/Mirroring",
        mirrordir), shell = True)

    logger.info('getting SRA Accessions file')
    subprocess.run("wget ftp://ftp.ncbi.nlm.nih.gov/sra/reports/Metadata/SRA_Accessions.tab -P {}".format(mirrordir),
                   shell = True)
    
        
import argparse
import omicidx.sra_parsers
import json
import logging
import collections
from xml.etree import ElementTree as et

import datetime

def dateconverter(o):
    if isinstance(o, (datetime.datetime)):
        return o.__str__()

@sra.command("parse-entity",
             help="""SRA XML to JSON

             Transforms an SRA XML mirroring metadata file into
             corresponding JSON format files. JSON is line-delimited
             JSON (not an array).""")
@click.argument('entity')
def process_xml_entity(entity):

    fname = "meta_{}_set.xml.gz".format(entity)

    entity=entity

    parsers = {
        'study': omicidx.sra_parsers.SRAStudyRecord,
        'sample': omicidx.sra_parsers.SRASampleRecord,
        'run': omicidx.sra_parsers.SRARunRecord,
        'experiment': omicidx.sra_parsers.SRAExperimentRecord
    }
    sra_parser = parsers[entity]

    import omicidx.sra.pydantic_models as p
    parsers = {
        'study': p.SraStudy,
        'sample': p.SraSample,
        'run': p.SraRun,
        'experiment': p.SraExperiment
    }
    pydantic_model = parsers[entity]

    logger.info('using {} entity type'.format(entity))
    logger.info('parsing {} records'.format(entity))
    n = 0
    outfname = "{}.json".format(entity)
    ENTITY = entity.upper()
    with open(outfname, 'w') as outfile:
        with omicidx.sra_parsers.open_file(fname) as f:
            for event, element in et.iterparse(f):
                if(event == 'end' and element.tag == ENTITY):
                    rec = sra_parser(element).data
                    n+=1
                    if((n % 100000)==0):
                        logger.info('parsed {} {} entries'.format(entity, n))
                    outfile.write(json.dumps(pydantic_model(**rec).dict(), default=dateconverter) + "\n")
                    element.clear()
            logger.info('parsed {} entity entries'.format(n))

@sra.command('upload',
             help="""Upload SRA json to GCS""")
@click.argument('mirrordir')
def upload_processed_sra_data(mirrordir):
    from ..gcs_utils import upload_blob_to_gcs

    for entity in 'study sample experiment run'.split():
        fname = i + '.json'
        loc_fname = os.path.join(mirrordir, fname)
        upload_processed_sra_data('temp-testing', loc_fname, 'abc/' + fname)

    fname = 'SRA_Accessions.tab'
    loc_fname = os.path.join(mirrordir, fname)
    upload_processed_sra_data('temp-testing', loc_fname, 'abc/' + fname)


    
@sra.command(help="""Load gcs files to Bigquery""")
def load_sra_data_to_bigquery():
    from ..bigquery_utils import (
        load_csv_to_bigquery,
        load_json_to_bigquery,
        parse_bq_json_schema)
    from importlib import resources

    for i in 'study sample experiment run'.split():
        with resources.path('omicidx.data.bigquery_schemas', f"{i}.schema.json") as schemafile:
            load_json_to_bigquery('omicidx_etl',
                                  f'sra_{i}',
                                  f'gs://temp-testing/abc/{i}.json',
                                  schema=parse_bq_json_schema(schemafile))
    
    load_csv_to_bigquery('omicidx_etl',
                         'sra_accessions',
                         'gs://temp-testing/abc/SRA_Accessions.tab',
             field_delimiter='\t', null_marker='-')


@sra.command(help="""ETL query to public schema for all SRA entities""")
def sra_to_bigquery():
    from ..bigquery_utils import query
    sql = """CREATE OR REPLACE TABLE `isb-cgc-01-0006.omicidx.sra_run` AS
SELECT 
  run.* EXCEPT (published, lastupdate, received, total_spots, total_bases, avg_length),
  CAST(acc.Updated as DATETIME) as lastupdate,
  CAST(acc.Published as DATETIME) as published,
  CAST(acc.Received as DATETIME) as received,
  CAST(acc.Spots as INT64) as total_spots,
  CAST(acc.Bases as INT64) as total_bases,
  CAST(acc.Bases AS NUMERIC)/CAST(acc.Spots AS NUMERIC) as avg_length,
  acc.Sample as sample_accession,
  acc.Study as study_accession
FROM 
    `isb-cgc-01-0006.omicidx_etl.sra_run` run
  JOIN 
    `isb-cgc-01-0006.omicidx_etl.sra_accessions` acc
  ON acc.Accession = run.accession;
"""
    query(sql)

    sql = """CREATE OR REPLACE TABLE `isb-cgc-01-0006.omicidx.sra_experiment` AS
SELECT 
  expt.* EXCEPT (published, lastupdate, received),
  CAST(acc.Updated as DATETIME) as lastupdate,
  CAST(acc.Published as DATETIME) as published,
  CAST(acc.Received as DATETIME) as received
FROM 
    `isb-cgc-01-0006.omicidx_etl.sra_experiment` expt
  JOIN 
    `isb-cgc-01-0006.omicidx_etl.sra_accessions` acc
  ON acc.Accession = expt.accession;
    """
    query(sql)

    sql = """CREATE OR REPLACE TABLE `isb-cgc-01-0006.omicidx.sra_sample` AS
SELECT 
  sample.* EXCEPT (published, lastupdate, received),
  CAST(acc.Updated as DATETIME) as lastupdate,
  CAST(acc.Published as DATETIME) as published,
  CAST(acc.Received as DATETIME) as received,
  acc.Study as study_accession
FROM 
    `isb-cgc-01-0006.omicidx_etl.sra_sample` sample
  JOIN 
    `isb-cgc-01-0006.omicidx_etl.sra_accessions` acc
  ON acc.Accession = sample.accession;
    """
    query(sql)

    sql = """CREATE OR REPLACE TABLE `isb-cgc-01-0006.omicidx.sra_study` AS
WITH stat_agg AS (
SELECT
  COUNT(a.Accession) as sample_count,
  a.Study as study_accession
FROM 
  `isb-cgc-01-0006.omicidx_etl.sra_accessions` a
WHERE
  a.Type='SAMPLE'
GROUP BY 
  study_accession)
SELECT 
  study.* EXCEPT (published, lastupdate, received),
  CAST(acc.Updated as DATETIME) as lastupdate,
  CAST(acc.Published as DATETIME) as published,
  CAST(acc.Received as DATETIME) as received
FROM 
    `isb-cgc-01-0006.omicidx_etl.study` study
  JOIN 
    `isb-cgc-01-0006.omicidx_etl.sra_accessions` acc
  ON acc.Accession = study.accession
  LEFT OUTER JOIN
    stat_agg on stat_agg.study_accession=study.accession;
    """
    query(sql)

######################
# Biosample handling #
######################

@cli.group(help="Use these commands to process biosample records.")
def biosample():
    pass

from ..biosample import BioSampleParser
def biosample_to_json(biosample_file):
    for i in BioSampleParser(biosample_file):
        if(i is None):
            break
        print(i.as_json())

def download_biosample():
    subprocess.run("wget ftp://ftp.ncbi.nlm.nih.gov/biosample/biosample_set.xml.gz", shell=True)

def upload_biosample():
    from ..gcs_utils import upload_blob_to_gcs

    fname = 'biosample.json'
    upload_blob_to_gcs('temp-testing', fname, 'abc/' + fname)

def load_biosample_from_gcs_to_bigquery():
    from ..bigquery_utils import load_json_to_bigquery

    load_json_to_bigquery('omicidx_etl', 'biosample',
                          'gs://temp-testing/abc/biosample.json')


@biosample.command("""download""",
                   help="Download biosample xml file from NCBI")
def download():
    download_biosample()


@biosample.command("""upload""",
                   help="Download biosample xml file from NCBI")
def upload():
    upload_biosample()

    
@biosample.command("""parse""",
                   help = "Parse xml to json, output to stdout")
@click.argument('biosample_file')
def to_json(biosample_file):
    biosample_to_json(biosample_file)


@biosample.command("""load""",
                   help = "Load the gcs biosample.json file to bigquery")
def load_biosample_to_bigquery():
    load_biosample_from_gcs_to_bigquery()


@biosample.command("""etl-to-public""",
                   help = "ETL process (copy) from etl schema to public")
def biosample_to_public():
    from ..bigquery_utils import copy_table
    copy_table('omicidx_etl','omicidx',
               'biosample', 'biosample')


    
if __name__ == '__main__':
    cli()
    
