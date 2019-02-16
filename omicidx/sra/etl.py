"""ETL from gcs to bigquery

1. create_all_tables()
2. load_accession_table()
3. load_livelist_table()
4. load_all_json_tables()
5. do_all_join_tables()
  # Does copy into SRA schema from etl
  #   AFTER deleting!
  # may need to delete_table('sra.experiment'), ... first
  1. study_join_accessions()
  2. sample_join_accessions()
  3. experiment_join_accessions()
  4. run_join_accessions()
"""



from google.cloud import bigquery
from google.cloud.bigquery.schema import SchemaField
import google

ETL_DATASET = 'omicidx_etl'
LIVE_DATASET = 'sra'

client = bigquery.Client()
try:
    sra = client.create_dataset(ETL_DATASET)
except google.cloud.exceptions.Conflict:
    sra = client.get_dataset(ETL_DATASET)

try:
    sra_live = client.create_dataset(LIVE_DATASET)
except google.cloud.exceptions.Conflict:
    sra_live = client.get_dataset(LIVE_DATASET)


############################################
# Prepare all schemas for going to biguery #
############################################

# not meant to be run by itself--called by others.
def prep_bigquery_schemas():

    attributes = SchemaField('attributes', 'RECORD', 'REPEATED', None, (
            SchemaField('value', 'STRING', 'NULLABLE', None, ()),
            SchemaField('tag', 'STRING', 'NULLABLE', None, ())))
    identifiers = SchemaField('identifiers', 'RECORD', 'REPEATED', None, (
            SchemaField('id', 'STRING', 'NULLABLE', None, ()),
            SchemaField('uuid', 'STRING', 'NULLABLE', None, ()),
            SchemaField('namespace', 'STRING', 'NULLABLE', None, ())))
    xrefs       = SchemaField('xrefs', 'RECORD', 'REPEATED', None, (
            SchemaField('id', 'STRING', 'NULLABLE', None, ()),
            SchemaField('db', 'STRING', 'NULLABLE', None, ())))
    common      = [
        SchemaField('accession', 'STRING', 'NULLABLE', None, ()),
        SchemaField('alias', 'STRING', 'NULLABLE', None, ()),
        SchemaField('title', 'STRING', 'NULLABLE', None, ()),
        SchemaField('center_name', 'STRING', 'NULLABLE', None, ()),
        SchemaField('broker_name', 'STRING', 'NULLABLE', None, ()),
        SchemaField('description', 'STRING', 'NULLABLE', None, ()),
        attributes,
        identifiers
    ]

    SCHEMA = {
        "study": common + [
            SchemaField('study_type', 'STRING', 'NULLABLE', None, ()),
            SchemaField('study_accession', 'STRING', 'NULLABLE', "The study accession", ()),
            SchemaField('abstract', 'STRING', 'NULLABLE', None, ()),
            SchemaField('BioProject', 'STRING', 'NULLABLE', None, ()),
            SchemaField('GEO', 'STRING', 'NULLABLE', None, ()),
            xrefs
        ],
        "sample": common + [
            SchemaField('organism', 'STRING', 'NULLABLE', None, ()),
            SchemaField('sample_accession', 'STRING', 'NULLABLE', None, ()),
            SchemaField('GEO', 'STRING', 'NULLABLE', None, ()),
            SchemaField('BioSample', 'STRING', 'NULLABLE', None, ()),
            SchemaField('taxon_id', 'INTEGER', 'NULLABLE', None, ()),
            xrefs
        ],
        "experiment": common + [
            SchemaField('instrument_model', 'STRING', 'NULLABLE', None, ()),
            SchemaField('library_selection', 'STRING', 'NULLABLE', None, ()),
            SchemaField('design', 'STRING', 'NULLABLE', None, ()),
            SchemaField('experiment_accession', 'STRING', 'NULLABLE', None, ()),
            SchemaField('library_layout_orientation', 'STRING', 'NULLABLE', None, ()),
            SchemaField('library_name', 'STRING', 'NULLABLE', None, ()),
            SchemaField('library_strategy', 'STRING', 'NULLABLE', None, ()),
            SchemaField('library_construction_protocol', 'STRING', 'NULLABLE', None, ()),
            SchemaField('library_layout', 'STRING', 'NULLABLE', None, ()),
            SchemaField('library_layout_length', 'INTEGER', 'NULLABLE', None, ()),
            SchemaField('library_layout_sdev', 'FLOAT', 'NULLABLE', None, ()),
            SchemaField('library_source', 'STRING', 'NULLABLE', None, ()),
            SchemaField('platform', 'STRING', 'NULLABLE', None, ()),
            SchemaField('sample_accession', 'STRING', 'NULLABLE', None, ()),
            SchemaField('study_accession', 'STRING', 'NULLABLE', None, ()),
            xrefs
        ],
        "run": common + [
            SchemaField('spot_length', 'INTEGER', 'NULLABLE', None, ()),
            SchemaField('run_date', 'TIMESTAMP', 'NULLABLE', None, ()),
            SchemaField('nreads', 'INTEGER', 'NULLABLE', None, ()),
            SchemaField('reads', 'RECORD', 'REPEATED', None,
                        (SchemaField('base_coord', 'INTEGER', 'NULLABLE', None, ()),
                         SchemaField('read_type', 'STRING', 'NULLABLE', None, ()),
                         SchemaField('read_class', 'STRING', 'NULLABLE', None, ()),
                         SchemaField('read_index', 'INTEGER', 'NULLABLE', None, ()))),
            SchemaField('experiment_accession', 'STRING', 'NULLABLE', None, ()),
            SchemaField('run_center', 'STRING', 'NULLABLE', None, ()),
            SchemaField('qualities', 'RECORD', '', None,
                        (SchemaField('quality', 'INTEGER', 'NULLABLE',None,()),
                        ))
        ]
    }


    
    ###############################
    # SRA_Accessions table schema #
    ###############################
    
    fields = """Accession       Submission      Status  Updated Published       Received        Type    \
                Center  Visibility      Alias   Experiment      Sample  Study   Loaded  Spots   Bases   \
                Md5sum  BioSample       BioProject      ReplacedBy""".split()

    date_fields = 'Updated Published Received'.split()
    TMPSCHEMA = []
    for field in fields:
        if(field not in date_fields):
            TMPSCHEMA.append(SchemaField(field,"STRING","NULLABLE",None, ()))
        else:
            TMPSCHEMA.append(SchemaField(field,"TIMESTAMP","NULLABLE",None, ()))
    SCHEMA['sra_accession']=TMPSCHEMA

    # SCHEMA is a dict with entries for all non-csv etl tables
    return SCHEMA



def delete_table(table):
    """Just supply schema.tablename as table"""
    t = client.get_table(table)
    client.delete_table(t)


######################################
# DDL for a bigquery ETL table       #
#                                    #
# - assumes that table is named with #
#   key from SCHEMA dict             #
######################################
def create_table(table):
    t_ref = sra.table(table)
    t = bigquery.Table(t_ref, schema=prep_bigquery_schemas()[table])
    try:
        t = client.create_table(t)
    except google.api_core.exceptions.Conflict:
        t = client.get_table(t)
    return t



def create_all_tables():
    for t in "experiment sample study run sra_accession".split():
        create_table(t)
             


#####################################
# Load SRA_accession table from GCS #
#####################################
def load_accession_table():
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.field_delimiter = "\t"
    job_config.null_marker = "-"
    job_config.quote_character = ""
    job_config.skip_leading_rows = 1
    # shouldn't be any bad records after sed s/--/-\\t-/g on the original file
    # job_config.max_bad_records = 100
    
    # TODO: refactor hard-coded URIs
    uri = 'gs://omicidx-cancerdatasci-org/sra/sra_accessions_fixed.tab'
    table_ref = sra.table('sra_accession')
    load_job = client.load_table_from_uri(
        uri,
        table_ref,
        job_config=job_config)  # API request
    print('Starting job {}'.format(load_job.job_id))

    from google.api_core.exceptions import BadRequest

    try:
        result = load_job.result() # Waits for table load to complete.
    except BadRequest as err:
        for er in err.errors:
            print(err)
        raise
    print('Job finished.')

    destination_table = client.get_table(table_ref)
    print('Loaded {} rows.'.format(destination_table.num_rows))



#########################################
# Load the livelist.csv file/table into #
# bigquery                              #
#########################################

def load_livelist_table():
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.autodetect = True
    job_config.skip_leading_rows = 1
    
    # TODO: refactor hard-coded URIs
    uri = 'gs://omicidx-cancerdatasci-org/sra/livelist.csv'
    table_ref = sra.table('livelist')
    load_job = client.load_table_from_uri(
        uri,
        table_ref,
        job_config=job_config)  # API request
    print('Starting job {}'.format(load_job.job_id))

    from google.api_core.exceptions import BadRequest

    try:
        result = load_job.result() # Waits for table load to complete.
    except BadRequest as err:
        for er in err.errors:
            print(err)
        raise
    print('Job finished.')

    destination_table = client.get_table(table_ref)
    print('Loaded {} rows.'.format(destination_table.num_rows))




#######################################
# Load an SRA json entity from GCS to #
# BigQuery                            #
#######################################

def load_json_table(table):
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    uri = 'gs://omicidx-cancerdatasci-org/sra/{}.json'.format(table)
    table_ref = sra.table(table)
    load_job = client.load_table_from_uri(
        uri,
        table_ref,
        job_config=job_config)  # API request
    print('Starting job {}'.format(load_job.job_id))

    load_job.result()  # Waits for table load to complete.
    print('Job finished.')

    destination_table = client.get_table(table_ref)
    print('Loaded {} rows.'.format(destination_table.num_rows))


def load_all_json_tables():
    for t in "experiment sample run study".split():
        load_json_table(t)

    

########################################
# Do the database joins separately for #
# - study                              #
# - experiment                         #
# - sample                             #
# - run                                #
########################################

def study_join_accessions():
    job_config = bigquery.QueryJobConfig()
    # Set the destination table
    table_ref = sra_live.table('study')
    job_config.destination = table_ref
    sql = """
    SELECT 
      a.Status as status,
      a.Updated as update_date,
      l.LastMetaUpdate as meta_update_date,
      a.Published as publish_date,
      a.Received as received_date,
      a.Visibility as visibility,
      l.Insdc as insdc,
      s.*
    FROM `isb-cgc-01-0006.omicidx_etl.study` s 
    JOIN `isb-cgc-01-0006.omicidx_etl.sra_accession` a ON a.accession=s.accession
    JOIN `isb-cgc-01-0006.omicidx_etl.livelist` l on l.accession=a.accession;
    """

    # Start the query, passing in the extra configuration.
    query_job = client.query(
        sql,
        job_config=job_config)  # API request - starts the query

    print('Starting job {}'.format(query_job.job_id))
    return query_job



def experiment_join_accessions():
    job_config = bigquery.QueryJobConfig()
    # Set the destination table
    table_ref = sra_live.table('experiment')
    job_config.destination = table_ref
    sql = """
    SELECT 
      a.Status as status,
      a.Updated as update_date,
      l.LastMetaUpdate as meta_update_date,
      a.Published as publish_date,
      a.Received as received_date,
      a.Visibility as visibility,
      l.Insdc as insdc,
      s.*
    FROM `isb-cgc-01-0006.omicidx_etl.experiment` s 
    JOIN `isb-cgc-01-0006.omicidx_etl.sra_accession` a ON a.accession=s.accession
    JOIN `isb-cgc-01-0006.omicidx_etl.livelist` l on l.accession=a.accession;
    """

    # Start the query, passing in the extra configuration.
    query_job = client.query(
        sql,
        job_config=job_config)  # API request - starts the query

    print('Starting job {}'.format(query_job.job_id))
    return query_job

def run_join_accessions():
    job_config = bigquery.QueryJobConfig()
    # Set the destination table
    table_ref = sra_live.table('run')
    job_config.destination = table_ref
    sql = """
    SELECT 
      a.Status as status,
      a.Updated as update_date,
      l.LastMetaUpdate as meta_update_date,
      a.Published as publish_date,
      a.Received as received_date,
      a.Visibility as visibility,
      l.Insdc as insdc,
      r.*,
      e.sample_accession,
      e.study_accession,
      a.spots,
      a.bases
    FROM `isb-cgc-01-0006.omicidx_etl.run` r 
    JOIN `isb-cgc-01-0006.omicidx_etl.experiment` e on e.accession=r.experiment_accession 
    JOIN `isb-cgc-01-0006.omicidx_etl.sra_accession` a ON a.accession=r.accession
    JOIN `isb-cgc-01-0006.omicidx_etl.livelist` l on l.accession=a.accession;
    """

    # Start the query, passing in the extra configuration.
    query_job = client.query(
        sql,
        job_config=job_config)  # API request - starts the query

    print('Starting job {}'.format(query_job.job_id))
    return query_job


def sample_join_accessions():
    job_config = bigquery.QueryJobConfig()
    # Set the destination table
    table_ref = sra_live.table('sample')
    job_config.destination = table_ref
    sql = """
    SELECT 
      a.Status as status,
      a.Updated as update_date,
      l.LastMetaUpdate as meta_update_date,
      a.Published as publish_date,
      a.Received as received_date,
      a.Visibility as visibility,
      l.Insdc as insdc,
      s.*,
      e.study_accession
    FROM `isb-cgc-01-0006.omicidx_etl.sample` s
    JOIN `isb-cgc-01-0006.omicidx_etl.experiment` e on e.sample_accession=s.accession 
    JOIN `isb-cgc-01-0006.omicidx_etl.sra_accession` a ON a.accession=s.accession
    JOIN `isb-cgc-01-0006.omicidx_etl.livelist` l on l.accession=a.accession;
    """

    # Start the query, passing in the extra configuration.
    query_job = client.query(
        sql,
        job_config=job_config)  # API request - starts the query

    print('Starting job {}'.format(query_job.job_id))
    return query_job


#############################################
# Do all the loading from the etl schema to #
# the final SRA schema                      #
#############################################

def do_all_join_tables():
    for i in 'study experiment run sample'.split():
        delete_table('sra.'+i)
    study_join_accessions()
    sample_join_accessions()
    experiment_join_accessions()
    run_join_accessions()

if __name__ == '__main__':
    pass
