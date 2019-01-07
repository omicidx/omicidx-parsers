from google.cloud import bigquery
from google.cloud.bigquery.schema import SchemaField

client = bigquery.Client()
sra    = client.dataset('sra')

attributes = SchemaField('attributes', 'RECORD', 'REPEATED', None, (
        SchemaField('value', 'STRING', 'NULLABLE', None, ()),
        SchemaField('tag', 'STRING', 'NULLABLE', None, ())))
identifiers = SchemaField('identifiers', 'RECORD', 'REPEATED', None, (
        SchemaField('id', 'STRING', 'NULLABLE', None, ()),
        SchemaField('namespace', 'STRING', 'NULLABLE', None, ())))
xrefs       = SchemaField('xrefs', 'RECORD', 'REPEATED', None, (
        SchemaField('id', 'STRING', 'NULLABLE', None, ()),
        SchemaField('db', 'STRING', 'NULLABLE', None, ())))
common      = [
    SchemaField('abstract', 'STRING', 'NULLABLE', None, ()),
    SchemaField('accession', 'STRING', 'NULLABLE', None, ()),
    SchemaField('alias', 'STRING', 'NULLABLE', None, ()),
    SchemaField('title', 'STRING', 'NULLABLE', None, ()),
    SchemaField('center_name', 'STRING', 'NULLABLE', None, ()),
    SchemaField('broker_name', 'STRING', 'NULLABLE', None, ()),
    SchemaField('description', 'STRING', 'NULLABLE', None, ()),
    attributes,
    identifiers,
    xrefs
]

SCHEMA = {
    "study": common + [
        SchemaField('study_accession', 'STRING', 'NULLABLE', "The study accession", ()),
        SchemaField('abstract', 'STRING', 'NULLABLE', None, ()),
        SchemaField('BioProject', 'STRING', 'NULLABLE', None, ()),
        SchemaField('GEO', 'STRING', 'NULLABLE', None, ())
    ],
    "sample": common + [
        SchemaField('organism', 'STRING', 'NULLABLE', None, ()),
        SchemaField('sample_accession', 'STRING', 'NULLABLE', None, ()),
        SchemaField('GEO', 'STRING', 'NULLABLE', None, ()),
        SchemaField('BioSample', 'STRING', 'NULLABLE', None, ()),
        SchemaField('taxon_id', 'INTEGER', 'NULLABLE', None, ())
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
        SchemaField('study_accession', 'STRING', 'NULLABLE', None, ())
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
        SchemaField('experiment_accession', 'STRING', 'NULLABLE', None, ())
    ]
}


fields = "Accession       Submission      Status  Updated Published       Received        Type    Center  Visibility      Alias   Experiment      Sample  Study   Loaded  Spots   Bases   Md5sum  BioSample       BioProject      ReplacedBy".split()

date_fields = 'Updated Published Received'.split()

def prep_sra_accession_schema():
    SCHEMA=[]
    for field in fields:
        #SCHEMA.append(SchemaField(field,"STRING","NULLABLE",None, ()))
        if(field not in date_fields):
            SCHEMA.append(SchemaField(field,"STRING","NULLABLE",None, ()))
        else:
            SCHEMA.append(SchemaField(field,"TIMESTAMP","NULLABLE",None, ()))
    return SCHEMA

def create_study_table():
    try:
        study_ref = sra.table('study')
        study = bigquery.Table(study_ref, schema=STUDY_SCHEMA)
        study = client.create_table(study)
    except:
        pass

def create_accession_table():
    try:
        t_ref = sra.table('sra_accession')
        t = bigquery.Table(t_ref, schema=prep_sra_accession_schema())
        t = client.create_table(t)
    except:
        pass

def load_accession_table():
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.field_delimiter = "\t"
    job_config.null_marker = "-"
    job_config.skip_leading_rows = 1
    job_config.max_bad_records = 10000
    uri = 'gs://omicidx-cancerdatasci-org/sra/sra_accessions_fixed.tab'
    table_ref = sra.table('sra_accession')
    load_job = client.load_table_from_uri(
        uri,
        table_ref,
        job_config=job_config)  # API request
    print('Starting job {}'.format(load_job.job_id))

    load_job.result()  # Waits for table load to complete.
    print('Job finished.')

    destination_table = client.get_table(table_ref)
    print('Loaded {} rows.'.format(destination_table.num_rows))

    
def load_study_table():
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    uri = 'gs://omicidx-cancerdatasci-org/sra/study.json'
    table_ref = sra.table('study')
    load_job = client.load_table_from_uri(
        uri,
        table_ref,
        job_config=job_config)  # API request
    print('Starting job {}'.format(load_job.job_id))

    load_job.result()  # Waits for table load to complete.
    print('Job finished.')

    destination_table = client.get_table(table_ref)
    print('Loaded {} rows.'.format(destination_table.num_rows))


def join_accessions(table):
    job_config = bigquery.QueryJobConfig()
    # Set the destination table
    table_ref = sra.table('study_joined')
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
    FROM `isb-cgc-01-0006.sra.study` s 
    JOIN `isb-cgc-01-0006.sra.sra_accession` a ON a.accession=s.accession
    JOIN `isb-cgc-01-0006.sra.livelist` l on l.accession=a.accession;
    """

    # Start the query, passing in the extra configuration.
    query_job = client.query(
        sql,
        job_config=job_config)  # API request - starts the query

    query_job.result()  # Waits for the query to finish
    print('Query results loaded to table {}'.format(table_ref.path))


def experiment_join_accessions():
    job_config = bigquery.QueryJobConfig()
    # Set the destination table
    table_ref = sra.table('experiment_joined')
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
    FROM `isb-cgc-01-0006.sra.experiment` s 
    JOIN `isb-cgc-01-0006.sra.sra_accession` a ON a.accession=s.accession
    JOIN `isb-cgc-01-0006.sra.livelist` l on l.accession=a.accession;
    """

    # Start the query, passing in the extra configuration.
    query_job = client.query(
        sql,
        job_config=job_config)  # API request - starts the query

    query_job.result()  # Waits for the query to finish
    print('Query results loaded to table {}'.format(table_ref.path))


def run_join_accessions():
    job_config = bigquery.QueryJobConfig()
    # Set the destination table
    table_ref = sra.table('run_joined')
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
    FROM `isb-cgc-01-0006.sra.run` r 
    JOIN `isb-cgc-01-0006.sra.experiment` e on e.accession=r.experiment_accession 
    JOIN `isb-cgc-01-0006.sra.sra_accession` a ON a.accession=r.accession
    JOIN `isb-cgc-01-0006.sra.livelist` l on l.accession=a.accession;
    """

    # Start the query, passing in the extra configuration.
    query_job = client.query(
        sql,
        job_config=job_config)  # API request - starts the query

    query_job.result()  # Waits for the query to finish
    print('Query results loaded to table {}'.format(table_ref.path))


def sample_join_accessions():
    job_config = bigquery.QueryJobConfig()
    # Set the destination table
    table_ref = sra.table('sample_joined')
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
    FROM `isb-cgc-01-0006.sra.sample` s
    JOIN `isb-cgc-01-0006.sra.experiment` e on e.sample_accession=s.accession 
    JOIN `isb-cgc-01-0006.sra.sra_accession` a ON a.accession=s.accession
    JOIN `isb-cgc-01-0006.sra.livelist` l on l.accession=a.accession;
    """

    # Start the query, passing in the extra configuration.
    query_job = client.query(
        sql,
        job_config=job_config)  # API request - starts the query

    query_job.result()  # Waits for the query to finish
    print('Query results loaded to table {}'.format(table_ref.path))

    
def main():
    create_study_table()
    load_study_table()

if __name__ == '__main__':
    main()
