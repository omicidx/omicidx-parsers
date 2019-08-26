from google.cloud import bigquery
from google.cloud import storage
import google
import os
import logging
import json
logging.basicConfig(level=logging.INFO,
                    format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')


client = bigquery.Client()

from google.cloud.bigquery import SchemaField

def _get_field_schema(field):
    name = field['name']
    field_type = field.get('type', 'STRING')
    mode = field.get('mode', 'NULLABLE')
    fields = field.get('fields', [])

    if fields:
        subschema = []
        for f in fields:
            fields_res = _get_field_schema(f)
            subschema.append(fields_res)
    else:
        subschema = []

    field_schema = SchemaField(name=name, 
        field_type=field_type,
        mode=mode,
        fields=subschema
    )
    return field_schema


def parse_bq_json_schema(schema_filename):
    schema = []
    with open(schema_filename, 'r') as infile:
        jsonschema = json.load(infile)

    for field in jsonschema:
        schema.append(_get_field_schema(field))

    return schema



def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket.

    Parameters
    ----------
    bucket name: str
    source_file_name: str
        A local filename
    destination_blob_name: str
        The ``path`` of the object in storage
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    logging.info(f'Uploading file {source_file_name} to gs://{bucket_name}/{destination_blob_name}.')

    
    blob.upload_from_filename(source_file_name)

    logging.info(f'File {source_file_name} uploaded to gs://{bucket_name}/{destination_blob_name}.')



def _load_file_to_bigquery(dataset, table, uri,
                           job_config: bigquery.LoadJobConfig,
                           schema = None,
                           drop: bool = True):
    
    client = bigquery.Client()
    dataset_id = dataset
    
    dataset_ref = client.dataset(dataset_id)

    if(drop):
        try:
            client.delete_table(dataset_ref.table(table))
            logging.info(f'Table {table} dropped from dataset {dataset}')
        except:
            pass
        
    uri = uri

    load_job = client.load_table_from_uri(
        uri,
        dataset_ref.table(table),
        location="US",  # Location must match that of the destination dataset.
        job_config=job_config,
    )  # API request
    logging.info("Starting job {}".format(load_job.job_id))
    logging.info(f"Loading {uri} to table {table}")
    try:
        load_job.result()  # Waits for table load to complete.
        logging.info("Job finished.")
    except google.api_core.exceptions.BadRequest:
        logging.error(f"Job loading {uri} into {dataset}.{table} failed.")
        logging.error(load_job.errors)
    
    destination_table = client.get_table(dataset_ref.table(table))
    logging.info("Loaded {} rows.".format(destination_table.num_rows))

    


def load_json(dataset, table, uri, schema = None, drop=True):
    """Load a file from google cloud storage into BigQuery

    Parameters
    ----------
    dataset: str
        The Bigquery dataset
    table: str
        The Bigquery table
    uri: str
        The google cloud storage uri (``gs://....``)
    schema: List[SchemaField] objects or ``None``
        The schema as a list of `bigquery.SchemaField` objects
    drop: boolean
        Drop the table or not.
    """
    job_config = bigquery.LoadJobConfig()
    if(schema is not None):
        job_config.schema = schema
    else:
        job_config.autodetect = True
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

    _load_file_to_bigquery(dataset, table, uri, job_config, schema, drop)


def load_csv(dataset, table, uri, schema = None, drop=True, **kwargs):
    """Load a file from google cloud storage into BigQuery

    Parameters
    ----------
    dataset: str
        The Bigquery dataset
    table: str
        The Bigquery table
    uri: str
        The google cloud storage uri (``gs://....``)
    schema: List[SchemaField] objects or ``None``
        The schema as a list of `bigquery.SchemaField` objects
    drop: boolean
        Drop the table or not.
    """
    job_config = bigquery.LoadJobConfig(**kwargs)
    if(schema is not None):
        job_config.schema = schema
    else:
        job_config.autodetect = True
    job_config.source_format = bigquery.SourceFormat.CSV

    _load_file_to_bigquery(dataset, table, uri, job_config, schema, drop)




    
def main():
    from importlib import resources
    # for i in 'study sample experiment run'.split():
    #     upload_blob('temp-testing', i + '.json', 'abc/' + i + '.json')
    #     with resources.path('omicidx.data.bigquery_schemas', f"{i}.schema.json") as schemafile:
    #         load_json('omicidx_etl', f'sra_{i}', f'gs://temp-testing/abc/{i}.json',
    #                   schema=parse_bq_json_schema(schemafile))
    
    # upload_blob('temp-testing', 'SRA_Accessions.tab', 'abc/SRA_Accessions.tab')
    load_csv('omicidx_etl', 'sra_accessions', 'gs://temp-testing/abc/SRA_Accessions.tab',
             field_delimiter='\t', null_marker='-')




if __name__ == '__main__':
    main()
