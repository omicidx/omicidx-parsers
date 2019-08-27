import google
import os
import logging
import json
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.bigquery import SchemaField

logging.basicConfig(level=logging.INFO,
                    format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def _get_field_schema(field):
    """convert bigquery field (dict) to SchemaField

    Works recursively if necessary to deal with 
    nested fields"""
    
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
    """Convert bigquery JSON file to python Bigquery Schema
    
    Parameters
    ----------
    schema_filename: str
        A json file with bigquery schema dump
    """
    schema = []
    with open(schema_filename, 'r') as infile:
        jsonschema = json.load(infile)

    for field in jsonschema:
        schema.append(_get_field_schema(field))

    return schema



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

    


def load_json_to_bigquery(dataset, table, uri, schema = None, drop=True):
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


def load_csv_to_bigquery(dataset, table, uri, schema = None, drop=True, **kwargs):
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


def copy_table(src_dataset: str, dest_dataset: str,
               src_table: str, dest_table: str,
               drop = True):
    client = bigquery.Client()
    source_dataset = client.dataset(src_dataset)
    source_table_ref = source_dataset.table(src_table)

    dest_dataset = client.dataset(dest_dataset)
    dest_table_ref = dest_dataset.table(dest_table)

    if(drop):
        try:
            client.delete_table(dest_table_ref)
            logging.info(f'Table {dest_table} dropped from dataset {dest_dataset}')
        except:
            pass
    job = client.copy_table(
        source_table_ref,
        dest_table_ref,
        # Location must match that of the source and destination tables.
        location="US",
    )  # API request
    
    logging.info("Starting job {}".format(job.job_id))
    logging.info(f"copying {src_dataset}.{src_table} to {dest_dataset}.{dest_table}")

    try:
        job.result()  # Waits for table load to complete.
        logging.info("Job finished.")
    except google.api_core.exceptions.BadRequest:
        logging.error(f"Job copying {src_dataset}.{src_table} to {dest_dataset}.{dest_table} failed")
        logging.error(load_job.errors)
    
