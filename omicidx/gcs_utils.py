"""Utilities for working with google cloud storage"""
from google.cloud import storage
import logging


def upload_blob_to_gcs(bucket_name, source_file_name, destination_blob_name):
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

