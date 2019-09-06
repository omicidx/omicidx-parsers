import elasticsearch
from elasticsearch.helpers import bulk
import json
import configparser
import logging
from .utils import open_file
logging.basicConfig(level = logging.INFO, format = logging.BASIC_FORMAT)


def _get_client() -> elasticsearch.client:
    config = configparser.ConfigParser()
    config.read('config.ini')
    es_config = config['elasticsearch']
    es = elasticsearch.Elasticsearch(
        hosts = es_config['nodes'],
        http_auth=(es_config['user'], es_config['password']),
        retry_on_timeout = True,
        max_retries = 3,
        timeout = 30
    )
    return es

def _prep_data(fname, index, id_field):
    with open_file(fname) as f:
        for line in f:
            d = json.loads(line)
            d['_index'] = index
            if(id_field is not None):
                if(id_field in d):
                    d['_id'] = d[id_field]
                else:
                    continue
            yield(d)

def bulk_index(fname, index, id_field = None, **kwargs):
    bulk(_get_client(), _prep_data(fname, index, id_field), **kwargs)


def bulk_index_from_gcs(bucket, prefix, index, id_field=None, **kwargs):
    """Perform bulk indexing from a set of gcs blobs

    Parameters
    ----------
    bucket: str
        GCS bucket name
    prefix: str
        The prefix string (without wildcard) to get the right blobs
    index: str
        The elasticsearch index name
    id_field: str
        The id field name (default None) that will be used as the 
        `_id` field in elasticsearch
    """
    from tempfile import NamedTemporaryFile
    from .gcs_utils import list_blobs
    flist = list(list_blobs(bucket, prefix))
    logging.info(f'Found {len(flist)} files for indexing')
    for i in flist:
        tmpfile = NamedTemporaryFile()
        if(i.name.endswith('.gz')):
            tmpfile = NamedTemporaryFile(suffix='.gz')
        logging.info('Downloading ' + i.name)
        i.download_to_filename(tmpfile.name)
        logging.info('Indexing ' + i.name)
        bulk_index(tmpfile.name, index, id_field=id_field, **kwargs)
        tmpfile.close()
        
        

