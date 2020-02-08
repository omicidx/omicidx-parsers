from .sra_parsers import *
import logging
import boto3
import decimal
from boto3.dynamodb.conditions import Key

logging.basicConfig(level = logging.INFO,
                    format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('lambda_handlers')
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
experiment_table = dynamodb.Table("sra_experiment")
run_table = dynamodb.Table("sra_run")
sample_table = dynamodb.Table("sra_sample")
study_table = dynamodb.Table("sra_study")

def replace_floats(obj):
    if isinstance(obj, list):
        for i in range(len(obj)):
            obj[i] = replace_floats(obj[i])
        return obj
    elif isinstance(obj, dict):
        for k, v in obj.items():
            obj[k] = replace_floats(v)
        return obj
    elif isinstance(obj, float):
        return decimal.Decimal(str(obj))
    else:
        return obj

def replace_decimals(obj):
    if isinstance(obj, list):
        for i in range(len(obj)):
            obj[i] = replace_decimals(obj[i])
        return obj
    elif isinstance(obj, dict):
        for k, v in obj.items():
            obj[k] = replace_decimals(v)
        return obj
    elif isinstance(obj, decimal.Decimal):
        if obj % 1 == 0:
            return int(obj)
        else:
            return float(obj)
    else:
        return obj
    
def full_experiment_package(accession):
    # note that this returns a list right now, as we can use any
    # accession type. In general, use SRXNNNNNN to get a list
    # of length 1.
    v = load_experiment_xml_by_accession(accession)
    s = list([SRAExperimentPackage(exptpkg).data for exptpkg in v.getroot().findall(".//EXPERIMENT_PACKAGE")])
    ret = []
    for expt in s:
        for run_idx in range(len(expt['runs'])):
            expt['runs'][run_idx] = run_from_runbrowser(expt['runs'][run_idx]['accession'])
            expt['run_count'] = len(expt['runs'])
    return s



def create_dynamodb_table(tname):
    table = dynamodb.create_table(
        TableName = tname,
        KeySchema = [
            {
                'AttributeName': 'accession',
                'KeyType': 'HASH'  #Partition key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'accession',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 20,
            'WriteCapacityUnits': 20
        }
    )
    return table


def insert_to_dynamo(expt_pkg):
    for expt in expt_pkg:
        logger.info('inserting {} to dynamodb'.format(expt['experiment']['accession']))
        with run_table.batch_writer() as batch:
            for run in expt['runs']:
                batch.put_item(Item=replace_floats(run))
                study_table.put_item(Item=replace_floats(expt['study']))
                sample_table.put_item(Item=replace_floats(expt['sample']))
        del(expt['runs'])
        del(expt['sample'])
        del(expt['study'])
        logger.info("inserted runs from {} to dynamodb".format(expt['experiment']['accession']))
        experiment_table.put_item(Item = replace_floats(expt['experiment']))
        logger.info("inserted experiment from {} to dynamodb".format(expt['experiment']['accession']))

def lambda_return_full_experiment_json(event, context):
    accession = event['accession']
    logger.info('processing accession: {}'.format(accession))
    # munge experiment package
    return full_experiment_package(accession)


def insert_if_empty(accession):
    logger.info('checking for accession {} in dynamodb'.format(accession))
    response = experiment_table.query(KeyConditionExpression=Key('accession').eq(accession))
    if(len(response['Items'])==0):
        expt_pkg = full_experiment_package(accession)
        logger.info('expt_pkg for accession {} obtained'.format(accession))
        insert_to_dynamo(expt_pkg)
        return(True)
    logger.info("skipped: {}".format(accession))


def lambda_insert_to_dynamo(event, context):
    accession = event['accession']
    logger.info('processing accession: {}'.format(accession))
    insert_if_empty(accession)
    return accession

def lambda_insert_from_sqs_to_dynamo(event, context):
    accession = event['Records'][0]['body']
    logger.info('processing accession: {}'.format(accession))
    insert_if_empty(accession)
    return accession
    


def main():
    import pandas as pd
    y = pd.read_csv('~/top10k_experiments')
    from multiprocessing.dummy import Pool as ThreadPool
    pool = ThreadPool(9)
    pool.map(lambda row: insert_if_empty(row),
             y.itertuples())
    
if __name__ == '__main__':
    main()
