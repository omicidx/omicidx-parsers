import os
from google.cloud import pubsub_v1
import click
import omicidx.geo.parser as gm
import logging

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('sra-to-postgresql')
logger.setLevel(logging.DEBUG)


TOPIC = os.getenv('PUBSUB_TOPIC','sra-run-to-process')
PROJECT_ID = project_id=os.getenv('GOOGLE_CLOUD_PROJECT', 'isb-cgc-01-0006')
SUBSCRIPTION_NAME = os.getenv("SUBSCRIPTION_NAME", 'sra-run-to-postgres')

publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()

from sqlalchemy.sql import text
from sqlalchemy import create_engine
import json
from concurrent.futures import ThreadPoolExecutor

import omicidx.sra.parser as s
import os
sqlalchemy_url = os.getenv('SQLALCHEMY_URL')

engine = create_engine(sqlalchemy_url)
con = engine.connect()

def load_srr(srr):
    # convert back to ascii from bytes
    # bytes needed by pubsub
    srr = srr.decode('UTF-8')
    z = s.models_from_runbrowser(srr)
        
    statement = text('insert into etl.srr_jsonb (doc, accession) values (:val, :accession)')
    d = {'val':z.json(), 'accession': z.accession}
    con.execute(statement, **d)
    logger.info('inserted {}'.format(srr))


@click.group()
def cli():
    pass
    
@cli.command(help="send a year's worth of GSE accessions to pubsub")
@click.option('--year')
def send_them(year):
    n = 0
#    for g in s.get_accession_list(from_date = '2018-01-01', to_date='2018-02-01', count=2000, type="RUN"):
    import sys
    for line in sys.stdin:
        n+=1
        if(n % 1000 == 0):
            logger.info('sent {} records'.format(n))
        publisher.publish(get_topic_name(), bytes(line.strip(), 'UTF-8'))
                                   


def get_topic_name():
    topic_name = 'projects/{project_id}/topics/{topic}'.format(
        project_id=PROJECT_ID,
        topic=TOPIC,  # Set this to something appropriate.
    )
    return topic_name

def get_subscription_name():
    subscription_name = 'projects/{project_id}/subscriptions/{sub}'.format(
        project_id=PROJECT_ID,
        sub=SUBSCRIPTION_NAME,  # Set this to something appropriate.
    )
    return subscription_name

@cli.command(help="send any message to the pubsub topic")
@click.option('--message')
def send_message(message):
    publisher.publish(get_topic_name(), bytes(message, 'UTF-8'))

    
#@cli.command(help="make a subscription--use only once")

def callback(message):
    try:
        load_srr(message.data)
        message.ack()
    except Exception as err:
        logger.error(message.data)
        logger.error(err)

@cli.command(help="open pubsub subscription and process results")
def subscribed():
    n = 0
    while(True):
        with ThreadPoolExecutor(max_workers=4) as tpe:
            n+=1
            if(n>100):
                n=0
                continue
            scheduler = pubsub_v1.subscriber.scheduler.ThreadScheduler(executor = tpe)
            future = subscriber.subscribe(get_subscription_name(), callback, scheduler = scheduler)
            try:
                future.result()
            except:
                future.cancel()
                raise
    
@cli.command(help="setup pubsub topic and subscription")
def setup_pubsub():
    publisher.create_topic(get_topic_name())
    subscriber.create_subscription(
        name=get_subscription_name(), topic=get_topic_name())

@cli.command(help="clean up subscription and topic")
def cleanup_pubsub():
    subscriber.delete_subscription(get_subscription_name())
    publisher.delete_topic(get_topic_name())

    
if __name__ == '__main__':
    cli()
