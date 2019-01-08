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

@cli.command(help = """Downloads the files necessary to build
the SRA json conversions of the XML files. """)
@click.option('--mirrordir',
              help="The mirror directory, like NCBI_SRA_Mirroring_20190101_Full")
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

@cli.command(help="""SRA XML to JSON

  Transforms an SRA XML mirroring metadata file into
  corresponding JSON format files. JSON is line-delimited
  JSON (not an array).""")
@click.argument('entity')
def process_xml_file(entity):

    fname = "meta_{}_set.xml.gz".format(entity)

    entity=entity

    parsers = {
        'study': omicidx.sra_parsers.SRAStudyRecord,
        'sample': omicidx.sra_parsers.SRASampleRecord,
        'run': omicidx.sra_parsers.SRARunRecord,
        'experiment': omicidx.sra_parsers.SRAExperimentRecord
    }
    sra_parser = parsers[entity]

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
                    outfile.write(json.dumps(rec) + "\n")
                    element.clear()
            logger.info('parsed {} entity entries'.format(n))



    
if __name__ == '__main__':
    cli()
    
