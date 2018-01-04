#!/usr/bin/env python
import argparse
import omicidx.sra_parsers
import json
import logging
import collections

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser("Dump an SRA xml file as json")

# optional
parser.add_argument('-l', '--livelist',
                    help = "the live list csv file, usually livelist.csv.gz")
parser.add_argument('-r', '--run_info',
                    help = "the fileinfo_run.csv.gz file, will be merged with run record")
parser.add_argument('-a', '--addons_info',
                    help="the fileinfo_addons.csv.gz file, will be merged with matching records")

#required
parser.add_argument('fname',
                    help = "The SRA xml filename")

opts = parser.parse_args()

if('study' in opts.fname):
    entity = "STUDY"
    sra_parser = omicidx.sra_parsers.study_parser
if('run' in opts.fname):
    entity = "RUN"
    sra_parser = omicidx.sra_parsers.run_parser
if('sample' in opts.fname):
    entity = "SAMPLE"
    sra_parser = omicidx.sra_parsers.sample_parser
if('experiment' in opts.fname):
    entity = "EXPERIMENT"
    sra_parser = omicidx.sra_parsers.experiment_parser
logger.info('configuration: {}'.format(str(opts)))
logger.info('using {} entity type'.format(entity))
if(opts.livelist):
    logger.info('parsing livelist [{}]'.format(opts.livelist))
    livelist={}
    n = 0
    for row in omicidx.sra_parsers.parse_livelist(opts.livelist):
        if(row['Type']!=entity):
            continue
        n+=1
        if(row['Type'] != 'STUDY'):
            del(row['BioProject'])
        if(row['Type'] != 'SAMPLE'):
            del(row['BioSample'])
        if((n % 100000)==0):
            logger.info('parsed {} livelist entries'.format(n))
        accession = row['Accession']
        del(row['Accession'])
        row['accession'] = accession
        livelist[accession]=row
    logger.info('parsed {} livelist entries'.format(n))    
if(opts.addons_info):
    logger.info('parsing addons info [{}]'.format(opts.addons_info))
    addons_info=collections.defaultdict(list)
    n = 0
    for row in omicidx.sra_parsers.parse_addons_info(opts.addons_info):
        n+=1
        if((n % 100000)==0):
            logger.info('parsed {} addons entries'.format(n))
        accession = row['Accession']
        newrow = {'file_name' : row['FileName'],
                  'file_size' : int(row['FileSize']),
                  'file_md5'  : row['FileMd5'],
                  'url'  : row['URL'],
                  'file_date' : row['FileDate']}
        del(row['Accession'])
        addons_info[accession].append(newrow)
    logger.info('parsed {} add_info entries'.format(n))    
if(opts.run_info and entity=='RUN'):
    logger.info('parsing run fileinfo [{}]'.format(opts.run_info))
    run_info={}
    n = 0
    for row in omicidx.sra_parsers.parse_run_info(opts.run_info):
        # this accession stuff is just to make the
        # names match up when merging.
        n+=1
        newrow = {'accession' : row['Accession'],
                  'file_size' : int(row['FileSize']),
                  'file_md5'  : row['FileMd5'],
                  'file_date' : row['FileDate']}
        if((n % 100000)==0):
            logger.info('parsed {} run fileinfo entries'.format(n))
        run_info[newrow['accession']]=newrow
    logger.info('parsed {} run fileinfo entries'.format(n))        
logger.info('parsing {} records'.format(entity))
n = 0
for rec in sra_parser(opts.fname):
    n+=1
    if((n % 100000)==0):
        logger.info('parsed {} {} entries'.format(entity, n))
    if(opts.livelist):
        rec = {**rec, **livelist[rec['accession']]}
        # mark those livelist entries already used
        livelist[rec['accession']]['used']=True
    if(opts.run_info and entity=='RUN'):
        try:
            rec = {**rec, **run_info[rec['accession']]}
        except:
            pass
    if(opts.addons_info):
        if(rec['accession'] in addons_info):
            addfiles = {'additional_files' :  addons_info[rec['accession']]}
            rec = {**rec, **addfiles}
    print(json.dumps(rec))
logger.info('parsed {} livelist entries'.format(n))
logger.info('filling with livelist entities'.format(n))
n = 0
for rec in livelist.values():
    if('used' in rec):
        continue
    if(opts.run_info and entity=='RUN'):
        try:
            rec = {**rec, **runinfo[llrec['accession']]}
        except:
            pass
    n+=1
    print(json.dumps(rec))
logger.info('added {} livelist entries'.format(n))
    
    
