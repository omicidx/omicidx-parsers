#!/usr/bin/env python
import argparse
import omicidx.sra_parsers
import json

parser = argparse.ArgumentParser("Dump an SRA xml file as json")

parser.add_argument('fname',
                    help="The SRA xml filename")
parser.add_argument('--livelist',
                    help="the live list csv file")

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
if(opts.livelist):
    livelist={}
    for row in omicidx.sra_parsers.parse_livelist(opts.livelist):
        if(row['Type']!=entity):
            continue
        # this accession stuff is just to make the
        # names match up when merging.
        accession = row['Accession']
        del(row['Accession'])
        row['accession'] = accession
        livelist[accession]=row
for rec in sra_parser(opts.fname):
    rec = {**rec, **livelist[rec['accession']]}
    # mark those livelist entries already used
    livelist[rec['accession']]['used']=True
    print(json.dumps(rec, indent=4))
for llrec in livelist.values():
    if('used' in llrec):
        continue
    print(json.dumps(llrec, indent=4))
    
    
