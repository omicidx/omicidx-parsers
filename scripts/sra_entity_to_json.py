#!/usr/bin/env python
import argparse
import omicidx.sra_parsers
import json

parser = argparse.ArgumentParser("Dump an SRA xml file as json")

parser.add_argument('fname',
                    help="The SRA xml filename")

opts = parser.parse_args()

if('study' in opts.fname):
    sra_parser = omicidx.sra_parsers.study_parser
if('run' in opts.fname):
    sra_parser = omicidx.sra_parsers.run_parser
if('sample' in opts.fname):
    sra_parser = omicidx.sra_parsers.sample_parser
if('experiment' in opts.fname):
    sra_parser = omicidx.sra_parsers.experiment_parser
for rec in sra_parser(opts.fname):
    print(json.dumps(rec, indent=4))
