#!/usr/bin/env python
from omicidx.biosample import BioSampleParser
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('fname')
opts = parser.parse_args()


es = Elasticsearch(['localhost:9200'],timeout=100)
print(es)
print(es.indices)
print(dir(es))

es.index(index='abc',doc_type='abc',body={'id':1},id=1)


k = ({"_index": "bioes",
      "_type" : "biosample",
      "_id"   : doc['id'],
      "_source": doc} for doc in BioSampleParser(opts.fname))

x = helpers.bulk(es,k)
     
