#!/usr/bin/env python
import argparse
import json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk

def main():

    parser = argparse.ArgumentParser()

    parser.add_argument("json", help="The json filename")
    parser.add_argument("index", help="The name of the elasticsearch index")
    parser.add_argument("connection_string", help = "The elasticsearch hosts string")
    parser.add_argument("--id", help="The field of the json document to use as the ID",
                    default = None)
    args = parser.parse_args()

    es = Elasticsearch(hosts=args.connection_string)
    print(es)

    print(args)
    def gen():
        n = 0
        with open(args.json, "r") as f:
            for line in f:
                j = json.loads(line)
                if(args.id is not None):
                    j['_id'] = j['accession']
                j['_index'] = args.index
                j['_type'] = 'doc'
                yield(j)
    


    #es.index(index='sra4', doc_type='doc', id = j['accession'], body = j)
    n = 0
    for y in parallel_bulk(es, gen()):
        if((n % 100000) == 0):
            print(n)
        n+=1
        z = y

if __name__=="__main__":
    main()