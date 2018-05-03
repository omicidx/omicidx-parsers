#!/bin/bash
# requires httpie (pip install or brew install or apt-get install or yum install)

http -a 'omicidx:Asdf1234%' DELETE https://c0b1083b4d78495d3b29a72d74fcea41.us-east-1.aws.found.io:9243/sra_sample
http -a 'omicidx:Asdf1234%' PUT https://c0b1083b4d78495d3b29a72d74fcea41.us-east-1.aws.found.io:9243/sra_sample
http -a 'omicidx:Asdf1234%' PUT https://c0b1083b4d78495d3b29a72d74fcea41.us-east-1.aws.found.io:9243/sra_sample/_mapping/doc < sra_sample_index_mapping.json

http -a 'omicidx:Asdf1234%' DELETE https://c0b1083b4d78495d3b29a72d74fcea41.us-east-1.aws.found.io:9243/sra_study
http -a 'omicidx:Asdf1234%' PUT https://c0b1083b4d78495d3b29a72d74fcea41.us-east-1.aws.found.io:9243/sra_study
http -a 'omicidx:Asdf1234%' PUT https://c0b1083b4d78495d3b29a72d74fcea41.us-east-1.aws.found.io:9243/sra_study/_mapping/doc < sra_study_index_mapping.json

http -a 'omicidx:Asdf1234%' DELETE https://c0b1083b4d78495d3b29a72d74fcea41.us-east-1.aws.found.io:9243/sra_experiment
http -a 'omicidx:Asdf1234%' PUT https://c0b1083b4d78495d3b29a72d74fcea41.us-east-1.aws.found.io:9243/sra_experiment
http -a 'omicidx:Asdf1234%' PUT https://c0b1083b4d78495d3b29a72d74fcea41.us-east-1.aws.found.io:9243/sra_experiment/_mapping/doc < sra_experiment_index_mapping.json

http -a 'omicidx:Asdf1234%' DELETE https://c0b1083b4d78495d3b29a72d74fcea41.us-east-1.aws.found.io:9243/sra_run
http -a 'omicidx:Asdf1234%' PUT https://c0b1083b4d78495d3b29a72d74fcea41.us-east-1.aws.found.io:9243/sra_run
http -a 'omicidx:Asdf1234%' PUT https://c0b1083b4d78495d3b29a72d74fcea41.us-east-1.aws.found.io:9243/sra_run/_mapping/doc < sra_run_index_mapping.json

http -a 'omicidx:Asdf1234%' DELETE https://c0b1083b4d78495d3b29a72d74fcea41.us-east-1.aws.found.io:9243/sra_full
http -a 'omicidx:Asdf1234%' PUT https://c0b1083b4d78495d3b29a72d74fcea41.us-east-1.aws.found.io:9243/sra_full
http -a 'omicidx:Asdf1234%' PUT https://c0b1083b4d78495d3b29a72d74fcea41.us-east-1.aws.found.io:9243/sra_full/_mapping/doc < sra_full_index_mapping.json
