#

# New process


## Steps

- Download xml
- Create basic json
- Upload json to s3
- munge basic json to parquet
- munge parquet to 
    - experiment joined
	- sample joined
	- run joined
	- study with aggregates
	- Include aggs in spark jobs:
		- number of samples, experiments, runs
		- sample, experiment, and run accessions (as array)
- Save munged spark data (json, parquet)
- Create elasticsearch index mappings
- Drop existing elasticsearch mappings
- Load elasticsearch index mappings


## lambda

zip lambdas.zip lambda_handlers.py sra_parsers.py


aws lambda create-function --function-name sra_to_json --zip-file fileb://lambdas.zip --handler lambda_handlers.lambda_return_full_experiment_json --runtime python3.6 --role arn:aws:iam::377200973048:role/lambda_s3_exec_role


# Invoke

aws lambda invoke --function-name sra_to_json --log-type Tail --payload '{"accession":"SRX000273"}' /tmp/abc.txt

# Concurrency

1000 total, reserve for certain functions to limit, etc.

aws lambda put-function-concurrency --function-name sra_to_json --reserved-concurrent-executions 20

# timeout and memory

aws lambda update-function-configuration --function-name sra_to_json --timeout 15


# logging

https://github.com/jorgebastida/awslogs

awslogs get /aws/lambda/sra_to_json ALL --watch


## dynamodb

aws dynamodb scan --table-name sra_experiment --select "COUNT"

# GEO

```
python -m omicidx.geometa --gse=GSE10
```

Will print json, one "line" per entity to stdout.

