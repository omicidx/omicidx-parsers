# Overview

This README describes how to run the Spark scripts in this directory. 

## SRA metadata processing

See `sra_etl.py`.

This will process the original parsed JSON files and csv files to 
json and parquet. 

```sh
spark-submit sra_etl.py \
    s3://omicidx.cancerdatasci.org/sra/NCBI_SRA_Mirroring_20180801_Full/ \
    s3://omicidx.cancerdatasci.org/sra/NCBI_SRA_Mirroring_20180801_Full/
```

```sh
# this needs some modification, but you get the gist
export CLUSTER_ID="j-3IDRZIAOCKY2I"
aws emr add-steps --cluster-id $CLUSTER_ID \
  --steps Type=spark,Name=sra_etl,Args=[--deploy-mode,cluster,--master,yarn,s3://omics_metadata/sra_etl.py],ActionOnFailure=CONTINUE
```


