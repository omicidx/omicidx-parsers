# Overview

This README describes how to run the Spark scripts in this directory. 

## SRA metadata processing

```sh
export CLUSTER_ID="j-3IDRZIAOCKY2I"
aws emr add-steps --cluster-id $CLUSTER_ID \
  --steps Type=spark,Name=sra_etl,Args=[--deploy-mode,cluster,--master,yarn,s3://omics_metadata/sra_etl.py],ActionOnFailure=CONTINUE
```


