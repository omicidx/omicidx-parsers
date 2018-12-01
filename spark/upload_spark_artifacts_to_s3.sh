#!/bin/bash
aws s3 cp emr-config.json s3://emrdata.cancerdatasci.org/
aws s3 cp emr_bootstrap.sh s3://emrdata.cancerdatasci.org/
