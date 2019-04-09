#!/usr/bin/env bash
DIRECTORY=$1
BUCKET=$2
LOCALDIR=/tmp/sra_metadata
mkdir -p $LOCALDIR
cd $LOCALDIR
wget -c --mirror -nH --cut-dirs=3 ftp://ftp.ncbi.nlm.nih.gov/sra/reports/Mirroring/$DIRECTORY/
curl ftp://ftp.ncbi.nlm.nih.gov/sra/reports/Metadata/SRA_Accessions.tab | gzip > $LOCALDIR/SRA_accessions.tab.gz
#aws s3 --profile=s3 sync /tmp/sra_metadata/$DIRECTORY s3://$BUCKET/sra/$DIRECTORY/raw/

