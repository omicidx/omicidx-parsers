#!/bin/bash
SRA_BUILD=1
SRA_MIRROR_BASE=http://ftp.ncbi.nlm.nih.gov/sra/reports/Mirroring
SRA_MIRROR_DIRECTORY=$1
OMICIDX_BUCKET=s3://omicidx.cancerdatasci.org
OMICIDX_OUTPUT_BASE=sra
$SRA_MIRROR_BASE/$SRA_MIRROR_DIRECTORY
wget -nH --no-parent --cut-dirs=3 -r -e robots=off $SRA_MIRROR_BASE/$SRA_MIRROR_DIRECTORY/
cd $SRA_MIRROR_DIRECTORY
for i in `ls | grep xml | grep -v analysis`
do
    j=`echo $i | sed s/meta_// | sed s/_set\.xml\.gz//`
    echo $i
    echo $j
    ../sra_entity_to_json.py $i $j.json
    bzip2 -f $j.json
done
rm *xml* *json *html
aws s3 sync . $OMICIDX_BUCKET/$OMICIDX_OUTPUT_BASE/$SRA_MIRROR_DIRECTORY/
    
