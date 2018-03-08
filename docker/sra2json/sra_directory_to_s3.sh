#!/usr/bin/env bash
BASEURL="http://ftp.ncbi.nlm.nih.gov/sra/reports/Mirroring"
DIRECTORY=$1
echo "$BASEURL/$DIRECTORY"
wget -e robots=off -nH --cut-dirs=3 --mirror -np $BASEURL/$DIRECTORY/
ls $DIRECTORY
#declare -a ENTITIES=('study','experiment','sample','run')
for i in study experiment sample run
do
    echo $i
    sra_entity_to_json.py $DIRECTORY/meta_${i}_set.xml.gz $i.json
    bzip2 $i.json
    aws s3 cp $i.json.bz2 s3://omics_metadata/sra/$DIRECTORY/$i.json.bz2
done


