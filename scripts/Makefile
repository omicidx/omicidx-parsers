OMICIDX_BUCKET=s3://omicidx.cancerdatasci.org
OMICIDX_OUTPUT_BASE=sra

SRC=$(wildcard *xml.gz)
objects := $(patsubst meta_%_set.xml.gz,%.json.bz2,$(wildcard *xml.gz))

final: $(objects)

%.bz2: %
	bzip2 $<

%.json: meta_%_set.xml.gz
	echo "Processing $< to $@, stem is $*"
	sra_entity_to_json.py $< $*.json
