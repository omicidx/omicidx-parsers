OMICIDX_BUCKET=s3://omicidx.cancerdatasci.org
OMICIDX_OUTPUT_BASE=sra

SRC=$(wildcard *xml.gz)
objectsgz := $(patsubst meta_%_set.xml.gz,%.json.gz,$(wildcard *xml.gz))
objectsbz2 := $(patsubst meta_%_set.xml.gz,%.json.bz2,$(wildcard *xml.gz))

final: $(objectsgz) $(objectsbz2)

%.bz2: %
	bzip2 -c $< > $<.bz2

%.gz: %
	gzip -c $< > $<.gz

%.json: meta_%_set.xml.gz
	echo "Processing $< to $@, stem is $*"
	sra_entity_to_json.py $< $*.json
