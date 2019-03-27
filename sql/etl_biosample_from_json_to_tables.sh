#!/bin/bash
psql -h $PG_HOST -U $PG_USER $PG_DB <<EOF

truncate biosample;
-----------------------------------------
--
-- BIOSAMPLE
--

create table biosample as 
select
	(doc->>'id')::int as id,
	doc->>'gsm' as gsm,
	doc->>'dbgap' as dbgap,
	doc->>'model' as model,
	doc->>'title' as title,
	doc->>'access' as access,
	doc->'id_recs' as id_recs,
	doc->'ids' as ids,
	(doc->>'taxon_id')::int as taxon_id,
	doc->>'accession' as accession,
	doc->'attributes' as attributes,
	doc->>'sra_sample' as sra_sample,
	doc->>'description' as description,
	(doc->>'last_update')::timestamp as last_update,
	(doc->>'publication_date')::timestamp as publication_date,
	(doc->>'submission_date')::timestamp as submission_date,
	doc->>'is_reference' as is_reference,
	doc->>'taxonomy_name' as taxonomy_name
from etl.biosample_jsonb;

EOF
