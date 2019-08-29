Bigquery
========

Bigquery_ is a cloud-based, fully-managed data warehouse and
relational database and analytical engine available from
Google. Bigquery is capable of storing and querying very large
datasets (think billions or even more rows, TB of data).

.. _Bigquery: https://bigquery.cloud.google.com/

For OmicIDX, though, Bigquery offers additional advantages besides
handling large datasets. Leveraging the ability to offer datasets
"publicly", combined with Bigquery's capabilities to join arbitrary
tables to each other, regardless of "owner". Bigquery, then, allows
any user the option to query OmicIDX, but also to join OmicIDX data to
other datasets of interest such as ontologies, enriched metadata
provided by hand curation, or even to perform large-scale machine
learning.

Tables
------


========== ==========
Repository Table Name
---------- ----------
 sra       sra_study  
 sra       sra_sample  
 sra       sra_experiment  
 sra       sra_run  
 biosample biosample
========== ==========
