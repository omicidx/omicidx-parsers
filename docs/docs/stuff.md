
# omicidx
Hello there


# omicidx.geometa
Usage

python -m omicidx.geometa --help


## get_entrez_instance
```python
get_entrez_instance(email='user@example.com')
```
Return a Bio::Entrez object

__Arguments__

- __email (str)__: the email to be used with the Entrez instance

Returns
=======
A Bio::Entrez instance


## get_geo_accessions
```python
get_geo_accessions(etyp='GSE', batch_size=1000, add_term=None, email='user@example.com')
```
get GEO accessions

Useful for getting all the ETYP accessions for
later bulk processing

Parameters
----------
etyp: str
    One of GSE, GPL, GSM, GDS
batch_size: int
    the number of accessions to return in one batch.
    Transparent to the user, as this returns an iterator.
add_term: str
    Add a search term for the query. Useful to limit
    by date or search for specific text.
email: str
    user email (not important)

Return
------
an iterator of accessions, each as a string


## get_geo_accession_soft
```python
get_geo_accession_soft(accession, targ='all')
```
Open a connection to get the GEO SOFT for an accession

Parameters
==========
accession: str the GEO accesssion
targ: str what to fetch. One of "all", "series", "platform",
    "samples", "self"

Returns
=======
A file-like object for reading or readlines

>>> handle = get_geo_accession_soft('GSE2553')
>>> handle.readlines()


## get_geo_entities
```python
get_geo_entities(txt)
```
Get the text associated with each entity from a block of text

Parameters
----------
txt: list(str)

Returns
-------
A dict of entities keyed by accession and values a list of txt lines


## GEOBase
```python
GEOBase(self, /, *args, **kwargs)
```
GEO Base class


## GEOChannel
```python
GEOChannel(self, d, ch)
```
Captures a single channel from a GSM

## geo_soft_entity_iterator
```python
geo_soft_entity_iterator(fh)
```
Returns an iterator of GEO entities

Given a GEO accession (typically a GSE,
will return an iterator of the GEO entities
associated with the record, including all
GSMs, GPLs, and the GSE record itself

Parameters
----------
fh: anything that can iterate over lines of text
   Could be a list of text lines, a file handle, or
   an open url.

Yields
------
Iterator of GEO entities


>>> for i in geo_soft_entity_iterator(get_geo_accession_soft('GSE2553')):
...     print(i)

