"""Usage

python -m omicidx.geometa --help
"""

from Bio import Entrez
import logging
import collections
import re
import datetime
import urllib
from xml import etree
import xml
from io import StringIO
import requests
logging.basicConfig(level=logging.INFO)

try:
    from urllib.error import HTTPError  # for Python 3
except ImportError:
    from urllib2 import HTTPError  # for Python 2

ETYP = ['GSE', 'GSM', 'GPL', 'GDS']

def get_entrez_instance(email = 'user@example.com'):
    """Return a Bio::Entrez object
    
    # Arguments
    email (str): the email to be used with the Entrez instance
    
    Returns
    =======
    A Bio::Entrez instance
    """
    ret = Entrez
    ret.email = email
    return ret


def get_geo_accessions(etyp='GSE', batch_size = 1000, add_term = None, email = "user@example.com"):
    """get GEO accessions

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
    """
    
    entrez = get_entrez_instance(email)
    if(etyp not in ETYP):
        raise Exception("etype {} not one of the accepted etyps {}".format(etyp, str(ETYP)))
    handle = entrez.esearch(db='gds', term = etyp + '[ETYP]' + str(add_term), usehistory='y')
    search_results = entrez.read(handle)
    webenv = search_results["WebEnv"]
    query_key = search_results["QueryKey"]
    count = int(search_results['Count'])
    data = []
    logging.info('found {} records for {} database'.format(count,etyp))
    from IPython.core.debugger import set_trace
    n = 0
    for start in range(0, count, batch_size):
        end = min(count, start+batch_size)
        attempt = 0
        fetch_handle = None
        while attempt < 10:
            attempt += 1
            try:
                fetch_handle = entrez.esummary(db="gds",
                                             rettype="acc", retmode="xml",
                                             retstart=start, retmax=batch_size,
                                             webenv=webenv, query_key=query_key)
                for g in entrez.read(fetch_handle):
                    n+=1
                    yield(g['Accession'])

                break
            except (HTTPError, RuntimeError) as err:
                import time
                if(isinstance(err, HTTPError)):
                    logging.error("Received error from server %s" % err)
                    logging.error("Attempt %i of 10" % attempt)
                    time.sleep(1*attempt*attempt)
                else:
                    logging.error("RuntimeError received: {}".format(err))
                    logging.error("Attempt %i of 10" % attempt)
                    time.sleep(1)
            else:
                raise

        
def get_geo_accession_xml(accession):
    url = "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?targ=self&acc={}&form=xml&view=quick".format(accession)
    attempt = 0
    while attempt < 10:
        attempt += 1
        try:
            return urllib.request.urlopen(url)
            break
        except Exception as err:
            print("Received error from server %s" % err)
            print("Attempt %i of 3" % attempt)
            import time
            time.sleep(1*attempt)

            
def get_geo_accession_soft(accession, targ = 'all', view = "brief"):
    """Open a connection to get the GEO SOFT for an accession

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
    """
    logging.info(f'Accessing accession {accession} in SOFT format')
    url = ("https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?targ={}&acc={}&form=text&view={}"
           .format(targ, accession, view))
    attempt = 0
    while attempt < 10:
        attempt += 1
        try:
            resp = requests.get(url)
            return StringIO(resp.text)
            break
        except Exception as err:
            print("Received error from server %s" % err)
            print("Attempt %i of 10" % attempt)
            import time
            time.sleep(1*attempt)


def _split_on_first_equal(line, val='='):
    """Split a line based on first occurrence of " ="

    Parameters
    ----------
    line: str
        string to split
    split: str
        split based on this. Can be multiple characters.
    >>> _split_on_first_equal("this = abc = 1")
    ('this', 'abc = 1')
    """
    l = line.split(" {} ".format(val))
    if(l[0].endswith(" {}".format(val))):
        l[0] = l[0].replace(' {}'.format(val),'')
    return (l[0], " {} ".format(val).join(l[1:]))



def get_geo_entities(txt):
    """Get the text associated with each entity from a block of text

    Parameters
    ----------
    txt: list(str)
    
    Returns
    -------
    A dict of entities keyed by accession and values a list of txt lines
    """
    logging.info('Parsing out geo entities')
    entities = {}
    accession = None
    for line in txt:
        line = line.strip()
        if(line.startswith('^')):
            if(accession is not None):
                entities[accession] = entity
            accession = _split_on_first_equal(line)[1]
            entity = []
        entity.append(line)
    logging.info(f'found {len(entities.keys())}')
    return entities


#####################################
# Utility functions for GSE parsing #
#####################################

def get_subseries_from_relations(relation_list):
    ret = []
    for i in relation_list:
        if(i.startswith("SuperSeries of:")):
            ret.append(i.replace('SuperSeries of: ', ''))
    return ret


def get_bioprojects_from_relations(relation_list):
    ret = []
    for i in relation_list:
        if(i.startswith("BioProject: https://www.ncbi.nlm.nih.gov/bioproject/")):
            ret.append(i.replace('BioProject: https://www.ncbi.nlm.nih.gov/bioproject/', ''))
    return ret


def get_SRA_from_relations(relation_list):
    ret = []
    for i in relation_list:
        if(i.startswith("SRA: https://www.ncbi.nlm.nih.gov/sra?term=")):
            ret.append(i.replace('SRA: https://www.ncbi.nlm.nih.gov/sra?term=', ''))
    return ret


def get_biosample_from_relations(relation_list):
    ret = []
    for i in relation_list:
        if(i.startswith("BioSample: https://www.ncbi.nlm.nih.gov/biosample/")):
            ret.append(i.replace('BioSample: https://www.ncbi.nlm.nih.gov/biosample/', ''))
    return ret


class GEOBase(object):
    """GEO Base class
    """
    def as_dict(self):
        """Return object as a dict"""
        return self.__dict__

class GEOContact(GEOBase):
    def __init__(self, d):
        """Create a new GEOContact

        Generally will not be called by a user, but when
        the parsing occurs by the parser itself.
        
        * filters to include any fields starting with "contact"
        * strips off "contact_" from key names
        * converts values to scalar strings from list
        * converts empty values to `None`
        
        Parameters
        ----------
        d: dict
            A dict as created by parsing the key/value
            pairs of a GEO accession header. 
        
        Returns
        -------
        A GEOContact object
        """
        for k, v in d.items():
            # convert to scalar for all keys
            v = v[0]
            # And empty string to None
            if(v == ''):
                v = None
            self.__setattr__(k, v)



class GEOEnitity(GEOBase):
    def __init__(self, d):
        """Create a new GEO Entity

        Generally will not be called by a user, but when
        the parsing occurs by the parser itself.
        
        * filters to include any fields starting with "contact"
        * strips off "contact_" from key names
        * converts values to scalar strings from list
        * converts empty values to `None`
        
        Parameters
        ----------
        d: dict
            A dict as created by parsing the key/value
            pairs of a GEO accession header. 
        
        Returns
        -------
        A GEO Entity object
        """
        for k, v in d.items():
            self.__setattr__(k, v)

    def __repr__(self):
        return "<GEO {}>".format(self.accession)

    def as_dict(self):
        d = {}
        for k, v in self.__dict__.items():
            if(isinstance(v, GEOBase)):
                d[k] = v.as_dict()
            else:
                d[k] = v
        return(d)


class GEOChannel(GEOBase):
    """Captures a single channel from a GSM"""
    
    def __init__(self, d, ch):
        ch_items = list([k for k in d.keys() if k.endswith('ch{}'.format(ch))])
        characteristics = []
        for k in ch_items:
            if(k.startswith('characteristics')):
                for characteristic in d[k]:
                    characteristics.append(tuple(characteristic.split(': ')))
                continue
            newk = k.replace("_ch{}".format(ch), "")
            self.__setattr__(k.replace("_ch{}".format(ch), ""), "\n".join(d[k]))
            if(newk == 'taxid'):
                self.__setattr__(newk, list([int(x) for x in self.__getattribute__(newk).split('\n')]))
        char = []
        for v in characteristics:
            if(len(v)==1):
                tag = v[0]
                val = None
            elif(len(v)==2):
                tag = v[0]
                val = v[1]
            elif(len(v)>2):
                tag = v[0]
                val = ":".join(v[1:])
            char.append({"tag": tag, "value": val})
        self.characteristics = char

class GEOSeries(GEOEnitity):
    pass


class GEOSample(GEOEnitity):

    def as_dict(self):
        res = super().as_dict()
        chdata = []
        for i in res['channels']:
            chdata.append(i.as_dict())
        res['channels'] = chdata
        return(res)


class GEOPlatform(GEOEnitity):
    pass


def _create_contact_from_parsed(d):
    contact_dict = {}
    for k, v in d.items():
        if(k.startswith('contact')):
            contact_dict[k.replace('contact_','')] = v
    return GEOContact(contact_dict)

###############################################
# Parse a single entity of SOFT format,       #
# generically. Then pass along to             #
# GSE, GSM, or GPL specific parser and return #
# appropriate class.                          #
###############################################
def _parse_single_entity_soft(entity_txt):
    # Deal with common stuff first:
    tups = [_split_on_first_equal(line) for line in entity_txt]
    accession = tups[0][1]
    entity_type = tups[0][0].replace('^','')
    tups = [(re.sub('![^_]+_', '',tup[0]), tup[1]) for tup in tups]
    d = collections.defaultdict(list)
    for tup in tups[1:]:
        d[tup[0]].append(tup[1])
    d2 = dict((k,v) for k, v in d.items())
    # int fields
    INT_FIELDS = ['pubmed_id', 'sample_taxid', 'platform_taxid']
    for i in INT_FIELDS:
        if(i in d2):
            try:
                d2[i] = list(map(lambda x: int(x), d2[i]))
            except:
                d2[i] = []
    # change "geo_accesion = [...]" to "accession = ..."
    try:
        d2['accession'] = d2['accession'][0]
    except KeyError:
        d2['accession'] = d2['geo_accession'][0]
        del(d2['geo_accession'])
    if('description =' in d2):
        del(d2['description ='])
    for k in ['status',
              'description',
              'data_processing',
              'title',
              'summary', # GSE only?
              'submission_date',  # to fix later
              'last_update_date', # to fix later
              'overall_design']: #GSE only ]:
        try:
            d2[k] = "\n".join(d2[k])
        except KeyError:
            d2[k] = None
    # contact details as object
    contact = _create_contact_from_parsed(d2)
    dkeys = d2.keys()
    for k in list(dkeys):
        if k.startswith('contact'):
            del(d2[k])
    d2['contact'] = contact
    if(entity_type=='SERIES'):
        return(_parse_single_gse_soft(d2))
    if(entity_type=='SAMPLE'):
        return(_parse_single_gsm_soft(d2))
    if(entity_type=='PLATFORM'):
        return(_parse_single_gpl_soft(d2))

    
#######################################
# Parse the GSE entity in SOFT format #
#######################################
def _parse_single_gse_soft(d2):
    try:
        d2['subseries'] = get_subseries_from_relations(d2['relation'])
        d2['bioprojects'] = get_bioprojects_from_relations(d2['relation'])
        d2['sra_studies'] = get_SRA_from_relations(d2['relation'])
    except KeyError:
        d2['subseries']=[]
        d2['bioprojects']=[]
        d2['sra_studies']=[]
    d2['_entity'] = 'GSE'
    return GEOSeries(d2)

            
def _create_gsm_channel_data(d):
    ch_count = int(d['channel_count'])
    ch_recs = []
    for i in range(0, ch_count):
        ch_recs.append(GEOChannel(d, i+1))
    return ch_recs

# Search for fields like _ch1 and _ch2 ....
import re
r1 = re.compile(r'_ch\d+$')
    
#######################################
# Parse the GSM entity in SOFT format #
#######################################
def _parse_single_gsm_soft(d2):
    # to singular:
    for k in ['platform_id',
              'channel_count',
              'library_strategy',
              'library_source',
              'library_selection',
              'instrument_model',
              'data_row_count',
              'anchor',
              'tag_length',
              'type',
              'tag_count']:
        try:
            d2[k] = "\n".join(d2[k])
        except KeyError:
            d2[k] = None
    try:
        d2['biosample'] = get_biosample_from_relations(d2['relation'])[0]
        d2['sra_experiment'] = get_SRA_from_relations(d2['relation'])[0]
    except:
        d2['biosample']=None
        d2['sra_experiment']=None
    d2['channels'] = _create_gsm_channel_data(d2)
    supp_files = []
    for k in list(d2.keys()):
        if r1.search(k): # have to use search here because endswith does not take regex
            del d2[k]
        if(k.startswith('supplementa')):
            supp_files += d2[k]
            del d2[k]
    d2['supplemental_files']=supp_files
    d2['_entity'] = 'GSM'
    return GEOSample(d2)


#######################################
# Parse the GPL entity in SOFT format #
#######################################
def _parse_single_gpl_soft(d2):
    for k in ['technology',
              'distribution',
              'organism',
              'taxid',
              'data_row_count']:
        try:
            d2[k] = "\n".join(d2[k])
        except KeyError:
            d2[k] = None
    d2['_entity'] = 'GPL'
    return GEOPlatform(d2)


def geo_soft_entity_iterator(fh):
    """Returns an iterator of GEO entities

    Given a GEO accession (typically a GSE,
    will return an iterator of the GEO entities
    associated with the record, including all
    GSMs, GPLs, and the GSE record itself

    Parameters
    ----------
    fh: Iterable[str]
       Anything that can iterate over lines of text
       Could be a list of text lines, a file handle, or
       an open url.
    
    Yields
    ------
    Iterator of GEO entities as classes


    >>> for i in geo_soft_entity_iterator(get_geo_accession_soft('GSE2553')):
    ...     print(i)
    """
    entity = []
    accession = None
    in_table = False
    for line in fh:
        try:
            if(isinstance(line, bytes)):
                line = line.decode()
        except:
            pass
        line = line.strip()
        if(line.endswith('table_begin')):
            in_table=True
        if(line.endswith('table_end')):
            in_table=False
            continue
        if(line.startswith('#')):
            continue
        if(in_table):
            continue
        if(line.startswith('^')):
            if(accession is not None):
                yield(_parse_single_entity_soft(entity))
            accession = _split_on_first_equal(line)[1]
            entity = []
            
        entity.append(line)
     


##############################################
# This function takes the entire SOFT        #
# format output from a call to the           #
# NCBI GEO accession viewer page and         #
# breaks up into individual entities         #
# and then calls _parse_single_entity_soft() #
##############################################
def geo_soft_iterator(self,txt):
    tups = [_split_on_first_equal(line) for line in txt]
    tups = [(re.sub('![^_]+_', '',tup[0]), tup[1]) for tup in tups]
    d = collections.defaultdict(list)
    for tup in tups:
        d[tup[0]].append(tup[1])
    d2 = dict((k,v) for k, v in d.items())
    for k in ['status',
              'title',
              'summary',
              'overall_design']:
        try:
            d2[k] = d2[k][0]
        except KeyError:
            d2[k] = None
    try:
        d2['subseries'] = get_subseries_from_relations(d2['relation'])
        d2['bioprojects'] = get_bioprojects_from_relations(d2['relation'])
        d2['sra_studies'] = get_SRA_from_relations(d2['relation'])
    except KeyError:
        d2['subseries']=[]
        d2['bioprojects']=[]
        d2['sra_studies']=[]

    for k in ['last_update_date', 'submission_date']:
        try:
            d2[k] = datetime.datetime.strptime(d2[k][0], '%b %d %Y').strftime("%Y-%m-%d %H:%M:%S")
        except KeyError:
            d2[k] = None
    if(d2['status'] is not None):
        if(d2['status'].startswith('Public on')):
            published_date_string = d2['status'].replace('Public on ','')
            d2['status']='public'
            d2['published_date'] = datetime.datetime.strptime(published_date_string, '%b %d %Y').strftime("%Y-%m-%d %H:%M:%S")
        else:
            d2['published_date'] = None
    else:
        d2['published_date'] = None
    for taxid_key in ['sample_taxid', 'platform_taxid']:
        try:
            d2[taxid_key]=list([int(x) for x in d2[taxid_key]])
        except KeyError:
            d2[taxid_key]=[]
    for k in d2.keys():
        if(k.startswith('contact')):
            d2[k] = d2[k][0]
    for k in list(filter(lambda k: k.startswith('^'), d2.keys())):
        del(d2[k])
    return(d2)


import click
import json

@click.group()
def cli():
    pass

def _gse_to_json(gse):
    res = []
    for z in geo_soft_entity_iterator(get_geo_accession_soft(gse)):
        res.append(json.dumps(z.as_dict()))
    return("\n".join(res))

@cli.command()
@click.option('--gse',
              help='A single GSE accession')
def gse_to_json(gse):
    print(_gse_to_json(gse))

@cli.command()
@click.option('--term',
              help='something like "2019/01/28:2019/02/28[PDAT]"')
@click.option("--outfile", type=click.File('w'),
              help="output file name, default is stdout")
def bulk_gse(term, outfile):
    for gse in get_geo_accessions(add_term = term):
        if(outfile is not None):
            logging.info("writing accession {}".format(gse))
            res = _gse_to_json(gse)
            if(res is not None):
                outfile.write(res + "\n")
            else:
                logging.warning("Accession {} not found, it appears.".format(gse))
                
@cli.command()
@click.option('--term',
              help='something like "2019/01/28:2019/02/28[PDAT]"')
@click.option("--outfile",
              help="output file name, default is stdout")
def gse_accessions(term, outfile):
    with open(outfile, 'w') as ofile:
        n = 0
        for gse in get_geo_accessions(add_term = term, batch_size=1000):
            n+=1
            if(n % 5000 ==0):
                print(n)
            ofile.write(gse + "\n")
        print(n)

if __name__ == '__main__':
    cli()
