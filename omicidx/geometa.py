from Bio import Entrez
import logging
import collections
import re
import datetime
import urllib
from xml import etree
import xml
from io import StringIO

try:
    from urllib.error import HTTPError  # for Python 3
except ImportError:
    from urllib2 import HTTPError  # for Python 2

ETYP = ['GSE', 'GSM', 'GPL', 'GDS']

def get_entrez_instance(email = 'user@example.com'):
    """Return a Bio::Entrez object
    
    Parameters
    ==========
    email: str the email to be used with the Entrez instance
    
    Returns
    =======
    A Bio::Entrez instance
    """
    ret = Entrez
    ret.email = email
    return ret


def get_geo_accessions(etyp='GSE', batch_size = 25, add_term = None, email = "user@example.com"):
    """get GEO accessions
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
                break
            except HTTPError as err:
                print("Received error from server %s" % err)
                print("Attempt %i of 10" % attempt)
                import time
                time.sleep(1*attempt*attempt)
            else:
                raise
        for g in entrez.read(fetch_handle):
            n+=1
            yield(g['Accession'])
        print(n)

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

def get_geo_accession_soft(accession, targ = 'all'):
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
    url = ("https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?targ={}&acc={}&form=text&view=quick"
           .format(targ, accession))
    attempt = 0
    while attempt < 10:
        attempt += 1
        try:
            return urllib.request.urlopen(url)
            break
        except Exception as err:
            print("Received error from server %s" % err)
            print("Attempt %i of 10" % attempt)
            import time
            time.sleep(1*attempt)


def split_on_first(line,split = ' = '):
    l = line.split(split)
    return (l[0], split.join(l[1:]))


def get_geo_entities(txt):
    entities = {}
    accession = None
    for line in txt:
        line = line.strip()
        if(line.startswith('^')):
            if(accession is not None):
                entities[accession] = entity
            accession = split_on_first(line)[1]
            entity = []
        entity.append(line)
    return entities

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

###############################################
# Parse a single entity of SOFT format,       #
# generically. Then pass along to             #
# GSE, GSM, or GPL specific parser and return #
# appropriate class.                          #
###############################################
def _parse_single_entity_soft():
    pass

#######################################
# Parse the GSE entity in SOFT format #
#######################################
def _parse_single_gse_soft():
    pass

#######################################
# Parse the GSM entity in SOFT format #
#######################################
def _parse_single_gsm_soft():
    pass

#######################################
# Parse the GPL entity in SOFT format #
#######################################
def _parse_single_gpl_soft():
    pass


##############################################
# This function takes the entire SOFT        #
# format output from a call to the           #
# NCBI GEO accession viewer page and         #
# breaks up into individual entities         #
# and then calls _parse_single_entity_soft() #
##############################################
def parse_geo_soft(self,txt):
    tups = [split_on_first(line) for line in txt]
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

class GEOEntity(dict):
    def __init__(self):
        super().__init__()
        return self

class GEOSeries(GEOEntity):
    SCALAR_FIELDS = ['status',
                     'title',
                     'summary',
                     'overall_design']

