from Bio import Entrez
import logging

try:
    from urllib.error import HTTPError  # for Python 3
except ImportError:
    from urllib2 import HTTPError  # for Python 2

import urllib
from lxml import etree
import lxml
from io import StringIO

available_etyp = ['GSE', 'GSM', 'GPL', 'GDS']

class GEOMeta(object):
    def __init__(self,email = 'user@example.com'):
        self.Entrez = Entrez
        self.Entrez.email = email

    def get_geo_accessions(self, etyp='GSE', batch_size = 25, add_term = None):
        """get GEO accessions
        """
        if(etype not in available_etyp):
            raise Exception("etype {} not one of the accepted etyps {}".format(etyp, str(available_etyp)))
        handle = self.Entrez.esearch(db='gds', term = etyp + '[ETYP]' + str(add_term), usehistory='y')
        search_results = self.Entrez.read(handle)
        webenv = search_results["WebEnv"]
        query_key = search_results["QueryKey"]
        count = int(search_results['Count'])
        data = []
        logging.info('found {} records for {} database'.format(count,etyp))
        for start in range(0, count, batch_size):
            end = min(count, start+batch_size)
            attempt = 0
            while attempt < 3:
                attempt += 1
                try:
                    fetch_handle = self.Entrez.esummary(db="gds",
                                                 rettype="fasta", retmode="xml",
                                                 retstart=start, retmax=batch_size,
                                                 webenv=webenv, query_key=query_key)
                except HTTPError as err:
                    if 500 <= err.code <= 599:
                        print("Received error from server %s" % err)
                        print("Attempt %i of 3" % attempt)
                        time.sleep(2)
                    else:
                        raise
            for g in self.Entrez.read(fetch_handle):
                yield(g['Accession'])
        raise StopIteration

    def get_geo_accession_xml(self, accession, ):
        url = "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?targ=self&acc={}&form=text&view=brief".format(accession)
        with urllib.request.urlopen(url) as res:
            return([line.decode().strip() for line in res.readlines()])
        
        
