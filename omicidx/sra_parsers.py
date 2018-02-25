"""SRA parsers including:

- study
- sample
- experiment
- run

These parsers each parse XML format files of the format
available from here:

http://ftp.ncbi.nlm.nih.gov/sra/reports/Mirroring/

Note that the FULL directories are "complete" while the 
others are incrementals. However, the format is the same, 
so these parsers should work on any of these.

The parsers all use SAX-like parsing, so parsing uses very
little memory (and is reasonably fast). 
"""

from lxml import etree as ET
import gzip
import bz2
import datetime
import os
import json
import csv
import re
from collections import defaultdict
import urllib
import urllib.request
import xml.etree.ElementTree as etree
from omicidx.utils import open_file

class SRAExperiment(object):
    def __init__(self,node):
        """Parse SRA Experiment to dict

        Parameters
        ----------
        node: xml.etree.ElementTree.Element
            Element

        """
        self.data = {
            'submission': self.parse_submission(node.find(".//SUBMISSION")),
            'organization': "TODO",
            'pool': "TODO",
#            'experiment' : self.experiment_xml_iter_parser(node),
#            'run' : self.run_xml_iter_parser(node),
            'sample' : self.parse_sample(node.find(".//SAMPLE")),
            'study' : self.parse_study(node.find(".//STUDY"))
        }
        


    def _safe_add_text_element(d, key, elem):
        """Add text from an xml element to a dict

        Because not all elements have text elements despite 
        their existence in the xml tree, this little 
        function checks for text existence and then
        adds the text conditionally. If no text is present,
        the key is not created. 

        Parameters
        ----------
        d : dict
            Add the text element to this dict
        key : str
            The key to which to add the text element
        elem : lxml.etree.Element
            From where to extract the text
        """
        txt = elem.text
        if(txt is not None):
            d[key] = txt.strip()
            
    def _parse_attributes(self, xml):
        if(xml is None):
            return None
        """Add attributes to a record

        Parameters
        ----------
        xml: xml.etree.ElementTree.ElementTree.Element
            An xml element of level "EXPERIMENT|STUDY|RUN|SAMPLE_ATTRIBUTES"
        """
        d = defaultdict(list)
        # Iterate over "XXX_ATTRIBUTES"
        for elem in xml:
            tag = elem.find('./TAG')
            value = elem.find('./VALUE')
            d.append({'tag': tag, 'value': value})
        return(d)

    def _get_special_ids(self, id_rec):
        namespace_map = {"geo": "GEO",
                         "gds": "GEO_Dataset",
                         "pubmed": "pubmed",
                         "biosample": "BioSample",
                         "bioproject": "BioProject"}
        # code below from sramongo/sra.py by jtfear
        # https://github.com/jfear/sramongo/blob/master/sramongo/sra.py
        # 
        # Make sure fully formed xref
        try:
            _id = id_rec['id']
            _db = id_rec['namespace']
        except:
            return False

        if (_id is None) | (_db is None):
            return False

        # normalize db name
        try:
            norm = _db.strip(' ()[].:').lower()
        except:
            norm = ''

        if norm in namespace_map.keys():
            # Normalize the ids a little
            id_norm = re.sub('geo|gds|bioproject|biosample|pubmed|pmid',
                             '', _id.lower()).strip(' :().').upper()
            return namespace_map[norm], id_norm
        else:
            return False

        
        
            
    def _parse_identifiers(self, xml, section):
        """Parse IDENTIFIERS section"""

        d = defaultdict(list)

        for _id in xml:
            if(_id.tag == "PRIMARY_ID"): 
                d[section + '_id'] = _id.text
            elif(_id.tag == "SUBMITTER_ID"):
                id_rec = {'namespace': _id.get("namespace"), 'id': _id.text}
                d[_id.tag.lower()].append(id_rec)
            elif(_id.tag == "UUID"):
                d[_id.tag.lower()].append(_id.text)
            else: # all other id types (secondary, external)
                id_rec = {'namespace': _id.get("namespace"), 'id': _id.text}
                d[_id.tag.lower()].append(id_rec)
                special = self._get_special_ids(id_rec)
                if(special):
                    d[special[0]] = special[1]
                else:
                    d[_id.tag.lower()].append(id_rec)
            
                
        return(d)

    def _process_path_map(self, xml, path_map):
        d = {}
        for k, v in path_map.items():
            try:
                if(v[1])=='text':
                    d[k] = xml.find(v[0]).text
            except:
                pass
        return(d)

    def _parse_study_type(self, xml):
        d = {}
        if(xml.get('existing_study_type')):
            d['study_type'] = xml.get('existing_study_type')
        if(xml.get('new_study_type')):
            d['study_type'] = xml.get('new_study_type')
        return(d)
        
    def parse_study(self, xml):
        """Parse an SRA xml STUDY element

        Parameters
        ----------
        xml: an xml.etree Element

        Returns
        -------
        A dict object parsed from the XML
        """

        d = {}
        d.update(xml.attrib)
        path_map = {
            'title': (".//STUDY_TITLE", "text"),
            'abstract': (".//STUDY_ABSTRACT", "text"),
            'description': (".//STUDY_DESCRIPTION", "text"),
        }
        d.update(self._process_path_map(xml, path_map))
        d.update(self._parse_identifiers(xml.find("IDENTIFIERS"),"study"))
        d.update(self._parse_study_type(xml.find('DESCRIPTOR/STUDY_TYPE')))
        # d.update(self._parse_attributes(xml.find("STUDY_ATTRIBUTES")))
        return(d)

    def parse_submission(self, xml):
        d = {}
        d.update(xml.attrib)
        d.update(self._parse_identifiers(xml.find("IDENTIFIERS"),"submission"))
        return d
    
    def run_xml_iter_parser(self, xml):
        """Parse an SRA xml RUN element

        Parameters
        ----------
        xml: an xml.etree Element
        include_run_info: boolean
            Whether or not to make a call out to the run_info api

        Returns
        -------
        A dict object parsed from the XML
        """
        d = {}
        path_map = {
            'title': (".//RUN_TITLE", "text"),
            'abstract': (".//STUDY_ABSTRACT", "text"),
            'description': (".//STUDY_DESCRIPTION", "text"),
        }
        
        d.update(xml.attrib)
        for elem in xml.iter():
            if(elem.tag == 'PRIMARY_ID' ):
                d['accession'] = elem.text
            if(elem.tag == 'EXPERIMENT_REF' ):
                d['experiment_accession'] = elem.attrib['accession']
            if(elem.tag == 'TITLE' ):
                d['title'] = elem.text
            if(elem.tag == 'RUN_ATTRIBUTE' ):
                _add_attributes(d, elem)
    #    if(include_run_info):
    #        url = 'https://trace.ncbi.nlm.nih.gov/Traces/sra/?sp=runinfo&acc={}'
    #        url = url.format(d['accession'])
    #        txt = urllib.request.urlopen(url).read().decode()
    #        reader = csv.DictReader(io.StringIO(txt),delimiter=",")
    #        vals = next(reader)
    #        for k in ["ReleaseDate","LoadDate","spots","bases","spots_with_mates","avgLength","size_MB"]:
    #            d[k.lower()] = vals[k]
        return d


            
    def experiment_xml_iter_parser(self,xml):
        """Parse an SRA xml EXPERIMENT element

        Parameters
        ----------
        xml: an xml.etree Element

        Returns
        -------
        A dict object parsed from the XML
        """
        d = {}
        library_tags = [
            "LIBRARY_STRATEGY",
            "LIBRARY_SOURCE",
            "LIBRARY_SELECTION",
            "LIBRARY_CONSTRUCTION_PROTOCOL",
            "LIBRARY_NAME"
            ]
        for elem in xml.iter():
            if(elem.tag == 'STUDY_REF'):
                try:
                    d['study_accession'] = elem.attrib['accession']
                except:
                    pass
            if(elem.tag == 'SUBMITTER_ID' ):
                db = elem.attrib['namespace'].lower()
                if(db == 'geo'):
                    d[db + '_accession'] = elem.text
            if(elem.tag == 'TITLE' ):
                d['title'] = elem.text
            if(elem.tag =='EXPERIMENT' ):
                d = {}
            if(elem.tag =='READ_SPEC' ):
                if('read_spec' not in d):
                    d['read_spec']=[]
                read_spec = {}
                read_spec['read_index'] = int(elem.find('.//READ_INDEX').text)
                read_spec['read_class'] = elem.find('.//READ_CLASS').text
                read_spec['read_type'] = elem.find('.//READ_TYPE').text
                try:
                    read_spec['base_coord'] = int(elem.find('.//BASE_COORD').text)
                except:
                    pass
                d['read_spec'].append(read_spec)
            if(elem.tag == "DESIGN_DESCRIPTION" ):
                _safe_add_text_element(d, 'design_description', elem)
            if(elem.tag == "SAMPLE_DESCRIPTOR" ):
                try:
                    d['sample_accession'] = elem.attrib['accession']
                except:
                    pass
            if(elem.tag in library_tags ):
                _safe_add_text_element(d, elem.tag.lower(), elem)
            if(elem.tag == "PAIRED" ):
                d['paired'] = True
            if(elem.tag == "SINGLE" ):
                d['paired'] = False
            if(elem.tag == "SPOT_LENGTH" ):
                d['spot_length'] = int(elem.text)
            if(elem.tag == "PLATFORM" ):
                d['platform'] = elem.find('.//INSTRUMENT_MODEL/..').tag
                d['instrument_model'] = elem.find('.//INSTRUMENT_MODEL').text
            if(elem.tag == 'EXPERIMENT_ATTRIBUTE' ):
                _add_attributes(d, elem)
        return(d)

    def parse_sample(self, xml):
        """Parse an SRA xml SAMPLE element

        Parameters
        ----------
        xml: an xml.etree Element

        Returns
        -------
        A dict object parsed from the XML
        """

        d = {}
        d.update(xml.attrib)
        path_map = {
            'title': (".//TITLE", "text"),
            'organism': (".//SCIENTIFIC_ABSTRACT", "text"),
            'description': (".//DESCRIPTION", "text"),
        }
        d.update(self._process_path_map(xml, path_map))
        d.update(self._parse_identifiers(xml.find("IDENTIFIERS"),"sample"))

        for elem in xml.iter():
            if(elem.tag == "TAXON_ID" ):
                d['taxon_id'] = int(elem.text)
            if(elem.tag == 'SAMPLE_ATTRIBUTE' ):
                _add_attributes(d, elem)

            if(elem.tag == 'SUBMITTER_ID' ):
                ns = elem.attrib['namespace']
                value = elem.text
                if('submitter_ids' not in d):
                    d['submitter_ids'] = []
                d['submitter_ids'].append({'namespace' : ns,
                                           'value': value})
        return(d)


def _custom_csv_parser(fname):
    with open_file(fname) as f:
        reader = csv.DictReader(f)
        for row in reader:
            for k in row:
                if(row[k] == ''):
                    row[k]=None
            yield(row)
    
def parse_livelist(fname):
    return _custom_csv_parser(fname)
    
def parse_run_info(fname):
    return _custom_csv_parser(fname)

def parse_addons_info(fname):
    return _custom_csv_parser(fname)

def load_experiment_xml_by_accession(accession):
    with urllib.request.urlopen('https://trace.ncbi.nlm.nih.gov/Traces/sra/sra.cgi?save=efetch&db=sra&rettype=FullXml&term={}'.format(accession)) as response:
        xml = etree.parse(response)
        return xml


import json
def get_accession_list(from_date="2004-01-01", count = 50, offset = 0, type="EXPERIMENT"):
    column_names = ["Accession", "Submission", "Type", "Received", "Published", "LastUpdate", "Status", "Insdc"]
    url = "https://www.ncbi.nlm.nih.gov/Traces/sra/status/srastatrep.fcgi/acc-mirroring?from_date={}&count={}&type={}&offset={}"
    url = url.format(from_date, count, type, offset)
    print(url)
    res = None
    with urllib.request.urlopen(url) as response:
        res = json.loads(response.read().decode('UTF-8'))
        c = 0
    print(res['fetched_count'], count, int(res['fetched_count']) == count)
    while(int(res['fetched_count']) == count): # or (c+1 <= res['fetched_count'])):
        offset += 1
        c+=1
        print(c,offset)
        if(c != int(res['fetched_count'])):
            retval = dict(zip(res['column_names'],res['rows'][c-1]))
            yield(retval)
        else:
            print(c,offset)
            url = "https://www.ncbi.nlm.nih.gov/Traces/sra/status/srastatrep.fcgi/acc-mirroring?from_date={}&count={}&type={}&offset={}"
            url = url.format(from_date, count, type, offset)
            with urllib.request.urlopen(url) as response:
                res = json.loads(response.read().decode('UTF-8'))
            print(res['fetched_count'])
            c = 0
    raise(StopIteration)
    
import io    
def get_study_records(from_date = "2004-01-01", count = 50, offset = 0):
    for row in get_accession_list(from_date, count, offset, type="STUDY"):
        xml = etree.parse(io.StringIO(row['Meta'])).getroot()
        yield(_study_xml_iter_parser(xml))



        
class LiveList(object):
    def __init__(self,from_date = "2004-01-01", count = 100, offset = 0):
        self.from_date = from_date
        self.count = count
        self.counter = offset
        self.done = False
        self._fill_buffer()

    def _url(self):
        columns = ",".join(["Accession", "Submission", "Type", "Received", "Published", "LastUpdate", "Status", "Insdc"])
        url = "https://www.ncbi.nlm.nih.gov/Traces/sra/status/srastatrep.fcgi/acc-mirroring?from_date={}&count={}&offset={}&columns={}"
        url = url.format(self.from_date, self.count, self.counter % self.count, columns)
        return(url)

    def _fill_buffer(self):
        if(not self.done):
            url = self._url()
            with urllib.request.urlopen(url) as response:
                self.buffer = json.loads(response.read().decode('UTF-8'))
                if(int(self.buffer['fetched_count']) != self.count):
                    self.done=True

    
