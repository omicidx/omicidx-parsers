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

def lambda_handler(event, context):
    accession = event['accession']
    v = load_experiment_xml_by_accession(accession)
    s = SRAExperiment(v.getroot())
    return json.dumps(s.data)


class SRAExperiment(object):
    def __init__(self, node):
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
            'experiment': self.parse_experiment(node.find(".//EXPERIMENT")),
            'run': "TODO",
            'sample': self.parse_sample(node.find(".//SAMPLE")),
            'study': self.parse_study(node.find(".//STUDY"))
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
            try:
                tag = elem.find('./TAG')
                value = elem.find('./VALUE')
                d['attributes'].append({'tag': tag.text, 'value': value.text})
            except AttributeError:
                # tag or value missing text, so skip
                pass
        if(len(d)==0):
            d={}
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
        except Exception:
            return False

        if (_id is None) | (_db is None):
            return False

        # normalize db name
        try:
            norm = _db.strip(' ()[].:').lower()
        except Exception:
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
                d[section + '_accession'] = _id.text
            elif(_id.tag == "SUBMITTER_ID"):
                id_rec = {'namespace': _id.get("namespace"), 'id': _id.text}
                d[_id.tag.lower()].append(id_rec)
            elif(_id.tag == "UUID"):
                d[_id.tag.lower()].append(_id.text)
            else:  # all other id types (secondary, external)
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
                # use "text" as second tuple value to
                # get the text value
                if(v[1] == 'text'):
                    d[k] = xml.find(v[0]).text
                # use the name of the attribute to
                # get a specific attribute
                elif(v[1] == 'child'):
                    child = xml.find(v[0]).getchildren()

                    if len(child) > 1:
                        raise Exception(
                            'There are too many elements')
                    elif v[2] == 'text':
                        d[k] = child[0].text
                    elif v[2] == 'tag':
                        d[k] = child[0].tag
                else:
                    d[k] = xml.find(v[0]).get(v[1])
            except Exception as e:
                pass
        return(d)

    def _parse_study_type(self, xml):
        d = {}
        if(xml.get('existing_study_type')):
            d['study_type'] = xml.get('existing_study_type')
        if(xml.get('new_study_type')):
            d['study_type'] = xml.get('new_study_type')
        return(d)

    def try_update(self, d, value):
        try:
            d.update(value)
            return(d)
        except:
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
        d = self.try_update(d, self._parse_attributes(xml.find("STUDY_ATTRIBUTES")))
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
            'experiment_accession': (".//EXPERIMENT_REF", "accession")
        }
        
        d.update(xml.attrib)
        for elem in xml.iter():
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


    def parse_experiment(self, xml):
        """Parse an SRA xml EXPERIMENT element

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
            'title': ('./TITLE', 'text'),
            'study_accession': ('./STUDY_REF/IDENTIFIERS/PRIMARY_ID', 'text'),
            'design': ('./DESIGN/DESIGN_DESCRIPTION', 'text'),
            'library_name': (
                './DESIGN/LIBRARY_DESCRIPTOR/LIBRARY_NAME', 'text'),
            'library_strategy': (
                './DESIGN/LIBRARY_DESCRIPTOR/LIBRARY_STRATEGY', 'text'),
            'library_source': (
                './DESIGN/LIBRARY_DESCRIPTOR/LIBRARY_SOURCE', 'text'),
            'library_selection': (
                './DESIGN/LIBRARY_DESCRIPTOR/LIBRARY_SELECTION', 'text'),
            'library_layout': (
                './DESIGN/LIBRARY_DESCRIPTOR/LIBRARY_LAYOUT',
                'child', 'tag'),
            'library_layout_orientation': (
                './DESIGN/LIBRARY_DESCRIPTOR/LIBRARY_LAYOUT/PAIRED',
                'ORIENTATION'),
            'library_layout_length': (
                './DESIGN/LIBRARY_DESCRIPTOR/LIBRARY_LAYOUT/PAIRED',
                'NOMINAL_LENGTH'),
            'library_layout_sdev': (
                './DESIGN/LIBRARY_DESCRIPTOR/LIBRARY_LAYOUT/PAIRED',
                'NOMINAL_SDEV'),
            'pooling_stategy': (
                './DESIGN/LIBRARY_DESCRIPTOR/POOLING_STRATEGY',
                'text'),
            'library_construction_protocol': (
                './DESIGN/LIBRARY_DESCRIPTOR/LIBRARY_CONSTRUCTION_PROTOCOL',
                'text'),
            'platform': ('./PLATFORM', 'child', 'tag'),
            'instrument_model': ('./PLATFORM/*/INSTRUMENT_MODEL', 'text')
        }

        d.update(self._process_path_map(xml, path_map))
        d.update(self._parse_identifiers(xml.find("IDENTIFIERS"),"experiment"))
        d = self.try_update(d, self._parse_attributes(xml.find("EXPERIMENT_ATTRIBUTES")))

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
            'organism': (".//SCIENTIFIC_NAME", "text"),
            'description': (".//DESCRIPTION", "text"),
        }
        d.update(self._process_path_map(xml, path_map))
        d.update(self._parse_identifiers(xml.find("IDENTIFIERS"), "sample"))
        d.update(self._parse_attributes(xml.find("SAMPLE_ATTRIBUTES")))

        for elem in xml.iter():
            if(elem.tag == "TAXON_ID"):
                d['taxon_id'] = int(elem.text)

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
    def __init__(self,from_date = "2004-01-01", count = 2500, offset = 0, entity = "EXPERIMENT"):
        self.from_date = from_date
        self.offset = offset
        self.count = count
        self.counter = 0
        self.entity = entity
        self.done = False
        self._fill_buffer()


    def __iter__(self):
        return self

    def __next__(self):
        if(not self.done):
            retval =  self.buffer[self.counter]
            self.counter += 1
            self.offset += 1
            if(self.counter == len(self.buffer)):
                self.counter = 0
                self._fill_buffer()
            return retval
        else:
            raise(StopIteration)

    def _url(self):
        print("current offset: ", self.offset)
        columns = ",".join(["Accession", "Submission", "Type", "Received", "Published", "LastUpdate", "Status", "Insdc"])
        url = "https://www.ncbi.nlm.nih.gov/Traces/sra/status/srastatrep.fcgi/acc-mirroring?from_date={}&count={}&offset={}&columns={}&format=tsv&type={}"
        url = url.format(self.from_date, self.count, self.offset, columns, self.entity)
        return(url)

    def _fill_buffer(self):
        if(not self.done):
            url = self._url()
            with io.StringIO(urllib.request.urlopen(url).read().decode()) as response:
                reader = csv.DictReader(response, delimiter="\t")
                self.buffer = [row for row in reader]
                if(len(self.buffer) == 0):
                    self.done=True

    
