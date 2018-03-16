"""SRA parsers including:

- study
- sample
- experiment
- run

These parsers each parse XML format files of the format
available from the fullxml api.

"""

import json
import csv
import re
from collections import defaultdict
import urllib
import urllib.request
import xml.etree.ElementTree as etree
import io
import gzip

def lambda_handler(event, context):
    accession = event['accession']
    v = load_experiment_xml_by_accession(accession)
    s = list([SRAExperimentPackage(exptpkg).data for exptpkg in v.getroot().findall(".//EXPERIMENT_PACKAGE")])
    return json.dumps(s)


def parse_study(xml):
    """Parse an SRA xml STUDY element

    Parameters
    ----------
    xml: an xml.etree Element

    Returns
    -------
    A dict object parsed from the XML
    """

    required_keys = ['abstract',
                     'BioProject',
                     'GEO',
                     'accession',
                     'alias',
                     'attributes',
                     'center_name',
                     'broker_name',
                     'description',
                     'external_id',
                     'study_type',
                     'submitter_id',
                     'secondary_id',
                     'study_accession',
                     'title']
    d = dict((k,None) for k in required_keys)
    d.update(xml.attrib)
    path_map = {
        'title': (".//STUDY_TITLE", "text"),
        'abstract': (".//STUDY_ABSTRACT", "text"),
        'description': (".//STUDY_DESCRIPTION", "text"),
    }
    d.update(_process_path_map(xml, path_map))
    d.update(_parse_identifiers(xml.find("IDENTIFIERS"),"study"))
    try_update(d, _parse_study_type(xml.find('DESCRIPTOR/STUDY_TYPE')))
    d = try_update(d, _parse_attributes(xml.find("STUDY_ATTRIBUTES")))
    d.update(_parse_links(
        xml.find("STUDY_LINKS")))
    return(d)


def parse_submission(xml):
    """Parse an SRA xml SUBMISSION element

    Parameters
    ----------
    xml: xml.etree.ElementTree.Element

    Returns
    -------
    a dict of experiment
    """
    d = {}
    d.update(xml.attrib)
    d.update(_parse_identifiers(xml.find("IDENTIFIERS"), "submission"))
    return d


def parse_run(xml):
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
    d.update(xml.attrib)
    for k in ['total_spots', 'total_bases', 'size']:
        try:
            d[k] = int(d[k])
        except:
            # missing some key elements
            pass
    path_map = {
        'experiment_accession': ("EXPERIMENT_REF", "accession")
    }

    d = try_update(d, _parse_taxon(xml.find("tax_analysis")))
    d = try_update(d, _parse_run_reads(xml.find(".//SPOT_DESCRIPTOR")))
    d.update(_process_path_map(xml, path_map))
    d.update(_parse_identifiers(xml.find("IDENTIFIERS"), "run"))
    d = try_update(d, _parse_attributes(xml.find("RUN_ATTRIBUTES")))
    return d


def parse_experiment(xml):
    """Parse an SRA xml EXPERIMENT element

    Parameters
    ----------
    xml: xml.etree.ElementTree.Element

    Returns
    -------
    a dict of experiment
    """
    required_keys = [
        'accession',
        'attributes',
        'alias',
        'center_name',
        'design',
        'description',
        'experiment_accession',
        'identifiers',
        'instrument_model',
        'library_name',
        'library_construction_protocol',
        'library_layout_orientation',
        'library_layout_length',
        'library_layout_sdev',
        'library_strategy',
        'library_source',
        'library_selection',
        'library_layout',
        'xrefs',
        'platform',
        'sample_accession',
        'study_accession',
        'title'
    ]
    
    d = dict((k,None) for k in required_keys)
    try:
        d.update(xml.attrib)
    except:
        import xml.etree.ElementTree as et
        et.tostring(xml)
    
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
        'sample_accession': ('.//SAMPLE_DESCRIPTOR','accession'),
        'instrument_model': ('./PLATFORM/*/INSTRUMENT_MODEL', 'text')
    }

    d.update(_process_path_map(xml, path_map))
    d.update(_parse_identifiers(xml.find("IDENTIFIERS"),
                                     "experiment"))

    d.update(_parse_attributes(
        xml.find("EXPERIMENT_ATTRIBUTES")))
    d.update(_parse_links(
        xml.find("EXPERIMENT_LINKS")))
    return d


def parse_sample(xml):
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
    d.update(_process_path_map(xml, path_map))
    d.update(_parse_identifiers(xml.find("IDENTIFIERS"), "sample"))
    d.update(_parse_attributes(xml.find("SAMPLE_ATTRIBUTES")))
    d.update(_parse_links(
        xml.find("SAMPLE_LINKS")))

    for elem in xml.iter():
        if(elem.tag == "TAXON_ID"):
            d['taxon_id'] = int(elem.text)

    return(d)


def _parse_run_reads(node):
    """Parse reads from runs."""
    d = dict()
    try:
        d['spot_length'] = int(node.find('.//SPOT_LENGTH').text)
    except Exception as e:
        d['spot_length'] = 0
    d['reads'] = []
    if node is None:
        # No read statistics present
        return(d)
    readrecs = node.findall(".//READ_SPEC")
    d['nreads'] = len(readrecs)

    for read in readrecs:
        r = {}
        try:
            r['read_index'] = int(read.find('./READ_INDEX').text)
        except:
            pass
        try:
            r['read_class'] = read.find('./READ_CLASS').text
        except:
            pass
        try:
            r['read_type'] = read.find('./READ_TYPE').text
        except:
            pass
        try:
            r['base_coord'] = int(read.find('./BASE_COORD').text)
        except:
            pass
        d['reads'].append(r)

    return d


def _parse_taxon(node):
    """Parse taxonomy informaiton."""

    def crawl(node, d=defaultdict(list)):
        for i in node:
            rank = i.get('rank', 'Unkown')
            d[rank].append({
                'name': i.get('name').replace('.', '_').replace('$', ''),
                'parent': node.get('name'),
                'total_count': int(i.get('total_count')),
                'self_count': int(i.get('self_count')),
                'tax_id': i.get('tax_id'),
                })
            if i.getchildren():
                d.update(crawl(i, d))
        return d
    try:
        d = {'tax_analysis': {'nspot_analyze': node.get('analyzed_spot_count'),
                              'total_spots': node.get('total_spot_count'),
                              'mapped_spots': node.get('identified_spot_count'),
                              'tax_counts': crawl(node)
        }
        }
    except AttributeError:
        # No tax_analysis node
        return {}

    try:
        if d['tax_analysis']['nspot_analyze'] is not None:
            d['tax_analysis']['nspot_analyze'] = int(d['tax_analysis']['nspot_analyze'])
    except:
        logger.debug('Non integer count: nspot_analyze')
        logger.debug(d['tax_analysis']['nspot_analyze'])
        d['tax_analysis']['nspot_analyze'] = None

    try:
        if d['tax_analysis']['total_spots'] is not None:
            d['tax_analysis']['total_spots'] = int(d['tax_analysis']['total_spots'])
    except:
        logger.debug('Non integer count: total_spots')
        logger.debug(d['tax_analysis']['total_spots'])
        d['tax_analysis']['total_spots'] = None

    try:
        if d['tax_analysis']['mapped_spots'] is not None:
            d['tax_analysis']['mapped_spots'] = int(d['tax_analysis']['mapped_spots'])
    except:
        logger.debug('Non integer count: mapped_spots')
        logger.debug(d['tax_analysis']['mapped_spots'])
        d['tax_analysis']['mapped_spots'] = None

    return d


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


def _parse_attributes(xml):
    if(xml is None):
        return {}
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

def _parse_links(xml):
    """Add attributes to a record

    Parameters
    ----------
    xml: xml.etree.ElementTree.ElementTree.Element
        An xml element of level "EXPERIMENT|STUDY|RUN|SAMPLE_LINKS"
    """
    if(xml is None):
        return {}
    d = defaultdict(list)
    # Iterate over "XXX_ATTRIBUTES"
    for elem in xml.findall(".//XREF_LINK"):
        try:
            tag = elem.find('./DB')
            value = elem.find('./ID')
            d['xrefs'].append({'db': tag.text, 'id': value.text})
        except AttributeError:
            # tag or value missing text, so skip
            pass
    if(len(d)==0):
        d={}
    return(d)


def _get_special_ids(id_rec):
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


def _parse_identifiers(xml, section):
    """Parse IDENTIFIERS section"""

    d = defaultdict(list)

    for _id in xml:
        if(_id.tag == "PRIMARY_ID"):
            d[section + '_accession'] = _id.text
        elif(_id.tag == "SUBMITTER_ID"):
            id_rec = {'namespace': _id.get("namespace"), 'id': _id.text}
            d['identifiers'].append(id_rec)
        elif(_id.tag == "UUID"):
            d['identifiers'].append(_id.text)
        else:  # all other id types (secondary, external)
            id_rec = {'namespace': _id.get("namespace"), 'id': _id.text}
            d['identifiers'].append(id_rec)
            special = _get_special_ids(id_rec)
            if(special):
                d[special[0]] = special[1]
            else:
                d['identifiers'].append(id_rec)
    return(d)


def _process_path_map(xml, path_map):
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


def _parse_study_type(xml):
    d = {}
    if(xml is None):
        return d
    if(xml.get('existing_study_type')):
        d['study_type'] = xml.get('existing_study_type')
    if(xml.get('new_study_type')):
        d['study_type'] = xml.get('new_study_type')
    return(d)


def try_update(d, value):
    try:
        d.update(value)
        return(d)
    except Exception as e:
        return(d)



class SRAXMLRecord(object):
    def __init__(self, xml):
        #if(type(xml) is not xml.etree.ElementTree.Element):
        #    raise(TypeError('xml should be of type xml.etree.ElementTree.Element'))
        self.xml = xml
        self.parse_xml()

    def parse_xml(self):
        d = {}
        d = d.update(self.xml.attrib)
        self.data = d


class SRAExperimentRecord(SRAXMLRecord):
    def __init__(self, xml):
        super().__init__(xml)

    def parse_xml(self):
        """Parse an SRA xml EXPERIMENT element"""
        xml = self.xml
        self.data = parse_experiment(xml)


class SRAStudyRecord(SRAXMLRecord):
    def __init__(self, xml):
        super().__init__(xml)

    def parse_xml(self):
        self.data = parse_study(self.xml)


class SRASampleRecord(SRAXMLRecord):
    def __init__(self, xml):
        super().__init__(xml)

    def parse_xml(self):
        self.data = parse_sample(self.xml)


class SRASubmissionRecord(SRAXMLRecord):
    def __init__(self, xml):
        super().__init__(xml)

    def parse_xml(self):
        self.data = parse_submission(self.xml)


class SRARunRecord(SRAXMLRecord):
    def __init__(self, xml):
        super().__init__(xml)

    def parse_xml(self):
        self.data = parse_run(self.xml)


class SRAExperimentPackage(object):
    def __init__(self, node):
        """Parse SRA Experiment to dict

        Parameters
        ----------
        node: xml.etree.ElementTree.Element
            Element

        """
        study = {}
        experiment = {}
        runs = []
        sample = {}


        # Currently, just skip all incomplete or
        # broken records by returning None in data slot
        try:
            study = {}
            experiment = SRAExperimentRecord(node.find(".//EXPERIMENT")).data
            try:
                study = SRAStudyRecord(node.find(".//STUDY")).data
            except:
                study = {'accession': experiment.data['study_accession']}
            sample = SRASampleRecord(node.find(".//SAMPLE")).data
            runs = list([SRARunRecord(run).data for run in node.findall(".//RUN")])
            self.data = {
                'experiment': experiment,
                'runs': runs,
                'sample': sample,
                'study': study
            }
        except:
            self.data = None
        

    def sample(self):
        return self.data['sample']

    def experiment(self):
        self.data['experiment']
        
    def expanded_experiment(self):
        expt = self.data['experiment']
        expt['runs'] = self.runs()
        expt['sample'] = self.sample()
        expt['study'] = self.study()
        return expt

    def runs(self):
        return self.data['runs']

    def nested_runs(self):
        retval = []
        for run in self.runs():
            run['experiment'] = self.experiment()
            run['study'] = self.study()
            run['sample'] = self.sample()
            retval.append(run)
        return retval

    def study(self):
        return self.data['study']


def open_file(fname, encoding = 'UTF-8'):
    if(fname.endswith('.gz')):
        return(gzip.open(fname, mode = "rt", encoding = encoding))
    return(open(fname, "r", encoding = encoding))

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
def get_accession_list(from_date="2004-01-01",
                       count = 50, offset = 0, type="EXPERIMENT"):
    column_names = ["Accession", "Submission", "Type",
                    "Received", "Published", "LastUpdate", "Status", "Insdc"]
    url = "https://www.ncbi.nlm.nih.gov/Traces/sra/status/" + \
          "srastatrep.fcgi/acc-mirroring?" + \
          "from_date={}&count={}&type={}&offset={}"
    url = url.format(from_date, count, type, offset)
    print(url)
    res = None
    with urllib.request.urlopen(url) as response:
        res = json.loads(response.read().decode('UTF-8'))
        c = 0
    print(res['fetched_count'], count, int(res['fetched_count']) == count)
    while(int(res['fetched_count']) == count):
        offset += 1
        c += 1
        print(c, offset)
        if(c != int(res['fetched_count'])):
            retval = dict(zip(res['column_names'], res['rows'][c-1]))
            yield(retval)
        else:
            print(c, offset)
            url = "https://www.ncbi.nlm.nih.gov/Traces/sra/" +\
                "status/srastatrep.fcgi/acc-mirroring" + \
                "?from_date={}&count={}&type={}&offset={}"
            url = url.format(from_date, count, type, offset)
            with urllib.request.urlopen(url) as response:
                res = json.loads(response.read().decode('UTF-8'))
            print(res['fetched_count'])
            c = 0
    raise(StopIteration)




class LiveList(object):
    def __init__(self, from_date="2004-01-01",
                 to_date=None, count=2500,
                 offset=0, entity="EXPERIMENT",
                 status='live'):
        self.from_date = from_date
        self.offset = offset
        self.count = count
        self.counter = 0
        self.status = status
        self.entity = entity
        self.done = False
        self.to_date = to_date
        self._fill_buffer()

    def __iter__(self):
        return self

    def __next__(self):
        if(not self.done):
            retval = self.buffer[self.counter]
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
        columns = ",".join(["Accession", "Submission", "Type",
                            "Received", "Published", "LastUpdate",
                            "Status", "Insdc"])
        url = "https://www.ncbi.nlm.nih.gov/Traces/" \
              "sra/status/srastatrep.fcgi/acc-mirroring?" \
              "from_date={}&count={}&offset={}&columns={}" \
              "&format=tsv&type={}&status={}"
        url = url.format(self.from_date, self.count,
                         self.offset, columns,
                         self.entity, self.status)
        if(self.to_date is not None):
            url += "&to_date={}".format(self.to_date)
        return(url)

    def _fill_buffer(self):
        if(not self.done):
            url = self._url()
            with io.StringIO(urllib.request.urlopen(url)
                             .read().decode()) as response:
                reader = csv.DictReader(response, delimiter="\t")
                self.buffer = [row for row in reader]
                if(len(self.buffer) == 0):
                    self.done = True

