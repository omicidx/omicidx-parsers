"""SRA parsers including:

- study
- sample
- experiment
- run

These parsers each parse XML format files of the format
available from the fullxml api.

The main entry point into this module is the `parse_xml_file`
function.

"""

import ujson
import csv
import re
import requests
from collections import defaultdict
import urllib
import urllib.error
import urllib.request
import xml.etree.ElementTree as etree
import io
import gzip
from . import pydantic_models
import logging
from typing import Iterator, List, Dict, Iterable

logger = logging.getLogger('sra_parser')

def parse_xml_file(xmlfilename):
    """Parse an NCBI SRA mirroring XML file

    This function returns an iterator over the 
    records in the xml file, returning a dict
    of parsed records.

    For example:

    wget --mirror -nH --cut-dirs=3 ftp://ftp.ncbi.nlm.nih.gov/sra/reports/Mirroring/NCBI_SRA_Mirroring_20181027/

    >>> import omicidx.sra_parsers as sp
    >>> studies = sp.parse_xml_file("NCBI_SRA_Mirroring_20181027/meta_study_set.xml.gz")
    >>> next(studies)
    ...


    Parameters
    ----------
    xmlfilename : string
        the filename to be parsed. Can be gzipped. Must include
        the "entity" name in the filename (eg., "run", "experiment")

    Returns
    -------
    iterator: 
        An iterator of dict records from parsing each xml record.

    """
    if('study' in xmlfilename):
        entity = "STUDY"
        sra_parser = SRAStudyRecord
    if('run' in xmlfilename):
        entity = "RUN"
        sra_parser = SRARunRecord
    if('sample' in xmlfilename):
        entity = "SAMPLE"
        sra_parser = SRASampleRecord
    if('experiment' in xmlfilename):
        entity = "EXPERIMENT"
        sra_parser = SRAExperimentRecord
    n=0
    with open_file(xmlfilename) as f:
        for event, element in etree.iterparse(f):
            if(event == 'end' and element.tag == entity):
                rec = sra_parser(element).data
                n+=1
                if((n % 100000)==0):
                    logger.info('parsed {} {} entries'.format(entity, n))
                element.clear()
                yield(rec)
    logger.info('parsed {} entity entries'.format(n))


def lambda_handler(event, context):
    accession = event['accession']
    v = load_experiment_xml_by_accession(accession)
    s = list([SRAExperimentPackage(exptpkg).data for exptpkg in v.getroot().findall(".//EXPERIMENT_PACKAGE")])
    return s


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
                     'study_type',
                     'study_accession',
                     'title']
    d = dict((k,None) for k in required_keys)
    try:
        d.update(xml.attrib)
    except AttributeError:
        pass
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
    pubmeds = []
    if('xrefs' in d):
        for xref in d['xrefs']:
            if(xref['db'] == 'pubmed'):
                if(xref['id'] is not None):
                    pubmeds.append(int(xref['id']))
    d.update({'pubmed_ids':pubmeds})
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


def dict_from_single_xml(txt):
    xml = etree.fromstring(txt)
    entity = xml.tag.lower()
    vals = globals()['parse_'+entity](xml)
    vals['entity_type'] = xml.tag.lower()
    return vals

def model_from_single_xml(txt):
    xml = etree.fromstring(txt)
    entity = xml.tag.lower()
    et = globals()['parse_'+entity](xml)
    return getattr(pydantic_models, 'Sra'+entity.capitalize())(**et)

def parse_run(xml):
    """Parse an SRA xml RUN element

    Parameters
    ----------
    xml: an xml.etree Element

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
    try:
        d['avg_length'] = float(d['total_bases'])/d['total_spots']
    except:
        pass
    path_map = {
        'experiment_accession': ("EXPERIMENT_REF", "accession"),
        'title': ("TITLE", 'text'),
    }

    d = try_update(d, _parse_taxon(xml.find("tax_analysis")))
    # d = try_update(d, _parse_run_reads(xml.find(".//SPOT_DESCRIPTOR")))
    d.update(_process_path_map(xml, path_map))
    d.update(_parse_identifiers(xml.find("IDENTIFIERS"), "run"))
    d = try_update(d, _parse_attributes(xml.find("RUN_ATTRIBUTES")))
    d = try_update(d, _parse_attributes(xml.find("RUN_ATTRIBUTES")))
    d = try_update(d, _parse_run_files(xml.find("SRAFiles")))
    d = try_update(d, _parse_run_stats(xml.find("Statistics")))
    d = try_update(d, _parse_run_bases(xml.find("Bases")))
    d = try_update(d, _parse_run_qualities(xml))
    try:
        # already have accession, so no need for this
        del(d['run_accession'])
    except:
        pass
    return d


def _parse_run_stats(xml):
    if(xml is None):
        return None
    stats = []
    for read in xml.findall('Read'):
        ret = {}
        ret['index'] = int(read.get('index',0))
        ret['count'] = int(read.get('count',0))
        ret['mean_length'] = float(read.get('average',0.0))
        ret['sd_length'] = float(read.get('stdev',0.0))
        stats.append(ret)
    return {"reads": stats}

def _parse_run_bases(xml):
    if(xml is None):
        return None
    ret = []
    for base in xml.findall('Base'):
        ret.append({base.get('value'): int(base.get('count'))})
    return {'base_counts':ret}



def _parse_run_files(xml):
    if(xml is None):
        return None
    files = xml.findall('./SRAFile')
    ret = []
    for f in files:
        retfile = {}
        for k in f.keys():
            retfile[k] = f.get(k)
        retfile['alternatives'] = []
        for alt in f.findall('Alternatives'):
            altfile = {}
            for k in alt.keys():
                altfile[k] = alt.get(k)
            retfile['alternatives'].append(altfile)
        ret.append(retfile)
    return({'files':ret})


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
    d = try_update(d, _parse_run_reads(xml.find(".//SPOT_DESCRIPTOR")))
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


def _parse_run_qualities(node):
    """Parse the quality stats, if available, from RUN"""
    d = dict()
    d['qualities'] = []
    qualrecs = node.findall(".//Quality")
    for qual in qualrecs:
        try:
            d['qualities'].append({"quality": int(qual.get('value')),
                                   "count": int(qual.get('count'))})
        except:
            pass

    return d


def _parse_taxon(node):
    """Parse taxonomy informaiton."""

    def crawl(node, d=[]):
        for i in node:
            rank = i.get('rank', 'Unkown')
            parent = None
            if(node.get('tax_id') is not None):
                parent = int(node.get('tax_id'))
            d.append({
                'rank': rank,
                'name': i.get('name').replace('.', '_').replace('$', ''),
                'parent': parent,
                'total_count': int(i.get('total_count')),
                'self_count': int(i.get('self_count')),
                'tax_id': int(i.get('tax_id')),
                })

            if(len(list(i))>0):
                d = d + crawl(i)
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
            d['identifiers'].append({'uuid':_id.text})
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
                child = list(xml.find(v[0]))

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
    """Open a file, generalized to deal with gzip files

    Parameters
    ----------
    fname: string
        The filename. If ends in '.gz', is opened with 
        gzip. 

    Returns
    -------
    an open file handle
    """
    if(fname.endswith('.gz')):
        return(gzip.open(fname, mode = "rt", encoding = encoding))
    return(open(fname, "r", encoding = encoding))



# this csv parser
# 1. uses DictReader
# 2. converts NCBI's standard of '' to None
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


def srastatrep(accessions):
    """Access the SRA statrep api

    Parameters
    ----------
    accessions: List[str]
        A list (or can be single string) of accessions

    Return
    ------
    A named tuple with statrep column names as field_names
    """
    if(not isinstance(accessions, list)):
        accessions = [accessions]
    with request.get('https://www.ncbi.nlm.nih.gov/Traces/sra/status/srastatrep.fcgi/acc-mirroring?acc={}'.format(",".join(accessions))) as response:
        vals = response.json
        cnames = vals['column_names']
        StatRep = namedtuple("StatRep", field_names = list([cname.lower() for cname in cnames]))
        ret = {}
        for row in vals['rows']:
            tmp = StatRep(*row)
            ret[tmp.accession]=tmp
        return ret


    
def load_experiment_xml_by_accession(accession):
    with urllib.request.urlopen('https://trace.ncbi.nlm.nih.gov/Traces/sra/sra.cgi?save=efetch&db=sra&rettype=FullXml&term={}'.format(accession)) as response:
        xml = etree.parse(response)
        return xml



def load_runbrowser_xml_by_accession(accession):
    from ..utils import requests_retry_session as session
    with session().get("https://trace.ncbi.nlm.nih.gov/Traces/sra/?run={}&retmode=xml".format(accession)) as response:
        xml = etree.fromstring(response.content)
        return xml

    

def run_from_runbrowser(accession):
    """Just the run part of runbrowser output"""
    
    runbrowser_xml = load_runbrowser_xml_by_accession(accession)
    return parse_run(runbrowser_xml.find('.//RUN'))


class SRAAccessionUnavailableException(Exception):
    """When an SRA API call suggests that an accession is not available"""
    def __init__(self, errormsg):
        Exception.__init__(self, errormsg)


def results_from_runbrowser(accession):
    """Return complete record from runbrowser

    Parameters
    ----------
    accession: string
        Any SRA accession, but the only one-to-one results are
        for SRR records. 

    Returns
    -------
    a dict with experiment, run, study, and sample records
    """
    
    n = 0
    runbrowser_xml = load_runbrowser_xml_by_accession(accession)
    print(runbrowser_xml)
    root = runbrowser_xml
    error = root.find('./ERROR')
    # This finds errors like "XXXXX has been removed"
    if(error is not None):
        raise SRAAccessionUnavailableException(error.text)
    res = {}
    try:
        res['run'] = parse_run(root.find('.//RUN'))
    except:
        return('ERROR: Nothing found for accession {}'.format(accession))
    res['experiment'] = parse_experiment(root.find('.//EXPERIMENT'))
    sample_root = root.find('./SAMPLE')
    if(sample_root is not None):
        res['sample'] = parse_sample(sample_root)
    else:
        res['sample'] = {'accession': res['experiment']['sample_accession']}
    study_root = root.find('./STUDY')
    if(study_root is not None):
        res['study'] = parse_study(study_root)
    else:
        res['study'] = {'accession': res['experiment']['study_accession']}
    res['xml'] = runbrowser_xml
    return res

def models_from_runbrowser(accession):
    try: 
        res = results_from_runbrowser(accession)
        if(not isinstance(res, dict)):
            return "error"
    except SRAAccessionUnavailableException:
        return None
    mapper = {'experiment': pydantic_models.SraExperiment,
              'run': pydantic_models.FullSraRun,
              'sample': pydantic_models.SraSample,
              'study': pydantic_models.SraStudy}
    ret = {}
    for k in res.keys():
        if(k in mapper):
            ret[k] = mapper[k](**res[k])
    ret['run'].experiment = ret['experiment']
    ret['run'].sample = ret['sample']
    ret['run'].study = ret['study']
    
    return ret['run']
              
              
def run_iterator(from_date="2001-01-01",to_date="2050-01-01",
                 count = 100, offset = 0):
    for res in get_accession_list(from_date, to_date, type="RUN"):
        yield(models_from_runbrowser(res['Accession']))


def get_accession_list(from_date="2001-01-01",to_date="2050-01-01",
                       count = 100, offset = 0, type="EXPERIMENT"):
    column_names = ["Accession", "Submission", "Type",
                    "Received", "Published", "LastUpdate", "Status", "Insdc"]
    column_names = ["Accession"]
    url = "https://www.ncbi.nlm.nih.gov/Traces/sra/status/" + \
          "srastatrep.fcgi/acc-mirroring?" + \
          "from_date={}&to_date={}&count={}&type={}&offset={}"
    url = url.format(from_date, to_date, count, type, offset)
    logger.error(url)
    res = None
    with urllib.request.urlopen(url) as response:
        res = ujson.loads(response.read().decode('UTF-8'))
        c = 0
    while True :
        offset += 1
        c += 1
        if(c != int(res['fetched_count'])):
            retval = dict(zip(res['column_names'], res['rows'][c-1]))
            yield(retval)
        else:
            if(c != count):
                break
            print(c, offset)
            url = "https://www.ncbi.nlm.nih.gov/Traces/sra/" +\
                "status/srastatrep.fcgi/acc-mirroring" + \
                "?from_date={}&to_date={}&count={}&type={}&offset={}"
            url = url.format(from_date, to_date, count, type, offset)
            try:
                with urllib.request.urlopen(url) as response:
                    res = ujson.loads(response.read().decode('UTF-8'))
            except urllib.error.HTTPError:
                continue
            logger.info("fetched: " + str(res['fetched_count']))
            logger.info("total: " + str(offset))
            c = 0
            
            
def sra_object_generator(fname):
    """Iterate over objects in an SRA meta_XXX_set xml file
    
    :param: fname str() name of xml file, may be gzipped or not
    
    :returns: An iterator over SRA objects. Access actual data 
        as a dict using Object.data
    
    """
    validClasses = ['experiment', 'run', 'study', 'sample']
    if(fname.endswith("gz")):
        fh = gzip.GzipFile(fname)
    else:
        fh = open(fname)
    for event, element in et.iterparse(fh):
        if((element.tag.lower() in validClasses) and (event == "end")):
            yield getattr(s, "SRA" + element.tag.title() + "Record")(element)


class LiveList(Iterable):
    """Return an iterator of pydantic_models"""
    
    def __init__(self, from_date="2004-01-01",
                 to_date=None, count=2500,
                 offset=0, entity="EXPERIMENT", status="live",
                 max_retries = 5
                 ):
        self.from_date = from_date
        self.offset = offset
        self.count = count
        self.retries = 0
        self.max_retries = max_retries
        self.counter = 0
        self.entity = entity
        self.status = status
        self.done = False
        self.to_date = to_date
        #self.pool = multiprocessing.Pool(8)
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

    def _url(self) -> str:
        logger.info("current offset: " + str(self.offset))
        columns = ",".join(["Accession", "Submission", "Type",
                            "Received", "Published", "LastUpdate",
                            "Status", "Insdc", "ReplacedBy", "Meta"])
        url = "https://www.ncbi.nlm.nih.gov/Traces/" \
              "sra/status/srastatrep.fcgi/acc-mirroring?" \
              "from_date={}&count={}&offset={}&columns={}" \
              "&format=json&type={}&status={}"
        url = url.format(self.from_date, self.count,
                         self.offset, columns,
                         self.entity, self.status)
        if(self.to_date is not None):
            url += "&to_date={}".format(self.to_date)
        logger.info('url:' + url)
        return(url)

    @staticmethod
    def _process_row(row):
            if("Meta" in row and row['Meta']!=''):
                d = dict_from_single_xml(row['Meta'])
                model_type = d['entity_type']
                d['published'] = row['Published']
                d['lastupdate'] = row['LastUpdate']
                d['received'] = row['Received']
                d['status'] = row['Status']
                d['insdc'] = False
                if(row['Insdc']=='True'):
                    d['insdc'] = True
                pydantic_model = getattr(pydantic_models, 'Sra'+model_type.capitalize())(**d)
                return pydantic_model
            else:
                return None

    def _prep_records(self,reader):
        ret = []
        for row in reader:
            pydantic_model = LiveList._process_row(row)
        #for pydantic_model in self.pool.imap_unordered(LiveList._process_row, reader):
            if(pydantic_model is not None):
                ret.append(pydantic_model)
            else:
                logger.debug(row)
                
        return ret

    def _fill_buffer(self) -> None:
        if(not self.done):
            url = self._url()
            import json
            try:
                response = requests.get(url)
                if(response.status_code == 200):
                    #reader = csv.DictReader(response, delimiter="\t")
                    vals = response.json()
                    reader = []
                    if('column_names' not in vals):
                        raise urllib.error.HTTPError
                    colnames = vals['column_names']
                    for row in vals['rows']:
                        reader.append(dict(zip(colnames,[val for val in row])))
                    self.buffer = self._prep_records(reader)
                    if(len(self.buffer) == 0):
                        self.done = True
                    self.retries = 0
                else:
                    raise Exception
            except:
                logger.exception(f'url {url} :: retries {self.retries}')
                import time
                time.sleep(2^self.retries)
                self.retries += 1
                if(self.retries > self.max_retries):
                    raise Exception
                self._fill_buffer()
                

