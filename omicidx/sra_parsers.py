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
from omicidx.utils import open_file

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

def _add_attributes(d, elem):
    tag = elem.find('./TAG')
    value = elem.find('./VALUE')
    if(value is not None):
        if('attributes' not in d):
            d['attributes'] = []
        d['attributes'].append({ "tag": tag.text,
                                 "value": value.text})
    else:
        if('tags' not in d):
            d['tags'] = []
        d['tags'].append(tag.text)

        


def study_parser(fname):
    """Parse an SRA study XML file

    Parameters
    ----------
    fname : str
        The filename (optionally gzipped) for parsing
    
    Returns
    -------
    generator of dict objects
        A generator that returns a simple dict for each
        record.
    """
    if(fname.endswith('.gz')):
        f = gzip.open(fname,'r')
        xml_iter = ET.iterparse(f, ['start', 'end'])
    else:
        f = open(fname,'r')
        xml_iter = ET.iterparse(f, ['start', 'end'])
    d = {}
    for event, elem in xml_iter:
        if(elem.tag =='STUDY' and event == 'end'):
            try:
                d['alias']=elem.attrib['alias']
            except:
                pass
            elem.clear()
            yield(d)
        if(elem.tag == 'STUDY' and event == 'start'):
            d = {}
        if(elem.tag == 'PRIMARY_ID' and event == 'end'):
            d['accession'] = elem.text
        if(elem.tag == 'EXTERNAL_ID' and event == 'end'):
            d['external_id'] = {}
            d['external_id']['id'] = elem.text
            d['external_id']['namespace'] = elem.attrib['namespace']
            if(d['external_id']['namespace'] == 'BioProject'):
                d['BioProject'] = elem.text
            if(d['external_id']['namespace'] == 'dbGaP'):
                d['dbGaP'] = elem.text
        if(elem.tag == 'SUBMITTER_ID' and event == 'end'):
            d['submitter_id'] = {}
            d['submitter_id']['id'] = elem.text
            d['submitter_id']['namespace'] = elem.attrib['namespace']
        if(elem.tag == 'STUDY_TITLE' and event == 'end'):
            d['title'] = elem.text
        if(elem.tag == 'STUDY_ABSTRACT' and event == 'end'):
            d['abstract'] = elem.text
        if(elem.tag == 'STUDY_DESCRIPTION' and event == 'end'):
            d['description'] = elem.text
        if(elem.tag == 'STUDY_ATTRIBUTE' and event == 'end'):
            _add_attributes(d, elem)

def run_parser(fname):
    """Parse an SRA run XML file

    Parameters
    ----------
    fname : str
        The filename (optionally gzipped) for parsing
    
    Returns
    -------
    generator of dict objects
        A generator that returns a simple dict for each
        record.
    """
    if(fname.endswith('.gz')):
        f = gzip.open(fname,'r')
        xml_iter = ET.iterparse(f, ['start', 'end'])
    else:
        f = open(fname,'r')
        xml_iter = ET.iterparse(f, ['start', 'end'])
    d = {}
    for event, elem in xml_iter:
        if(elem.tag =='RUN' and event == 'end'):
            try:
                d['alias']=elem.attrib['alias']
            except:
                pass
            d['accession'] = elem.attrib['accession']
            #d['run_date'] = datetime.datetime.strptime("2010-02-27T00:00:00Z","%Y-%m-%dT%H:%M:%SZ")
            try:
                d['run_date']=elem.attrib['run_date']
            except:
                pass
            elem.clear()
            yield(d)
        if(elem.tag == 'PRIMARY_ID' and event == 'end'):
            d['accession'] = elem.text
        if(elem.tag == 'EXPERIMENT_REF' and event == 'end'):
            d['experiment_accession'] = elem.attrib['accession']
        if(elem.tag == 'TITLE' and event == 'end'):
            d['title'] = elem.text
        if(elem.tag =='RUN' and event == 'end'):
            d = {}
        if(elem.tag == 'RUN_ATTRIBUTE' and event == 'end'):
            _add_attributes(d, elem)

def experiment_parser(fname):
    """Parse an SRA experiment XML file

    Parameters
    ----------
    fname : str
        The filename (optionally gzipped) for parsing
    
    Returns
    -------
    generator of dict objects
        A generator that returns a simple dict for each
        record.
    """
    if(fname.endswith('.gz')):
        f = gzip.open(fname,'r')
        xml_iter = ET.iterparse(f, ['start', 'end'])
    else:
        f = open(fname,'r')
        xml_iter = ET.iterparse(f, ['start', 'end'])
    d = {}
    library_tags = [
        "LIBRARY_STRATEGY",
        "LIBRARY_SOURCE",
        "LIBRARY_SELECTION",
        "LIBRARY_CONSTRUCTION_PROTOCOL",
        "LIBRARY_NAME"
        ]
    for event, elem in xml_iter:
        if(elem.tag =='EXPERIMENT' and event == 'end'):
            try:
                d['alias']=elem.attrib['alias']
            except:
                pass
            d['accession'] = elem.attrib['accession']
            try:
                d['center_name']=elem.attrib['center_name']
            except:
                pass
            elem.clear()
            yield(d)
        if(elem.tag == 'STUDY_REF' and event == 'end'):
            try:
                d['study_accession'] = elem.attrib['accession']
            except:
                pass
        if(elem.tag == 'SUBMITTER_ID' and event == 'end'):
            db = elem.attrib['namespace'].lower()
            if(db == 'geo'):
                d[db + '_accession'] = elem.text
        if(elem.tag == 'TITLE' and event == 'end'):
            d['title'] = elem.text
        if(elem.tag =='EXPERIMENT' and event == 'start'):
            d = {}
        if(elem.tag =='READ_SPEC' and event == 'end'):
            if('read_spec' not in d):
                d['read_spec']=[]
            read_spec = {}
            read_spec['read_index'] = int(elem.find('.//READ_INDEX').text)
            read_spec['read_class'] = elem.find('.//READ_CLASS').text
            read_spec['read_type'] = elem.find('.//READ_TYPE').text
            read_spec['base_coord'] = int(elem.find('.//BASE_COORD').text)
            d['read_spec'].append(read_spec)
        if(elem.tag == "DESIGN_DESCRIPTION" and event == 'end'):
            _safe_add_text_element(d, 'design_description', elem)
        if(elem.tag == "SAMPLE_DESCRIPTOR" and event == 'end'):
            try:
                d['sample_accession'] = elem.attrib['accession']
            except:
                pass
        if(elem.tag in library_tags and event == 'end'):
            _safe_add_text_element(d, elem.tag.lower(), elem)
        if(elem.tag == "PAIRED" and event == 'end'):
            d['paired'] = True
        if(elem.tag == "SINGLE" and event == 'end'):
            d['paired'] = False
        if(elem.tag == "SPOT_LENGTH" and event == 'end'):
            d['spot_length'] = int(elem.text)
        if(elem.tag == "PLATFORM" and event == 'end'):
            d['platform'] = elem.find('.//INSTRUMENT_MODEL/..').tag
            d['instrument_model'] = elem.find('.//INSTRUMENT_MODEL').text
        if(elem.tag == 'EXPERIMENT_ATTRIBUTE' and event == 'end'):
            _add_attributes(d, elem)

def sample_parser(fname):
    """Parse an SRA sample XML file
        
    Parameters
    ----------
    fname : str
        The filename (optionally gzipped) for parsing
    
    Returns
    -------
    generator of dict objects
        A generator that returns a simple dict for each
        record.
    """

    if(fname.endswith('.gz')):
        f = gzip.open(fname,'r')
        xml_iter = ET.iterparse(f, ['start', 'end'])
    else:
        f = open(fname,'r')
        xml_iter = ET.iterparse(f, ['start', 'end'])
    d = {}
    for event, elem in xml_iter:
        if(elem.tag =='SAMPLE' and event == 'start'):
            d = {}
        if(elem.tag =='SAMPLE' and event == 'end'):
            try:
                d['alias']=elem.attrib['alias']
            except:
                pass
            try:
                d['center_name']=elem.attrib['center_name']
            except:
                pass
            d['accession'] = elem.attrib['accession']
            elem.clear()
            yield(d)
        if(elem.tag == "TAXON_ID" and  event == 'end'):
            d['taxon_id'] = int(elem.text)
        if(elem.tag == "SCIENTIFIC_NAME" and  event == 'end'):
            d['organism'] = elem.text
        if(elem.tag == "EXTERNAL_ID" and  event == 'end'):
            try:
                if(elem.attrib['namespace']=='BioSample'):
                    d['biosample_accession']=elem.text
                if(elem.attrib['namespace']=='GEO'):
                    d['geo_accession']=elem.text
                if(elem.attrib['namespace']=='ArrayExpress'):
                    d['arrayexpress_accession']=elem.text
            except:
                pass
        if(elem.tag == 'TITLE' and event == 'end'):
            d['title'] = elem.text
        if(elem.tag == 'SAMPLE_ATTRIBUTE' and event == 'end'):
            _add_attributes(d, elem)
        
        if(elem.tag == 'DESCRIPTION' and event == 'end'):
            d['description'] = elem.text
        if(elem.tag == 'SUBMITTER_ID' and event == 'end'):
            ns = elem.attrib['namespace']
            value = elem.text
            if('submitter_ids' not in d):
                d['submitter_ids'] = []
            d['submitter_ids'].append({ns :
                                      value})


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


def dump_data(root_dir,out_dir):
    with open(os.path.join(out_dir,'sra_study.json'), 'w') as f:
        for d in study_parser(os.path.join(root_dir, 'meta_study_set.xml.gz')):
            json.dump(d, f)
            f.write('\n')

    with open(os.path.join(out_dir,'sra_run.json'), 'w') as f:
        for d in run_parser(os.path.join(root_dir, 'meta_run_set.xml.gz')):
            json.dump(d, f)
            f.write('\n')

    with open(os.path.join(out_dir,'sra_sample.json'), 'w') as f:
        for d in sample_parser(os.path.join(root_dir, 'meta_sample_set.xml.gz')):
            json.dump(d, f)
            f.write('\n')

    with open(os.path.join(out_dir,'sra_experiment.json'), 'w') as f:
        for d in experiment_parser(os.path.join(root_dir, 'meta_experiment_set.xml.gz')):
            json.dump(d, f)
            f.write('\n')
            
