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
            tag = elem.find('./TAG')
            value = elem.find('./VALUE')
            if('tags' not in d):
                d['tags'] = []
            d['tags'].append(tag.text)
            if(value is not None):
                if('attributes' not in d):
                    d['attributes'] = []
                    d['values'] = []                
                d['tags'].append(tag.text)
                d['values'].append(value.text)
                d['attributes'].append({ "tag": tag.text,
                                         "value": value.text})


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
            d['study_accession'] = elem.attrib['accession']
        if(elem.tag == 'TITLE' and event == 'end'):
            d['title'] = elem.text
        if(elem.tag =='EXPERIMENT' and event == 'start'):
            d = {}
        if(elem.tag == "DESIGN_DESCRIPTION" and event == 'end'):
            d['design_description'] = elem.text.strip()
        if(elem.tag == "SAMPLE_DESCRIPTOR" and event == 'end'):
            d['sample_accession'] = elem.attrib['accession']
        if(elem.tag == "LIBRARY_NAME" and event == 'end'):
            d['library_name'] = elem.text
        if(elem.tag == "LIBRARY_STRATEGY" and event == 'end'):
            d['library_name'] = elem.text
        if(elem.tag == "LIBRARY_SOURCE" and event == 'end'):
            d['library_name'] = elem.text
        if(elem.tag == "LIBRARY_SELECTION" and event == 'end'):
            d['library_name'] = elem.text
        if(elem.tag == "PAIRED" and event == 'end'):
            d['paired'] = True
        if(elem.tag == "SINGLE" and event == 'end'):
            d['paired'] = False
        if(elem.tag == "SPOT_LENGTH" and event == 'end'):
            d['spot_length'] = int(elem.text)
        if(elem.tag == "PLATFORM" and event == 'end'):
            d['platform'] = elem.getchildren()[0].tag
            d['instrument_model'] = elem.find('.//INSTRUMENT_MODEL').text

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
            tag = elem.find('./TAG')
            value = elem.find('./VALUE')
            if('tag' not in d):
                d['tags'] = []
            d['tags'].append(tag.text)
            if(value is not None):
                if('attributes' not in d):
                    d['attributes'] = []
                    d['values'] = []
                d['tags'].append(tag.text)
                d['values'].append(value.text)
                d['attributes'].append({ "tag": tag.text,
                                         "value": value.text})
        
        if(elem.tag == 'DESCRIPTION' and event == 'end'):
            d['description'] = elem.text
        if(elem.tag == 'SUBMITTER_ID' and event == 'end'):
            ns = elem.attrib['namespace']
            value = elem.text
            if('submitter_ids' not in d):
                d['submitter_ids'] = []
            d['submitter_ids'].append({ns :
                                      value})


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
            
