"""Biosample parser

Implemented as an iterator


>>> import omicidx.biosample as b
>>> for bios in b.BioSampleParser('biosample_set.xml.gz'):
>>>     print(bios.as_json())
"""

import os.path
import xml.etree.ElementTree as ET
import gzip
import json
import logging
import click
import subprocess

class BioSample(dict):
    """BioSample class"""
    def __init__(self):
        self.update()
    
    def as_json(self,indent=None):
        return(json.dumps(self))

    def __str__(self):
        return('{0}'.format(self['title']))

    def __repr__(self):
        return('BioSample <id={0} title={1}>'.format(self['id'],self['title']))
    
class BioSampleParser(object):
    """Parse a BioSample xml file.

    Implemented as an iterator"""

    
    def __init__(self,fname):
        """Init

        Handles .gz as well as plain text"""
        self.fhandle = None
        if(fname.endswith('gz')):
            self.fhandle = gzip.open(fname)
        else:
            self.fhandle = open(fname)
        self.context = ET.iterparse(self.fhandle, events=("start", "end"))
        event, self.root = next(self.context)

        
    def __iter__(self):
        return(self)

    
    def __next__(self):
        for event, elem in self.context:
            if event == "end" and elem.tag == "BioSample":
                bios = BioSample()
                bios['is_reference']=None
                for k,v in elem.items():
                    bios[k] = v
                bios['id_recs'] = []
                bios['ids'] = []
                bios['sra_sample']=None
                bios['dbgap']=None
                bios['gsm']=None
                for id in elem.iterfind('.//Id'):
                    idrec = {'db': id.get('db'), 'label': id.get('db_label'), 'id':id.text}
                    bios['ids'].append(idrec['id'])
                    bios['id_recs'].append(idrec)
                    # add xref fields for SRA, dbGaP, and GEO
                    
                    if(id.get('db')=='SRA'):
                        bios['sra_sample']=id.text
                    if(id.get('db')=='dbGaP'):
                        bios['dbgap']=id.text
                    if(id.get('db')=='GEO'):
                        bios['gsm']=id.text
                bios['title'] = elem.findtext('.//Description/Title')
                bios['description'] = elem.findtext('.//Description/Comment/Paragraph')
                organism = elem.find('.//Organism')
                bios['taxonomy_name'] = organism.get('taxonomy_name')
                bios['taxon_id'] = int(organism.get('taxonomy_id'))
                bios['attribute_recs'] = []
                bios['attributes'] = []
                for attribute in elem.findall('.//Attribute'):
                    attrec = attribute.attrib
                    attrec['value'] = attribute.text
                    bios['attribute_recs'].append(attrec)
                    try:
                        bios['attributes'].append(attrec['harmonized_name'])
                    except:
                        bios['attributes'].append(attrec['attribute_name'])
                        
                bios['model'] = elem.findtext('.//Model')
                #print(json.dumps(bios))
                #res = es.index(index="bioes", doc_type='biosample', id=bios['id'], body=bios)
                elem.clear()
                return bios
