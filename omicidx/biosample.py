import os.path
import xml.etree.ElementTree as ET
import gzip
import json
import logging

class BioSample(dict):
    def __init__(self):
        self.update()
    
    def as_json(self,indent=None):
        return(json.dumps(self))

    def __str__(self):
        return('{0}'.format(self['title']))

    def __repr__(self):
        return('BioSample <id={0} title={1}>'.format(self['id'],self['title']))
    
class BioSampleParser(object):
    def __init__(self,fname):
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
                for k,v in elem.items():
                    bios[k] = v
                bios['ids'] = []
                for id in elem.iterfind('.//Id'):
                    idrec = {'db': id.get('db'), 'label': id.get('db_label'), 'id':id.text}
                bios['ids'].append(idrec)
                bios['title'] = elem.findtext('.//Description/Title')
                bios['description'] = elem.findtext('.//Description/Comment/Paragraph')
                organism = elem.find('.//Organism')
                bios['taxonomy_name'] = organism.get('taxonomy_name')
                bios['taxonomy_id'] = organism.get('taxonomy_id')
                bios['attributes'] = []
                for attribute in elem.findall('.//Attribute'):
                    attrec = attribute.attrib
                    attrec['value'] = attribute.text
                    bios['attributes'].append(attrec)
                bios['model'] = elem.findtext('.//Model')
                #print(json.dumps(bios))
                #res = es.index(index="bioes", doc_type='biosample', id=bios['id'], body=bios)
                elem.clear()
                return(bios)


