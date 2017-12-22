import tarfile
from lxml import etree as ET

class SRAXMLParser(object):
    def __init__(self, metadatafile):
        self.tarfile=tarfile.open(metadatafile,'r:gz')
        self.parser_map = {
            'EXPERIMENT_SET': self.parse_experiment, 
            'RUN_SET': self.parse_run,
            'SUBMISSION': self.parse_submission,
            'SAMPLE_SET': self.parse_sample,
            'STUDY_SET': self.parse_study,
            'ANALYSIS_SET': self.parse_analysis
        }


    def __iter__(self):
        return(self)
        
    def iterator(self):
        while True:
            n = self.tarfile.next()
            if((not n.isfile()) or (not n.name.endswith('xml'))):
                continue
            if(n is None):
                raise StopIteration
            f = tarfile.TarFile.extractfile(self.tarfile,n)
            parser = ET.XMLParser(remove_blank_text=True)
            for val in self.parse_xml(ET.XML(f.read(), parser=parser)):
                yield(val)

    def parse_xml(self,xml):
        d = {}
   #     d['accession'] = xml.attrib['accession']
   #     d['_id'] = xml.attrib['accession']
        d['_type'] = xml.tag.lower()
        for k in xml.iter():
            if(k.text is not None):
                if(k.text.strip()!=''):
                    d[k.tag.lower()]=k.text.strip()
            if(len(k.attrib)>0):
                for i,j in k.items():
                    if(j!=''):
                        d[k.tag.lower() + "_" + i.lower()] = j
        return([d])
        return(self.parser_map[xml.tag](xml))

    def parse_experiment(self,xml):
        e1 = []
        for x in xml.xpath('./EXPERIMENT'):
            e = SRAExperiment()
            print(x.attrib)
            print(x.findall('./*'))
            e['_id'] = x.xpath('./@accession')[0]
            e['_type'] = 'experiment'
            e['accession'] = x.xpath('./@accession')[0]
            e['title'] = x.xpath('./TITLE/text()')[0]
            e['study'] = { 'accession': x.xpath('./STUDY_REF/@accession')[0],
                           'bioproject':x.xpath('./STUDY_REF/IDENTIFIERS/EXTERNAL_ID[@namespace="BioProject"]/text()')[0]
            }
            e['sample'] = { 'accession': x.xpath('./DESIGN/SAMPLE_DESCRIPTOR/@accession')[0],
                           'biosample':x.xpath('./DESIGN/SAMPLE_DESCRIPTOR/IDENTIFIERS/EXTERNAL_ID[@namespace="BioSample"]/text()')[0]
            }
            e['library'] = {'name': x.xpath('./DESIGN/LIBRARY_DESCRIPTOR/LIBRARY_NAME/text()')[0],
                            'strategy': x.xpath('./DESIGN/LIBRARY_DESCRIPTOR/LIBRARY_STRATEGY/text()')[0],
                            'source': x.xpath('./DESIGN/LIBRARY_DESCRIPTOR/LIBRARY_SOURCE/text()')[0],
                            'selection': x.xpath('./DESIGN/LIBRARY_DESCRIPTOR/LIBRARY_SELECTION/text()')[0],
                            #'construction_protocol': x.xpath('./DESIGN/LIBRARY_DESCRIPTOR/LIBRARY_CONSTRUCTION_PROTOCOL/text()')[0],
                            'layout': x.xpath('./DESIGN/LIBRARY_DESCRIPTOR/LIBRARY_LAYOUT/*')[0].tag
            }
            e['platform'] = {'manufacturer': x.xpath('./PLATFORM/*')[0].tag,
                             'instrument_model': x.xpath('./PLATFORM//INSTRUMENT_MODEL/text()')[0]
            }
            e1.append(e)
        return(e1, xml)

    def parse_submission(self,xml):
        return((xml,'submission'))

    def parse_study(self,xml):
        
        d['alias'] = get_attrib'alias')
        
        return((xml,'study'))

    def parse_analysis(self,xml):
        return((xml,'analysis'))

    def parse_sample(self,xml):
        return((xml,'sample'))

    def parse_run(self,xml):
        return((xml,'run'))

    

class SRAExperiment(dict):
    pass
        
    
    
