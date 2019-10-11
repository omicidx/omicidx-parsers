"""Simple utility functions for EBI ENA"""
import urllib.request
import xml.etree.ElementTree as etree
import csv
import io
import omicidx.sra_parsers

class EBIEna(object):
    def __init__(self):
        pass

    def get_xml_for_accession(self, accession):
        """Get an XML record for a single accession as an ElementTree Node from EBI

        Parameters
        ----------
        accession: str
            Any SRA/ENA accession

        Returns
        -------
        an etree ElementNode parsed from the retrieved XML."""
        url = "https://www.ebi.ac.uk/ena/data/view/{accession}&display=xml&download=xml&filename={accession}.xml"
        url = url.format(accession = accession)
        with urllib.request.urlopen(url) as response:
            return(etree.parse(response).getroot()[0])

    def get_file_report_for_accession(self, accession):
        """Get a file report from EBI

        Parameters
        ----------
        accession: str
            Any SRA/ENA accession

        Returns
        -------
        a list of rows representing available files for the accession
        """
        url = "https://www.ebi.ac.uk/ena/data/warehouse/filereport?accession={accession}&result=read_run"
        url = url.format(accession = accession)
        print(url)
        response = urllib.request.urlopen(url)
        reader = csv.DictReader(io.StringIO(response.read().decode()),delimiter="\t")
        return(list(reader))

    def get_parsed_xml_for_accession(self, accession):
        xml = self.get_xml_for_accession(accession)
        xmlfunc = getattr(omicidx.sra_parsers,'{}_xml_iter_parser'.format(xml.tag.lower()))
        return(xmlfunc(xml))
        
        
    
