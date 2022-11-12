"""Biosample parser

Implemented as an iterator


>>> import omicidx.biosample as b
>>> for bios in b.BioSampleParser('biosample_set.xml.gz'):
>>>     print(bios.as_json())
"""

import os.path
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import Element
import gzip
import json
import typing
import logging
import click
import subprocess
import re


class BioSample(dict):
    """BioSample class"""

    def __init__(self):
        self.update()

    def as_json(self, indent=None):
        return json.dumps(self)

    def __str__(self):
        return "{0}".format(self["title"])

    def __repr__(self):
        return "BioSample <id={0} title={1}>".format(self["id"], self["title"])


class BioSampleParser(object):
    """Parse a BioSample xml file.

    Implemented as an iterator"""

    def __init__(self, fh: typing.IO):
        """Initialize a new parser

        Args:
            fh (typing.IO): An open file-like object containing Biosample XML records.
        """
        self.fhandle = fh
        self.context = ET.iterparse(self.fhandle, events=("end",))
        event, self.root = next(self.context)

    def __iter__(self):
        return self

    def __next__(self):
        for event, elem in self.context:
            if elem.tag == "BioSample":
                bios = BioSample()
                bios["is_reference"] = None
                for k, v in elem.items():
                    bios[k] = v
                bios["id_recs"] = []
                bios["ids"] = []
                bios["sra_sample"] = None
                bios["dbgap"] = None
                bios["gsm"] = None
                for id in elem.iterfind(".//Id"):
                    idrec = {
                        "db": id.get("db"),
                        "label": id.get("db_label"),
                        "id": id.text,
                    }
                    bios["ids"].append(idrec["id"])
                    bios["id_recs"].append(idrec)
                    # add xref fields for SRA, dbGaP, and GEO

                    if id.get("db") == "SRA":
                        bios["sra_sample"] = id.text
                    if id.get("db") == "dbGaP":
                        bios["dbgap"] = id.text
                    if id.get("db") == "GEO":
                        bios["gsm"] = id.text
                bios["title"] = elem.findtext(".//Description/Title")
                bios["description"] = elem.findtext(".//Description/Comment/Paragraph")
                organism = elem.find(".//Organism")
                bios["taxonomy_name"] = organism.get("taxonomy_name")
                bios["taxon_id"] = int(organism.get("taxonomy_id"))
                bios["attribute_recs"] = []
                bios["attributes"] = []
                for attribute in elem.findall(".//Attribute"):
                    attrec = attribute.attrib
                    attrec["value"] = attribute.text
                    bios["attribute_recs"].append(attrec)
                    try:
                        bios["attributes"].append(attrec["harmonized_name"])
                    except:
                        bios["attributes"].append(attrec["attribute_name"])

                bios["model"] = elem.findtext(".//Model")
                # print(json.dumps(bios))
                # res = es.index(index="bioes", doc_type='biosample', id=bios['id'], body=bios)
                elem.clear()
                return bios
        raise StopIteration


def parse_bioproject_xml_element(element: Element) -> dict:
    """Parse a BioProject xml element

    Args:
        element (Element): An lxml.etree.Element

    Returns:
        dict: A BioProject dict.
    """
    projtop = element.find("./Project")
    d2 = {}
    d2['title'] = projtop.findtext('./Project/ProjectDescr/Title')
    d2['description'] = projtop.findtext(
        './Project/ProjectDescr/Description')
    d2['name'] = projtop.findtext('./Project/ProjectDescr/Name')
    archive_id = projtop.find('./Project/ProjectID/ArchiveID')
    d2['accession'] = archive_id.attrib['accession']
    pubs = []
    for pub in projtop.findall(".//Publication"):
        pubs.append(
            {
                "pubdate": pub.get("date", None),
                "id": pub.get("id", None),
                "db": re.sub("^e", "", pub.findtext("./DbType").replace("^e", "", 1)),
            }
        )
    d2["publications"] = pubs
    ext_links = []
    for link in projtop.findall(".//ExternalLink"):
        ext_links.append(
            {
                "category": link.get("category", None),
                "label": link.get("label", None),
                "url": link.findtext("./URL"),
            }
        )
    data_types = []
    for datatype in projtop.findall(".//ProjectDataTypeSet"):
        data_types.append(datatype.findtext("./DataType"))
    d2["data_types"] = data_types
    d2["external_links"] = ext_links
    return d2


class BioProjectParser(typing.Iterable):
    """Parse a BioProject xml file.
    Implemented as an iterator"""
    def __init__(self, fh: typing.IO):
        """Initialize a BioProjectParser

        Args:
            fh (typing.IO): An open file-like object containing BioProject records.
        """
        self.fhandle = fh
        self.context = ET.iterparse(self.fhandle, events=("end",))
        event, self.root = next(self.context)

    def __iter__(self):
        return self

    def __next__(self):
        for event, elem in self.context:
            if elem.tag == "Package":
                results = parse_bioproject_xml_element(elem)
                elem.clear()
                return results

        raise StopIteration
