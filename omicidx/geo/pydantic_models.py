import pydantic
from pydantic import (BaseModel, constr)
import json
from datetime import datetime, date
from typing import List, Dict


class GEOBase(BaseModel):
    title: str
    status: str
    submission_date: date = None
    last_update_date: date = None


class GEOName(BaseModel):
    first: str = None
    middle: str = None
    last: str = None


class GEOContact(BaseModel):
    city: str = None
    name: GEOName = None
    email: str = None
    state: str = None
    address: str = None
    department: str = None
    country: str = None
    web_link: str = None
    institute: str = None
    zip_postal_code: str = None
    phone: str = None


class GEOPlatform(GEOBase):
    accession: constr(regex='GPL\d+')
    status: str
    _entity: str = "GPL"
    contact: GEOContact = None
    summary: str = None
    organism: str = None
    sample_id: List[constr(regex='GSM\d+')] = []
    series_id: List[constr(regex='GSE\d+')] = []
    technology: str = None
    description: str = None
    distribution: str = None
    manufacturer: List[str] = []
    data_row_count: int = None
    contributor: List[GEOName] = []
    relation: List[str] = []
    manufacture_protocol: str = None


class GEOCharacteristic(BaseModel):
    tag: str
    value: str = None  # there are apparently some of these


class GEOChannel(BaseModel):
    label: str = None
    taxid: List[int] = []
    molecule: str = None
    organism: str = None
    source_name: str = None
    label_protocol: str = None
    growth_protocol: str = None
    extract_protocol: str = None
    treatment_protocol: str = None
    characteristics: List[GEOCharacteristic] = []


class GEOSample(GEOBase):
    type: str
    anchor: str = None
    _entity: None
    contact: GEOContact = None
    description: str = None
    accession: constr(regex='GSM\d+')
    biosample: constr(regex='SAM[A-Z]+\d+') = None
    tag_count: int = None
    tag_length: float = None
    platform_id: constr(regex='GPL\d+')
    hyb_protocol: str = None
    channel_count: int = 0
    scan_protocol: str = None
    data_row_count: int = 0
    library_source: str = None
    overall_design: str = None
    sra_experiment: constr(regex='[DES]RX\d+') = None
    data_processing: str = None
    supplemental_files: List[str] = []
    channels: List[GEOChannel] = []
    contributor: List[GEOName] = []


class GEOSeries(GEOBase):
    accession: constr(regex='GSE\d+')
    subseries: List[constr(regex='GSE\d+')] = []
    bioprojects: List[constr(regex='PRJ[A-Z]+\d+')] = []
    sra_studies: List[constr(regex='[ESD]RP\d+')] = []
    _entity: str = "GSE"
    contact: GEOContact = None
    type: List[str] = []
    summary: str = None
    relation: List[str] = []
    pubmed_id: List[int] = []
    sample_id: List[constr(regex='GSM\d+')]=[]
    sample_taxid: List[int] = []
    sample_organism: List[str] = []
    platform_id: List[constr(regex='GPL\d+')]=[]
    platform_taxid: List[int] = []
    platform_organism: List[str] = []
    data_processing: str = None
    description: str = None
    supplemental_files: List[str] = []
    overall_design: str = None
    contributor: List[GEOName] = []
