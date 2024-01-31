import pydantic
from pydantic import BaseModel, constr
import json
from datetime import datetime, date
from typing import List, Dict, Any, Optional



class GEOBase(BaseModel):
    title: str
    status: str
    submission_date: Optional[date] = None
    last_update_date: Optional[date] = None


class GEOName(BaseModel):
    first: Optional[str] = None
    middle: Optional[str] = None
    last: Optional[str] = None


class GEOContact(BaseModel):
    city: Optional[str] = None
    name: GEOName = None
    email: Optional[str] = None
    state: Optional[str] = None
    address: Optional[str] = None
    department: Optional[str] = None
    country: Optional[str] = None
    web_link: Optional[str] = None
    institute: Optional[str] = None
    zip_postal_code: Optional[str] = None
    phone: Optional[str] = None


class GEOPlatform(GEOBase):
    accession: str #constr(regex="GPL[0-9]+")
    status: str
    _entity: str = "GPL"
    contact: GEOContact = None
    summary: Optional[str] = None
    organism: Optional[str] = None
    sample_id: Optional[List[str]] #List[constr(regex="GSM[0-9]+")] = []
    series_id: Optional[List[str]] #List[constr(regex="GSE[0-9]+")] = []
    technology: Optional[str] = None
    description: Optional[str] = None
    distribution: Optional[str] = None
    manufacturer: List[str] = []
    data_row_count: Optional[int] = None
    contributor: List[GEOName] = []
    relation: List[str] = []
    manufacture_protocol: Optional[str] = None


class GEOCharacteristic(BaseModel):
    tag: str
    value: Optional[str] = None  # there are apparently some of these


class GEOChannel(BaseModel):
    label: Optional[str] = None
    taxid: List[int] = []
    molecule: Optional[str] = None
    organism: Optional[str] = None
    source_name: Optional[str] = None
    label_protocol: Optional[str] = None
    growth_protocol: Optional[str] = None
    extract_protocol: Optional[str] = None
    treatment_protocol: Optional[str] = None
    characteristics: List[GEOCharacteristic] = []


class GEOSample(GEOBase):
    type: str
    anchor: Optional[str] = None
    _entity: None
    contact: GEOContact = None
    description: Optional[str] = None
    accession: constr(regex="GSM[0-9]+")
    biosample: Optional[constr(regex="SAM[A-Z]+[0-9]+")] = None
    tag_count: int = None
    tag_length: float = None
    platform_id: constr(regex="GPL[0-9]+")
    hyb_protocol: Optional[str] = None
    channel_count: int = 0
    scan_protocol: Optional[str] = None
    data_row_count: int = 0
    library_source: Optional[str] = None
    overall_design: Optional[str] = None
    sra_experiment: Optional[constr(regex="[DES]RX[0-9]+")] = None
    data_processing: Optional[str] = None
    supplemental_files: List[str] = []
    channels: List[GEOChannel] = []
    contributor: List[GEOName] = []


class GEOSeries(GEOBase):
    accession: constr(regex="GSE[0-9]+")
    subseries: List[constr(regex="GSE[0-9]+")] = []
    bioprojects: List[constr(regex="PRJ[A-Z]+[0-9]+")] = []
    sra_studies: List[constr(regex="[ESD]RP[0-9]+")] = []
    _entity: str = "GSE"
    contact: GEOContact = None
    type: List[str] = []
    summary: Optional[str] = None
    relation: List[str] = []
    pubmed_id: List[int] = []
    sample_id: List[constr(regex="GSM[0-9]+")] = []
    sample_taxid: List[int] = []
    sample_organism: List[str] = []
    platform_id: List[constr(regex="GPL[0-9]+")] = []
    platform_taxid: List[int] = []
    platform_organism: List[str] = []
    data_processing: Optional[str] = None
    description: Optional[str] = None
    supplemental_files: List[str] = []
    overall_design: Optional[str] = None
    contributor: List[GEOName] = []
