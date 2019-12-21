import pydantic
from pydantic import BaseModel
import json
from datetime import datetime
from typing import List, Dict

class Attribute(BaseModel):
    tag: str = None
    value: str = None


class Xref(BaseModel):
    db: str = None
    id: str = None

    
class Identifier(BaseModel):
    namespace: str = None
    id: str = None


class FileAlternative(BaseModel):
    url: str = None
    free_egress: str = None
    access_type: str = None
    org: str = None


class FileSet(BaseModel):
    cluster: str = "public"
    filename: str = None
    url: str = None
    size: int = 0
    date: datetime = None
    md5: str = None
    sratoolkit: str = '1'
    alternatives: List[FileAlternative]


class BaseQualityCount(BaseModel):
    quality: int = 0
    count: int = 0
    
class BaseQualities(List[BaseQualityCount]):
    pass
    

class TaxCountEntry(BaseModel):
    rank: str = None
    name: str = None
    parent: int = None
    total_count: int = 0
    self_count: int = 0
    tax_id: int

class TaxCountAnalysis(BaseModel):
    nspot_analyze: int = None
    total_spots: int = None
    mapped_spots: int = None
    tax_counts: List[TaxCountEntry] = None

class RunRead(BaseModel):
    index: int
    count: int
    mean_length: float = 0.0
    sd_length: float = 0.0


class BaseCounts(List[Dict[str, int]]):
    pass


class LiveList(BaseModel):
    lastupdate: datetime = None
    published: datetime = None
    received: datetime = None
    status: str = "live"
    insdc: bool = True

class SraRun(LiveList, BaseModel):
    alias: str = None
    run_date: datetime = None
    run_center: str = None
    center_name: str = None
    accession: str
    total_spots: int = 0
    total_bases: int = 0
    size: int = 0
    load_done: bool = True
    published: datetime = None
    is_public: bool = True
    cluster_name: str = "public"
    static_data_available: str = "1"
    avg_length: float = 0.0
    experiment_accession: str
    attributes: List[Attribute] = None
    files: List[FileSet] = None
    qualities: BaseQualities = None
    base_counts: BaseCounts = None
    reads: List[RunRead] = None
    tax_analysis: TaxCountAnalysis = None


class SraStudy(LiveList, BaseModel):
    abstract: str = None
    BioProject: str = None
    Geo: str = None
    accession: str
    alias: str = None
    center_name: str = None
    broker_name: str = None
    description: str = None
    study_type: str = None
    title: str = None
    identifiers: List[Identifier] = None
    attributes: List[Attribute] = None
    pubmed_ids: List[int] = None


class SraExperiment(LiveList, BaseModel):
    accession: str
    attributes: List[Attribute] = None
    alias: str = None
    center_name: str = None
    design: str = None
    description: str = None
    identifiers: List[Identifier] = None
    instrument_model: str = None
    library_name: str = None
    library_construction_protocol: str = None
    library_layout_orientation: str = None
    library_layout_length: float = None
    library_layout_sdev: float = None
    library_strategy: str = None
    library_source: str = None
    library_selection: str = None
    library_layout: str = None
    xrefs: List[Xref] = None
    platform: str = None
    sample_accession: str = None
    study_accession: str = None
    title: str = None


class SraSample(LiveList, BaseModel):
    accession: str
    geo: str = None
    BioSample: str = None
    title: str = None
    alias: str = None
    organism: str = None
    taxon_id: int = None
    description: str = None
    identifiers: List[Identifier] = None
    attributes: List[Attribute] = None
    xrefs: List[Xref] = None
    
class FullSraRun(SraRun):
    experiment: SraExperiment = None
    sample: SraSample = None
    study: SraStudy = None
    
