# coding: utf-8
from sqlalchemy import BigInteger, Column, DateTime, ForeignKey, Integer, Table, Text, text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, TSVECTOR
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy.dialects.postgresql as postgresql

Base = declarative_base()
metadata = Base.metadata

etl_schema = 'etl'
public_schema = 'public2'
staging_schema = 'staging'

class IdentifiersMixin(object):
    identifier = Column(Text)
    namespace = Column(Text)

class XrefsMixin(object):
    db = Column(Text)
    identifier = Column(Text)
    uuid = Column(Text)

class AttributesMixin(object):
    tag = Column(Text)
    value = Column(Text)



t_biosample_jsonb = Table(
    'biosample_jsonb', metadata,
    Column('doc', JSONB(astext_type=Text())),
    schema=etl_schema
)


t_biosample_test = Table(
    'biosample_test', metadata,
    Column('id', Integer),
    Column('gsm', Text),
    Column('dbgap', Text),
    Column('model', Text),
    Column('title', Text),
    Column('access', Text),
    Column('id_recs', JSONB(astext_type=Text())),
    Column('ids', JSONB(astext_type=Text())),
    Column('taxon_id', Integer),
    Column('accession', Text),
    Column('attributes', JSONB(astext_type=Text())),
    Column('sra_sample', Text),
    Column('description', Text),
    Column('last_update', DateTime),
    Column('publication_date', DateTime),
    Column('submission_date', DateTime),
    Column('is_reference', Text),
    Column('taxonomy_name', Text),
    Column('textsearchable_index_col', TSVECTOR, index=True),
    schema=etl_schema
)


t_experiment_jsonb = Table(
    'experiment_jsonb', metadata,
    Column('doc', JSONB(astext_type=Text())),
    schema=etl_schema
)


t_fileinfo_addons = Table(
    'fileinfo_addons', metadata,
    Column('accession', Text, index=True),
    Column('file_size', BigInteger),
    Column('file_md5', Text),
    Column('file_date', DateTime),
    Column('filename', Text),
    Column('url', Text),
    schema=etl_schema
)


t_fileinfo_runs = Table(
    'fileinfo_runs', metadata,
    Column('accession', Text, index=True),
    Column('file_size', BigInteger),
    Column('file_md5', Text),
    Column('file_date', DateTime),
    schema=etl_schema
)


t_geo_jsonb = Table(
    'geo_jsonb', metadata,
    Column('doc', JSONB(astext_type=Text())),
    Column('update_time', DateTime, server_default=text("'2019-03-20 12:55:47.158602'::timestamp without time zone")),
    schema=etl_schema
)


t_run_jsonb = Table(
    'run_jsonb', metadata,
    Column('doc', JSONB(astext_type=Text())),
    schema=etl_schema
)


t_sample_jsonb = Table(
    'sample_jsonb', metadata,
    Column('doc', JSONB(astext_type=Text())),
    schema=etl_schema
)


class SraAccession(Base):
    __tablename__ = 'sra_accession'
    __table_args__ = {'schema': etl_schema}

    accession = Column(Text, primary_key=True, index=True)
    submission = Column(Text)
    status = Column(Text)
    updated = Column(DateTime)
    published = Column(DateTime)
    received = Column(DateTime)
    type = Column(Text)
    center = Column(Text)
    visibility = Column(Text)
    alias = Column(Text)
    experiment = Column(Text)
    sample = Column(Text)
    study = Column(Text)
    loaded = Column(Text)
    spots = Column(BigInteger)
    bases = Column(BigInteger)
    md5sum = Column(Text)
    bio_sample = Column(Text)
    bio_project = Column(Text)
    replaced_by = Column(Text)



class SraStudy(Base):
    __tablename__ = 'sra_study'
    __table_args__ = {'schema': public_schema}

    bioproject = Column(Text)
    gse = Column(Text)
    abstract = Column(Text)
    accession = Column(Text, primary_key=True)
    alias = Column(Text)
    attributes = Column(JSONB(astext_type=Text()))
    broker_name = Column(Text)
    center_name = Column(Text)
    description = Column(Text)
    identifiers = Column(JSONB(astext_type=Text()))
    study_type = Column(Text)
    title = Column(Text)
    xrefs = Column(JSONB(astext_type=Text()))
    status = Column(Text)
    updated = Column(DateTime)
    published = Column(DateTime)
    received = Column(DateTime)
    visibility = Column(Text)
    bio_project = Column(Text)
    replaced_by = Column(Text)



class SraSample(Base):
    __tablename__ = 'sra_sample'
    __table_args__ = {'schema': public_schema}

    accession = Column(Text, primary_key=True)
    alias = Column(Text)
    attributes = Column(JSONB(astext_type=Text()))
    bio_sample = Column(Text)
    broker_name = Column(Text)
    center_name = Column(Text)
    description = Column(Text)
    gsm = Column(Text)
    identifiers = Column(JSONB(astext_type=Text()))
    organism = Column(Text)
    title = Column(Text)
    taxon_id = Column(Integer)
    xrefs = Column(JSONB(astext_type=Text()))
    status = Column(Text)
    updated = Column(DateTime)
    published = Column(DateTime)
    received = Column(DateTime)
    visibility = Column(Text)
    replaced_by = Column(Text)
    study_accession = Column(ForeignKey(public_schema + '.sra_study.accession'))

    sra_study = relationship('SraStudy')

t_study_jsonb = Table(
    'study_jsonb', metadata,
    Column('doc', JSONB(astext_type=Text())),
    schema=etl_schema
)


class SraExperiment(Base):
    __tablename__ = 'sra_experiment'
    __table_args__ = {'schema': public_schema}

    accession = Column(Text, primary_key=True)
    alias = Column(Text)
    attributes = Column(JSONB(astext_type=Text()))
    broker_name = Column(Text)
    center_name = Column(Text)
    description = Column(Text)
    design = Column(Text)
    identifiers = Column(JSONB(astext_type=Text()))
    instrument_model = Column(Text)
    library_construction_protocol = Column(Text)
    library_layout_length = Column(Text)
    library_layout_orientation = Column(Text)
    library_layout_sdev = Column(Text)
    library_name = Column(Text)
    library_strategy = Column(ForeignKey(public_schema + '.sra_library_strategy.value'), index=True)
    library_source = Column(ForeignKey(public_schema + '.sra_library_source.value'), index=True)
    library_layout = Column(ForeignKey(public_schema + '.sra_library_layout.value'), index=True)
    library_selection = Column(ForeignKey(public_schema + '.sra_library_selection.value'), index=True)
    platform = Column(ForeignKey(public_schema + '.sra_platform.value'), index=True)
    sample_accession = Column(ForeignKey(public_schema + '.sra_sample.accession'))
    study_accession = Column(ForeignKey(public_schema + '.sra_study.accession'))
    title = Column(Text)
    xrefs = Column(Text)
    status = Column(Text)
    updated = Column(DateTime)
    published = Column(DateTime)
    received = Column(DateTime)
    visibility = Column(Text)
    replaced_by = Column(Text)

    sra_sample = relationship('SraSample')
    sra_study = relationship('SraStudy')


class SraRun(Base):
    __tablename__ = 'sra_run'
    __table_args__ = {'schema': public_schema}

    accession = Column(Text, primary_key=True)
    broker_name = Column(Text)
    identifiers = Column(JSONB(astext_type=Text()))
    run_center = Column(Text)
    center_name = Column(Text)
    run_date = Column(DateTime)
    attributes = Column(JSONB(astext_type=Text()))
    nreads = Column(Integer)
    alias = Column(Text)
    spot_length = Column(Integer)
    experiment_accession = Column(ForeignKey(public_schema + '.sra_experiment.accession'))
    study_accession = Column(ForeignKey(public_schema + '.sra_study.accession'))
    sample_accession = Column(ForeignKey(public_schema + '.sra_sample.accession'))
    reads = Column(JSONB(astext_type=Text()))
    status = Column(Text)
    updated = Column(DateTime)
    published = Column(DateTime)
    received = Column(DateTime)
    visibility = Column(Text)
    bio_project = Column(Text)
    replaced_by = Column(Text)
    loaded = Column(Text)
    spots = Column(BigInteger)
    bases = Column(BigInteger)

    sra_experiment = relationship('SraExperiment')
    sra_study = relationship('SraStudy')
    sra_sample = relationship('SraSample')


class SraLibraryStrategy(Base):
    __tablename__ = 'sra_library_strategy'
    __table_args__ = {'schema': public_schema}
    
    value = Column(Text, primary_key=True)

class SraLibrarySource(Base):
    __tablename__ = 'sra_library_source'
    __table_args__ = {'schema': public_schema}
    
    value = Column(Text, primary_key=True)

class SraLibraryLayout(Base):
    __tablename__ = 'sra_library_layout'
    __table_args__ = {'schema': public_schema}
    
    value = Column(Text, primary_key=True)

class SraLibrarySelection(Base):
    __tablename__ = 'sra_library_selection'
    __table_args__ = {'schema': public_schema}
    
    value = Column(Text, primary_key=True)

class SraPlatform(Base):
    __tablename__ = 'sra_platform'
    __table_args__ = {'schema': public_schema}
    
    value = Column(Text, primary_key=True)



#########################
# GEO models below here #
#########################


geo_gse_gsm = Table('geo_gse_gsm', Base.metadata,
    Column('gse', Text, ForeignKey('geo_gse.accession')),
    Column('gsm', Text, ForeignKey('geo_gsm.accession'))
)


geo_gse_gpl = Table('geo_gse_gpl', Base.metadata,
    Column('gse', Text, ForeignKey('geo_gse.accession')),
    Column('gpl', Text, ForeignKey('geo_gpl.accession'))
)


class GEOSeries(Base):
    __tablename__ = 'geo_gse'
    __table_args__ = {'schema': public_schema}

    accession = Column(Text, primary_key=True)
    title = Column(Text)
    status = Column(Text)
    summary = Column(Text)
    description = Column(Text)
    overall_design = Column(Text)
    submission_date = Column(DateTime)
    last_update_date = Column(DateTime)
    data_processing = Column(Text)
    contributors = Column(postgresql.ARRAY(Text))
    gpls = relationship("GEOPlatform",
                        secondary = geo_gse_gpl,
                        back_populates = "gses")
    gsms = relationship("GEOSample",
                        secondary = geo_gse_gsm,
                        back_populates = "gses")


class GEOPlatform(Base):
    __tablename__ = 'geo_gpl'
    __table_args__ = {'schema': public_schema}
    
    accession = Column(Text, primary_key=True)
    title = Column(Text)
    status = Column(Text)
    summary = Column(Text)
    description = Column(Text)
    submission_date = Column(DateTime)
    last_update_date = Column(DateTime)
    technology = Column(Text)
    distribution = Column(Text)
    data_row_count = Column(Integer)
    contributors = Column(postgresql.ARRAY(Text))
    gses = relationship("GEOSeries",
                        secondary = geo_gse_gpl,
                        back_populates = "gpls")
    gsms = relationship("GEOSample",
                        secondary = geo_gse_gsm,
                        back_populates = "gpls")


    
class GEOContact(Base):
    __tablename__ = 'geo_contact'
    __table_args__ = {'schema': public_schema}

    email = Column(Text, primary_key=True)
    name = Column(Text)
    phone = Column(Text)
    fax = Column(Text)
    city = Column(Text)
    address = Column(Text)
    country = Column(Text)
    web_link = Column(Text)
    institute = Column(Text)
    postal_code = Column(Text)


biosample_to_attributes = Table(
    'biosample_to_attributes', Base.metadata,
    Column('id', Integer, primary_key=True),
    Column('biosample_id', ForeignKey('biosample.id')),
    Column('biosample_attribute_id', ForeignKey('biosample_attribute.id')),
)

class BiosampleModel(Base):
    __tablename__ = "biosample_model"

    id = Column(Integer, primary_key=True)
    name = Column(Text, index=True)


class Biosample(Base):
    __tablename__ = "biosample"

    id = Column(BigInteger, primary_key=True)
    submission_date = Column(DateTime, index=True, nullable=False)
    last_update = Column(DateTime, index=True, nullable=False)
    publication_date = Column(DateTime, index=True, nullable=False)
    access = Column(Text, index=True, comment="access control, public or controlled")
    # srs = Column(ForeignKey('sra_sample.accession'), index=True)
    # gsm = Column(ForeignKey('geo_gsm.accession'), index=True)
    dbgap = Column(Text, index=True, comment = "dbGaP identifier")
    title = Column(Text)
    description = Column(Text)
    taxonomy_name = Column(Text)
    taxon_id = Column(Integer, index=True, comment = "NCBI Taxonomy id")
    model = Column(ForeignKey("biosample_model.id"), index=True)
    attributes = relationship("BiosampleAttribute", secondary = biosample_to_attributes)
                                
class BiosampleAttribute(Base):
    __tablename__ = "biosample_attribute"

    id = Column(Integer, primary_key=True)
    name = Column(Text, index=True)
    harmonized_name = Column(Text, index = True)
    display_name = Column(Text, index=True)
    value = Column(Text, index=True)
    UniqueConstraint('name', 'harmonized_name', 'value')
    biosamples = relationship("Biosample", secondary = biosample_to_attributes)


class BiosampleIdentifier(Base):
    __tablename__ = "biosample_identifier"

    id = Column(Integer, primary_key=True)
    biosample = Column(ForeignKey('biosample.id'), index=True)
    db = Column(Text, index=True)
    label = Column(Text, index = True)
    identifier = Column(Text, index=True)
