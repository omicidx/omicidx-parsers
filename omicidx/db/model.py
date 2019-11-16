# alembic
# need to create schema by hand:
# upgrade:     op.execute("create schema etl")
# downgrade:     op.execute("drop schema etl")
#  - alembic revision --autogenerate -m "Added SRA study table skeleton"
#  - alembic upgrade head
#  - alembic history
#  - alembic downgrade HASH
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql.json import JSONB
from sqlalchemy import (Column, Integer, String, UniqueConstraint,
                        ForeignKeyConstraint, Table, Text, ForeignKey,
                        PrimaryKeyConstraint, Boolean, DateTime, Numeric, text,
                        Index)
from sqlalchemy.orm import (relationship)

Base = declarative_base()


class SraStudyJson(Base):
    __tablename__ = 'sra_study_json'
    accession = Column(String(20), primary_key=True)
    doc = Column(JSONB)


class SraSampleJson(Base):
    __tablename__ = 'sra_sample_json'
    accession = Column(String(20), primary_key=True)
    doc = Column(JSONB)


class SraExperimentJson(Base):
    __tablename__ = 'sra_experiment_json'
    accession = Column(String(20), primary_key=True)
    doc = Column(JSONB)


class SraRunJson(Base):
    __tablename__ = 'sra_run_json'
    accession = Column(String(20), primary_key=True)
    doc = Column(JSONB)


##########
# MIXINS #
##########


class SraNamespace(Base):
    __tablename__ = 'sra_namespace'

    namespace = Column(Text, primary_key=True)
    identifiers = relationship('SraSampleIdentifier')


class SraIdentifierMixin(object):
    identifier = Column(Text, primary_key=True)
    namespace = Column(ForeignKey('sra_namespace.namespace'), primary_key=True)


class SraStudyIdentifier(Base):
    __tablename__ = 'sra_study_identifier'

    study_accession = Column(ForeignKey('sra_study.accession'),
                             primary_key=True)
    study = relationship('SraStudy', back_populates='identifiers')
    identifier = Column(Text, primary_key=True)
    namespace = Column(ForeignKey('sra_namespace.namespace'), primary_key=True)


class SraStudy(Base):
    __tablename__ = 'sra_study'

    abstract = Column(Text)
    BioProject = Column(String(30))
    Geo = Column(String(15), index=True)
    accession = Column(String(20), primary_key=True)
    center_name = Column(Text)  # needs to be pulled out
    broker_name = Column(Text)  # needs to be pulled out
    description = Column(Text, comment="full description of study")
    study_type = Column(Text)  # needs to be pulled out
    title = Column(Text)
    received = Column(DateTime)
    updated = Column(DateTime)
    published = Column(DateTime)
    status = Column(Text)  # needs to be pulled out
    insdc = Column(Boolean)
    identifiers = relationship('SraStudyIdentifier', back_populates='study')


class LibrarySource(Base):
    __tablename__ = 'library_source'

    id = Column(Integer, primary_key = True)
    value = Column(String)


class LibraryStrategy(Base):
    __tablename__ = 'library_strategy'

    id = Column(Integer, primary_key = True)
    value = Column(String)


class LibrarySelection(Base):
    __tablename__ = 'library_selection'

    id = Column(Integer, primary_key = True)
    value = Column(String)


class LibraryLayout(Base):
    __tablename__ = 'library_layout'

    id = Column(Integer, primary_key = True)
    value = Column(String)


class CenterName(Base):
    __tablename__ = 'center_name'

    id = Column(Integer, primary_key = True)
    value = Column(String)


class InstrumentModel(Base):
    __tablename__ = 'instrument_model'

    id = Column(Integer, primary_key = True)
    value = Column(String)


class Platform(Base):
    __tablename__ = 'platform'

    id = Column(Integer, primary_key = True)
    value = Column(String)


def to_tsvector_ix(*columns):
    s = " || ' ' || ".join(columns)
    return func.to_tsvector('english', text(s))


from sqlalchemy.sql import func


class SraExperimentIdentifier(Base):
    __tablename__ = 'sra_experiment_identifier'

    experiment_accession = Column(ForeignKey('sra_experiment.accession'),
                                  primary_key=True)
    experiment = relationship('SraExperiment', back_populates='identifiers')
    identifier = Column(Text, primary_key=True)
    namespace = Column(ForeignKey('sra_namespace.namespace'), primary_key=True)


class SraExperiment(Base):
    __tablename__ = 'sra_experiment'

    accession = Column(String(20), primary_key=True)
    alias = Column(String)
    # attributes
    center_name_id = Column(ForeignKey('center_name.id'), index=True)
    design = Column(Text)
    description = Column(Text)
    identifiers = relationship('SraExperimentIdentifier',
                               back_populates='experiment')
    instrument_model = Column(ForeignKey('instrument_model.id'), index=True)
    library_name = Column(Text)
    library_construction_protocol = Column(Text)
    library_layout_orientation = Column(Text)  #appears to always be NULL
    library_layout_length = Column(Numeric)
    library_layout_sdev = Column(Numeric)
    library_selection_id = Column(Integer,
                               ForeignKey('library_selection.id'),
                               index=True)
    library_strategy_id = Column(Integer,
                              ForeignKey('library_strategy.id'),
                              index=True)
    library_source_id = Column(Integer,
                            ForeignKey('library_source.id'),
                            index=True)
    library_layout_id = Column(Integer,
                            ForeignKey('library_layout.id'),
                            index=True)
    platform_id = Column(Integer,
                      ForeignKey('platform.id'),
                      index=True,
                      comment='The sequencing platform')
    # xreefs
    sample_accession = Column(String(20),
                              ForeignKey('sra_sample.accession'),
                              index=True)
    study_accession = Column(String(20),
                             ForeignKey('sra_study.accession'),
                             index=True)
    title = Column(Text)
    # ?visibility
    # ?replacedby

    # TODO: Need to figure out how to add this with alembic
    __table_args__ = (Index('ix_fts_experiment',
                            to_tsvector_ix('title', 'description', 'design',
                                           'alias', 'accession'),
                            postgresql_using='gin'), )


class SraSampleIdentifier(Base):
    __tablename__ = 'sra_sample_identifier'

    sample_accession = Column(ForeignKey('sra_sample.accession'),
                              primary_key=True)
    sample = relationship('SraSample', back_populates='identifiers')
    identifier = Column(Text, primary_key=True)
    namespace = Column(ForeignKey('sra_namespace.namespace'), primary_key=True)


class SraSample(Base):
    __tablename__ = 'sra_sample'

    accession = Column(String(20), primary_key=True)
    # TODO: foreign key to geo
    geo = Column(String(15), index=True)
    # TODO: foreigh key to biosample
    BioSample = Column(String, index=True)
    title = Column(Text)
    alias = Column(Text)
    experiments = relationship('SraExperiment', backref='sample')
    # leaving out organism
    # TODO: Taxonomy link
    taxon_id = Column(Integer, index=True)
    description = Column(Text)
    identifiers = relationship('SraSampleIdentifier', back_populates='sample')
    # attributes
    # xrefs
    # link to experiment


class SraRun(Base):
    __tablename__ = 'sra_run'

    accession = Column(String(20), primary_key=True)
    alias = Column(String)
    run_date = Column(DateTime, index=True)
    run_center = Column(String)
    center_name_id = Column(ForeignKey('center_name.id'), index=True)
    total_spots = Column(Integer, index=True)
    total_bases = Column(Integer, index=True)
    size = Column(Integer)
    load_done = Column(Boolean)
    published = Column(DateTime)
    is_public = Column(Boolean)
    cluster_name = Column(String)
    avg_length = Column(Numeric, index=True)
    experiment_accession = Column(String(20),
                                  ForeignKey('sra_experiment.accession'),
                                  index=True)
    experiment = relationship('SraExperiment', back_populates='runs')
    # attributes
    # files
    # qualities
    # basecounts
    # reads


geo_series_contributors = Table(
    'geo_series_contributors', Base.metadata,
    Column('gse_accession',
           ForeignKey('geo_series.accession'),
           primary_key=True),
    Column('geo_name_id', ForeignKey('geo_name.id'), primary_key=True))

class TaxonCountAnalysis(Base):
    __tablename__ = 'taxon_count_analysis'

    id = Column(Integer, primary_key = True)
    nspot_analyze = Column(Integer, index=True)
    total_spots = Column(Integer, index = True)
    run_accession = Column(ForeignKey("sra_run.accession"), index=True)
    run = relationship('SraRun', backref = 'taxon_analysis')

class Taxonomy(Base):
    __tablename__ = "taxonomy"

    id = Column(Integer, primary_key = True)
    rank = Column(String, index = True) # ? normalize further?
    name = Column(String, index = True) # ? unique
    parent = Column(Integer, ForeignKey('taxonomy.id'), index = True)
    
class TaxonCountEntry(Base):
    __tablename__ = 'taxon_count_entry'

    taxon_analysis_id = Column(Integer, ForeignKey('taxon_count_analysis.id'), primary_key=True)
    taxon_id = Column(Integer, ForeignKey('taxonomy.id'), primary_key = True)
    self_count = Column(Integer)
    total_count = Column(Integer)
    taxon_analysis = relationship('TaxonCountAnalysis', backref = 'taxon_counts')


class GEOName(Base):
    __tablename__ = 'geo_name'
    __table_args__ = (UniqueConstraint('first_name',
                                       'middle_name',
                                       'last_name',
                                       name='ix_uq_geo_name'), )

    id = Column(Integer, primary_key=True)
    first_name = Column(String)
    middle_name = Column(String)
    last_name = Column(String)

    series = relationship('GEOSeries', secondary=geo_series_contributors)


class GEOSeries(Base):
    __tablename__ = 'geo_series'

    accession = Column(String(15), primary_key=True)
    summary = Column(String)
    contributors = relationship('GEOName', secondary=geo_series_contributors)
