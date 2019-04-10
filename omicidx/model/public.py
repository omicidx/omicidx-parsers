# coding: utf-8
from sqlalchemy import BigInteger, Column, DateTime, ForeignKey,\
    Integer, Table, Text, text, Index, UniqueConstraint
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import JSONB, TSVECTOR, UUID
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy.dialects.postgresql as postgresql

Base = declarative_base()
metadata = Base.metadata

class IdentifiersMixin(object):
    id = Column(Integer, primary_key=True)
    identifier = Column(Text)
    namespace = Column(Text)
    uuid = Column(UUID(as_uuid=True))

class XrefsMixin(object):
    id = Column(Integer, primary_key=True)
    db = Column(Text)
    identifier = Column(Text)
    uuid = Column(UUID(as_uuid=True))

class AttributesMixin(object):
    id = Column(Integer, primary_key=True)
    tag = Column(Text)
    value = Column(Text)

def to_tsvector_ix(*columns):
    """create tsvector string from column names"""
    s = " || ' ' || ".join(columns)
    return func.to_tsvector('english', text(s))


class SraStudy(Base):
    __tablename__ = 'sra_study'

    bioproject = Column(Text)
    gse = Column(Text, index=True, comment="related GEO series")
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
    replaced_by = Column(Text)

    __table_args__ = (
        Index(
            'ix_examples_tsv',
            to_tsvector_ix(
                "gse",
                "alias",
                "abstract",
                "accession",
                "study_type",
                "title",
                "bioproject"
            ),
            postgresql_using='gin'
        ),
    )

sra_sample2attribute = Table('sra_sample2attribute', Base.metadata,
    Column('accession', Text, ForeignKey('sra_sample.accession'), index=True),
    Column('attribute_id', Integer, ForeignKey('sra_sample_attribute.id'), index=True),
    UniqueConstraint('accession', 'attribute_id')                          
)

sra_study2attribute = Table('sra_study2attribute', Base.metadata,
    Column('accession', Text, ForeignKey('sra_study.accession'), index=True),
    Column('attribute_id', Integer, ForeignKey('sra_study_attribute.id'), index=True),
    UniqueConstraint('accession', 'attribute_id')                          
)

sra_experiment2attribute = Table('sra_experiment2attribute', Base.metadata,
    Column('accession', Text, ForeignKey('sra_experiment.accession'), index=True),
    Column('attribute_id', Integer, ForeignKey('sra_experiment_attribute.id'), index=True),
    UniqueConstraint('accession', 'attribute_id')                          
)

sra_run2attribute = Table('sra_run2attribute', Base.metadata,
    Column('accession', Text, ForeignKey('sra_run.accession'), index=True),
    Column('attribute_id', Integer, ForeignKey('sra_run_attribute.id'), index=True),
    UniqueConstraint('accession', 'attribute_id')                          
)

class SraSample(Base):
    __tablename__ = 'sra_sample'

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
    study_accession = Column(ForeignKey('sra_study.accession'))

    attributes = relationship("SraSampleAttribute",
                              secondary=sra_sample2attribute)
    
class SraExperiment(Base):
    __tablename__ = 'sra_experiment'

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
    library_strategy = Column(ForeignKey('sra_library_strategy.value'), index=True)
    library_source = Column(ForeignKey('sra_library_source.value'), index=True)
    library_layout = Column(ForeignKey('sra_library_layout.value'), index=True)
    library_selection = Column(ForeignKey('sra_library_selection.value'), index=True)
    platform = Column(ForeignKey('sra_platform.value'), index=True)
    sample_accession = Column(ForeignKey('sra_sample.accession'))
    study_accession = Column(ForeignKey('sra_study.accession'))
    title = Column(Text)
    xrefs = Column(Text)
    status = Column(Text)
    updated = Column(DateTime)
    published = Column(DateTime)
    received = Column(DateTime)
    visibility = Column(Text)
    replaced_by = Column(Text)




class SraRun(Base):
    __tablename__ = 'sra_run'

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
    experiment_accession = Column(ForeignKey('sra_experiment.accession'))
    study_accession = Column(ForeignKey('sra_study.accession'))
    sample_accession = Column(ForeignKey('sra_sample.accession'))
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


class SraExperimentIdentifier(Base, IdentifiersMixin):
    __tablename__ = 'sra_experiment_identifier'

    accession = Column(Text, ForeignKey('sra_experiment.accession'))

class SraStudyIdentifier(Base, IdentifiersMixin):
    __tablename__ = 'sra_study_identifier'
    
    accession = Column(Text, ForeignKey('sra_study.accession'))

class SraSampleIdentifier(Base, IdentifiersMixin):
    __tablename__ = 'sra_sample_identifier'
    
    accession = Column(Text, ForeignKey('sra_sample.accession'))
    
class SraSampleXref(Base, XrefsMixin):
    __tablename__ = 'sra_sample_xref'

    accession = Column(Text, ForeignKey('sra_sample.accession'))

class SraStudyXref(Base, XrefsMixin):
    __tablename__ = 'sra_study_xref'

    accession = Column(Text, ForeignKey('sra_study.accession'))


class SraExperimentAttribute(Base, AttributesMixin):
    __tablename__ = 'sra_experiment_attribute'

    
class SraStudyAttribute(Base, AttributesMixin):
    __tablename__ = 'sra_study_attribute'

    
class SraSampleAttribute(Base, AttributesMixin):
    __tablename__ = 'sra_sample_attribute'
    

class SraRunAttribute(Base, AttributesMixin):
    __tablename__ = 'sra_run_attribute'


class SraLibraryStrategy(Base):
    __tablename__ = 'sra_library_strategy'
    
    value = Column(Text, primary_key=True)

class SraLibrarySource(Base):
    __tablename__ = 'sra_library_source'
    
    value = Column(Text, primary_key=True)

class SraLibraryLayout(Base):
    __tablename__ = 'sra_library_layout'
    
    value = Column(Text, primary_key=True)

class SraLibrarySelection(Base):
    __tablename__ = 'sra_library_selection'
    
    value = Column(Text, primary_key=True)

class SraPlatform(Base):
    __tablename__ = 'sra_platform'
    
    value = Column(Text, primary_key=True)



#############
# Biosample #
#############

class BiosampleIdRec(Base):
    __tablename__ = 'biosample_idrec'


    id = Column(Integer, primary_key=True)
    biosample_accession = Column(Text, index = True)
    identifier = Column(Text, index = True)
    db = Column(Text, index = True)
    label = Column(Text)

class BiosampleAttribute(Base):

    __tablename__ = 'biosample_attribute'
    __table_args__ = (
        UniqueConstraint('name', 'harmonized_name', 'value'),
    )

    id = Column(Integer, primary_key=True)
    name = Column(Text, index=True)
    harmonized_name = Column(Text)
    display_name = Column(Text)
    value = Column(Text)

biosample2attribute = Table('biosample2attribute', Base.metadata,
    Column('accession', Text, ForeignKey('biosample.accession'), index=True),
    Column('attribute_id', Integer, ForeignKey('biosample_attribute.id'), index=True),
    UniqueConstraint('accession', 'attribute_id')                          
)



class Biosample(Base):
    __tablename__ = 'biosample'
    
    accession = Column(Text, primary_key=True)
    gsm = Column(Text, index=True)
    dbgap = Column(Text, index=True)
    model = Column(Text)
    title = Column(Text)
    access = Column(Text, index=True)
    taxon_id = Column(Integer, index=True)
    sra_sample = Column(Text, index=True)
    description = Column(Text)
    last_update = Column(DateTime)
    publication_date = Column(DateTime)
    submission_date = Column(DateTime)
    is_reference = Column(Text)
    taxonomy_name = Column(Text)
    textsearchable_index_col = Column(TSVECTOR, index=True)

    attributes = relationship("BiosampleAttribute",
                              secondary=sra_sample2attribute)

    identifiers = relationship("BiosampleIdRec")



def create_all_in_schema(schema='public2'):
    from sqlalchemy import create_engine
    dbschema = schema
    local_engine = create_engine('postgresql://bigrna@omicidx-aurora-cluster.cluster-cpmth1vkdqqx.us-east-1.rds.amazonaws.com/bigrna', 
                                 connect_args={'options': '-c search_path={}'.format(dbschema)})
    Base.metadata.create_all(local_engine)
    print(Base.metadata.tables.keys())

if __name__ == '__main__':
    create_all_in_schema('public2')

