from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import *
from sqlalchemy import MetaData
from sqlalchemy.dialects.postgresql import JSON, JSONB
meta = MetaData(schema="testing")
Base = declarative_base(meta)

class RunJson(Base):
    __tablename__ =  "run_jsonb"

    id = Column( Integer, primary_key=True)
    doc = Column(JSONB)
    
class StudyJson(Base):
    __tablename__ =  "study_jsonb"

    id = Column( Integer, primary_key=True)
    doc = Column(JSONB)
    
class SampleJson(Base):
    __tablename__ =  "sample_jsonb"

    id = Column( Integer, primary_key=True)
    doc = Column(JSONB)
    
class ExperimentJson(Base):
    __tablename__ =  "experiment_jsonb"

    id = Column( Integer, primary_key=True)
    doc = Column(JSONB)

class SRAStudy(Base):
    __tablename__ = "sra_study"

    accession        = Column(Text, primary_key = True)
    bioproject       = Column(Text, index=True)
    gse_accession    = Column(Text, index=True)
    abstract         = Column(Text)
    alias            = Column(Text)
    attributes       = Column(JSONB)
    broker_name      = Column(Text, index = True)
    center_name      = Column(Text, index = True)
    description      = Column(Text)
    identifiers      = Column(JSONB)
    study_type       = Column(Text)
    title            = Column(Text, index = True)
    xrefs            = Column(JSONB)
    status           = Column(Text, index = True)
    updated          = Column(DateTime)
    published        = Column(DateTime)
    received         = Column(DateTime)
    visibility       = Column(Text, index = True)
    replaced_by      = Column(Text)

class SRAExperiment(Base):
    __tablename__ = "sra_experiment"

    accession        = Column(Text, primary_key = True)
    study_accession  = Column(Text)
    
    

def main():
    from sqlalchemy.engine import create_engine
    engine = create_engine('postgres+psycopg2://localhost/mydb')
    Base.metadata.create_all(engine)

    
if __name__ == '__main__':
    main()
