from sqlalchemy import Integer, String, Column, Boolean, BigInteger, ForeignKey, Numeric, TIMESTAMP
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.dialects import postgresql

Base = declarative_base()

def compile_query(query):
    """Via http://nicolascadou.com/blog/2014/01/printing-actual-sqlalchemy-queries"""
    compiler = query.compile if not hasattr(query, 'statement') else query.statement.compile
    return compiler(dialect=postgresql.dialect())


def upsert(session, model, rows, no_update_cols=[]):
    table = model.__table__

    print(rows)
    print(model)
    
    #stmt = insert(table).values(**rows[0])

    #print(stmt)
    #print(stmt.excluded)

    update_cols = [c for c in rows[0].keys() #table.c
                   if c not in list(table.primary_key.columns)
                   and c not in no_update_cols
                   and c in [c.name for c in table.c]
    ]

    print("updatecols:", sorted(update_cols))
    print("row_keys:  ", list(sorted(rows[0].keys())))

    on_conflict_stmt = stmt.on_conflict_do_nothing(
        index_elements=table.primary_key.columns
#        set_={k: getattr(stmt.excluded, k) for k in update_cols}
        )

    print(compile_query(on_conflict_stmt))

    session.execute(on_conflict_stmt)

class SRAProvenance(Base):
    __abstract__ = True
    
    status             = Column(String)
    updated            = Column(TIMESTAMP, index=True)
    published          = Column(TIMESTAMP, index=True)
    received           = Column(TIMESTAMP, index=True)
    visibility         = Column(String)
    replaced_by        = Column(String)
    
    
class SRAStudy(SRAProvenance):
    __tablename__ = 'sra_study'

    bio_project        = Column(String, index=True)
    gse                = Column(String, index=True)
    abstract           = Column(String)
    accession          = Column(String, primary_key = True)
    alias              = Column(String, index=True)
    attributes         = Column(JSONB)
    broker_name        = Column(String)
    center_name        = Column(String)
    description        = Column(String)
    study_type         = Column(String, index=True)
    title              = Column(String, index=True)
    xrefs              = Column(JSONB)

#    experiments        = relationship('SRAExperiment')


class SRAExperiment(SRAProvenance):
    __tablename__ = 'sra_experiment'
    
    accession          = Column(String, primary_key=True)
    alias              = Column(String, index=True)
    attributes         = Column(JSONB)
    broker_name        = Column(String)
    center_name        = Column(String)
    description        = Column(String)
    design             = Column(String, index=True)
    identifiers        = Column(JSONB)
    instrument_model   = Column(String, index=True)
    library_construction_protocol = Column(String)
    library_layout     = Column(String, index=True)
    library_layout_orientation = Column(String, index=True)
    library_layout_length = Column(String, index=True)
    library_layout_sdev = Column(String)
    library_name       = Column(String)
    library_selection  = Column(String, index=True)
    library_source     = Column(String, index=True)
    library_strategy   = Column(String, index=True)
    platform           = Column(String, index=True)
    study_accession    = Column(String, index=True) #, ForeignKey('sra_study.accession'))
#    submitter_id       = Column(JSONB)
    title              = Column(String, index=True)

#    study              = relationship("SRAStudy", back_populates="experiments")
#    runs               = relationship('SRARun')

class SRARun(SRAProvenance):
    __tablename__ = 'sra_run'

    accession = Column(String, primary_key = True)
    attributes         = Column(JSONB)
    alias              = Column(String, index=True)
    center_name        = Column(String, index=True)
    description        = Column(String)
    experiment_accession = Column(String) #, ForeignKey('sra_experiment.accession'))
    nreads             = Column(Integer)
    published          = Column(String)
    reads              = Column(JSONB)
    run_center         = Column(String)
    run_date           = Column(String)
    size               = Column(BigInteger)
    submitter_id       = Column(JSONB)
    title              = Column(String)
    tax_analysis       = Column(JSONB)
    total_bases        = Column(BigInteger)
    total_spots        = Column(BigInteger)

#    experiment         = relationship("SRAExperiment", back_populates="runs")
    
class SRASample(SRAProvenance):
    __tablename__ = 'sra_sample'

    BioSample          = Column(String)
    accession          = Column(String, primary_key = True)
    alias              = Column(String)
    attributes         = Column(JSONB)
    center_name        = Column(String)
    description        = Column(String)
    external_id        = Column(JSONB)
    organism           = Column(String)
    sample_accession   = Column(String)
    submitter_id       = Column(JSONB)
    taxon_id           = Column(Integer)
    title              = Column(String)


def get_x(row):
    x = s.SRAExperimentPackage(s.load_experiment_xml_by_accession(row['Accession']).getroot())
    print(row)
    return x
    
    
if __name__ == '__main__':
    from sqlalchemy import create_engine
    engine = create_engine('postgresql+psycopg2://sdavis2:Asdf1234%@omicidx.cpmth1vkdqqx.us-east-1.rds.amazonaws.com/omicidx', echo=True)
    try:
        Base.metadata.drop_all(engine)
    except:
        pass
    Base.metadata.create_all(engine)
    from sqlalchemy.orm import sessionmaker
    import logging

    logging.basicConfig(level = logging.ERROR)
    Session = sessionmaker(engine)
    session = Session()
    from omicidx import sra_parsers as s

    import gzip
    import xml.etree.ElementTree as et

    def gen_exp():
        with gzip.GzipFile('/Users/sdavis2/Downloads/NCBI_SRA_Mirroring_20180301_Full/meta_experiment_set.xml.gz') as f:
            for event, element in et.iterparse(f):
                if(element.tag == 'EXPERIMENT'):
                    x = s.SRAExperimentRecord(element).data
                    yield(x)

    n = 0
    rows = []
    for row in gen_exp():
        rows.append(row)
        n+=1
        if((n % 1000) == 0):
            session.execute(SRAExperiment.__table__.insert(), rows)
            session.commit()
            rows=[]
    session.execute(SRAExperiment.__table__.insert(), rows)
    session.commit()

    def gen_study():
        with gzip.GzipFile('/Users/sdavis2/Downloads/NCBI_SRA_Mirroring_20180301_Full/meta_study_set.xml.gz') as f:
            for event, element in et.iterparse(f):
                if(element.tag == 'STUDY'):
                    x = s.SRAStudyRecord(element).data
                    yield(x)

    n = 0
    rows = []
    for row in gen_study():
        rows.append(row)
        n+=1
        if((n % 1000) == 0):
            session.execute(SRAStudy.__table__.insert(), rows)
            session.commit()
            rows=[]
    session.execute(SRAStudy.__table__.insert(), rows)
    session.commit()

        #for e in session.query(SRAExperiment):
    #    print(e.runs)
    #for s in session.query(SRAStudy):
    #    print(s.experiments)
    #for r in session.query(SRARun):
    #    print(r.experiment, r.accession)
