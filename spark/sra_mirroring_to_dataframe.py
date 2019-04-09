import os
from pyspark import SparkContext, SQLContext, Row
import urllib.request
import io
import gzip
import omicidx.sra_parsers as s
import xml.etree.ElementTree as et

def as_row(obj):
    if isinstance(obj, dict):
        dictionary = {k: as_row(v) for k, v in obj.items()}
        return Row(**dictionary)
    elif isinstance(obj, list):
        return [as_row(v) for v in obj]
    else:
        return obj

def element_iterator(base_uri, sra_parser, entity):
    with io.StringIO(gzip.decompress(urllib.request.urlopen(os.path.join(base_uri, 'meta_experiment_set.xml.gz')).read()).decode('UTF-8')) as f:
        for event, element in et.iterparse(f):
            if(event == 'end' and element.tag == entity):
                r = as_row(sra_parser(element).data)
                print(r)
                yield(r)
                
    
def main(sql, base_url):
    df = sql.createDataFrame(element_iterator(base_url, s.SRAExperimentRecord, 'EXPERIMENT'))
    df.printSchema()
    
        
                        

if __name__ == "__main__":
    sc = SparkContext(appName="omicidx_mirroring_to_spark")
    sql = SQLContext(sc)
    main(sql, base_url = "https://ftp.ncbi.nlm.nih.gov/sra/reports/Mirroring/NCBI_SRA_Mirroring_20180312/")
    sc.stop()
