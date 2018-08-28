import pyspark

sc = pyspark.SparkContext(appName="test_jupyterlab")
from pyspark import SQLContext
sql = SQLContext(sc)
base_url = "s3://omicidx.cancerdatasci.org/sra/NCBI_SRA_Mirroring_20180801_Full/"
def get_sra_dfs(base_url):
    """Read sra data from parquet (stored previously)
    
    :param: base_url the url of the parquet directory
    
    Returns: a dict with keys of entities ("experiment", "sample", etc.)
    """
    entities = "experiment sample run study".split()
    import os
    return dict((n, sql.read.parquet(os.path.join(base_url,"parquet","{}_parquet".format(n)))) for n in entities)
sra_dfs = get_sra_dfs(base_url)
def prepend_colnames(df, prepend, sep = "_"):
    for c in df.columns:
        if(c.startswith(prepend)):
            continue
        if(c.endswith('accession') & (c!='accession')):
            continue
        df = df.withColumnRenamed(c, prepend + sep + c)
    return(df)
e = prepend_colnames(sra_dfs['experiment'].drop('experiment_accession'), 'experiment')
r = prepend_colnames(sra_dfs['run'], 'run')
st = prepend_colnames(sra_dfs['study'].drop('study_accession'), 'study')
sa = prepend_colnames(sra_dfs['sample'], 'sample')
x=r.join(e, 'experiment_accession', 'left')   .join(st, 'study_accession', 'left')   .join(sa, 'sample_accession', 'left')   .cache()
# may need to change last line to: .save('sra_full2/doc', overwrite="true") 
# if the index exists and want to overwrite. Note that this will remove
# all index entries prior to writing.
x.write.format("org.elasticsearch.spark.sql")    .option("es.nodes.wan.only","true")    .option("es.net.http.auth.user","omicidx")    .option("es.net.http.auth.pass","Asdf1234%")    .option("es.net.ssl","true")    .option("es.nodes","c0b1083b4d78495d3b29a72d74fcea41.us-east-1.aws.found.io")    .option("es.port","9243")    .option("es.mapping.id", "run_accession")    .save('sra_full/doc', mode='overwrite')
sra_dfs['study'] = sra_dfs['study'].drop('study_accession')
sra_dfs['experiment'] = sra_dfs['experiment'].drop('experiment_accession')
for i in sra_dfs.keys():
    print(i)
    sra_dfs[i].write.format("org.elasticsearch.spark.sql")        .option("es.nodes.wan.only","true")        .option("es.net.http.auth.user","omicidx")        .option("es.net.http.auth.pass","Asdf1234%")        .option("es.net.ssl","true")        .option("es.nodes","c0b1083b4d78495d3b29a72d74fcea41.us-east-1.aws.found.io")        .option("es.port","9243")        .option("es.mapping.id", "accession")        .save('sra_{}/doc'.format(i), mode='overwrite')
