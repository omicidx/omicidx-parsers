from pyspark import SparkContext, SQLContext
import sys
import argparse
from pyspark.sql.functions import to_timestamp


def extract_experiment(base_url):
    """Read the experiment.json.gz file
    
    Usage: 
        >>> from sra.etl import extract_experiment
        >>> new_acc_info = extract_experiment(base_url)
    
    Just read in the table. 
    
    :param base_url: a String, the s3://....../ url.
    :rtype: a :class:``pyspark.sql.DataFrame``
    """
    experiment = sql.read.json(base_url + 'experiment.json.gz')
    return(experiment)



def extract_run(base_url):
    """Read the run.json.gz file
    
    Usage: 
        >>> from sra.etl import extract_run
        >>> new_acc_info = extract_run(base_url)
    
    Just read in the table. 
    
    :param base_url: a String, the s3://....../ url.
    :rtype: a :class:``pyspark.sql.DataFrame``
    """
    run = sql.read.json(base_url + "run.json.gz")
    return(run)



def extract_sample(base_url):
    """Read the sample.json.gz file
    
    Usage: 
        >>> from sra.etl import extract_sample
        >>> new_acc_info = extract_sample(base_url)
    
    Just read in the table. 
    
    :param base_url: a String, the s3://....../ url.
    :rtype: a :class:``pyspark.sql.DataFrame``
    """
    sample = sql.read.json(base_url + "sample.json.gz")
    return(sample)



def extract_study(base_url):
    """Read the study.json.gz file
    
    Usage: 
        >>> from sra.etl import extract_study
        >>> new_acc_info = extract_study(base_url)
    
    Just read in the table. 
    
    :param base_url: a String, the s3://....../ url.
    :rtype: a :class:``pyspark.sql.DataFrame``
    """
    study = sql.read.json(base_url + "study.json.gz")
    return(study)



def extract_livelist(base_url):
    """Read the livelist.csv.gz file
    
    Usage: 
        >>> from sra.etl import extract_livelist
        >>> new_acc_info = extract_livelist(base_url)
    
    Just read in the table. 
    
    :param base_url: a String, the s3://....../ url.
    :rtype: a :class:``pyspark.sql.DataFrame``
    """
    ll = (sql
          .read
          .format('csv')
          .options(header=True)
          .load(base_url + "livelist.csv.gz"))
    return(ll)



def extract_addons(base_url):
    """Read in the addons table
    """
    addons = (sql
              .read
              .format('csv')
              .options(header=True)
              .load(base_url + "fileinfo_addons.csv.gz").repartition(25))
    addons = addons.withColumn('FileSize', addons.FileSize.cast("integer"))
    addons = addons.withColumn("FileDate", to_timestamp("FileDate", "yyyy-MM-dd HH:mm:ss"))
    return addons


def extract_accession_info(base_url):
    """Read the SRA_Accessions.tab.gz file table
    
    Usage: 
        >>> from sra.etl import extract_accession_info
        >>> new_acc_info = extract_accession_info(base_url)
    
    Just read in the table. 
    
    :param base_url: a String, the s3://....../ url.
    :rtype: a :class:``pyspark.sql.DataFrame``
    """
    accession_info = sql.read.format('csv').options(header=True).options(delimiter="\t")\
                                                                .load(base_url + "SRA_Accessions.tab.gz")
    return(accession_info)



def transform_run(run, accession_info):
    """Transform the run info table
    
    Usage: 
        >>> from sra.etl import transform_run
        >>> new_acc_info = transform_run(run, accession_info)
    
    Fixes date and joins to accession_info

    :param accession_info: a :class:``pyspark.sql.DataFrame``, 
        typically deriving from ``transform_accession_info``
    :param run: a :class:``pyspark.sql.DataFrame``, 
        typically deriving from ``extract_run``
    :rtype: a :class:``pyspark.sql.DataFrame``
    """
    run = (
        run
        .withColumn("run_date", to_timestamp("run_date", "yyyy-MM-dd HH:mm:ss")))
    run = (
        run
        .join(accession_info, run.accession==accession_info.accinfo_accession, "left")
        .drop("accinfo_accession"))
    return(run)



def transform_accession_info(accession_info):
    """Transform the SRA_Accessions.tab table
    
    Usage: 
        >>> from sra.etl import transform_accession_info
        >>> new_acc_info = transform_accession_info(acc_info)
    
    In particular, do some name cleanup and cast
    numbers to long. Also, rename the accession
    column to facilitate joining later
    :param accession_info: a :class:``pyspark.sql.DataFrame``, 
        typically deriving from ``extract_accession_info``
    :rtype: a :class:``pyspark.sql.DataFrame``
    """
    accession_info = (
        accession_info
            .select("Accession", "Spots", "Bases")
            .withColumnRenamed('Bases','bases')
            .withColumnRenamed('Spots','spots')
            .withColumnRenamed('Accession','accinfo_accession'))
    accession_info = accession_info.withColumn("bases", accession_info.bases.cast('long'))
    accession_info = accession_info.withColumn("spots", accession_info.spots.cast('long'))
    return(accession_info)


def transform_livelist(ll):
    """Transform the SRA_Accessions.tab table
    
    Usage: 
        >>> from sra.etl import transform_livelist
        >>> new_ll = transform_livelist(ll)
    
    :param accession_info: a :class:``pyspark.sql.DataFrame``, 
        typically deriving from ``extract_livelist``
    :rtype: a :class:``pyspark.sql.DataFrame``
    """
    datecols = "Received Published LastUpdate LastMetaUpdate".split()
    for c in datecols:
        ll = ll.withColumn(c, to_timestamp(c, "yyyy-MM-dd HH:mm:ss"))
    ll = (ll
          .withColumn('Insdc', (ll.Insdc == "True").cast("boolean"))
          .withColumnRenamed("Accession", "livelist_accession")
          .drop("BioProject")
          .drop("BioSample")
          .drop("Type"))
    return ll



def transform_study(study, ll):
    """Transform the study info table
    
    Usage: 
        >>> from sra.etl import transform_study
        >>> new_study = transform_study(study, ll)
    
    Joins study to livelist

    :param ll: a :class:``pyspark.sql.DataFrame``, 
        typically deriving from ``transform_livelist``
    :param study: a :class:``pyspark.sql.DataFrame``, 
        typically deriving from ``extract_study``
    :rtype: a :class:``pyspark.sql.DataFrame``
    """
    study = (study
             .join(ll, ll.livelist_accession == study.accession, "left")
             .drop("livelist_accession"))
    return study

def write_json(df, entity_name, outdir, overwrite = True, gzip = True):
    writer = df.write
    if(overwrite):
        writer.mode('overwrite')
    if(gzip):
        writer.option('compression', 'gzip')
    writer.json(outdir + 'json/{}_json/'.format(entity_name))

    
    
def write_parquet(df, entity_name, outdir, overwrite = True):
    writer = df.write
    if(overwrite):
        writer.mode('overwrite')
    writer.parquet(outdir + 'parquet/{}_parquet/'.format(entity_name))

    
def main(sql, base_url, outdir):
    experiment = extract_experiment(base_url)
    run = extract_run(base_url)
    accession_info = extract_accession_info(base_url)
    accession_info = transform_accession_info(accession_info)

    ll = extract_livelist(base_url)
    ll = transform_livelist(ll)

    study = extract_study(base_url)
    study = transform_study(study, ll)
    study.printSchema()
    
    run = transform_run(run, accession_info)
    run.printSchema()
    
    sample = extract_sample(base_url)
    sample.printSchema()

    addons = extract_addons(base_url)
    addons.printSchema()

    #metasra = sql.read.parquet('s3n://omics_metadata/metasra/v1.4/metasra_parquet/')
    #sample = sample.join(metasra, metasra.sample_accession == sample.accession, "left").drop("sample_accession")

    experiment = experiment.join(ll, experiment.accession == ll.livelist_accession, "left").drop("livelist_accession")
    experiment = experiment\
                 .withColumn('library_layout_length',experiment.library_layout_length.cast('double'))\
                 .withColumn('library_layout_length',experiment.library_layout_sdev.cast('double'))\
                 .drop('experiment_accession')
    experiment.cache()
    
    sample = sample.join(ll, ll.livelist_accession == sample.accession, "left").drop("livelist_accession")
    sample.cache()
    
    run = run.join(ll, run.accession == ll.livelist_accession, "left").drop("livelist_accession")
    run.cache()

    from pyspark.sql.functions import struct, col, collect_list
    nested_addons = addons.select(addons.Accession.alias("nested_accession"), struct([col(c) for c in addons.drop("Accession").columns])\
                                  .alias("file_addons"))\
                          .groupBy("nested_accession")\
                          .agg(collect_list('file_addons')\
                               .alias('file_addons'))
    r1 = run.join(nested_addons,run.accession == nested_addons.nested_accession, "left").drop("nested_accession")

    runinfo = sql.read.format('csv').options(header=True).load(base_url + "fileinfo_runs.csv.gz").repartition(100)
    runinfo = runinfo.withColumn('FileSize', runinfo.FileSize.cast("integer"))
    runinfo = runinfo.withColumn("FileDate", to_timestamp("FileDate", "yyyy-MM-dd HH:mm:ss"))

    r2 = r1.join(runinfo, runinfo.Accession == r1.run_accession, "left").drop("Accession").withColumnRenamed('run_accession', 'accession')

    nested_experiment = experiment.select(experiment.accession.alias("nested_accession"), struct([col(c) for c in experiment.drop("accession").columns]).alias("experiment"))
    r3 = r2.join(nested_experiment, nested_experiment.nested_accession == r2.experiment_accession, "left").drop("nested_accession")
    nested_sample = sample.select(sample.accession.alias("nested_accession"), struct([col(c) for c in sample.drop("accession").columns]).alias("sample"))
    r4 = r3.join(nested_sample, nested_sample.nested_accession == r3.experiment.sample_accession, "left").drop("nested_accession")
    nested_study = study.select(study.accession.alias("nested_accession"), struct([col(c) for c in study.drop("accession").columns]).alias("study"))
    r5 = r4.join(nested_study, nested_study.nested_accession == r3.experiment.study_accession, "left").drop("nested_accession")

    r5.write.options(compression="gzip").mode("overwrite").json(outdir + "run_centered_json/")

    # 
    # PARQUET
    #
    experiment.write.mode("overwrite").parquet(outdir + 'parquet/experiment_parquet')
    sample.write.mode("overwrite").parquet(outdir + 'parquet/sample_parquet')
    # includes file addons
    r2.write.mode("overwrite").parquet(outdir + 'parquet/run_parquet')
    study.write.mode("overwrite").parquet(outdir + 'parquet/study_parquet')
    write_parquet(experiment, 'experiment', outdir)
    write_parquet(sample, 'sample', outdir)
    write_parquet(study, 'study', outdir)
    write_parquet(r2, 'run', outdir)
    
    # 
    # JSON
    #
    #experiment.write.mode("overwrite").json(outdir + 'json/experiment_json')
    write_json(experiment, 'experiment', outdir)
    write_json(sample, 'sample', outdir)
    write_json(study, 'study', outdir)
    write_json(r2, 'run', outdir)

    
def create_spark(appName = "sra_etl"):
    return SparkContext(appName = appName)

def create_sql(sc):
    return SQLContext(sc)
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('jsonbase',
                        help="The location of the preliminary json and csv files")
    parser.add_argument("outputdir",
                        help="The URI base of the output files from transformation")
    args = parser.parse_args()
    sc = SparkContext(appName="omicidx")
    sql = SQLContext(sc)
    main(sql, base_url = args.jsonbase, outdir = args.outputdir)
    sc.stop()
