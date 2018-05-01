from pyspark import SparkContext, SQLContext
import sys
import argparse

def main(sql, base_url, outdir):
    experiment = sql.read.json(base_url + 'experiment.json.bz2')

    from pyspark.sql.functions import to_timestamp
    run = sql.read.json(base_url + "run.json.bz2")
    run = run.withColumn("run_date", to_timestamp("run_date", "yyyy-MM-dd HH:mm:ss"))

    study = sql.read.json(base_url + "study.json.bz2")

    sample = sql.read.json(base_url + "sample.json.bz2")
    metasra = sql.read.parquet('s3n://omics_metadata/metasra/v1.4/metasra_parquet/')
    sample = sample.join(metasra, metasra.sample_accession == sample.accession, "left").drop("sample_accession")

    from pyspark.sql.functions import to_timestamp
    datecols = "Received Published LastUpdate LastMetaUpdate".split()
    ll = sql.read.format('csv').options(header=True).load(base_url + "livelist.csv.gz").repartition(25)
    for c in datecols:
        ll = ll.withColumn(c, to_timestamp(c, "yyyy-MM-dd HH:mm:ss"))
        ll = ll.withColumn('Insdc', (ll.Insdc == "True").cast("boolean"))
        ll = ll.withColumnRenamed("Accession", "livelist_accession").drop("BioProject").drop("BioSample").drop("Type")

    study = study.join(ll, ll.livelist_accession == study.study_accession, "left").drop("livelist_accession")
    study.cache()

    experiment = experiment.join(ll, experiment.accession == ll.livelist_accession, "left").drop("livelist_accession")
    experiment = experiment\
                 .withColumn('library_layout_length',experiment.library_layout_length.cast('float'))\
                 .withColumn('library_layout_length',experiment.library_layout_sdev.cast('float'))
    experiment.cache()
    
    sample = sample.join(ll, ll.livelist_accession == sample.accession, "left").drop("livelist_accession")
    sample.cache()
    
    run = run.join(ll, run.accession == ll.livelist_accession, "left").drop("livelist_accession")
    run.cache()

    addons = sql.read.format('csv').options(header=True).load(base_url + "fileinfo_addons.csv.gz").repartition(25)
    addons = addons.withColumn('FileSize', addons.FileSize.cast("integer"))
    addons = addons.withColumn("FileDate", to_timestamp("FileDate", "yyyy-MM-dd HH:mm:ss"))

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

    r2 = r1.join(runinfo, runinfo.Accession == r1.run_accession, "left").drop("Accession")

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
    run.write.mode("overwrite").parquet(outdir + 'parquet/run_parquet')
    study.write.mode("overwrite").parquet(outdir + 'parquet/study_parquet')

    # 
    # JSON
    #
    experiment.write.mode("overwrite").json(outdir + 'json/experiment_json')
    sample.write.mode("overwrite").json(outdir + 'json/sample_json')
    run.write.mode("overwrite").json(outdir + 'json/run_json')
    study.write.mode("overwrite").json(outdir + 'json/study_json')

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
