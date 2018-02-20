from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.functions import to_timestamp

def convert_col_to_timestamp(df, column):
    return(df.withColumn(column, to_timestamp(column, 'yyyy-MM-dd HH:mm:ss')))

def load_experiment(spark, root):
    sql = SQLContext(spark)
    experiment = sql\
        .read\
        .format('com.databricks.spark.xml')\
        .options(rowTag = "EXPERIMENT")\
        .load(root+'meta_experiment_set.xml')
    return(experiment)
    

def convert_experiment(experiment):
    from pyspark.sql.types import (
        ArrayType, LongType, StringType, StructField, StructType)
    struct_schema = ArrayType(StructType([
        StructField("tag", StringType()),
        StructField("value", StringType())
    ]))
    experiment2 = experiment.select(col('_accession').alias("experiment_accession"), 
                                    col("DESIGN.DESIGN_DESCRIPTION").alias("design_description"),   
                                    col("DESIGN.LIBRARY_DESCRIPTOR.LIBRARY_CONSTRUCTION_PROTOCOL").alias("library_construction_protocol"),
                                    col("DESIGN.LIBRARY_DESCRIPTOR.LIBRARY_NAME").alias("library_name"),
                                    col("DESIGN.LIBRARY_DESCRIPTOR.LIBRARY_SELECTION").alias("library_selection"),
                                    col("DESIGN.LIBRARY_DESCRIPTOR.LIBRARY_SOURCE").alias("library_source"),
                                    col("DESIGN.LIBRARY_DESCRIPTOR.LIBRARY_STRATEGY").alias("library_strategy"),
                                    col("DESIGN.SAMPLE_DESCRIPTOR._accession").alias("sample_accession"),
                                    col("TITLE").alias("experiment_title"),
                                    col("_alias").alias("experiment_alias"),
                                    col("_broker_name").alias("broker_name"),
                                    col("_center_name").alias("center_name"),
                                    col("EXPERIMENT_ATTRIBUTES.EXPERIMENT_ATTRIBUTE").cast(struct_schema).alias('experiment_attributes'))
    return(experiment2)

def main(spark, root):
    sql = SQLContext(spark)
    livelist = sql.read.options(header="true").csv(root + 'livelist.csv')
    livelist.show()
    ts_cols = ["Received", "Published", "LastUpdate", "LastMetaUpdate"]
    for i in ts_cols:
        livelist = convert_col_to_timestamp(livelist, i)
    livelist.printSchema()


if __name__=="__main__":
    spark = SparkSession\
        .builder\
        .appName("PythonKMeans")\
        .getOrCreate()
    root = "../../../data/NCBI_SRA_Mirroring_20180202/"
    main(spark, root)
    e = load_experiment(spark, root)
    e2 = convert_experiment(e)
    e2.write.mode("overwrite").json('/tmp/experiment2/')
    e2.printSchema()
