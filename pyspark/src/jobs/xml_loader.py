from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import argparse
import logging

def read_xml(spark, in_uri, rowTag, partitions = None):
    sql = SQLContext(spark)

    xml = sql.read.format("com.databricks.spark.xml")\
        .options(rowTag = rowTag)\
        .load(in_uri)
    if(partitions is not None):
        xml = xml.repartition(partitions)
    return(xml)

def write_xml(spark, xml, out_uri):
    xml.write.mode("overwrite").parquet(out_uri)


if __name__ == '__main__':
    spark = SparkSession\
        .builder\
        .appName("XMLLoadToParquet")\
        .getOrCreate()
    parser = argparse.ArgumentParser()
    parser.add_argument('input',
                        help = "input URI")
    parser.add_argument("output",
                        help = "output URI")
    parser.add_argument('rowTag',
                        help = "rowTag")
    parser.add_argument("--partitions", "-p", type=int,
                        help = "partitions", default = None)

    opts = parser.parse_args()
    logging.info("HERRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRE")
    

    xml = read_xml(spark, opts.input, opts.rowTag, opts.partitions)
    logging.info(xml.printSchema())
    write_xml(spark, xml, opts.output)
