try:
    import pyspark
except:
    import findspark
    findspark.init()
    import pyspark
from pyspark import SparkContext, SQLContext, SparkConf
from pyspark.sql.functions import col, struct, collect_list

def get_spark():
    return(SparkContext())

def get_sql(sc):
    return(SQLContext(sc))

def all_entity_names():
    return 'study experiment sample run'.split()
    
def load_base_data(entity):
    return sql.read.parquet('s3://omicidx.cancerdatasci.org/sra/NCBI_SRA_Mirroring_20180802/parquet/{}_parquet/'.format(entity))

def create_nested_dataframe(df, entity):
    """Create nested dataframe based on accession
    
    This function creates a nested data frame with one
    struct column and one key column.
    
    Parameters
    ----------
    df : a pyspark data frame
        The "accession" column is required and is assumed to be
        unique.
    entity : string
        The name of the entity (eg., experiment, study). The 
        nested column will have that name.
        
    Returns
    -------
    A data frame with two columns named <entity>_accession (string)
    and <entity> that is a struct column containing the original data
    frame.
    """
    nested_df = (df
                 .select(df
                         .accession
                         .alias('{}_accession'.format(entity)),
                         struct([col(x) for x in df.columns]).alias(entity)))
    return nested_df


def main(sql):
#    sc = get_spark()
#    sql = get_sql(sc)
    df_map = {}
    for entity in all_entity_names():
        df_map[entity] = load_base_data(entity)
    df_nested = {}
    for entity in all_entity_names():
        df_nested[entity] = create_nested_dataframe(df_map[entity], entity)
    run_joined = (df_map['run']
                  .drop("run_accession")
                  .join(df_nested["experiment"], "experiment_accession")
                  .drop("experiment_accession")
                  .join(df_nested["sample"], col("experiment.sample_accession")==col("sample_accession"))
                  .drop("sample_accession")
                  .join(df_nested["study"], col("experiment.study_accession")==col("study_accession"))
                  .drop("study_accession"))
    experiment_joined = (df_map['run']
                         .groupBy('experiment_accession')
                         .agg(collect_list(struct([col(x) for x in df_map['run'].columns])).alias('runs'))
                         .join(df_map['experiment'], df_map['experiment'].accession==df_map['run'].experiment_accession, how='right_outer')
                         .drop('experiment_accession')
                         .join(df_nested["sample"], "sample_accession", how='left_outer')
                         .drop("sample_accession")
                         .join(df_nested["study"], "study_accession", how='left_outer')
                         .drop("study_accession"))
    experiment_joined.printSchema()
    run_joined.printSchema()
    
    with open('run_joined_schema.json', 'w') as f:
        f.writelines(run_joined.schema.json())
    with open('experiment_joined_schema.json', 'w') as f:
        f.writelines(experiment_joined.schema.json())
 
if __name__ == "__main__":
#    parser = argparse.ArgumentParser()
#    parser.add_argument('jsonbase',
#                        help="The location of the preliminary json and csv files")
#    parser.add_argument("outputdir",
#                        help="The URI base of the output files from transformation")
#    args = parser.parse_args()
    sc = SparkContext(appName="sra_make_nested_data")
    sql = SQLContext(sc)
    main(sql) #, base_url = args.jsonbase, outdir = args.outputdir)
    sc.stop()
