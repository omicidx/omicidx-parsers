from google.cloud import bigquery
client = bigquery.Client()

def make_final_table(entity, project='isb-cgc-01-0006', src_dataset='omicidx_etl', dest_dataset='omicidx'):
    try:
        client.query('drop table {}.{}'.format(dest_dataset, entity))
        result = query_job.result()
    except:
        pass

    #       s.* except ({}_accession)
    query = """
    create table {}.{} as
SELECT
      l.Status as status,
      l.LastUpdate as update_date,
      l.LastMetaUpdate as meta_update_date,
      l.Published as publish_date,
      l.Received as received_date,
      l.Insdc as insdc,
      s.* 
    FROM `isb-cgc-01-0006.{}.{}` s
    JOIN `isb-cgc-01-0006.{}.livelist` l on l.accession=s.accession;""".format(dest_dataset,entity,src_dataset, entity, src_dataset)
    query_job = client.query(query)
    result = query_job.result()
    print('created table {}.{} with {} rows'.format(dest_dataset, entity, result.total_rows))
    try:
        client.query('alter table {}.{} drop column {}_accession'.format(dest_dataset, entity, entity))
        result = query_job.result()
    except:
        pass
    

def main():
    for i in 'study sample run experiment'.split():
        make_final_table(i)

if __name__ == '__main__':
    main()
