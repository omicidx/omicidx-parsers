import psycopg2
from psycopg2.extras import Json, execute_batch
import omicidx.sra_parsers as s
import multiprocessing
import logging
logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s', level=logging.DEBUG)


def get_record(row):
    accession = row['Accession']
    try:
        j = s.SRAExperimentPackage(s.load_experiment_xml_by_accession(accession).getroot())
        return({'experiment_accession': j.data['experiment']['accession'], 'doc': Json(j.data)})
    except Exception as e:
        import traceback
        logging.warning(traceback.format_exc())
        logging.warning("Error({}): {}".format(e,accession))
        return None

def gen():
    n = 0
    l = s.LiveList('2018-01-02',to_date="2018-01-31")
    for row in l:
        accession = row['Accession']
        try:
            j = s.SRAExperimentPackage(s.load_experiment_xml_by_accession(accession).getroot())
            n+=1
            if((n % 1000)==0):
                logging.info(str(n) + " records processed")
            yield({'experiment_accession': j.data['experiment']['accession'], 'doc': Json(j.data)})
        except KeyboardInterrupt:
            exit(-1)

def main():
    conn = psycopg2.connect(host='omicidx.cpmth1vkdqqx.us-east-1.rds.amazonaws.com',
                            user='sdavis2',
                            password='Asdf1234%',
                            dbname='omicidx')
    cur = conn.cursor()
    conn.autocommit = True

    p = multiprocessing.Pool()
    def gen():
        for x in p.imap(get_record,
                        s.LiveList('2018-01-02', to_date="2018-01-31")):
            if(x is not None):
                yield(x)
            else:
                continue

    execute_batch(cur, """INSERT INTO fsra(experiment_accession,doc) 
    VALUES (%(experiment_accession)s, %(doc)s) 
    ON CONFLICT (experiment_accession)
    DO UPDATE SET doc = %(doc)s""", gen())
    conn.commit()

if __name__ == '__main__':
    main()
