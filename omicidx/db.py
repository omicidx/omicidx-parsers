"""Class to insert accession and json

Not used right now.

Needs a config object with 'db' uri in it.
"""

import psycopg2

class OmicIDXDb(object):
    def __init__(self, config):
        self.uri = config['db']
        self.conn = None
        
    def get_conn(self, *args, **kwargs):
        if(self.conn is not None):
            return self.conn
        self.conn = psycopg2.connect(self.uri, *args, **kwargs)
        return self.conn

    def save_object(self, obj):
        con = self.get_conn()
        cur = con.cursor()
        accession = obj.accession
        json = obj.json()
        cur.execute("""insert into tbl (accession, json) values (%s, %s)
                     on conflict (accession) do update set json = tbl.json""",
                    (accession, json))
        con.commit()
            
    def create_table(self):
        con = self.get_conn()
        cur = con.cursor()
        try:
            cur.execute(
                """create table tbl (
                   accession varchar(30) primary key,
                   json JSONB)"""
            )
            con.commit()
        except:
            con.rollback()
                       
