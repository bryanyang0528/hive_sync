from impala.dbapi import connect

class HiveMetaCrawler():
    def __init__(self, *args, **kwargs):
        self.conn = connect(auth_mechanism='PLAIN', *args, **kwargs)
        self.cursor = self.conn.cursor()
        self.meta = None
        
    def get_databases(self):
        if not self.meta:
            self.meta={}
            self.cursor.execute("show databases")
            result = self.cursor.fetchall()
            for db in result:
                if db[0] not in self.meta.keys():
                    self.meta[db[0]] = {}
        return self.meta

    def get_tables(self, db):
        if db not in self.meta.keys():
            self.get_databases()
            if db not in self.meta.keys():
                raise ValueError("There is no database named {}.".format(db))
        
        self.cursor.execute("use {}".format(db))
        self.cursor.execute("show tables")
        result = self.cursor.fetchall()
        for table in result:
            self.meta[db][table[0]] = {}

        return self.meta[db]

    def get_create_table_script(self, db, table):
        self.cursor.execute("SHOW CREATE TABLE {}.{}".format(db, table))
        result = self.cursor.fetchall()
        script=[]
        for line in result:
            if 'TBLPROPERTIES' not in line[0] and 'transient_lastDdlTime' not in line[0]:
                script.extend(line[0])

        return ''.join(script)

