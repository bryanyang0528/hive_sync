from impala.dbapi import connect

class HiveMetaCrawler():
    def __init__(self, *args, **kwargs):
        self.conn = connect(auth_mechanism='PLAIN', *args, **kwargs)
        self.cursor = self.conn.cursor()
        self.meta = None
    
    @property
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
            if 'TBLPROPERTIES' in line[0]:
                break
            else:
                script.extend(line[0])
            
        script=''.join(script)
        self.meta[db][table] = script

        return self.meta[db][table]

    @property
    def get_all_tables(self):
        self.get_databases
        for db in self.meta.keys():
            self.get_tables(db)
        
        return self.meta

    @property
    def get_all_create_table_scripts(self):
        self.get_databases
        self.get_all_tables
        for db in self.meta.keys():
            for table in self.meta[db].keys():
                self.get_create_table_script(db, table)

        return self.meta

