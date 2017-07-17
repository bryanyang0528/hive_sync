from impala.dbapi import connect

class HiveMetaCrawler():
    def __init__(self, *args, **kwargs):
        self.conn = connect(auth_mechanism='PLAIN', *args, **kwargs)
        self.cursor = self.conn.cursor()
        self.meta = self._get_all_tables

    @property
    def _get_databases(self):
        meta={}
        self.cursor.execute("show databases")
        result = self.cursor.fetchall()
        for db in result:
            if db[0] not in meta.keys():
                meta[db[0]] = {}

        return meta

    def get_databases(self):
        return self.meta.keys()

    def _get_tables(self, db):
        tables = []
        self.cursor.execute("use {}".format(db))
        self.cursor.execute("show tables")
        result = self.cursor.fetchall()
        
        for table in result:
            tables.append(table[0])
        
        return tables

    def get_tables(self, db):
        if db not in self._get_databases:
            raise ValueError("There is no database named {}.".format(db))
        else:
            return self.meta[db].keys()

    def get_create_table_script(self, db, table):
        self.cursor.execute("SHOW CREATE TABLE {}.{}".format(db, table))
        result = self.cursor.fetchall()
        script=[]
        for line in result:
            if 'TBLPROPERTIES' in line[0]:
                break
            else:
                script.append(line[0])
        
        script=''.join(script)
        self.meta[db][table] = script

        return self.meta[db][table]

    @property
    def _get_all_tables(self):
        meta = {}
        for db in self._get_databases:
            meta[db] = {}
            tables = self._get_tables(db)
            for table in tables:
                meta[db][table] = {}

        return meta

    @property
    def get_all_tables(self):
        result = {}
        for db in self.meta.keys():
            result[db] = []
            for table in self.meta[db].keys():
                result[db].append(table)
        return result

    @property
    def _get_all_create_table_scripts(self):
        for db in self.meta.keys():
            for table in self.meta[db].keys():
                self.get_create_table_script(db, table)

        return self.meta

