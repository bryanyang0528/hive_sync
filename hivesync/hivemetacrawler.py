from impala.dbapi import connect

class HiveMetaCrawler():
    def __init__(self, *args, **kwargs):
        self.conn = connect(auth_mechanism='PLAIN', *args, **kwargs)
        self.cursor = self.conn.cursor()
        self.meta = {}
        
    def get_databases(self):
        self.cursor.execute("show databases")
        result = self.cursor.fetchall()
        for db in result:
            if db[0] not in self.meta.keys():
                self.meta[db[0]] = {}
        
        return self.meta
