from impala.dbapi import connect
from hivesync.hivemetacrawler import HiveMetaCrawler

class HiveMetaWriter(HiveMetaCrawler):
    

    def _if_db_exits(self, db):
        dbs = self.get_databases()
        if db in dbs:
            return True
        else:
            return False

    def _if_table_exits(self, db, table):
        if self._if_db_exits(db):
            tables = self.get_tables(db)
            if table in tables:
                return True
            else:
                return False
        else:
            raise ValueError("This db {} does not exit.".format(db))

    def create_table(self, db, table, script, dryrun=False):
        if self._if_table_exits(db, table):
            raise ValueError("This table {}.{} exits.".format(db, table))
        else:
            if dryrun:
                result = {}
                result[db] = {}
                result[db][table] = script
                return result
            else:
                self.cursor.execute(script)
                result = self.cursor.fetchall()
                return result

