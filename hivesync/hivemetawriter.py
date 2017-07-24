from impala.dbapi import connect
from hivesync.hivemetacrawler import HiveMetaCrawler
import time

class HiveMetaWriter(HiveMetaCrawler):
    
    def _if_db_exits(self, db):
        if db in self.meta.keys():
            return True
        else:
            return False

    def _if_table_exits(self, db, table):
        if self._if_db_exits(db):
            if table in self.meta[db].keys():
                return True
            else:
                return False
        else:
            raise ValueError("[ERROR] This db {} does not exist.".format(db))

    def if_table_exits(self, db, table):
        if self._if_table_exits(db, table):
            return True
        else:
            return False

    def create_table(self, db, table, script, dryrun=False):
        if self._if_table_exits(db, table):
            raise ValueError("[ERROR] This table {}.{} exists.".format(db, table))
        else:
            if dryrun:
                result = {}
                result[db] = {}
                result[db][table] = script
                return result
            else:
                self.cursor.execute(script)
                self.cursor.execute("MSCK REPAIR TABLE {}.{}".format(db, table))
                print("[INFO] Table {}.{} has been created successfully.".format(db, table))
                return True

    def repair_partition(self, db, table):
        if self._if_table_exits(db, table):
            try:
                self.cursor.execute("MSCK REPAIR TABLE {}.{}".format(db, table))
                time.sleep(5)
                print("[INFO] Table {}.{} has been updated successfully.".format(db, table))
                return True
            except:
                print("[ERROR] This table {}.{} can not be updated.".format(db, table))
        else:
            raise ValueError("[ERROR] This table {}.{} does not exists.".format(db, table))
