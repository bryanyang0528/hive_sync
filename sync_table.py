import argparse
from hivesync.hivemetacrawler import HiveMetaCrawler
from hivesync.hivemetawriter import HiveMetaWriter


def run(*args, **kwargs):
    src = kwargs.get('src')
    src_port = kwargs.get('srcport')
    dest = kwargs.get('dest')
    dest_port = kwargs.get('destport')
    dryrun = kwargs.get('dryrun')

    hmc_source = HiveMetaCrawler(host=src, port=src_port)
    hmc_dest = HiveMetaWriter(host=dest, port=dest_port)
    meta = hmc_source.get_all_tables

    for db in meta.keys():
        for table in meta[db]:
            try:
                script = hmc_source.get_create_table_script(db, table)
                hmc_dest.create_table(db, table, script, dryrun)
            except ValueError as e:
                print(e)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.set_defaults(func=run)
    parser.add_argument('-src', type=str, required=True, metavar='source hiveserver')
    parser.add_argument('-srcport', type=int, required=False, default=10000, metavar='port of src')
    parser.add_argument('-dest', type=str, required=True, metavar='detination hiveserver')
    parser.add_argument('-destport', type=int, required=False, default=10000, metavar='port of dest')
    parser.add_argument('-dryrun', type=bool, required=False, default=False, metavar='dry run')
    args = parser.parse_args()
    args.func(**vars(args))
