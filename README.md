# hive_sync
Sync tables between two hive metastore through hiveserver

## Usage

*usage*: python sync_table.py [-h] -src source hiveserver [-srcport port of src] -dest
 detination hiveserver [-destport port of dest]
 [-dryrun dry run]

*optional arguments*:
 
 -h, --help show this help message and exit
 
 -src source hiveserver
 
 -srcport port of src
 
 -dest detination hiveserver
 
 -destport port of dest
 
 -dryrun dry run

## Example

`python sync_table.py --src 172.17.0.2 --dest localhost --dryrun True`

