from collectos import *
from database import *
import argparse
session_name = "cluster_1"
db_name = "cluster_1_DB"
collectos =[FRA, NLI, STO, SER, AMS, MOS]

import bgpBuilder as bgpb
import log

if __name__ == '__main__':
    global session_name
    global db_name

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--start", help="startTime", default=1438417216)
    parser.add_argument("-e", "--end", help="endTime", default=1438417216)
    parser.add_argument("-c", "--chunks", help="chunks", default=2)
    parser.add_argument("-db", "--database", help="DB name")
    parser.add_argument("-se", "--session", help="Session name")
    parser.add_argument("-t", "--transform", help="Transform database", default=0)

    args = parser.parse_args()

    startTime = int(args.start)
    endTime = int(args.end)
    chunks = int(args.chunks)
    transform = int(args.transform)
    if transform == 1:
        memoryDB = initDB("cluster_1_DB")
        filter_entrys()
        saveDB("Detection_DB_3")

    memoryDB = initDB()
    bgpb.build_sql_db(collectos, start_time=startTime, end_time=endTime, memoryDB=memoryDB, chunks=chunks)
    saveDB(db_name)
    memoryDB.close()
    log.rootLogger.info("[!] all done")