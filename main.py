from collectos import *
from database import *
import argparse
session_name = "cluster_1"
db_name = "cluster_1_DB"
collectos =[FRA, NLI, STO, SER, AMS, MOS]

import bgpBuilder as bgpb
import log

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", help="startTime")
    parser.add_argument("-e", help="endTime")
    parser.add_argument("-c", help="chunks")
    parser.add_argument("-db", help="DB name")
    parser.add_argument("-se", help="Session name")
    memoryDB = initDB()
    bgpb.build_sql_db(collectos, start_time=1530063631, end_time=1531563631, memoryDB=memoryDB, chunks=4)
    saveDB(db_name)
    log.rootLogger.info("[!] all done")