from collectos import *
from database import *
session_name = "test4"
db_name = "memoryTestDB"
collectos =[FRA, NLI, STO, SER, AMS, MOS]

import bgpBuilder as bgpb
import log

if __name__ == '__main__':
    memoryDB = initDB()
    bgpb.build_sql_db(collectos, start_time=(1526382859-4800), end_time=1526382859-2400, memoryDB=memoryDB, chunks=4)
    saveDB(db_name)
    log.rootLogger.info("[!] all done")