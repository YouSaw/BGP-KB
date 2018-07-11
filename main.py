from collectos import *
from database import *
session_name = "test4"
db_name = "memoryTestDB"
collectos =[FRA, NLI, STO, SER, AMS, MOS]

import bgpBuilder as bgpb


if __name__ == '__main__':
    initDB()
    bgpb.prepare_sql_database()
    bgpb.build_sql_db(collectos, start_time=(1526382859-4800), end_time=1526382859-2400, chunks=4)
    saveDB(db_name)