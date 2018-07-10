from collectos import *
session_name = "test4"
db_name = 'bgp_stage4.db'
collectos =[FRA, NLI, STO, SER, AMS, MOS]

import bgpBuilder as bgpb


if __name__ == '__main__':
    bgpb.prepare_sql_database()
    bgpb.build_sql_db(collectos, start_time=(1526382859-4800), end_time=1526382859-2400, chunks=4)
