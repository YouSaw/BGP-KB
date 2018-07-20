#!/usr/bin/env python3
from main import *
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, wait
import multiprocessing as mp
from _pybgpstream import BGPStream, BGPRecord, BGPElem
import gc
from bgpRecordParsing import *
import database
import time
import log

####Multiprocessing helpers####
def values_in_future(future_list):
    """
    checks if all jobs in the future list are done
    :param future_list:
    :return:
    """
    for future in future_list:
        if future.result():
            return True
    return False

def futures_done(future_list):
    for future in future_list:
        if not future.done():
            return False
    return True

def make_chunks(start_time, end_time, chunks, realTime = False):
    full_time = end_time - start_time
    time_chunk = full_time // chunks
    print(full_time, time_chunk)
    chunk_list = []
    for x in range(0, chunks*time_chunk, time_chunk):
        chunk_list.append([start_time + x, start_time + x + time_chunk])

    chunk_list[-1][1] = end_time
    if realTime:
        chunk_list.append([end_time, 0])
        log.rootLogger.info("Realtime chunk: "+ str(chunk_list[-1]))

    return chunk_list

def pull_bgp_records(mt_queue, start_time = 1438416516, end_time = 1438416516, collector_nr ="rrc12", chunk_nr = 0):
    """
    fetches bgp records
    :param mt_queue: queue
    :param start_time:
    :param end_time:
    :param collector_nr: string
    :return: fetch_count, emtpy_count, element_count
    """
    #### Adjust priority####
    current_nice = os.nice(0)
    os.nice(7-current_nice)


    ####BGP collector pulling init####
    rec = BGPRecord()
    stream = BGPStream()
    stream.add_filter('collector', collector_nr)
    stream.add_interval_filter(start_time, end_time)
    stream.start()

    log.rootLogger.info("[!] Starting multiprocess for collector: " + collector_nr + " range:" + str(start_time) + "-" + str(end_time) + "(" + str(end_time-start_time) + ")")

    ####Statistic####
    idx = 0
    element_count = 0
    empty_count = 0
    none_count = 0
    record_list = []

    ####Get bgp records####
    while stream.get_next_record(rec):
        recordInformation = get_record_information(rec)
        if recordInformation == [] or recordInformation is False:
            empty_count += 1
        else:
            idx += 1
            record_list.extend(recordInformation)
            #transform records to sql ready batch
            if idx % 2000 == 0:
                record_processed, tmp_none_count = build_sql(record_list)
                none_count +=  tmp_none_count
                mt_queue.put(record_processed)
                record_list = []
                gc.collect()
            element_count += len(recordInformation)

    #Last row
    record_processed, tmp_none_count = build_sql(record_list)
    none_count += tmp_none_count

    mt_queue.put(record_processed)
    log.rootLogger.info("[+] " + collector_nr + " at chunk " + str(chunk_nr) +" is done with fetching")
    return idx, empty_count, element_count, none_count

def build_sql_db(collector_list, start_time, end_time, memoryDB,  chunks = 4, realtime = False):
    """
    Save bgp information in sql db. Makes threads and splitting of data
    :param collector_list:
    :param start_time:
    :param end_time:
    :param chunks:
    :return:
    """
    collector_count = len(collector_list)

    ####Database Stuff####
    memoryDB.isolation_level = None
    memoryDB_cursor = memoryDB.cursor()


    ####Index and lock####
    idx = 0
    fullidx = 0

    ####Timings####
    full_processing_time = time.time()

    ####MP fetch of values####
    multithreadingManager = mp.Manager()
    mt_queue = multithreadingManager.Queue(maxsize=500)
    fetch_futures = []

    chunked_time = make_chunks(start_time=start_time, end_time=end_time, chunks=chunks, realTime=realtime)
    fetch_executer = ProcessPoolExecutor(max_workers=collector_count * len(chunked_time))

    for i in range(0,collector_count):
        for x in range(len(chunked_time)):
            fetch_futures.append(fetch_executer.submit(pull_bgp_records, mt_queue, chunked_time[x][0], chunked_time[x][1], collector_list[i], x))


    #Adjust priority
    current_nice = os.nice(0)
    os.nice(0-current_nice)

    log.rootLogger.info("[!] Beginning with database building!" + str(collector_list))
    time.sleep(5)

    ####Checking if all threads are done####
    while(not futures_done(fetch_futures) or not mt_queue.empty()):
        idx += 1

        try:
            record_list = mt_queue.get(timeout = 10)
        except:
            log.rootLogger.info("[!] queue is emtpy")
            continue

        #Processing in queue and then execute many

        memoryDB_cursor.executemany("INSERT INTO prefix_as VALUES(?,?,?,?,?)", record_list[0])
        memoryDB_cursor.executemany("INSERT INTO as_link VALUES(?,?,?,?)", record_list[1])
        memoryDB_cursor.executemany("INSERT INTO as_prefix VALUES(?,?,?,?,?)", record_list[2])
        fullidx += len(record_list[0])
        
        if idx % (200) == 0: #Avoid to manny commits
            log.rootLogger.info("[!] Commit. Processed : "+ str(fullidx))
            memoryDB.commit()

        if idx % (2000) == 0:
            database.aggregate_entrys()

        if idx % 50000 == 0:
            database.saveDB(db_name+"-idx-"+str(idx))
        gc.collect()

    ####Last commits####
    memoryDB.commit()
    database.aggregate_entrys()
    ####Statistic stuff####
    empty_count = 0
    fetch_idx = 0
    none_count = 0
    elem_count = 0
    for future in fetch_futures:
        idx, count, elems, none_c = future.result()
        fetch_idx += idx
        elem_count += elems
        empty_count += count
        none_count += none_c

    log.rootLogger.info("Time: " + str(time.time() - full_processing_time) + "\nfetched records: " + str(fetch_idx) + " fetched elements: " +
                str(elem_count) + " fetched empty records: " + str(empty_count)+ " none count: "+ str(none_count) + " processed elements: " + str(fullidx))




def print_db():
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute("SELECT * FROM as_link")
    cursor = c.fetchall()
    for entry in cursor:
        print("\n",entry, end=", ")

    c.execute("SELECT * FROM prefix_as")
    cursor = c.fetchall()
    for entry in cursor:
        print("\n",entry, end=", ")
    conn.close()
