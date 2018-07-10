#!/usr/bin/env python3
from main import *
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, wait
import multiprocessing as mp
from _pybgpstream import BGPStream, BGPRecord, BGPElem
from bgpRecordParsing import *
from log import *



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

def make_chunks(start_time, end_time, chunks):
    full_time = end_time - start_time
    time_chunk = full_time // chunks
    print(full_time, time_chunk)
    chunk_list = []
    for x in range(0, chunks*time_chunk, time_chunk):
        chunk_list.append([start_time + x, start_time + x + time_chunk])

    chunk_list[-1][1] = end_time
    return chunk_list


def pull_bgp_records(q, start_time = 1438416516, end_time = 1438416516, collector_nr ="rrc12", chunk_nr = 0):
    """
    fetches bgp records
    :param q: queue
    :param start_time:
    :param end_time:
    :param collector_nr: string
    :return: fetch_count, emtpy_count, element_count
    """
    current_nice = os.nice(0)
    os.nice(7-current_nice)

    rec = BGPRecord()
    stream = BGPStream()
    stream.add_filter('collector', collector_nr)
    stream.add_interval_filter(start_time, end_time)
    stream.start()

    rootLogger.info("[!] Starting multiprocess for collector: " + collector_nr + " range:" + str(start_time) + "-" + str(end_time) + "(" + str(end_time-start_time) + ")")

    idx = 0
    element_count = 0
    empty_count = 0
    none_count = 0
    record_list = []

    while stream.get_next_record(rec):
        d = get_record_information(rec)
        if d == [] or d is False:
            empty_count += 1
        else:
            idx += 1
            record_list.extend(d)
            if idx % 1000 == 0:
                record_processed, nc = build_sql(record_list)
                none_count +=  nc
                q.put(record_processed)
                record_list = []
            element_count += len(d)

    record_processed, nc = build_sql(record_list)
    none_count += nc

    q.put(record_processed)
    rootLogger.info("[+] " + collector_nr + " at chunk " + str(chunk_nr) +" is done with fetching")
    return idx, empty_count, element_count, none_count

def build_sql_db(collector_list, start_time, end_time, chunks = 4):
    """
    Save bgp information in sql db. Makes threads and splitting of data
    :param collector_list:
    :param start_time:
    :param end_time:
    :param chunks:
    :return:
    """
    collector_count = len(collector_list)

    conn = sqlite3.connect(db_name)
    conn.isolation_level = None
    c = conn.cursor()

    fetch_executer = ProcessPoolExecutor(max_workers=collector_count * chunks)

    ####Index and lock####
    idx = 0
    fullidx = 0
    begin_trans = True

    ####Timings####
    full_processing_time = time.time()

    ####MP fetch of values####
    m = mp.Manager()
    q = m.Queue(maxsize=500)
    fetch_futures = []

    chunked_time = make_chunks(start_time=start_time, end_time=end_time, chunks=chunks)
    for i in range(0,collector_count):
        for x in range(chunks):
            fetch_futures.append(fetch_executer.submit(pull_bgp_records, q, chunked_time[x][0], chunked_time[x][1], collector_list[i], x))


    #Adjust priority
    current_nice = os.nice(0)
    os.nice(0-current_nice)
    while(not futures_done(fetch_futures) or not q.empty()):
        idx += 1
        if begin_trans:
            c.execute('BEGIN')
            begin_trans = False

        try:
            record_list = q.get(timeout = 10)
        except:
            rootLogger.info("[!] queue is emtpy")
            continue

        #Processing in queue and then execute many
        c.executemany("INSERT INTO prefix_as VALUES(?,?,?,?,?)", record_list[0])
        c.executemany("INSERT INTO as_link VALUES(?,?,?,?)", record_list[1])
        fullidx += len(record_list[0])
        
        if idx % 100 == 0: #Avoid to manny commits
            rootLogger.info("[!] Commit. Processed : "+ str(fullidx))
            begin_trans = True
            c.execute("COMMIT")

        if idx % 1000 == 0:
            aggregate_entrys()

    conn.commit()
    aggregate_entrys()
    conn.close()

    #Statistic stuff
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

    rootLogger.info("Time: " + str(time.time() - full_processing_time) + "\nfetched records: " + str(fetch_idx) + " fetched elements: " +
                    str(elem_count) + " fetched empty records: " + str(empty_count)+ " none count: "+ str(none_count) + " processed elements: " + str(fullidx))


def aggregate_entrys():
    """
    Aggregates entrys for smaller dbs
    1. creates tables for aggregation
    2.1 copy data from prefix_as aggregated to prefix_as_aggregate
    2.2. copy data from as_link aggregated to link_as_aggregate
    3.1.1 creates tmp table and aggregate prefix_as_aggregate to tmp (second time step 2 could duplicate... therefore this)
    3.1.2 alter tmp name to prefix_as_aggregate
    3.2.1 same as 3.1.1 with other names
    4. drop tables and VAAAACUUUM

    :return: True if successesfull
    """
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    rootLogger.info("[!] Starting aggregation")

    t1 = time.time()
    try:
        c.execute('''CREATE TABLE IF NOT EXISTS prefix_as_aggregate
                     (ip_min TEXT, ip_max TEXT, as_o INTEGER, count INTEGER, first_update INTEGER, last_update INTEGER)''')

        c.execute('''CREATE TABLE IF NOT EXISTS link_as_aggregate
                     (as_o INTEGER, as_n INTEGER, count INTEGER)''')

        c.execute("INSERT INTO prefix_as_aggregate SELECT ip_min, ip_max, as_o, count(*) AS count, MIN(last_update)"
                  " AS first_update, MAX(last_update) AS last_update FROM prefix_as GROUP BY ip_min, ip_max, as_o")
        c.execute("INSERT INTO link_as_aggregate SELECT as_o, as_n, count(*) AS count FROM as_link GROUP BY as_o, as_n")

        c.execute("CREATE TABLE tmp AS SELECT ip_min, ip_max, as_o, sum(count) AS count, MIN(last_update) AS first_update,"
                  " MAX(last_update) AS last_update FROM prefix_as_aggregate GROUP BY ip_min, ip_max, as_o")

        c.execute("DROP TABLE prefix_as_aggregate")
        c.execute("ALTER TABLE tmp RENAME TO prefix_as_aggregate")

        c.execute("CREATE TABLE tmp AS SELECT as_o, as_n, sum(count) AS count FROM link_as_aggregate GROUP BY as_o, as_n")
        c.execute("DROP TABLE link_as_aggregate")
        c.execute("ALTER TABLE tmp RENAME TO link_as_aggregate")

        c.execute("DROP TABLE prefix_as")
        c.execute("DROP TABLE as_link")

        c.execute('''CREATE TABLE IF NOT EXISTS as_link
                     (as_o INTEGER, as_n INTEGER, count INTEGER, last_update INTEGER)''')

        c.execute('''CREATE TABLE IF NOT EXISTS prefix_as
                     (ip_min TEXT, ip_max TEXT, as_o INTEGER, count INTEGER, last_update INTEGER)''')
        c.execute("VACUUM")

    except Exception as e:
        rootLogger.critical("[-] Something went wrong in the aggregation: " + e)
        return False

    rootLogger.info("[+] Aggregation time:" + str(time.time() - t1))
    conn.commit()
    conn.close()
    return True

def filter_entrys():
    """
    Filter prefix entrys by substracting withdraws count from the announcemens and save all this in a final Database.
    Entrys below a threshold will be pruned
    :return: False or True maybe
    """

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
