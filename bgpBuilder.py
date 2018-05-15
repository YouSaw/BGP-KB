#!/usr/bin/env python3
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, wait
import multiprocessing as mp

import logging

session_name = "test"

logFormatter = logging.Formatter("%(asctime)-10s %(levelname)-6s %(message)-10s", datefmt='%d %H:%M:%S')
rootLogger = logging.getLogger()
rootLogger.level = logging.INFO

fileHandler = logging.FileHandler("{0}/{1}.log".format("logs", session_name))
fileHandler.setFormatter(logFormatter)
rootLogger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
rootLogger.addHandler(consoleHandler)

from _pybgpstream import BGPStream, BGPRecord, BGPElem
import sqlite3
import iptools
import time
import os


db_name = 'bgp_stage0.db'

def calculate_min_max(ip):
    ip_range = iptools.IpRange(ip)
    return inflate_ip(ip_range[0]), inflate_ip(ip_range[-1])

def inflate_ip(ip):
    if iptools.ipv4.validate_ip(ip):
        return inflate_ipv4(ip)
    return inflate_ipv6(ip)

def inflate_ipv4(ip):
    ip = ip.split(".")
    ip = ".".join([str(i).zfill(3) for i in ip])
    return ip

def inflate_ipv6(ip):
    ip = ip.split(":")
    ip_len = len(ip)
    for _ in range(ip_len, 8):
        ip.append(0)
    ip = ":".join([str(i).zfill(4) for i in ip])
    return ip

def prepare_sql_database():
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS as_link
                 (as_o INTEGER, as_n INTEGER, count INTEGER, last_update INTEGER)''')

    c.execute('''CREATE TABLE IF NOT EXISTS prefix_as
                 (ip_min TEXT, ip_max TEXT, as_o INTEGER, count INTEGER, last_update INTEGER)''')
    #c.execute('PRAGMA synchronous=OFF')
    # c.execute('PRAGMA journal_mode=MEMORY')
    # c.execute('PRAGMA main.page_size = 4096')
    # c.execute('PRAGMA main.cache_size=10000')
    # c.execute('PRAGMA main.locking_mode=EXCLUSIVE')
    # c.execute('PRAGMA main.synchronous=NORMAL')
    c.execute('PRAGMA main.journal_mode=WAL')
    conn.commit()
    conn.close()

class record_information(object):
    def __init__(self):
        self.type = "N"
        self.origin = -1
        self.as_path = []
        self.max_ip = 0
        self.min_ip = 0
        self.time = 0


def get_record_information(rec):
    record_information_list = []

    if rec.status != "valid":
        return False
    else:
        elem = rec.get_next_elem()
        while (elem):
            ri = record_information()
            if elem.type == "A":
                ri.type = "A"
                prefix = elem.fields["prefix"]
                ri.as_path = elem.fields["as-path"].split(" ")
                ri.origin = ri.as_path[-1]
                ri.time = elem.time
                ri.min_ip, ri.max_ip = calculate_min_max(prefix)

            elif elem.type == "W":
                ri.type = "W"
                prefix = elem.fields["prefix"]
                ri.time = elem.time
                ri.min_ip, ri.max_ip = calculate_min_max(prefix)
            record_information_list.append(ri)
            elem = rec.get_next_elem()
    return record_information_list


def values_in_future(future_list):
    for future in future_list:
        if future.result():
            return True
    return False

def futures_done(future_list):
    for future in future_list:
        if not future.done():
            return False
    return True

def get_bgp_record(q, start_time = 1438416516, end_time = 1438416516, collector_nr = "rrc12"):
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

    rootLogger.info("[!] Starting multiprocess, range:" + str(start_time) + "-" + str(end_time) + "(" + str(end_time-start_time) + ")")
    #print("[!] Starting multiprocess, range:", start_time, "-" ,end_time, "(",end_time-start_time,")")

    idx = 0
    element_idx = 0
    empty_count = 0

    record_list = []
    while stream.get_next_record(rec):
        d = get_record_information(rec)
        if d == [] or d is False:
            empty_count += 1
        else:
            try:
                idx += 1
                record_list.extend(d)
                if idx % 100 == 0:
                    q.put(record_list)
                    record_list = []
            except:
                rootLogger.warning("[!] Queue is full. "+ q.qsize())
                while q.Full():
                    time.sleep(10)
                q.put(record_list)
                record_list = []
            element_idx += len(d)
    q.put(record_list)
    rootLogger.info("[+] Done fetching")
    return idx, empty_count, element_idx


def build_sql_db(thread_count):
    conn = sqlite3.connect(db_name)
    conn.isolation_level = None
    c = conn.cursor()

    fetch_executer = ProcessPoolExecutor(max_workers=thread_count)

    ####Index and lock####
    idx = 0
    fullidx = 0
    begin_trans = True

    ####Timings####
    full_processing_time = time.time()
    fetch_stream_sum = 0
    sql_link_sum = 0
    sql_prefix_sum = 0

    ####MP fetch of values####
    m = mp.Manager()
    q = m.Queue()
    fetch_futures = []
    for i in range(0,thread_count):
        fetch_futures.append(fetch_executer.submit(get_bgp_record, q, 1438532516+2000*i, 1438532516+2000*i+2000))

    #Adjust priority
    current_nice = os.nice(0)
    os.nice(0-current_nice)
    while(not futures_done(fetch_futures) or not q.empty()):
        idx += 1
        if begin_trans:
            c.execute('BEGIN')
            begin_trans = False

        fetch_stream_time = time.time()
        try:
            record_list = q.get(timeout = 1)
        except:
            rootLogger.info("[!] queue is emtpy")
            time.sleep(2)
            continue

        fetch_stream_sum =  time.time() - fetch_stream_time + fetch_stream_sum
        #print(record_list)
        for parsed_record in record_list:
            fullidx += 1

            if parsed_record.type != "N":
                sql_prefix_withdraw_time = time.time()
                c.execute("INSERT INTO prefix_as VALUES(?,?,?,?,?)", (parsed_record.min_ip, parsed_record.max_ip, parsed_record.origin, 1, parsed_record.time))
                sql_prefix_sum = time.time() - sql_prefix_withdraw_time + sql_prefix_sum

                sql_link_time = time.time()
                for as1,as2 in zip(parsed_record.as_path, parsed_record.as_path[1:]) :
                    c.execute("INSERT INTO as_link VALUES(?,?,?,?)", (as1, as2, 1, 0))
                sql_link_sum = time.time() - sql_link_time + sql_link_sum

        if idx % 1000 == 0: #Avoid to manny commits
            rootLogger.info("[!] Commit. Processed : "+ str(fullidx))
            begin_trans = True
            c.execute("COMMIT")

        if idx % 100000 == 0:
            rootLogger.info("[!] Aggregation started!")
            aggregate_entrys()
    conn.commit()
    conn.close()

    empty_count = 0
    fetch_idx = 0
    elem_count = 0
    for future in fetch_futures:
        idx, count, elems = future.result()
        fetch_idx += idx
        elem_count += elems
        empty_count += count

    rootLogger.info("Time: " + str(time.time() - full_processing_time) + " Fetch time: " + str(fetch_stream_sum) + " sql_prefix: " +
                    str(sql_prefix_sum) + " sql_link " + str(sql_link_sum) +"\nfetched records: " + fetch_idx + " fetched elements: " +
                    str(elem_count) + " fetched empty records: " + str(empty_count)  + " processed elements: " + fullidx)



def test_sql():
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute("SELECT * FROM as_link WHERE as_o = 901022")
    cursor = c.fetchall()
    if len(cursor) == 0:
        print("LOL")
    for entry in cursor:
        for e in entry:
            print(e,end=" ")
        print("")
    c.execute("BEGIN")
    c.execute("UPDATE as_link SET count = count + 1, last_update = (?) WHERE as_o = (?) AND as_n = (?)", (6666, 9008, 9009))
    c.execute("INSERT INTO as_link VALUES(?,?,?,?)", (90022, 9009, 1, 12345))
    conn.commit()
    conn.close()

#TODO
def make_chunks(start_time, end_time, chunks):
    pass

def aggregate_entrys():
    """
    Aggregates entrys for smaller dbs
    :return: True if successesfull
    """
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

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

    rootLogger.info("Aggregation time:" + str(time.time() - t1))
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

if __name__ == '__main__':
    prepare_sql_database()
    build_sql_db(thread_count=10)
    #test_sql()
    #aggregate_entrys()
    #print_db()
   #test_sql(2
   #ip_min, ip_max = cal8late_min_max("2001:db8::/32")
   #print("[!] Min IP:", ip_min, "Max IP:", ip_max)