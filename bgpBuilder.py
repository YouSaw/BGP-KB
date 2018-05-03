#!/usr/bin/env python3
from _pybgpstream import BGPStream, BGPRecord, BGPElem
import sqlite3
import iptools
import time

db_name = 'bgp_stage1.db'

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
#283.39312648773193 19.172956466674805 86.84735012054443 85.06265759468079 171.36269617080688 170.65625143051147 66840

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

def build_sql_db():
    conn = sqlite3.connect(db_name)
    conn.isolation_level = None
    c = conn.cursor()

    # Create a new bgpstream instance and a reusable bgprecord instance
    stream = BGPStream()
    rec = BGPRecord()

    stream.add_filter('collector','rrc12')
    stream.add_interval_filter(1438416516,1438416516+100)
    stream.start()


    ####Index and lock####
    idx = 0
    fullidx = 0
    begin_trans = True

    ####Timings####
    full_processing_time = time.time()
    fetch_stream_sum = 0
    sql_link_sum = 0
    sql_prefix_sum = 0
    ip_inflate_sum = 0
    fetch_stream_time = time.time()

    while(stream.get_next_record(rec)): #4 elements multithreaded?
        fetch_stream_sum = time.time() - fetch_stream_time + fetch_stream_sum
        idx += 1

        if begin_trans:
            c.execute('BEGIN')
            print("Begin Trans")
            begin_trans = False

        ip_inflate_time = time.time()
        record_list = get_record_information(rec)
        ip_inflate_sum = ip_inflate_sum + time.time() - ip_inflate_time

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
            print("Commit")
            begin_trans = True
            c.execute("COMMIT")
        fetch_stream_time = time.time()
    conn.commit()
    conn.close()
    print(time.time() - full_processing_time,"fetch:", fetch_stream_sum, "sql_prefix:",sql_prefix_sum, "ip_inflate", ip_inflate_sum ,"sql_link",sql_link_sum, idx, fullidx)



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

def aggregate():
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute("")
#    as_prefix ALL min_ip, max_ip, AS, count
#    as_prefix ALL W same^
#    as_prefix_
    conn.close()

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
    #test_sql()
    build_sql_db()
    #print_db()
   #test_sql()
   #ip_min, ip_max = calculate_min_max("2001:db8::/32")
   #print("[!] Min IP:", ip_min, "Max IP:", ip_max)