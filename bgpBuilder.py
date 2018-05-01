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



def build_sql_db():
    conn = sqlite3.connect(db_name)
    conn.isolation_level = None
    c = conn.cursor()

    # Create a new bgpstream instance and a reusable bgprecord instance
    stream = BGPStream()
    rec = BGPRecord()

    stream.add_filter('collector','rrc12')
    stream.add_interval_filter(1438416516+2*3600,1438416516+4*3600)
    stream.start()

    sum_time = 0
    idx = 0
    begin_trans = True
    st1 = time.time()

    t_time = time.time()

    fullidx = 0
    withdraw_time_sum = 0
    link_time_sum = 0
    prefix_time_sum = 0
    prefix_update_time = 0
    link_update_time = 0
    ip_inflate_sum = 0

    while(stream.get_next_record(rec)): #4 elements multithreaded?
        st2 = time.time()
        sum_time = st2 - st1 + sum_time

        if begin_trans:
            c.execute('BEGIN')
            print("Begin Trans")
            begin_trans = False

        idx += 1

        if rec.status != "valid":
            continue
        else:
            elem = rec.get_next_elem()
            while(elem):

                if elem.type == "A":
                    prefix1_time = time.time()

                    prefix = elem.fields["prefix"]
                    as_path = elem.fields["as-path"].split(" ")
                    origin = as_path[-1]
                    e_time = elem.time
                    fullidx += 1

                    #prefix_update1 = time.time()
                    #IP Prefix database
                    inflate1 = time.time()
                    ip_min, ip_max = calculate_min_max(prefix)
                    ip_inflate_sum = ip_inflate_sum + time.time()-inflate1
                    #c.execute("UPDATE prefix_as SET count = count + 1  WHERE ip_min = (?) AND ip_max = (?) AND as_o = (?)", (ip_min, ip_max, origin))
                    #prefix_update_time = time.time() - prefix_update1 + prefix_update_time
                    #if c.rowcount == 0:
                    c.execute("INSERT INTO prefix_as VALUES(?,?,?,?,?)", (ip_min, ip_max, origin, 1, e_time))

                    prefix_time_sum = time.time() - prefix1_time + prefix_time_sum


                    link1_time = time.time()
                    #AS link database
                    for as1,as2 in zip(as_path, as_path[1:]) :
                        link_update1 = time.time()
                        #c.execute("UPDATE as_link SET count = count + 1 WHERE as_o = (?) AND as_n = (?)",
                        #              (as1, as2))
                        link_update_time = time.time() - link_update1 + link_update_time
                        #if c.rowcount == 0:
                        c.execute("INSERT INTO as_link VALUES(?,?,?,?)", (as1, as2, 1, 0))
                    link_time_sum = time.time() - link1_time + link_time_sum

                elif elem.type == "W":
                    fullidx += 1
                    withdraw1 = time.time()
                    prefix = elem.fields["prefix"]
                    e_time = elem.time
                    ip_min, ip_max = calculate_min_max(prefix)
                    #196.72094464302063 19.156189680099487 8.407627582550049 0 1.2567062377929688 0.03907585144042969 165.27871417999268 66840 108945
                    #196.72094464302063 19.156189680099487 8.407627582550049 0 1.2567062377929688 0.03907585144042969 165.27871417999268 66840 108945
                    #IP Prefix database
                    c.execute("INSERT INTO prefix_as VALUES(?,?,?,?,?)", (ip_min, ip_max, -1, 1, e_time))

                    #c.execute(
                    #    "UPDATE prefix_as SET count = count - 1, last_update = (?) WHERE ip_min = (?) AND ip_max = (?) AND last_update - (?) > 86400",
                    #    (e_time, ip_min, ip_max, e_time))
                    withdraw_time_sum = time.time() - withdraw1 + withdraw_time_sum
                elem = rec.get_next_elem()

            if idx % 1000 == 0: #Avoid to manny commits
                print("Commit")
                begin_trans = True
                c.execute("COMMIT")
                #conn.commit()

        st1 = time.time()
    conn.commit()
    print(time.time() - t_time, sum_time, prefix_time_sum, prefix_update_time, ip_inflate_sum ,link_time_sum, link_update_time,withdraw_time_sum ,idx, fullidx)
    conn.close()
#274.5066967010498 19.085091829299927 83.94422888755798 82.19763445854187 165.22026348114014 164.51573514938354 66840
#206.89421820640564

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
    test_sql()
    build_sql_db()
    #print_db()
   #test_sql()
   #ip_min, ip_max = calculate_min_max("2001:db8::/32")
   #print("[!] Min IP:", ip_min, "Max IP:", ip_max)