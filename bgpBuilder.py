#!/usr/bin/env python3
from _pybgpstream import BGPStream, BGPRecord, BGPElem
import sqlite3
import iptools

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
    conn = sqlite3.connect('bgp_stage.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS as_link
                 (as_o INTEGER, as_n INTEGER, count INTEGER, last_update INTEGER)''')

    c.execute('''CREATE TABLE IF NOT EXISTS prefix_as
                 (ip_min TEXT, ip_max TEXT, as_o INTEGER, count INTEGER, last_update INTEGER)''')
    conn.commit()
    conn.close()

def build_sql_db():
    conn = sqlite3.connect('bgp_stage.db')
    c = conn.cursor()

    # Create a new bgpstream instance and a reusable bgprecord instance
    stream = BGPStream()
    rec = BGPRecord()

    stream.add_filter('collector','rrc11')
    stream.add_interval_filter(1438417216,1438417316)
    stream.start()

    while(stream.get_next_record(rec)):
        if rec.status != "valid":
            continue
        else:
            elem = rec.get_next_elem()
            while(elem):
                if elem.type == "AB":
                    prefix = elem.fields["prefix"]
                    as_path = elem.fields["as-path"].split(" ")
                    origin = as_path[-1]
                    time = elem.time


                    #IP Prefix database
                    ip_min, ip_max = calculate_min_max(prefix)
                    c.execute("SELECT ip_min FROM prefix_as WHERE ip_min = (?) AND ip_max = (?) AND as_o = (?)", (ip_min, ip_max, origin))
                    row = c.fetchone()
                    if len(row) != 0:
                        c.execute("UPDATE prefix_as SET count = count + 1  WHERE ip_min = (?) AND ip_max = (?) AND as_o = (?)", (ip_min, ip_max, origin))
                    else:
                        c.execute("INSERT INTO prefix_as VALUES(?,?,?,?,?)", (ip_min, ip_max, origin, 1, time))


                    #AS link database
                    for as1,as2 in zip(as_path, as_path[1:]) :
                        c.execute("SELECT as_o FROM as_link WHERE as_o = (?) AND as_n = (?)",(as1,as2))
                        row = c.fetchone()
                        if len(row) != 0:
                            c.execute("UPDATE as_link SET count = count + 1 WHERE as_o = (?) AND as_n = (?)",
                                      (as1, as2))
                        else:
                            c.execute("INSERT INTO as_link VALUES(?,?,?,?)", (as1, as2, 1, 0))

                elif elem.type == "WA":
                    prefix = elem.fields["prefix"]
                    time = elem.time
                    #Needs research

                    print(rec.project, rec.collector, rec.type, rec.time, rec.status,
                        elem.type, elem.peer_address, elem.peer_asn, elem.fields)
                    print(prefix,elem.time, "W")

                print(rec.project, rec.collector, rec.type, rec.time, rec.status,
                      elem.type, elem.peer_address, elem.peer_asn, elem.fields)
                elem = rec.get_next_elem()
            conn.commit()
    conn.close()


def test_sql():
    conn = sqlite3.connect('bgp_stage.db')
    c = conn.cursor()
    c.execute("SELECT * FROM as_link WHERE as_o = 901022")
    cursor = c.fetchall()
    if len(cursor) == 0:
        print("LOL")
    for entry in cursor:
        for e in entry:
            print(e,end=" ")
        print("")
    c.execute("UPDATE as_link SET count = count + 1, last_update = (?) WHERE as_o = (?) AND as_n = (?)", (6666, 9008, 9009))
    c.execute("INSERT INTO as_link VALUES(?,?,?,?)", (90022, 9009, 1, 12345))
    conn.commit()
    conn.close()


if __name__ == '__main__':
    build_sql_db()
    prepare_sql_database()
    test_sql()
    ip_min, ip_max = calculate_min_max("2001:db8::/32")
    print("[!] Min IP:", ip_min, "Max IP:", ip_max)