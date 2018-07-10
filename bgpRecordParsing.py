from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, wait
import multiprocessing as mp
from main import *
import logging
from collectos import *
from _pybgpstream import BGPStream, BGPRecord, BGPElem
import sqlite3
import iptools
import time
import os

def calculate_min_max(ip):
    """
    Calculates min, max ip range
    :param ip:
    :return:
    """
    ip_range = iptools.IpRange(ip)
    return inflate_ip(ip_range[0]), inflate_ip(ip_range[-1])

def inflate_ip(ip):
    """
    Padds the ip from 192.168.0.1 -> 192.168.000.001
    :param ip:
    :return:
    """
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
    """
    Prepares SQL DB with the right tables
    Tables: as_link
    Table: prefix_as (with ip min and max)
    :return:
    """
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS as_link
                 (as_o INTEGER, as_n INTEGER, count INTEGER, last_update INTEGER)''')

    c.execute('''CREATE TABLE IF NOT EXISTS prefix_as
                 (ip_min TEXT, ip_max TEXT, as_o INTEGER, count INTEGER, last_update INTEGER)''')

    c.execute('''CREATE TABLE IF NOT EXISTS as_prefix
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
    """
    Record information objekt
    """
    def __init__(self):
        self.type = "N"
        self.origin = -1
        self.as_path = []
        self.max_ip = 0
        self.min_ip = 0
        self.time = 0

def get_record_information(rec):
    """
    Parser for the record information
    :param rec:
    :return:
    """
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

def build_sql(record_list):
    record_prefix = []
    record_links = []
    record_prefix_linker = []
    none_count = 0

    for parsed_record in record_list:
        if parsed_record.type != "N":
            record_prefix.append(
                (parsed_record.min_ip, parsed_record.max_ip, parsed_record.origin, 1, parsed_record.time))
            for as1, as2 in zip(parsed_record.as_path, parsed_record.as_path[1:]):
                record_links.append((as1, as2, 1, 0))
            for as1 in parsed_record.as_path:
                record_prefix_linker.append((parsed_record.min_ip, parsed_record.max_ip, as1, 1, parsed_record.time))
        else:
            none_count += 1
    return [record_prefix, record_links, record_prefix_linker], none_count