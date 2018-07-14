import time
import sqlite3
from io import StringIO
import log as log
memoryDB = None

def initDB(path=None):
    global memoryDB
    # Read database to tempfile
    memoryDB = sqlite3.connect(":memory:")

    if path != None:
        con = sqlite3.connect(path)
        tempfile = StringIO()
        for line in con.iterdump():
            tempfile.write('%s\n' % line)
        con.close()
        tempfile.seek(0)

        memoryDB.cursor().executescript(tempfile.read())
        memoryDB.commit()
    prepare_sql_database()
    return memoryDB

def saveDB(path):
    log.rootLogger.info("[!] Writing memorydatabase to file")
    tempfile = StringIO()
    for line in memoryDB.iterdump():
        tempfile.write('%s\n' % line)
    tempfile.seek(0)
    fileDB = sqlite3.connect(path)
    fileDB.cursor().executescript(tempfile.read())
    fileDB.commit()
    fileDB.close()
    log.rootLogger.info("[!] Wrote database to file")


def prepare_sql_database():
    """
    Prepares SQL DB with the right tables
    Tables: as_link
    Table: prefix_as (with ip min and max)
    :return:
    """
    memoryDB_cursor = memoryDB.cursor()
    memoryDB_cursor.execute('''CREATE TABLE IF NOT EXISTS as_link
                 (as_o INTEGER, as_n INTEGER, count INTEGER, last_update INTEGER)''')

    memoryDB_cursor.execute('''CREATE TABLE IF NOT EXISTS prefix_as
                 (ip_min TEXT, ip_max TEXT, as_o INTEGER, count INTEGER, last_update INTEGER)''')

    memoryDB_cursor.execute('''CREATE TABLE IF NOT EXISTS as_prefix
                 (ip_min TEXT, ip_max TEXT, as_o INTEGER, count INTEGER, last_update INTEGER)''')

    memoryDB_cursor.execute('PRAGMA synchronous=OFF')
    memoryDB_cursor.execute('PRAGMA journal_mode=MEMORY')
    memoryDB_cursor.execute('PRAGMA page_size = 4096')
    memoryDB_cursor.execute('PRAGMA cache_size=10000')
    memoryDB_cursor.execute('PRAGMA locking_mode=EXCLUSIVE')
    # c.execute('PRAGMA main.synchronous=NORMAL')
    #c.execute('PRAGMA journal_mode=WAL')
    memoryDB.commit()


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
    memoryDBCursor = memoryDB.cursor()
    log.rootLogger.info("[!] Starting aggregation")

    timepoint1 = time.time()
    try:
        memoryDBCursor.execute('''CREATE TABLE IF NOT EXISTS prefix_as_aggregate
                     (ip_min TEXT, ip_max TEXT, as_o INTEGER, count INTEGER, first_update INTEGER, last_update INTEGER)''')

        memoryDBCursor.execute('''CREATE TABLE IF NOT EXISTS as_prefix_aggregate
                     (ip_min TEXT, ip_max TEXT, as_o INTEGER, count INTEGER, first_update INTEGER, last_update INTEGER)''')

        memoryDBCursor.execute('''CREATE TABLE IF NOT EXISTS link_as_aggregate
                     (as_o INTEGER, as_n INTEGER, count INTEGER)''')

        log.rootLogger.info("[+] Aggregation init time:" + str(time.time() - timepoint1))

        memoryDBCursor.execute("INSERT INTO prefix_as_aggregate SELECT ip_min, ip_max, as_o, count(*) AS count, MIN(last_update)"
                  " AS first_update, MAX(last_update) AS last_update FROM prefix_as GROUP BY ip_min, ip_max, as_o")

        memoryDBCursor.execute("INSERT INTO as_prefix_aggregate SELECT ip_min, ip_max, as_o, count(*) AS count, MIN(last_update)"
                  " AS first_update, MAX(last_update) AS last_update FROM as_prefix GROUP BY ip_min, ip_max, as_o")

        memoryDBCursor.execute("INSERT INTO link_as_aggregate SELECT as_o, as_n, count(*) AS count FROM as_link GROUP BY as_o, as_n")

        log.rootLogger.info("[+] Aggregation insert into aggregate tables time:" + str(time.time() - timepoint1))

        memoryDBCursor.execute(
            "CREATE TABLE tmp AS SELECT ip_min, ip_max, as_o, sum(count) AS count, MIN(last_update) AS first_update,"
            " MAX(last_update) AS last_update FROM prefix_as_aggregate GROUP BY ip_min, ip_max, as_o")
        memoryDBCursor.execute("DROP TABLE prefix_as_aggregate")
        memoryDBCursor.execute("ALTER TABLE tmp RENAME TO prefix_as_aggregate")

        memoryDBCursor.execute(
            "CREATE TABLE tmp AS SELECT as_o, as_n, sum(count) AS count FROM link_as_aggregate GROUP BY as_o, as_n")
        memoryDBCursor.execute("DROP TABLE link_as_aggregate")
        memoryDBCursor.execute("ALTER TABLE tmp RENAME TO link_as_aggregate")

        memoryDBCursor.execute(
            "CREATE TABLE tmp AS SELECT ip_min, ip_max, as_o, sum(count) AS count, MIN(last_update) AS first_update,"
            " MAX(last_update) AS last_update FROM as_prefix_aggregate GROUP BY ip_min, ip_max, as_o")
        memoryDBCursor.execute("DROP TABLE as_prefix_aggregate")
        memoryDBCursor.execute("ALTER TABLE tmp RENAME TO as_prefix_aggregate")
        log.rootLogger.info("[+] Aggregation aggregate time:" + str(time.time() - timepoint1))

        memoryDBCursor.execute("DROP TABLE prefix_as")
        memoryDBCursor.execute("DROP TABLE as_link")
        memoryDBCursor.execute("DROP TABLE as_prefix")

        memoryDBCursor.execute('''CREATE TABLE IF NOT EXISTS as_link
                     (as_o INTEGER, as_n INTEGER, count INTEGER, last_update INTEGER)''')

        memoryDBCursor.execute('''CREATE TABLE IF NOT EXISTS prefix_as
                     (ip_min TEXT, ip_max TEXT, as_o INTEGER, count INTEGER, last_update INTEGER)''')

        memoryDBCursor.execute('''CREATE TABLE IF NOT EXISTS as_prefix
                     (ip_min TEXT, ip_max TEXT, as_o INTEGER, count INTEGER, last_update INTEGER)''')

        tmp = memoryDB.isolation_level
        memoryDB.isolation_level = None
        memoryDBCursor.execute("VACUUM")
        memoryDB.isolation_level = tmp

        log.rootLogger.info("[+] Aggregation vaacum time:" + str(time.time() - timepoint1))

    except Exception as e:
        log.rootLogger.critical("[-] Something went wrong in the aggregation: " + str(e))
        return False

    log.rootLogger.info("[+] Aggregation time:" + str(time.time() - timepoint1))
    memoryDB.commit()
    return True

def filter_entrys():
    """
    Filter prefix entrys by substracting withdraws count from the announcemens and save all this in a final Database.
    Entrys below a threshold will be pruned
    :return: False or True maybe
    """