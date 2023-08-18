#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()
    cur.execute("""
    CREATE TABLE {Ratings}(
    userid integer,
    movieid integer,
    rating NUMERIC NOT NULL)
    """.format(Ratings=ratingstablename))

    with open(ratingsfilepath,"r") as file:
        for line in file:
            [userId, movieId, rating, timestamp] = line.split("::")
            cur.execute('INSERT INTO {rate_tab} VALUES ({usr},{miv},{rate})'.format(rate_tab = ratingstablename, usr = userId, miv = movieId, rate = rating))
    openconnection.commit()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    
    cur = openconnection.cursor()
    data_table = 'range_part'
    cur.execute("CREATE TABLE IF NOT EXISTS range (partition INT, frm FLOAT, rtng_to float)")

    
    i = 0
    while (i < numberofpartitions):
        tmp = float(5 / numberofpartitions)
        offset = i * tmp
        table = (i + 1) * tmp
        data_name = data_table + str(i)
        cur.execute("CREATE TABLE IF NOT EXISTS {rate_tab} (userid INT, movieID INT, rating FLOAT)".format(rate_tab = data_name))
        openconnection.commit()
        if (i != 0):
            tableinsert = "INSERT INTO {rate_tab} select * from {rate}  where {rate}.rating > {off} AND {tab} >= {rate}.rating ".format(rate_tab = data_name, rate = ratingstablename, off = offset, tab = table)
        else:
            tableinsert = "INSERT INTO {rate_tab} select * from {rate}  where {rate}.rating BETWEEN {off} AND {tab}  ".format(rate_tab = data_name, rate = ratingstablename, off = offset, tab = table)
        cur.execute(tableinsert)
        openconnection.commit()
        range_insert = "INSERT INTO range  VALUES ({partition},{off},{tab})".format(partition = i, off = offset, tab = table)
        i = i + 1
        cur.execute(range_insert)
        openconnection.commit()

def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    data_table = 'rrobin_part'
    cur.execute("CREATE TABLE IF NOT EXISTS round_robin_mid(partition INT, index INT)")
    openconnection.commit()
    cur.execute("CREATE TABLE IF NOT EXISTS round_robin_temp (userid INT, movieid INT, rating FLOAT, index INT)")
    openconnection.commit()
    cur.execute("INSERT INTO round_robin_temp (SELECT {rate_tab}.userid, {rate_tab}.movieid, {rate_tab}.rating , (ROW_NUMBER() OVER() -1) % {n} as index from {rate_tab})".format(n = str(numberofpartitions), rate_tab = ratingstablename))
    openconnection.commit()
    i = 0
    while (i < numberofpartitions):
        cur.execute( "CREATE TABLE IF NOT EXISTS {rate_tab} (userid INT, movieid INT, rating FLOAT)".format(rate_tab = data_table + str(i)))
        openconnection.commit()
        ins_round_robin = "INSERT INTO {rate_tab} select userid,movieid,rating from round_robin_temp where index = {index}".format(rate_tab = data_table + str(i), index = str(i))
        i = i + 1
        cur.execute(ins_round_robin)
        openconnection.commit()

    cur.execute( "INSERT INTO round_robin_mid SELECT {nos} AS partition, count(*) % {nos} from {rate_tab}".format(rate_tab = ratingstablename, nos = numberofpartitions))
    openconnection.commit()
    deleteTables('round_robin_temp', openconnection)

    openconnection.commit()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT * from round_robin_mid")
    parts_count, i = cur.fetchone()
    offset = i % parts_count
    cur.execute("DELETE from round_robin_mid")
    openconnection.commit()
    cur.execute("Insert into round_robin_mid VALUES ({first},{second})".format(first = parts_count, second = offset + 1))
    openconnection.commit()
    cur.execute("Insert into {rate_tab} values ({usr},{itm},{rate})".format(rate_tab =ratingstablename, usr = userid, itm =itemid, rate = rating))
    openconnection.commit()
    cur.execute("Insert into rrobin_part{i} values ({usr},{itm},{rate})".format(i = offset, usr = userid, itm = itemid, rate = rating))
    openconnection.commit()

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT MIN(r.partition) FROM range  as r where r.frm <= {rat} and r.rtng_to >= {rat} ".format(rat =rating ))
    openconnection.commit()
    part_count = cur.fetchone()
    first_p = part_count[0]
    cur.execute("Insert into {rate_tab} values ({usr},{itm},{rate})".format(rate_tab = ratingstablename, usr = userid, itm = itemid, rate = rating))
    openconnection.commit()
    cur.execute("Insert into range_part{i} values ({usr},{itm},{rate})".format(i = first_p, usr = userid, itm = itemid, rate = rating))
    openconnection.commit()

def createDB(dbname='dds_assignment'):
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = con.cursor()

    cursor.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cursor.fetchone()[0]
    if count == 0:
        cursor.execute('CREATE DATABASE %s' % (dbname,))  
    else:
        print 'A database named {0} already exists'.format(dbname)

    
    cursor.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cursor = openconnection.cursor()
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cursor:
        l.append(row[0])
    for tablename in l:
        cursor.execute("drop table if exists {0} CASCADE".format(tablename))

    cursor.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    finally:
        if cursor:
            cursor.close()