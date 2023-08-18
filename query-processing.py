#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):

    curr = openconnection.cursor()

    curr.execute("Select PartitionNum from RangeRatingsMetadata where " + str(ratingMinValue) + "<=MinRating or " + str(ratingMaxValue) + " >= MaxRating")
    range_partition_number = curr.fetchall()

    p_range = "RangeRatingsPart"
    res = []
    resultant_range_list = []

    for partition_number in range_partition_number:
        table1 = p_range + str(partition_number[0])
        curr.execute("Select * from " + table1 + " where Rating >= " + str(ratingMinValue) + " AND Rating <= " + str(ratingMaxValue))
        res = curr.fetchall()

        for tuple in res:
            resultant_range_list.append(",".join([table1 ,str(tuple[0]),str(tuple[1]) , str(tuple[2])]))

    res = res + resultant_range_list

    curr.execute("Select PartitionNum from RoundRobinRatingsMetadata")
    round_part_num = curr.fetchall()

    x = []

    range_round = "RoundRobinRatingsPart"

    for partition_number1 in range(0, round_part_num[0][0]):
        table1 = range_round + str(partition_number1)
        curr.execute("Select * from " + table1 + " where Rating >= " + str(ratingMinValue) + " AND Rating <= " + str(ratingMaxValue))
        r = curr.fetchall()

        for tuple in r:
            x.append(",".join([table1 ,str(tuple[0]) ,str(tuple[1]) ,str(tuple[2])]))

    res = res + x

    writeToFile('RangeQueryOut.txt', res)

def PointQuery(ratingsTableName, ratingValue, openconnection):

    curr = openconnection.cursor()

    curr.execute("Select PartitionNum from RangeRatingsMetadata where " + str(ratingValue) + " >= MinRating and " + str(ratingValue) + " <= MaxRating")
    range_partition_number = curr.fetchall()

    p_range = "RangeRatingsPart"
    res = []
    resultant_range_list = []

    for partition_number in range_partition_number:
        table1 = p_range + str(partition_number[0])
        curr.execute("Select * from " + table1 + " where Rating = " + str(ratingValue))
        fetchall = curr.fetchall()

        for tuple in fetchall:
            resultant_range_list.append(",".join([table1 ,str(tuple[0]) ,str(tuple[1]) ,str(tuple[2])]))
    res = res + resultant_range_list

    curr.execute("Select PartitionNum from RoundRobinRatingsMetadata")
    round_part_num = curr.fetchall()

    x = []

    range_round = "RoundRobinRatingsPart"

    for partition_number1 in range(0, round_part_num[0][0]):
        table1 = range_round + str(partition_number1)
        curr.execute("Select * from " + table1 + " where Rating =  " + str(ratingValue))
        r = curr.fetchall()

        for tuple in r:
            x.append(",".join([table1 ,str(tuple[0]) ,str(tuple[1]) ,str(tuple[2])]))

    res = res + x

    writeToFile('PointQueryOut.txt', res)


def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(''.join(str(s) for s in line))
        f.write('\n')
    f.close()