#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys

# Do not close the connection inside this file i.e. do not perform openconnection.close()
TEMP_TABLE = 'temp'

def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    # Implement RangeQuery Here.
    cur = openconnection.cursor()
    cur.execute("create table if not exists " + TEMP_TABLE + " (TableName varchar(40), UserID INT, MovieID INT, Rating REAL)")
    cur.execute("select count(*)" + " from RangeRatingsMetadata")
    p_no = cur.fetchone()[0]
    # print "RangeP: " + str(p_no)

    for i in range(p_no):
            cur.execute(
            "insert into " + TEMP_TABLE + " select 'RangeRatingsPart"
            + str(i) + "', UserID, MovieID, Rating from " + " RangeRatingsPart" +
            str(i) + " where Rating >= " + str(ratingMinValue) + " and Rating <= " + str(ratingMaxValue))


    cur.execute("select PartitionNum" + " from RoundRobinRatingsMetadata")
    p_no = cur.fetchone()[0]
    # print "RoundRobinP: " + str(p_no)

    for i in range(p_no):
            cur.execute(
            "insert into " + TEMP_TABLE + " select 'RoundRobinRatingsPart"
            + str(i) + "', UserID, MovieID, Rating from " + " RoundRobinRatingsPart" +
            str(i) + " where Rating >= " + str(ratingMinValue) + " and Rating <= " + str(ratingMaxValue))

    rangeQueryFile = open('RangeQueryOut.txt', 'w')
    cur.copy_to(rangeQueryFile, TEMP_TABLE, sep=',')

    cur.execute("drop table" + " if exists " + TEMP_TABLE)
    # pass  # Remove this once you are done with implementation


def PointQuery(ratingsTableName, ratingValue, openconnection):
    # Implement PointQuery Here.
    cur = openconnection.cursor()
    cur.execute(
        "create table if not exists " + TEMP_TABLE + " (TableName varchar(40), UserID INT, MovieID INT, Rating REAL)")
    cur.execute("select count(*)" + " from RangeRatingsMetadata")
    p_no = cur.fetchone()[0]
    # print "RangeP: " + str(p_no)

    for i in range(p_no):
        cur.execute(
            "insert into " + TEMP_TABLE + " select 'RangeRatingsPart"
            + str(i) + "', UserID, MovieID, Rating from " + " RangeRatingsPart" +
            str(i) + " where Rating = " + str(ratingValue))

    cur.execute("select PartitionNum" + " from RoundRobinRatingsMetadata")
    p_no = cur.fetchone()[0]
    # print "RoundRobinP: " + str(p_no)

    for i in range(p_no):
        cur.execute(
            "insert into " + TEMP_TABLE + " select 'RoundRobinRatingsPart"
            + str(i) + "', UserID, MovieID, Rating from " + " RoundRobinRatingsPart" +
            str(i) + " where Rating = " + str(ratingValue))

    rangeQueryFile = open('PointQueryOut.txt', 'w')
    cur.copy_to(rangeQueryFile, TEMP_TABLE, sep=',')

    cur.execute("drop table" + " if exists " + TEMP_TABLE)
    # pass  # Remove this once you are done with implementation
