#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import threading

##################### This needs to changed based on what kind of table we want to sort. ##################
##################### To know how to change this, see Assignment 3 Instructions carefully #################
FIRST_TABLE_NAME = 'ratings'
SECOND_TABLE_NAME = 'movies'
SORT_COLUMN_NAME_FIRST_TABLE = 'Rating'
SORT_COLUMN_NAME_SECOND_TABLE = 'MovieId1'
JOIN_COLUMN_NAME_FIRST_TABLE = 'MovieId'
JOIN_COLUMN_NAME_SECOND_TABLE = 'MovieId1'
##########################################################################################################


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.
    cur = openconnection.cursor()
    jobs = []
    threads = 5;

    # Citation: https://stackoverflow.com/questions/109325/postgresql-describe-table
    # getting the schema of the input table so as to create partition and output tables with the same schema
    cur.execute("select column_name, data_type " + "from INFORMATION_SCHEMA.COLUMNS where table_name = '"
                + InputTable + "'")
    table_schema = cur.fetchall()

    # getting the range of the sorting column so as to perform range partitioning
    cur.execute("select min(" + SortingColumnName + "), max(" + SortingColumnName + ") from " + InputTable)
    column_range = cur.fetchall()
    min_sortcolumn_val = column_range[0][0]
    max_sortcolumn_val = column_range[0][1]
    sortcolumn_interval = (max_sortcolumn_val - min_sortcolumn_val)/5.0

    for i in range(threads):
        # creating partition tables as per the schema of input table
        cur.execute("drop table if exists " + "partition" + str(i))
        cur.execute("create table if not exists partition" + str(i) + " (" + table_schema[0][0] + " "
                    + table_schema[0][1] + ")")

        for col_idx in (1, len(table_schema) - 1):
            cur.execute("alter table partition" + str(i) + " add column " +
                        table_schema[col_idx][0] + " " + table_schema[col_idx][1])

        if(i == 0):
            # Citation: https://www.quantstart.com/articles/parallelising-python-with-threading-and-multiprocessing
            # creating thread for parallel sorting
            thread = threading.Thread(
                target=ThreadSort(InputTable, SortingColumnName, i, min_sortcolumn_val,
                                  min_sortcolumn_val + sortcolumn_interval, openconnection))
        else:
            thread = threading.Thread(
                target=ThreadSort(InputTable, SortingColumnName, i, min_sortcolumn_val + (sortcolumn_interval * i),
                                  min_sortcolumn_val + (sortcolumn_interval * (i + 1)), openconnection))

        # jobs list holds all the threads
        jobs.append(thread)

        # starting thread
        thread.start()

    # Join to ensure all the threads have completed
    for j in jobs:
        j.join()

    # creating output table based on the schema of input table
    cur.execute("drop table if exists " + OutputTable)
    cur.execute("create table if not exists " + OutputTable + " (" + table_schema[0][0] + " "
                + table_schema[0][1] + ")")
    for col_idx in (1, len(table_schema) - 1):
        cur.execute("alter table " + OutputTable + " add column " +
                    table_schema[col_idx][0] + " " + table_schema[col_idx][1])

    # inserting sorted records from partition tables to output table
    for i in range(threads):
        cur.execute("insert into " + OutputTable + " select * from partition" + str(i))

    cur.close()
    openconnection.commit()

def ThreadSort (InputTable, SortingColumnName, ThreadId, col_min, col_max, openconnection):

    cur = openconnection.cursor()

    # sorting and inserting records into partition tables
    if(ThreadId == 0):
        cur.execute(
            "insert into partition" + str(ThreadId) + " select * from " + InputTable + " where " +
            SortingColumnName + " >= " + str(col_min) + " and " + SortingColumnName + " <= " + str(col_max) +
            " order by " + SortingColumnName)
    else:
        cur.execute(
            "insert into partition" + str(ThreadId) + " select * from " + InputTable + " where " +
            SortingColumnName + " > " + str(col_min) + " and " + SortingColumnName + " <= " + str(col_max) +
            " order by " + SortingColumnName)

    cur.close()
    openconnection.commit()

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    cur = openconnection.cursor()
    jobs = []
    threads = 5;

    # Citation: https://stackoverflow.com/questions/109325/postgresql-describe-table
    # getting the schema of the input tables so as to create output table with the combined schema
    cur.execute("select column_name, data_type " + "from INFORMATION_SCHEMA.COLUMNS where table_name = '"
                + InputTable1 + "'")
    table_schema1 = cur.fetchall()
    cur.execute("select column_name, data_type " + "from INFORMATION_SCHEMA.COLUMNS where table_name = '"
                + InputTable2 + "'")
    table_schema2 = cur.fetchall()

    # getting the most min and most max of the 2 input tables to determine range partition intervals
    cur.execute("select min(" + Table1JoinColumn + "), max(" + Table1JoinColumn + ") from " + InputTable1)
    column_range1 = cur.fetchall()
    cur.execute("select min(" + Table2JoinColumn + "), max(" + Table2JoinColumn + ") from " + InputTable2)
    column_range2 = cur.fetchall()

    min_sortcolumn_val = column_range1[0][0] if column_range1[0][0] < column_range2[0][0] else column_range2[0][0]
    max_sortcolumn_val = column_range1[0][1] if column_range1[0][1] > column_range2[0][1] else column_range2[0][1]
    sortcolumn_interval = (max_sortcolumn_val - min_sortcolumn_val) / 5.0

    # perform range partition on the 2 input tables separately
    for i in range(threads):
        if(i == 0):
            RangePartition(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, i, min_sortcolumn_val,
                                  min_sortcolumn_val + sortcolumn_interval, openconnection)
        else:
            RangePartition(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, i,
                                  min_sortcolumn_val + (sortcolumn_interval * i),
                                  min_sortcolumn_val + (sortcolumn_interval * (i + 1)), openconnection)

    # creating output table with the combined schema of both input tables
    cur.execute("drop table if exists " + OutputTable)
    cur.execute("create table if not exists " + OutputTable + " (" + table_schema1[0][0] + " "
                + table_schema1[0][1] + ")")

    for col_idx1 in (1, len(table_schema1)-1):
        cur.execute("alter table " + OutputTable + " add column " +
                    table_schema1[col_idx1][0] + " " + table_schema1[col_idx1][1])

    for col_idx2 in range(len(table_schema2)):
        cur.execute("alter table " + OutputTable + " add column " +
                    table_schema2[col_idx2][0] + " " + table_schema2[col_idx2][1])

    # creating threads to perform parallel join
    for i in range(threads):
            thread = threading.Thread(
                target=ThreadJoin(i, OutputTable, Table1JoinColumn, Table2JoinColumn, openconnection))
            jobs.append(thread)
            thread.start()

    # Ensure that all the threads have completed
    for j in jobs:
        j.join()

    cur.close()
    openconnection.commit()

def RangePartition(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, ThreadId, col_min, col_max,
                   openconnection):

    cur = openconnection.cursor()
    # perform range partition on the 2 input tables separately
    if (ThreadId == 0):
        cur.execute(
            "create table if not exists tab1_part_" + str(ThreadId) + " as select * from " + InputTable1 + " where " +
            Table1JoinColumn + " >= " + str(col_min) + " and " + Table1JoinColumn + " <= " + str(col_max))

        cur.execute(
            "create table if not exists tab2_part_" + str(ThreadId) + " as select * from " + InputTable2 + " where " +
            Table2JoinColumn + " >= " + str(col_min) + " and " + Table2JoinColumn + " <= " + str(col_max))
    else:
        cur.execute(
            "create table if not exists tab1_part_" + str(ThreadId) + " as select * from " + InputTable1 + " where " +
            Table1JoinColumn + " > " + str(col_min) + " and " + Table1JoinColumn + " <= " + str(col_max))

        cur.execute(
            "create table if not exists tab2_part_" + str(ThreadId) + " as select * from " + InputTable2 + " where " +
            Table2JoinColumn + " > " + str(col_min) + " and " + Table2JoinColumn + " <= " + str(col_max))

    cur.close()
    openconnection.commit()

def ThreadJoin(ThreadId, OutputTable, JoinColumn1, JoinColumn2, openconnection):

    cur = openconnection.cursor()

    # joining partitions from input tables 1 and 2 and inserting into the output table
    cur.execute("insert into " + OutputTable + " select * from " + " tab1_part_" + str(ThreadId)
                + " inner join tab2_part_" + str(ThreadId) + " on tab1_part_" + str(ThreadId) + "."
                + JoinColumn1 + " = tab2_part_" + str(ThreadId) + "." + JoinColumn2)

    cur.close()
    openconnection.commit()


################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
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
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

if __name__ == '__main__':
    try:
	# Creating Database ddsassignment3
	print "Creating Database named as ddsassignment3"
	createDB();
	
	# Getting connection to the database
	print "Getting connection from the ddsassignment3 database"
	con = getOpenConnection();

	# Calling ParallelSort
	print "Performing Parallel Sort"
	ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con);

	# Calling ParallelJoin
	print "Performing Parallel Join"
	ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE, 'parallelJoinOutputTable', con);
	
	# Saving parallelSortOutputTable and parallelJoinOutputTable on two files
	saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con);
	saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con);

	# Deleting parallelSortOutputTable and parallelJoinOutputTable
	deleteTables('parallelSortOutputTable', con);
       	deleteTables('parallelJoinOutputTable', con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail
