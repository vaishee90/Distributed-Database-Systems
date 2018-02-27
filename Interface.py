#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

DATABASE_NAME = 'dds_assgn1'
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
TEMP_TABLE = 'temp'


def getopenconnection(user='postgres', password='1234', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):

    cur = openconnection.cursor()

    # dropping temp and ratings tables if already present
    cur.execute("drop table if exists " + ratingstablename)
    cur.execute("drop table if exists " + TEMP_TABLE)

    # reading data file from given path
    ratingfile = open(ratingsfilepath)

    # creating temp table to store data copied from ratingfile
    cur.execute("create table " + TEMP_TABLE +
                " (userID integer, extra1 char, movieID integer, extra2 char, rating real, extra3 char, time integer)")

    # copying data from rating file to temp table
    cur.copy_from(ratingfile, TEMP_TABLE, sep=':')

    # creating ratings table by selecting only required columns from temp table
    cur.execute("create table " + ratingstablename + " as select userID, movieID, rating from " + TEMP_TABLE)

    # dropping temp table as no longer required
    cur.execute("drop table if exists " + TEMP_TABLE)

    cur.close()
    # pass


def rangepartition(ratingstablename, numberofpartitions, openconnection):

    cur = openconnection.cursor()

    # setting the starting value of rating range in range_part0 to 0
    ratinginit = 0

    # obtaining rating interval for each partition
    global rating_interval
    rating_interval = round((5.0/numberofpartitions), 2)

    # setting initial partition number to 0
    global partition_no
    partition_no = 0

    # creating range_part partition tables while inserting tuples into them
    # based on the range of ratings obtained by adding rating intervals
    # as long as the current range starts with a rating value less than 5
    while ratinginit < 5:
        cur.execute("drop table if exists " + RANGE_TABLE_PREFIX + str(partition_no))

        if ratinginit == 0:
            cur.execute("create table " + RANGE_TABLE_PREFIX + str(partition_no) + " as select * from " +
                        ratingstablename + " where rating >= " + str(ratinginit) + " and rating <= " + str(ratinginit + rating_interval))
        else:
            cur.execute("create table " + RANGE_TABLE_PREFIX + str(partition_no) + " as select * from " +
                        ratingstablename + " where rating > " + str(ratinginit) + " and rating <= " + str(ratinginit + rating_interval))

        # incrementing ratinginit to move to the next range of ratings to be used for the next partition
        ratinginit += rating_interval

        # partition number incremented since moving on to the next partition
        partition_no += 1
        # print "partition " + str(partition_no) + " created"

    cur.close()
    # pass


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):

    cur = openconnection.cursor()

    global n_of_part
    n_of_part = numberofpartitions

    # partition number to start with
    global part_init
    part_init = -1

    # dropping and creating new partition tables
    for n in range(0, numberofpartitions):
        cur.execute("drop table IF EXISTS " + RROBIN_TABLE_PREFIX + str(n))
        cur.execute("create table " + RROBIN_TABLE_PREFIX + str(n) +
                    " (userID integer, movieID integer, rating real)")

    # adding an ID to each tuple in ratings table
    cur.execute("alter table " + ratingstablename + " add column p_id serial")

    # tuples are added to the partition tables grouped by the value: ID mod total partitions
    # this allows all the tuples to be inserted in a round robin fashion
    # into the respective partition tables
    while 1:
            part_init += 1

            if part_init != numberofpartitions - 1:
                cur.execute("insert into " + RROBIN_TABLE_PREFIX + str(part_init) +
                            " select userID, movieID, rating from " + ratingstablename + " where p_id % " + str(
                                    numberofpartitions) + " = " + str(part_init + 1))

            else:
                cur.execute("insert into " + RROBIN_TABLE_PREFIX + str(part_init) +
                            " select userID, movieID, rating from " + ratingstablename + " where p_id % " + str(
                                    numberofpartitions) + " = " + str(0))
                break

    # dropping the ID since no longer required
    cur.execute("alter table " + ratingstablename + " drop column p_id")

    cur.close()
    # pass


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):

    global part_init

    # setting the current partition number as the last inserted partition under roundrobinpartition() + 1
    # since the new tuple needs to be added to the next partition
    cur_part = part_init + 1

    # if the current partition equals the total number of partitions, setting it back to 0
    if cur_part == n_of_part:
        cur_part = 0

    cur = openconnection.cursor()

    # inserting new record into the main ratings table
    cur.execute("insert into " + ratingstablename + "(userID, movieID, rating) values (%s, %s, %s)",
                (userid, itemid, rating))

    # inserting new record into the respective partition table
    cur.execute("insert into " + RROBIN_TABLE_PREFIX + str(cur_part) + " (userID, movieID, rating) values (%s, %s, %s)",
                (userid, itemid, rating))

    # setting the last inserted partition number to the currently inserted partition number
    part_init = cur_part

    cur.close()
    # pass


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):

    cur = openconnection.cursor()

    # inserting new tuple into the main ratings table
    cur.execute("insert into " + ratingstablename + "(userID, movieID, rating) values (%s, %s, %s)",
                (userid, itemid, rating))

    # getting the rating interval for each partition from the rangepartition()
    interval = rating_interval

    global partition_no

    # if the new tuple has a rating value which when subtracted by the current ratings range returns a negative value,
    # it is clear that the tuple needs to be inserted the partition table having tuples of that ratings range
    for p_no in range(0, partition_no):
        if rating - interval > 0:
            interval += rating_interval

        else:
            cur.execute(
                "insert into " + RANGE_TABLE_PREFIX + str(p_no) + " (userID, movieID, rating) values (%s, %s, %s) ",
                (userid, itemid, rating))
            break

    cur.close()
    # pass


def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
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
    con.close()


# Middleware
def before_db_creation_middleware(databasename):
    # Use it if you want to
    pass


def after_db_creation_middleware(databasename):
    # Use it if you want to
    pass


def before_test_script_starts_middleware(openconnection, databasename):
    # Use it if you want to
    pass


def after_test_script_ends_middleware(openconnection, databasename):
    # Use it if you want to
    pass


def deletepartitionsandexit(openconnection):

    cur = openconnection.cursor()

    # Referenced from: https://stackoverflow.com/questions/3327312/drop-all-tables-in-postgresql
    cur.execute("select 'drop table \"' || tablename || '\" cascade;'" + "from pg_tables where schemaname = 'public';")

    stmts = cur.fetchall()

    # dropping all tables created as part of this script
    for stmt in stmts:
        cur.execute(stmt[0])

    cur.close()
    print "Table deletion completed"
    exit(1)

if __name__ == '__main__':
    try:

        # Use this function to do any set up before creating the DB, if any
        before_db_creation_middleware(DATABASE_NAME)

        create_db(DATABASE_NAME)

        # Use this function to do any set up after creating the DB, if any
        after_db_creation_middleware(DATABASE_NAME)

        with getopenconnection() as con:
            # Use this function to do any set up before I starting calling your functions to test, if you want to
            before_test_script_starts_middleware(con, DATABASE_NAME)

            # Here is where I will start calling your functions to test them. For example,
            loadratings('ratings', 'test_data.dat', con)
            # rangepartition('ratings', 5, con)
            # rangeinsert('ratings', 2, 234, 5, con)
            # roundrobinpartition('ratings', 5, con)
            # ###################################################################################
            # Anything in this area will not be executed as I will call your functions directly
            # so please add whatever code you want to add in main, in the middleware functions provided "only"
            # ###################################################################################

            # Use this function to do any set up after I finish testing, if you want to
            after_test_script_ends_middleware(con, DATABASE_NAME)

    except Exception as detail:
        print "OOPS! This is the error ==> ", detail
