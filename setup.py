#!/usr/bin/python
# -*- coding: utf-8 -*-

# Challenge 1: Automated Crime Data Analysis
# Qian Yang (qyang1)

# This script includes
# 1) a main function load_hood_base() loads neighborhood ground truth into the database
# 2) conn_to_psql() -- sets up connection to postgress batabase
# 3) setup_logger() -- sets up logger
# 4) read_crime_csv() -- reads lines of crime files and returns seperate variables

import psycopg2
import logging


# ============================ setup database connection ============================ #

def conn_to_psql():
    try:
        # define connection string
        conn = \
            'host=pg.stat.cmu.edu dbname=qyang1 user=qyang1 password=6s76zY07o97314W'

        # get a connection
        conn = psycopg2.connect(conn)
        conn.autocommit = True

    except:
        # if a connect cannot be made an exception will be raised here
        print "Unable to connect to the database\n	->%s" % conn
    
    return (conn.cursor(), conn)

# =================================== setup logger =================================== #
def setup_logger():
    logger = logging.getLogger('test_logger')
    logger.setLevel(logging.DEBUG)
    # create file handler which logs even debug messages
    fh = logging.FileHandler('crime_data_qyang1.log')
    fh.setLevel(logging.DEBUG)
    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)
    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)
    # add the handlers to logger
    logger.addHandler(ch)
    logger.addHandler(fh)
    return logger

# ================== Load neighborhood ground truth into the database ================== #

def read_crime_csv(crime_line):
    splited = [x.strip() for x in crime_line.split(',')]

    return (splited[0],
            splited[1],
            splited[2],
            splited[3],
            splited[4],
            splited[5],
            splited[6],
            splited[7])

# read from police-neighborhoods.csv
def load_hood_base():
    with open('data/police-neighborhoods.csv', 'r') as fin:
        hoods = fin.readlines()[1:]  # skip header line

        for hood in hoods:

            # Split each row on ','

            data = [x.strip() for x in hood.split(',')]

            (
                intptlat10,
                intptlon10,
                neighborhood,
                hood_no,
                acres,
                sqmiles,
                ) = (
                data[0],
                data[1],
                data[2],
                data[3],
                data[4],
                data[5],
                )

            # Put the splited data to the teams table
            try:
                cursor.execute('INSERT INTO neighborhoods (intptlat10, intptlon10, neighborhood, hood_no, acres, sqmiles) VALUES (%s, %s, %s, %s, %s, %s);', (intptlat10, intptlon10, neighborhood, hood_no, acres, sqmiles,))
            except psycopg2.IntegrityError as err:
                print ("duplicate neighborhood")
                conn.rollback()
            except:
                print sys.exc_info()

    fin.close()

# =================================== main =================================== #  

if __name__ == '__main__':
    (cursor, conn) = conn_to_psql()
    load_hood_base();
    #load_crime_base();