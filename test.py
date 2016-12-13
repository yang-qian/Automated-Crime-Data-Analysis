#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Challenge 1: Automated Crime Data Analysis
# Qian Yang (qyang1)

import psycopg2
import sys
import logging
import setup
import unittest
from ingest import read_crime_stdin, regulated_hood, upsert_hood_zone, insert_crime_code

# setting up connection to a seperate database for testing
def conn_to_test_psql():
    try:
        # define connection string
        conn = \
            'host=pg.stat.cmu.edu dbname=qyang1_unittest user=qyang1 password=6s76zY07o97314W'
        # get a connection
        conn = psycopg2.connect(conn)
        conn.autocommit = True
    except:
        # if a connect cannot be made an exception will be raised here
        print "Unable to connect to the database\n	->%s" % conn
    return (conn.cursor(), conn)
        

#============= Test ingest.py functions =============#

class test_ingest(unittest.TestCase):
    def test_read_crime_stdin(self):
        self.assertEqual(read_crime_stdin("40.4510923,-79.9473004,Shadyside,68,592.104,0.921"),
                         (40.4510923,-79.9473004,"Shadyside",68,592.104,0.921))
        self.assertEqual(read_crime_stdin("40.4400199,-80.0409711,Elliott,30,389.87,0.606"),
                         (40.4400199,-80.0409711,"Elliott",30,389.87,0.606))

class test_ingest_to_database(unittest.TestCase):
    @classmethod
    def setUpClass(cases):
        # set up a seperate database for testing
        (cursor, conn) = conn_to_test_psql()
        
        # set up a seperate logger
        logger = setup.setup_logger()
        
        # set up the test tables with the same schema as the ones the tested functions operate on
        cursor.execute(slurp('schema.sql'))
        
        # load "police-neighborhoods.csv" into the test database
        setup.load_hood_base()

    @classmethod
    def tearDownClass(cases):
        # delete the test tables
        # so that the random records added during previous unittest iterations do not pollute the database
        cursor.execute('''DROP TABLE crimes CASCADE; DROP TABLE crime_code; DROP TABLE neighborhoods;''')
        conn.close()
        logger.removeHandler(ch)
        logger.removeHandler(fh)
        
    def test_regulate_hood(self):
        # if a neighborhood str has an exact match in the neighborhood table, return the original one
        # here the crime id parameter is for logger use only
        self.assertEqual(regulate_hood(cursor, "Mount Oliver Borough", "00000"), "Mount Oliver Borough")
        self.assertEqual(regulate_hood(cursor, "East Carnegie", "00000"), "East Carnegie")
        
        # if one neighbourhood in the table is a substring of the query hood
        # return the one in the table (the correct one)
        self.assertEqual(regulate_hood(cursor, "xville","00000"), "knoxville")
        self.assertEqual(regulate_hood(cursor, "-Lemington","00000"),"Lincoln-Lemington-Belmar")
        
        # if >1 neighbourhoods in the table is similar to the query hood, return the original one
        self.assertEqual(regulate_hood(cursor, "Shore","00000"), "Shore")
        self.assertEqual(regulate_hood(cursor, "Hill","00000"), "Hill")
        
    def test_upsert_hood_zone(self):
        # if insert an excisitng neighbourhood
        # the neighbourhood table remains the same
        cursor.execute('''SELECT neighborhood FROM neighborhoods WHERE neighborhood = %s;''' , ("East Liberty", ))
        before = cursor.fetchall()
        upsert_hood_zone(5, "East Liberty")
        cursor.execute('''SELECT neighborhood FROM neighborhoods WHERE neighborhood = %s;''' , ("East Liberty", ))
        after = cursor.fetchall()
        self.assertEqual(before, after)
        
        # if the query neighborhood is unseen, add it to the table
        upsert_hood_zone(10, "Random Neighborhood Name")
        cursor.execute('''SELECT neighborhood FROM neighborhoods WHERE neighborhood = %s;''' , ("Random Neighborhood Name", ))
        self.assertGreater(cursor.rowcount, 1);
                         
        
    def test_insert_crime_code(self):
        # test the crime code table insertion in the same ways
        
        # if inserting an existing crime code...
        cursor.execute('''SELECT * FROM crime_code WHERE section = %s;''' , (4120, ))
        before = cursor.fetchall()
        insert_crime_code(4120, "Identity Theft", "OFFENSE 2.0")
        cursor.execute('''SELECT * FROM crime_code WHERE section = %s;''' , (4120, ))
        after = cursor.fetchall()
        self.assertEqual(before, after)
        
        # if a new crime code...
        insert_crime_code(9999, "Mediocre Food", "OFFENSE 9999")
        cursor.execute('''SELECT * FROM crime_code WHERE section = %s;''' , ("Mediocre Food", ))
        self.assertGreater(cursor.rowcount, 1);
        
    def test_insert_crimes(self):
        # test the crimes table insertion in the same ways
        
        # if inserting an existing crime record...
        cursor.execute('''SELECT * FROM crimes WHERE _id = %s;''' , ("East Liberty", ))
        before = cursor.fetchall()
        insert_crimes(20886, 3309, "2015-03-10T02:29:00", "7th Ave & Grant St", "Golden Triangle/Civic Arena")
        cursor.execute('''SELECT * FROM crimes WHERE _id = %s;''' , ("East Liberty", ))
        after = cursor.fetchall()
        self.assertEqual(before, after)
        
        # if a new crime record...
        insert_crimes(99999, 8888, "2999-12-31T11:59:59", "random address", "not so fancy neighborhood")
        cursor.execute('''SELECT * FROM crimes WHERE _id = %s;''' , (999999999, ))
        self.assertGreater(cursor.rowcount, 1);
        
if __name__ == '__main__':
    unittest.main()
