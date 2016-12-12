#!/usr/bin/python
# -*- coding: utf-8 -*-

# Challenge 1: Automated Crime Data Analysis
# Qian Yang (qyang1)

# This script ingests the filtered data into corresponding databases.
# 1) accepts filtered data via STDin
# 2) regulate the altered neighborhood strings to the correct ones
# 3) upsert neighborhood and crime_code databases
# 4) ingest to crimes database, with duplicates skipped

import psycopg2
import csv
import sys
import logging
import setup

# ===================== setup database connection and logger ===================== #

(cursor, conn) = setup.conn_to_psql()
logger = setup.setup_logger()

# ==================== functions ==================== #

# split a csv-format crime event entry into variables
def read_crime_stdin(crime_line):
    splited = [x for x in crime_line.split(',')]

    return (
        splited[1],
        splited[2],
        splited[3],
        splited[4],
        splited[5],
        splited[6],
        splited[7],
        splited[8],
        )

# check if a neighborhood string matches excisting directory
# if not, try get substitute by a similar one

def regulate_hood(cursor, raw_hood, _id):
    raw_hood = raw_hood.title()
    similar_hood_list = []
    
    # try find exact match in neighborhood table

    cursor.execute('''SELECT neighborhood FROM neighborhoods WHERE neighborhood = %s;''' , (raw_hood, ))
    if cursor.rowcount > 0:
        pass
    else:

        # try find neighborhoods containing the query hood

        cursor.execute('''SELECT neighborhood FROM neighborhoods WHERE neighborhood LIKE '%%' || %s || '%%';'''
                       , (raw_hood, ))
        if cursor.rowcount > 0:
            pass
        else:
            # try find neighborhood that's substring of the query hood
            cursor.execute("SELECT neighborhood FROM neighborhoods WHERE %s LIKE FORMAT(%s, neighborhood);", (raw_hood, '%%%s%%', ))

    try:
        similar_hood_list = cursor.fetchall()
    except AttributeError as err:
        # if no similar neighborhood found
        # return raw neighborhood input
        logger.warning('Can\'t recognize neighborhood of crime#%s: %s.' % (_id, raw_hood))
        similar_hood_list = [[raw_hood]]

    if len(similar_hood_list) == 1:
        new_hood = similar_hood_list[0][0]
        if new_hood != raw_hood:
            # neighborhood replaced by a similar but correct neighborhood string
            logger.info('Neighborhood of crime#%s %s is corrected to %s.' % (_id, raw_hood, new_hood))
            return new_hood
        else:
            # neighborhood remains the same
            return raw_hood
    
    elif len(similar_hood_list) > 1:

        # if more than one matches found
        # log the matches and return the raw neighborhood input
        logger.warning('Ambiguous neighborhood (crime#%s) %s: similar to %s' % (_id, raw_hood, str(similar_hood_list)))
        return raw_hood;


# upsert neighborhood-zone_no pairs into neighborhoods database

def upsert_hood_zone(zone_no, neighborhood):
    try:
        cursor.execute('UPDATE neighborhoods SET zone_no = %s WHERE neighborhood = %s;'
                       , (zone_no, neighborhood))

    except psycopg2.DataError, err:
        # found new neighborhood string in the incoming data
        conn.rollback()
        
        # alternatively, update the neighborhood db
        # cursor.execute('INSERT INTO neighborhoods (zone_no, neighborhood) VALUES (%s, %s);', (zone_no, neighborhood))
    
    except psycopg2.IntegrityError, err:
        # print ('duplicate neighborhood:', err[0])
        conn.rollback()
    else:
        conn.commit()


def insert_crime_code(section, description, report_name):
    try:
        cursor.execute('INSERT INTO crime_code (section, description, report_name) VALUES (%s, %s, %s);'
                       , (section, description, report_name))
    
    except psycopg2.IntegrityError, err:
        # print ('duplicate crime_code:', err[0])
        conn.rollback()
    except:
        print sys.exc_info()
        conn.commit()


# insert crime entries to crimes db
# while skipping duplicates

def insert_crimes(_id, section, arrest_time, address, neighborhood):
    try:
        cursor.execute('INSERT INTO crimes (id, section, arrest_time, address, neighborhood) VALUES (%s, %s, %s, %s, %s);'
                       , (_id, section, arrest_time, address, neighborhood))
        logger.info('New crime case added! #%s' % _id)
    except psycopg2.IntegrityError, err:
        logger.info('Duplicate crime case detected! #%s' % _id)
        conn.rollback()
    else:
        conn.commit()


# ================== Load filtered crime data into the database ================== #

# read from stdin, which contains filtered crime events
for crime_line in sys.stdin:
    
    # identify effective crime entries by the '//' prefix
    if crime_line.startswith('//'):  
        
        # remove the prefix
        crime_line = crime_line.strip( '//,' )
        
        # read crime entries
        (_id, report_name, section, description, arrest_time, address, neighborhood, zone_no) = setup.read_crime_csv(crime_line)
        
        # regulate the altered neighborhood strings to the correct ones
        neighborhood = regulate_hood(cursor, neighborhood, _id)
        
        # upsert neighborhood and crime_code databases
        # given the incoming crime entries
        upsert_hood_zone(zone_no, neighborhood)
        insert_crime_code(section, description, report_name)

        # try insert the crime event to the crimes table
        # while skipping duplicate
        insert_crimes(_id, section, arrest_time, address, neighborhood)

conn.close()


