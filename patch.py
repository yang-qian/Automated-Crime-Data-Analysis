#!/usr/bin/python
# -*- coding: utf-8 -*-

# Challenge 1: Automated Crime Data Analysis
# Qian Yang (qyang1)

import psycopg2
import sys
import ingest
import setup

# ===================== setup database connection and logger ===================== #

(cursor, conn) = setup.conn_to_psql()
logger = setup.setup_logger()

# =============================== patching  =============================== #

# check if the input file is a patch file
# check_if_patch();

# read from stdin, which contains filtered crime events
for crime_line in sys.stdin:
    
    # identify effective crime entries by the '//' prefix
    if crime_line.startswith('//'):  
        
        # remove the prefix
        crime_line = crime_line.strip( '//,' )
        
        # read crime entries
        (_id, report_name, section, description, arrest_time, address, neighborhood, zone_no) = setup.read_crime_csv(crime_line)
        
        # patch the entries to crimes db
        # (updating the duplicates)
        patch_crimes(section, arrest_time, address, neighborhood, _id);
    
conn.close()

# =============================== functions =============================== #

# def check_if_patch():
    # print ("check if patch");

def patch_crimes(section, arrest_time, address, neighborhood, _id):
    try:
        # UPSERT crime events to crimes table
        cursor.execute('UPDATE crimes SET section = %s, arrest_time= %s, address= %s, neighborhood= %s WHERE ID = %s;' , (section, arrest_time, address, neighborhood, _id))
        logger.info('Crime case #%s updated by patch file.' % _id)
        
    except psycopg2.DataError, err:
        # insert as a new event if the _id does not exist
        ingest.insert_crimes(_id, section, arrest_time, address, neighborhood);
        logger.info('Crime case #%s added by patch file.' % _id)
    
    except psycopg2.IntegrityError, err:
        # skip the event whose neighborhood is unlisted
        conn.rollback()
    
    else:
        conn.commit()

