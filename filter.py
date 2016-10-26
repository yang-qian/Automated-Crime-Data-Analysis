#!/usr/bin/python
# -*- coding: utf-8 -*-

# Challenge 1: Automated Crime Data Analysis
# Qian Yang (qyang1)

# This script coarsely filters the input crime data.
# 1) accepts the name of a CSV file as an argument
# 2) filter the data, only including OFFENSE 2.0 reports of pre-identified kinds AND crimes with legal neighbor and zone attributes.
# 3) print the result as a CSV file to STDOUT.

import psycopg2
import csv
import sys
import setup

# ===================== setup database connection and logger ===================== #

(cursor, conn) = setup.conn_to_psql()
logger = setup.setup_logger()

# ================== Extract Offense 2.0 codes from PSQL ================== #

try:
    offense2_section_list = []
    cursor.execute('select section from crime_code;')
    for section in cursor.fetchall():
        offense2_section_list.append(section[0])

    # print offense2_section_list
except:
    print 'Unable to pull offense 2.0 section codes from the crime_code table.'

    
# ======================== Load input csv file ======================== #

# identify stdout writer
writer = csv.writer(sys.stdout)

# read csv header from stdin
# write header
header = sys.stdin.readline()
sys.stdin.seek(1)
sys.stdout.write(header)


for crime_line in sys.stdin.readlines():
    # Split each row on ','
    (_id, report_name, section, description, arrest_time, address, neighborhood, zone_no) = setup.read_crime_csv(crime_line);
    
    # if the crime event has a zone number and falls into the identified offense 2.0 sections
    # write this event into STDOUT

    if zone_no and report_name == 'OFFENSE 2.0' and section in offense2_section_list:
            
            # for debug
            # print >> fout, _id + ',' + report_name + ',' + section + ',' + description + ',' + arrest_time + ',' + address + ',' + neighborhood + ',' + zone_no
            
            # to prevent other console messages confuses the STDin/out reader
            # add "//" as a identifier of an effective crime entry
            sys.stdout.write('//,' + _id + ',' + report_name + ',' + section + ',' + description + ',' + arrest_time + ',' + address + ',' + neighborhood + ',' + zone_no + ',\n')
    else:
        # print "skipped crime #" + _id + " at zone #" + zone_no + ", " + report_name
        pass
 

conn.close()
