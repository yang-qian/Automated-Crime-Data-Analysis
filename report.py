#!/usr/bin/python
# -*- coding: utf-8 -*-

# Challenge 1: Automated Crime Data Analysis
# Qian Yang (qyang1)

import psycopg2
import setup

# ===================== setup database connection and logger ===================== #

(cursor, conn) = setup.conn_to_psql()
logger = setup.setup_logger()

# =============================== functions =============================== #


cursor.execute('''SELECT * FROM crimes 
LEFT OUTER JOIN neighborhoods ON crimes.neighborhood = neighborhoods.neighborhood
LEFT OUTER JOIN crime_code ON crime_code.section = crimes.section''')


