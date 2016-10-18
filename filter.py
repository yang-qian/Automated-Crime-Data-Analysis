# Challenge 1: Automated Crime Data Analysis
# Qian Yang 2016/10/11

# Requirements:
# Provide a script to filter data. Make sure they are well-documented and tested as needed, and can ingest all data from weeks 1 through 4.

# This script
# 1) accepts the name of the blotter CSV file as an argument,
# 2) filter the data, only including OFFENSE 2.0 reports,
# 3) skip crimes with no zone.
# 4) print the result as a CSV file to STDOUT.

import psycopg2
import pandas as pd

# ======= Read Filtering Criteria from crime_code_offense2 via SQL ======= #

def main():
    #Define connection string
    conn = psycopg2.connect(host = 'pg.stat.cmu.edu',
                           database = 'qyang1',
                           user = 'qyang1',
                           password = '6s76zY07o97314W')

    # print the connection string to connect
    print "Connecting to database\n	->%s" % (conn)

    # get a connection, if a connect cannot be made an exception will be raised here
    conn = psycopg2.connect(conn)

    # conn.cursor will return a cursor object, you can use this cursor to perform queries
    cursor = conn.cursor()
    print "Connected!\n"

    # retrieve the records from the database
    cursor.execute("select * from crime_code_offense2")
    criteria = cursor.fetchall()        

if __name__ == "__main__":
	main()

# ======================== Load input speadsheets ======================== #

def load_this(file_name):
    assert file_name.startswith('crime-week-'), "Unusual file name."
    
    this_blotter = pd.read_csv('data/%s.csv' % file_name)
    print this_botter.head()
    
    # annotate whether the file is a new one or a patch
    this_blotter['new'] = not (file_name.endswith('-patch'))
    
    return this_blotter

def filter_offense2(df):
    
    # skip the crime entries without zone #
    df = df[df.ZONE.notnull()]
    
    # in case of the REPORT_NAME, SECTION, and DESCRIPTION names in the input file do not match
    # filter crime entries by section code in the input datafile
    df_offense2 = df[df['SECTION'].isin([item[0] for item in criteria])]
        
    # uniform description column texts to lower case
    
    # clean non-alphabet characters in the dataframe description column
    
    
    
filtered.to_csv('crime_data.csv')

# ================================ Testing ================================ #

