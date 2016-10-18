# Challenge 1: Automated Crime Data Analysis
# Qian Yang 2016/10/11

# Requirements:
# Provide a script to filter data. Make sure they are well-documented and tested as needed. 

# 1) read the filter_offense2 CSV from STDIN
# 2) load the incidents into the table you defined in schema.sql
# Note) If the second script encounters data that is ill-formed, such as a row with no ID, an invalid zone, or other problems, it must catch the error, print an appropriate message identifying the row that is invalid, and continue its work with the next row, rather than crashing.


\copy neighbourhoods FROM 'police-neighborhoods.csv' with DELIMITER ',' csv header;

# ======= Extract section-description correlations from crime base data ====== #
# ==================== and store into the crime_code table =================== #
# ** The crime_code table is incomplete.
crime-base.csv