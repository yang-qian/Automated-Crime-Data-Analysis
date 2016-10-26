Challenge 1: Automated Crime Data Analysis
Qian Yang 2016/10/11

This README file specifies the commands to be used to ingest data and generate
a report.

/* ==================== SHELL COMMANDS ==================== */
python setup.py
python filter.py < crime-base.csv | python ingest.py

python filter.py < crime-week-1-patch.csv | python patch.py
python filter.py < crime-week-1.csv | python ingest.py

python filter.py < crime-week-2-patch.csv | python patch.py
python filter.py < crime-week-2.csv | python ingest.py

python filter.py < crime-week-3-patch.csv | python patch.py
python filter.py < crime-week-3.csv | python ingest.py

python filter.py < crime-week-4-patch.csv | python patch.py
python filter.py < crime-week-4.csv | python ingest.py

/* ==================== FILE DIRECTORY ==================== */

setup.py
# main function: load_hood_base() -- loads neighborhood ground truth into the database
# common functions
# 1) conn_to_psql() -- sets up connection to postgress batabase
# 2) setup_logger() -- sets up logger
# 3) read_crime_csv() -- reads lines of crime files and returns seperate variables

filter.py
# coarsely filters the input crime data.
# 1) accepts the name of a CSV file as an argument
# 2) filter the data, only including OFFENSE 2.0 reports of pre-identified kinds AND crimes with legal neighbor and zone attributes.
# 3) print the result as a CSV file to STDOUT.

ingest.py
# This script ingests the filtered data into corresponding databases.
# 1) accepts filtered data via STDin
# 2) regulate the altered neighborhood strings to the correct ones
# 3) upsert neighborhood and crime_code databases
# 4) ingest to crimes database, with duplicates skipped

patch.py
# This script upserts the patch data into the crims database.
# 1) accepts a filtered patch file as input via STDin
# 2) upsert crimes database, with duplicates updated

report.py

test.py


/* ==================== Q & A ==================== */
Q: What is the benefit of writing a set of scripts to be used from the command line, instead
of a single large script? How easy was it to load multiple weeks of data and generate
new reports?

A: I feel the major benefits are (1) flexibility, especially in reusing the functions and scripts thus to keep the code dry globally, (2) easy to automate through shell commands, (3) user friendly interfaces. The design keeps the interface (at terminal) readable and easily interactive. On a side note, through working on this challenge, I also feel the simplicity of this approach comes with the price of interpretability and engineering complexities. For example, reading data via STDin/out is susceptible to other messages popped up in the shell, i.e. errors, debugging logs.


