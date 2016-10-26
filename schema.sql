-- Challenge 1: Automated Crime Data Analysis
-- Qian Yang 2016/10/11

/* Requirements: Provide schema.sql with your CREATE TABLE commands, along with a command or separate script to load the neighborhood and zone data. Make sure your SQL is clear and readable and your schema is DRY. */

/*
*/

-- Create table to store neighourhood directory --

create table neighborhoods (
    INTPTLAT10 numeric,
    INTPTLON10 numeric,
    NEIGHBORHOOD text unique not null primary key,
    HOOD_NO integer unique,
    ZONE_NO integer,
    ACRES numeric CHECK (ACRES > 0),
    SQMILES numeric CHECK (SQMILES > 0)
);

/*
 intptlat10 | intptlon10 | neighborhood | hood_no | zone_no | acres | sqmiles 
------------+------------+--------------+---------+---------+-------+---------
(0 rows)
*/

-- Create table to store crime codes --

create TYPE report_type as enum ('ARREST', 'OFFENSE 2.0');

create table crime_code (
    SECTION varchar unique not null PRIMARY KEY,
    DESCRIPTION text unique CHECK (char_length(DESCRIPTION) > 0),
    REPORT_NAME report_type default 'OFFENSE 2.0'
);

-- load identified offense 2.0 crime codes into the table -- 

insert into crime_code (SECTION, DESCRIPTION, REPORT_NAME)
       values ('3304', 'Criminal mischief', 'OFFENSE 2.0'),
              ('2709', 'Harassment', 'OFFENSE 2.0'),
              ('3502', 'Burglary', 'OFFENSE 2.0'),
              ('13(a)(16)', 'Possession of a controlled substance', 'OFFENSE 2.0'),
              ('13(a)(30)', 'Possession w/ intent to deliver', 'OFFENSE 2.0'),
              ('3701', 'Robbery', 'OFFENSE 2.0'),
              ('3921', 'Theft', 'OFFENSE 2.0'),
              ('3921(a)', 'Theft of movable property', 'OFFENSE 2.0'),
              ('3934', 'Theft from a motor vehicle', 'OFFENSE 2.0'),
              ('3929', 'Retail theft', 'OFFENSE 2.0'),
              ('2701', 'Simple assault', 'OFFENSE 2.0'),
              ('2702', 'Aggravated assault', 'OFFENSE 2.0'),
              ('2501', 'Homicide', 'OFFENSE 2.0');
/*
  section  |             description              | report_name 
-----------+--------------------------------------+-------------
 3304      | Criminal mischief                    | OFFENSE 2.0
 2709      | Harassment                           | OFFENSE 2.0
 3502      | Burglary                             | OFFENSE 2.0
 13(a)(16) | Possession of a controlled substance | OFFENSE 2.0
 13(a)(30) | Possession w/ intent to deliver      | OFFENSE 2.0
 3701      | Robbery                              | OFFENSE 2.0
 3921      | Theft                                | OFFENSE 2.0
 3921(a)   | Theft of movable property            | OFFENSE 2.0
 3934      | Theft from a motor vehicle           | OFFENSE 2.0
 3929      | Retail theft                         | OFFENSE 2.0
 2701      | Simple assault                       | OFFENSE 2.0
 2702      | Aggravated assault                   | OFFENSE 2.0
 2501      | Homicide                             | OFFENSE 2.0
(13 rows)
*/


-- Create table to store crime entries --

create table crimes (
    ID integer unique not null PRIMARY KEY,
    SECTION varchar references crime_code,
    ARREST_TIME timestamp,
    ADDRESS text,
    NEIGHBORHOOD text default 'NO NEIGHBORHOOD' references neighborhoods
);

/*
 id | section | arrest_time | address | neighborhood 
----+---------+-------------+---------+--------------
(0 rows)
*/


