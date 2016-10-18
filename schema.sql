-- Challenge 1: Automated Crime Data Analysis
-- Qian Yang 2016/10/11

-- Requirements:
/* Provide schema.sql with your CREATE TABLE commands, along with a command or separate script to load the neighborhood and zone data. Make sure your SQL is clear and readable and your schema is DRY. */

-- 1) Create table
create table crimes (
    ID integer unique not null PRIMARY KEY,
    CREATE TYPE REPORT_NAME AS ENUM ('ARREST', 'OFFENSE 2.0') DEFAULT 'OFFENSE 2.0',
    SECTION varchar REFERENCES crime_code,
    ARREST_TIME timestamp,
    ADDRESS text,
    NEIGHBORHOOD text default 'NO NEIGHBORHOOD' REFERENCES neighbourhoods,
    ZONE integer default 'NO ZONE' CHECK (ZONE > 0)
);

create table crime_code (
    SECTION varchar unique not null PRIMARY KEY,
    DESCRIPTION text unique CHECK (char_length(DESCRIPTION) > 0) 
);

create table crime_code_offense2 (
    SECTION varchar unique not null PRIMARY KEY,
    DESCRIPTION text unique CHECK (char_length(DESCRIPTION) > 0) 
);

insert into crime_code_offense2 (SECTION, DESCRIPTION)
       values ('3304', 'Criminal mischief'),
              ('2709', 'Harassment'),
              ('3502', 'Burglary'),
              ('13(a)(16)', 'Possession of a controlled substance'),
              ('13(a)(30)', 'Possession w/ intent to deliver'),
              ('3701', 'Robbery'),
              ('3921', 'Theft'),
              ('3921(a)', 'Theft of movable property'),
              ('3934', 'Theft from a motor vehicle'),
              ('3929', 'Retail theft'),
              ('2701', 'Simple assault'),
              ('2702', 'Aggravated assault'),
              ('2501', 'Homicide');

-- 2) Load neighbourhood
-- Using COPY commands load the neighborhood data into your tables.

create table neighbourhoods (
    INTPTLAT10 numeric,
    INTPTLON10 numeric,
    NEIGHBORHOOD text unique not null,
    NEIGHBORHOOD_NO integer unique CHECK (NEIGHBORHOOD_NO >= 0),
    ACRES numeric CHECK (ACRES > 0),
    SQMILES numeric CHECK (SQMILES > 0),
    
    CHECK (SQMILES == 640 * ACRES )
    PRIMARY KEY (NEIGHBORHOOD, NEIGHBORHOOD_NO)
);

\copy neighbourhoods FROM '/Users/qyang1/Challenge1_crime_data/data/crime_base.csv' WITH DELIMITER ',' csv header;

