## Project 3: Data Warehouse

<p>The main goal of this project is to build an ETL pipeline for a database hosted on Redshift.</p>

### Database Schema Design
<p>The staging tables are described below:</p>

- staging_songs

<p> It contains data of songs and artists. It is a subset of real data from the Million Song Dataset.</p>

- staging_events

<p>It contains fake activity logs from an imaginary music streaming app based on configuration settings.</p>

<p>The star schema tables are described below:</p>

- songplays

<p> This is the fact table of the schema. It contains information regarding the songs that were played on the app and also artist information and app user information </p>

- songs

<p> Dimension table which constains information about songs played on the app </p>

- users

<p> Dimension table which contains information about users of the streaming app </p>


- time

<p> Dimension table which contains information about user events of the streaming app </p>

- artists

<p> Dimension table which contains information about artists </p>


### ETL Pipeline
<p> The ETL pipeline was built with python scripts. The pipeline inserts on tables staging_events, staging_songs, songplays, songs, users, time and artists.</p>

The pipeline contains the files below:

- sql_queries.py

<p> This script creates all tables that will be necessary during the ETL process. It also tries to drop all tables
before creating them.</p>

- etl.py

<p> This script loads data into the staging tables and also inserts data on fact and dimension tables.</p>

## DUPLICATED ROWS

<p> Tables artist, songs and time had duplicated rows. This problem was solved by the clause "DISTINCT" in the insert query.</p>


### COMMANDS

- command to create tables: python create_tables.py
- command to run the ETL Pipeline: python etl.py
