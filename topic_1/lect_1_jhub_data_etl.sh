#!/bin/bash

pip install names pyarrow pandas

file_path=/home/jovyan/__DATA/IBDT_Spring_2024/topic_1/jhub_logs.csv

echo ${file_path}

python lect_1_jhub_data_processing.py ${file_path}

database=jhub

# users

psql -c 'create database '${database}';'
psql -d ${database} -c 'CREATE TABLE users (login varchar(32) NOT NULL, name varchar(128), email varchar(128));'
psql -d ${database} \
    -c "COPY users FROM '/home/jovyan/ibdt_course_miba_24/topic_1/users.csv' \
    DELIMITER ';' CSV HEADER"

# Logs

psql -d ${database} -c 'CREATE TABLE logs (idx bigint primary key, timestamp timestamp, hub varchar(20), event_code bigint, event_type varchar(32), log varchar(512), login varchar(32));'
psql -d ${database}  \
    -c "COPY logs FROM '/home/jovyan/ibdt_course_miba_24/topic_1/logs.csv' \
    DELIMITER ';' CSV HEADER"