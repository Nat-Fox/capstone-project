import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_checkout_table_drop = "DROP table IF EXISTS staging_checkout;"
staging_visit_table_drop = "DROP table IF EXISTS staging_visit;"

# query: Deliveries are being made within the time windows?
time_window_table_drop = "DROP table IF EXISTS time_window;"
checkout_time_table_drop = "DROP table IF EXISTS checkout_time;"
time_window_met_table_drop = "DROP table IF EXISTS time_window_met;"

# CREATE TABLES
staging_checkout_table_create = ("""CREATE TABLE IF NOT EXISTS staging_checkout (id INTEGER, status VARCHAR, status_changed TIMESTAMP, checkout_time TIMESTAMP, checkout_latitude DOUBLE PRECISION, checkout_longitude DOUBLE PRECISION, checkout_comment VARCHAR (1000))""")

staging_visit_table_create = ("""CREATE TABLE IF NOT EXISTS staging_visit (id INTEGER, created TIMESTAMP, modified TIMESTAMP, latitude DOUBLE PRECISION, longitude DOUBLE PRECISION, load DOUBLE PRECISION, window_start VARCHAR, window_end VARCHAR, duration VARCHAR, notes VARCHAR(5000), account_id INTEGER, estimated_time_arrival VARCHAR, estimated_time_departure VARCHAR, planned_date VARCHAR, route_id VARCHAR, "order" INTEGER, load_2 DOUBLE PRECISION, load_3 DOUBLE PRECISION, window_start_2 VARCHAR, window_end_2 VARCHAR, calculated_service_time VARCHAR)""")

time_window_table_create = ("""
CREATE TABLE IF NOT EXISTS time_window (time_window_id int IDENTITY(0,1) PRIMARY KEY, window_start VARCHAR, window_end VARCHAR,window_start_2 VARCHAR, window_end_2 VARCHAR, planned_date VARCHAR, account_id INTEGER, visit_id INTEGER);
""")

checkout_time_table_create = ("""
CREATE TABLE IF NOT EXISTS checkout_time (checkout_time_id int IDENTITY(0,1) PRIMARY KEY, checkout_time TIMESTAMP, visit_id INTEGER);
""")

time_window_met_table_create = ("""
CREATE TABLE IF NOT EXISTS time_window_met (time_window_met_id int IDENTITY(0,1) PRIMARY KEY, time_window_id INTEGER, checkout_time_id INTEGER, met BOOLEAN, checkout_time TIMESTAMP, account_id INTEGER, visit_id INTEGER);
""")

# STAGING TABLES
staging_checkout_copy = (""" 
copy staging_checkout from {} ignoreheader 1 iam_role {} csv region 'us-west-2' delimiter ';';
""").format(config['S3']['checkouts'], config['IAM_ROLE']['ARN'])

staging_visit_copy = ("""
copy staging_visit from {} iam_role {} json 'auto' region 'us-west-2' timeformat 'YYYY-MM-DD HH:MI:SS';
""").format(config['S3']['visits'], config['IAM_ROLE']['ARN'])


# INSERT TABLES
time_window_table_insert = ("""
INSERT INTO time_window (window_start, window_end, window_start_2, window_end_2, planned_date, account_id, visit_id) SELECT DISTINCT window_start, window_end, window_start_2, window_end_2, planned_date, account_id, id FROM staging_visit;
""")

checkout_time_table_insert = ("""
INSERT INTO checkout_time (checkout_time, visit_id) SELECT DISTINCT checkout_time, id FROM staging_checkout;
""")

time_window_met_table_insert = ("""
INSERT INTO time_window_met (time_window_id, checkout_time_id, checkout_time, account_id, visit_id, met) SELECT
  time_window.time_window_id,
  checkout_time.checkout_time_id,
  checkout_time.checkout_time,
  time_window.account_id,
  time_window.visit_id,
  (CASE
  WHEN (checkout_time.checkout_time
  BETWEEN (time_window.planned_date || ' ' || time_window.window_start)
    AND (time_window.planned_date || ' ' || time_window.window_end))

    OR (checkout_time.checkout_time
       BETWEEN (time_window.planned_date || ' ' || time_window.window_start_2)
    AND (time_window.planned_date || ' ' || time_window.window_end_2))
    THEN TRUE
    ELSE FALSE
    END) AS met
FROM time_window
JOIN checkout_time
ON time_window.visit_id = checkout_time.visit_id;
""")


# QUERY LISTS
create_table_queries = [time_window_table_create, checkout_time_table_create, time_window_met_table_create]
drop_table_queries = [time_window_table_drop, checkout_time_table_drop, time_window_met_table_drop]
copy_table_queries = [staging_checkout_copy, staging_visit_table_create]
insert_table_queries = [time_window_table_insert, checkout_time_table_insert, time_window_met_table_insert]


