CREATE TABLE public.staging_checkout (
  id INTEGER, 
  status VARCHAR, 
  status_changed TIMESTAMP, 
  checkout_time TIMESTAMP, 
  checkout_latitude DOUBLE PRECISION, 
  checkout_longitude DOUBLE PRECISION, 
  checkout_comment VARCHAR (1000)
)

CREATE TABLE public.staging_visit (
  id INTEGER, 
  created TIMESTAMP, 
  modified TIMESTAMP, 
  latitude DOUBLE PRECISION, 
  longitude DOUBLE PRECISION, 
  load DOUBLE PRECISION, 
  window_start VARCHAR, 
  window_end VARCHAR, 
  duration VARCHAR, 
  notes VARCHAR(5000), 
  account_id INTEGER, 
  estimated_time_arrival VARCHAR, 
  estimated_time_departure VARCHAR, 
  planned_date VARCHAR, 
  route_id VARCHAR, 
  "order" INTEGER, 
  load_2 DOUBLE PRECISION, 
  load_3 DOUBLE PRECISION, 
  window_start_2 VARCHAR, 
  window_end_2 VARCHAR, 
  calculated_service_time VARCHAR
)

CREATE TABLE public.time_window (
  time_window_id int IDENTITY(0,1) PRIMARY KEY, 
  window_start VARCHAR, 
  window_end VARCHAR,
  window_start_2 VARCHAR, 
  window_end_2 VARCHAR, 
  planned_date VARCHAR, 
  account_id INTEGER, 
  visit_id INTEGER
)

CREATE TABLE public.checkout_time (
  checkout_time_id int IDENTITY(0,1) PRIMARY KEY, 
  checkout_time TIMESTAMP, 
  visit_id INTEGER
)

CREATE TABLE public.time_window_met (
  time_window_met_id int IDENTITY(0,1) PRIMARY KEY, 
  time_window_id INTEGER, 
  checkout_time_id INTEGER, 
  met BOOLEAN, 
  checkout_time TIMESTAMP, 
  account_id INTEGER, 
  visit_id INTEGER
)