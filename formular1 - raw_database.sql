-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1.create circuit table - CSV

-- COMMAND ----------

CREATE OR REPLACE TABLE f1_raw.circuits(
  circuitId INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING)
USING CSV
OPTIONS (path "s3://<bucket>/raw/circuits.csv", header true);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2.create race table - CSV

-- COMMAND ----------

CREATE OR REPLACE TABLE f1_raw.races(
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING)
USING CSV
OPTIONS (path "s3://<bucket>/raw/races.csv", header true);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3.create constuctor table - JSON

-- COMMAND ----------

CREATE OR REPLACE TABLE f1_raw.constructors(
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING)
USING JSON
OPTIONS (path "s3://<bucket>/raw/constructors.json");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4.create driver table - JSON

-- COMMAND ----------

CREATE OR REPLACE TABLE f1_raw.drivers(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  dob DATE,
  nationality STRING,
  url STRING)
USING JSON
OPTIONS (path "s3://<bucket>/raw/drivers.json");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 5.create result table - JSON

-- COMMAND ----------

CREATE OR REPLACE TABLE f1_raw.results(
  resultId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  grid INT,
  position INT,
  positionText STRING,
  positionOrder INT,
  points INT,
  laps INT,
  time STRING,
  milliseconds INT,
  rank INT,
  fastestLapTime STRING,
  fastestLapSpeed FLOAT,
  statusId STRING)
USING JSON
OPTIONS (path "s3://<bucket>/raw/results.json");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 6.create pit_stop table - JSON (multiline)

-- COMMAND ----------

CREATE OR REPLACE TABLE f1_raw.pit_stops(
  driverId INT,
  duration STRING,
  lap INT,
  milliseconds INT,
  raceId INT,
  stop INT,
  time STRING)
USING JSON
OPTIONS (path "s3://<bucket>/raw/results.json", multiLine true);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 7.create lap_time table - FOLDER_CSV

-- COMMAND ----------

CREATE OR REPLACE TABLE f1_raw.lap_times(
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT)
USING CSV
OPTIONS (path "s3://<bucket>/raw/lap_times");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 8.create qualifying table - FOLDER_JSON

-- COMMAND ----------

CREATE OR REPLACE TABLE f1_raw.qualifying(
  constructorId INT,
  driverId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING,
  qualifyId INT,
  raceId INT)
USING JSON
OPTIONS (path "s3://<bucket>/raw/qualifying", multiLine true);
