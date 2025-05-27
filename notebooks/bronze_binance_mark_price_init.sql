-- Databricks notebook source
USE dbacademy.labuser10394792_1748311734

-- COMMAND ----------

CREATE TABLE bronze_binance_mark_price (
  event_type STRING,
  event_timestamp TIMESTAMP,
  symbol STRING,
  mark_price STRING,
  index_price STRING,
  estimated_settle_price STRING,
  funding_rate STRING,
  next_funding_timestamp TIMESTAMP,
  received_timestamp TIMESTAMP,
  interval_partition TIMESTAMP
)
PARTITIONED BY (interval_partition);