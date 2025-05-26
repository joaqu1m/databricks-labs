-- Databricks notebook source
USE dbacademy.labuser10394792_1748293561

-- COMMAND ----------

CREATE TABLE bronze_binance_agg_trade (
  event_type STRING,
  event_timestamp TIMESTAMP,
  symbol STRING,
  aggregate_trade_id LONG,
  price STRING,
  qnt STRING,
  first_trade_id LONG,
  last_trade_id LONG,
  trade_timestamp TIMESTAMP,
  is_buyer_market_marker BOOLEAN,
  received_timestamp TIMESTAMP,
  trade_date STRING
)
PARTITIONED BY (trade_date);