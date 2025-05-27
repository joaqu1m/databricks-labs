-- Databricks notebook source
USE dbacademy.labuser10394792_1748311734

-- COMMAND ----------

CREATE TABLE gold_binance_mark_price_by_symbol (
  `date` TIMESTAMP,
  partition_interval TIMESTAMP,
  symbol STRING,
  index_price DECIMAL(23, 8),
  mark_price DECIMAL(23, 8)
)
PARTITIONED BY (partition_interval);