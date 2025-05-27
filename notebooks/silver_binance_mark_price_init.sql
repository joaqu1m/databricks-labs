-- Databricks notebook source
USE dbacademy.labuser10394792_1748293561

-- COMMAND ----------

CREATE TABLE silver_binance_mark_price (
  `date` TIMESTAMP,
  partition_interval TIMESTAMP,
  symbol STRING,
  index_price DECIMAL(23, 8),
  mark_price DECIMAL(23, 8)
)
PARTITIONED BY (partition_interval);