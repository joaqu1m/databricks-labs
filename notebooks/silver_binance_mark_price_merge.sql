-- Databricks notebook source
USE dbacademy.labuser10394792_1748311734

-- COMMAND ----------

MERGE INTO silver_binance_mark_price AS T
USING (
  SELECT
    date_trunc('MINUTE', event_timestamp) AS `date`,
    date_trunc('HOUR', event_timestamp) AS partition_interval,
    symbol AS symbol,
    avg(cast(index_price AS DECIMAL(23, 8))) AS index_price,
    avg(cast(mark_price AS DECIMAL(23, 8))) AS mark_price
  FROM
    bronze_binance_mark_price
  WHERE
    received_timestamp >= now() - INTERVAL 10 MINUTE
  GROUP BY ALL
) AS S
ON T.`date` = S.`date` AND T.symbol = S.symbol AND T.partition_interval = S.partition_interval
WHEN MATCHED THEN
  UPDATE SET
    T.index_price = S.index_price,
    T.mark_price = S.mark_price
WHEN NOT MATCHED THEN
  INSERT (`date`, symbol, index_price, mark_price, partition_interval)
  VALUES (
    S.`date`,
    S.symbol,
    S.index_price,
    S.mark_price,
    S.partition_interval
  );