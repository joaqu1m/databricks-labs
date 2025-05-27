-- Databricks notebook source
USE dbacademy.labuser10394792_1748311734

-- COMMAND ----------

MERGE INTO gold_binance_mark_price_by_symbol AS T
USING (
WITH time_range AS (
  SELECT explode(sequence(
      date_trunc('MINUTE', now() - interval 9 minutes),
      date_trunc('MINUTE', now() - interval 3 minutes),
      interval 1 minute
    )) AS date
  ),
  symbols AS (
    SELECT DISTINCT symbol
    FROM silver_binance_mark_price
    WHERE date >= now() - interval 10 minutes
  ),
  base_minutes AS (
    SELECT t.date, s.symbol
    FROM time_range t
    CROSS JOIN symbols s
  ),
  raw_data AS (
    SELECT
      `date`,
      symbol,
      index_price,
      mark_price
    FROM silver_binance_mark_price
    WHERE date >= now() - interval 10 minutes
  ),
  joined AS (
    SELECT
      bm.date,
      bm.symbol,
      rd.index_price,
      rd.mark_price
    FROM base_minutes bm
    LEFT JOIN raw_data rd
      ON bm.date = rd.date AND bm.symbol = rd.symbol
  ),
  filled AS (
    SELECT
      date,
      symbol,
      last(index_price, TRUE) OVER (
        PARTITION BY symbol ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS index_price,
      last(mark_price, TRUE) OVER (
        PARTITION BY symbol ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS mark_price
    FROM joined
  )
  SELECT
    date,
    date_trunc('HOUR', date) AS partition_interval,
    symbol,
    index_price,
    mark_price
  FROM filled
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