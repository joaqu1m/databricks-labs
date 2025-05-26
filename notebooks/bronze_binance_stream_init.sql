CREATE TABLE bronze_binance_stream (
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
