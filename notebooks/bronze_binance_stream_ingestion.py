# Databricks notebook source
# MAGIC %pip install websockets

# COMMAND ----------

# MAGIC %sql
# MAGIC USE dbacademy.labuser10394792_1748213278

# COMMAND ----------

import asyncio
import websockets
import json
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, BooleanType
from pyspark.sql import Row
import pytz

schema = StructType([
    StructField("event_type", StringType(), True),      
    StructField("event_timestamp", TimestampType(), True),  
    StructField("symbol", StringType(), True),          
    StructField("aggregate_trade_id", LongType(), True),
    StructField("price", StringType(), True),           
    StructField("qnt", StringType(), True),             
    StructField("first_trade_id", LongType(), True),    
    StructField("last_trade_id", LongType(), True),     
    StructField("trade_timestamp", TimestampType(), True),
    StructField("is_buyer_market_marker", BooleanType(), True),
    StructField("received_timestamp", TimestampType(), True),
    StructField("trade_date", StringType(), True),
])

def parse_trade(raw):
    return Row(
        event_type=raw["e"],
        event_timestamp=datetime.fromtimestamp(raw["E"] / 1000.0, pytz.utc),
        symbol=raw["s"],
        aggregate_trade_id=raw["a"],
        price=raw["p"],
        qnt=raw["q"],
        first_trade_id=raw["f"],
        last_trade_id=raw["l"],
        trade_timestamp=datetime.fromtimestamp(raw["T"] / 1000.0, pytz.utc),
        is_buyer_market_marker=raw["m"],
        received_timestamp=datetime.now(pytz.utc),
        trade_date=datetime.now(pytz.utc).strftime("%Y-%m-%d"),
    )

async def listen_and_store():
    url = "wss://fstream.binance.com/ws/bnbusdt@aggTrade"

    async with websockets.connect(url) as ws:
        buffer = []
        batch_size = 10

        while True:
            data = await ws.recv()
            parsed = json.loads(data)
            row = parse_trade(parsed)
            buffer.append(row)

            if len(buffer) >= batch_size:
                df = spark.createDataFrame(buffer, schema=schema)

                df.write.format("delta") \
                    .mode("append") \
                    .partitionBy("trade_date") \
                    .saveAsTable("bronze_binance_stream")

                buffer = []
                print(f"Lote salvo: {batch_size} registros")

await listen_and_store()
