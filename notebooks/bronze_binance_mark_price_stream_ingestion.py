# Databricks notebook source
# MAGIC %pip install websockets

# COMMAND ----------

# MAGIC %sql
# MAGIC USE dbacademy.labuser10394792_1748311734

# COMMAND ----------

import websockets
import json
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql import Row
import pytz

schema = StructType([
    StructField("event_type", StringType(), True),      
    StructField("event_timestamp", TimestampType(), True),  
    StructField("symbol", StringType(), True),          
    StructField("mark_price", StringType(), True),
    StructField("index_price", StringType(), True),           
    StructField("estimated_settle_price", StringType(), True),             
    StructField("funding_rate", StringType(), True),    
    StructField("next_funding_timestamp", TimestampType(), True),     
    StructField("received_timestamp", TimestampType(), True),
    StructField("interval_partition", TimestampType(), True),
])

def parse_trade(raw):
    event_timestamp = datetime.fromtimestamp(raw["E"] / 1000.0, pytz.utc)
    return Row(
        event_type=raw["e"],
        event_timestamp=event_timestamp,
        symbol=raw["s"],
        mark_price=raw["p"],
        index_price=raw["i"],
        estimated_settle_price=raw.get("P", None),
        funding_rate=raw["r"],
        next_funding_timestamp=datetime.fromtimestamp(raw["T"] / 1000.0, pytz.utc),
        received_timestamp=datetime.now(pytz.utc),
        interval_partition=event_timestamp.replace(minute=0, second=0, microsecond=0),
    )

BATCH_SIZE = 10000

async def listen_and_store():
    url = "wss://fstream.binance.com/ws/!markPrice@arr"

    async with websockets.connect(url) as ws:
        buffer = []

        while True:
            data = await ws.recv()
            parsed_array = json.loads(data)

            for item in parsed_array:
                row = parse_trade(item)
                buffer.append(row)

            if len(buffer) >= BATCH_SIZE:
                df = spark.createDataFrame(buffer, schema=schema)

                df.write.format("delta") \
                    .mode("append") \
                    .partitionBy("interval_partition") \
                    .saveAsTable("bronze_binance_mark_price")

                buffer = []
                print(f"{datetime.now(pytz.utc).strftime('%Y-%m-%d %H:%M:%S')} Batch saved with {BATCH_SIZE} records")

await listen_and_store()