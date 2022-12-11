#!/usr/bin/env python
# coding: utf-8

import argparse
import pyspark

from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

parser = argparse.ArgumentParser()


parser.add_argument('--output_pm25', required=True)
parser.add_argument('--output_all_pm', required=True)

args = parser.parse_args()

output_pm25 = args.output_pm25
output_all_pm = args.output_all_pm


spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

# Change `date` field from string to date type
mod_schema = types.StructType([
    types.StructField('averagingPeriod', types.StructType([types.StructField('unit', types.StringType(), True), types.StructField('value', types.DoubleType(), True)]), True),
    types.StructField('attribution', types.ArrayType(types.StructType([types.StructField('name', types.StringType(), True), types.StructField('url', types.StringType(), True)]), True), True),
  types.StructField('city', types.StringType(), True),
    types.StructField('coordinates', types.StructType([types.StructField('latitude', types.DoubleType(), True), types.StructField('longitude', types.DoubleType(), True)]), True),
    types.StructField('country', types.StringType(), True),
    types.StructField('date', types.StructType([types.StructField('local', types.DateType(), True), types.StructField('utc', types.DateType(), True)]), True),
    types.StructField('location', types.StringType(), True),
    types.StructField('mobile', types.BooleanType(), True),
    types.StructField('parameter', types.StringType(), True),
    types.StructField('sourceName', types.StringType(), True),
    types.StructField('sourceType', types.StringType(), True),
    types.StructField('unit', types.StringType(), True),
    types.StructField('value', types.DoubleType(), True)])

# Replace new schema when read
df = spark.read \
    .option("header", "true") \
    .schema(mod_schema) \
    .json('gs://openaq_data_lake_de-project-370906/raw/realtime-gzipped/*')

# Create temporary view
df.createOrReplaceTempView('openaq')

df_pm25 = spark.sql("""
    SELECT
        country,
        city,
        coordinates,
        coordinates.latitude latitude,
        coordinates.longitude longitude,
        date.local date,
        location,
        parameter,
        sourceName,
        sourceType,
        unit,
        value
    FROM openaq
    WHERE 
        parameter = 'pm25' AND
        (date.local >= '2020-01-19' AND
         date.local <= '2020-01-25')
""")

df_pm25.coalesce(1).write.parquet(output_pm25)

df_all_pm = spark.sql("""
    SELECT
        country,
        city,
        coordinates,
        coordinates.latitude latitude,
        coordinates.longitude longitude,
        date.local date,
        location,
        parameter,
        sourceName,
        sourceType,
        unit,
        value
    FROM openaq
    WHERE 
        parameter IN ('pm25', 'pm10') AND
        (date.local >= '2020-01-19' AND
         date.local <= '2020-01-25')
""")

df_all_pm.coalesce(1).write.parquet(output_all_pm)

