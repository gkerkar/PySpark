# -*- coding: utf-8 -*-
"""Birds PySpark.ipynb

PySpark ETL with DataFrames and without SparkSQL.

"""

# !pip install pyspark

# import PySpark Libraries

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import udf

# Initiate PySpark Session

spark = SparkSession\
        .builder\
        .appName("Birds")\
        .getOrCreate()

# csv file input path.
input_path = '/content/sample_data/birds.csv'

# dataframe schema structure.
input_schema = StructType(
    [
     StructField("Species", StringType()),
     StructField("Category", StringType()),
     StructField("Period", StringType()),
     StructField("Annual Percentage Change", DoubleType())
     ]
)

# Read the csv file.
df = spark.read.csv(path = input_path, header=True, schema=input_schema)

# Print Dataframe Schema.
df.printSchema()

# Get the count of DataFrame records.
df.count()

# Display DataFrame data.
df.show()

# this function derives the English name (from English and Latin species combination) from the Species column. 
def get_english_name(species):
  return species.split('(')[0].strip()

# print('test: {}'.format(get_english_name('Greenfinch (Chloris chloris)')))

# this function returns the year (when the data collection began) from the Period column.
def get_start_year(period):
  return period.split('-')[0].strip('(')

# print('test: {}'.format(get_start_year('(1970-2014)')))

# this function returns the change trend category from the Annual Percentage Change column.
def get_trend(annual_percentage_change):
  trend = ''

  if annual_percentage_change < -3.0:
    trend = 'strong decline'
  elif annual_percentage_change >= -3.0 and annual_percentage_change <= -0.50:
    trend = 'weak decline'
  elif annual_percentage_change > -0.50 and annual_percentage_change < 0.50:
    trend = 'no change'
  elif annual_percentage_change >= 0.50 and annual_percentage_change <= 3.0:
    trend = 'weak increase'
  elif annual_percentage_change > 3.0:
    trend = 'strong increase'
  else:
    trend = 'unknown'

  return trend

# print('test: {}'.format(get_trend(0.44)))

get_english_name = udf(get_english_name, StringType())
get_start_year = udf(get_start_year, StringType())
get_trend = udf(get_trend, StringType())

# spark.udf.register("get_english_name",lambda x: get_english_name(x),StringType())
# spark.udf.register("get_start_year", lambda x: get_start_year(x), StringType())
# spark.udf.register("get_trend", lambda x: get_trend(x), StringType())

df = df.withColumn("species", get_english_name("Species"))
df = df.withColumnRenamed("Category","category")
df = df.withColumn("collect_from_year", get_start_year("Period"))
# new_df = new_df.withColumnRenamed("`Annual Percentage Change`","annual_percentage_change")
df = df.withColumn("annual_percentage_change",df['Annual Percentage Change'])
df = df.withColumn("trend", get_trend("Annual Percentage Change"))
df = df.drop('Period','Annual Percentage Change')

df.show()
