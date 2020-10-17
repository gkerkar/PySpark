# -*- coding: utf-8 -*-
"""Fire_Department_PySpark_ETL.ipynb

"""

!pip install pyspark

from pyspark.sql import SparkSession

spark = SparkSession\
        .builder\
        .appName("Fire_Department_Data")\
        .getOrCreate()

df = spark.read.csv(path = '/content/drive/My\ Drive/Colab\ Datasets/Fire_Department_Calls_for_Service.csv', header=True, inferSchema=True)

df.count()

df.printSchema()

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType

fireSchema = StructType([
StructField('CallNumber', IntegerType(), True),
StructField('UnitID', StringType(), True),
StructField('IncidentNumber', IntegerType(), True),
StructField('CallType', StringType(), True),
StructField('CallDate', StringType(), True),
StructField('WatchDate', StringType(), True),
StructField('ReceivedDtTm', StringType(), True),
StructField('EntryDtTm', StringType(), True),
StructField('DispatchDtTm', StringType(), True),
StructField('ResponseDtTm', StringType(), True),
StructField('OnSceneDtTm', StringType(), True),
StructField('TransportDtTm', StringType(), True),
StructField('HospitalDtTm', StringType(), True),
StructField('CallFinalDisposition', StringType(), True),
StructField('AvailableDtTm', StringType(), True),
StructField('Address', StringType(), True),
StructField('City', StringType(), True),
StructField('ZipcodeofIncident', IntegerType(), True),
StructField('Battalion', StringType(), True),
StructField('StationArea', StringType(), True),
StructField('Box', StringType(), True),
StructField('OriginalPriority', StringType(), True),
StructField('Priority', StringType(), True),
StructField('FinalPriority', IntegerType(), True),
StructField('ALSUnit', BooleanType(), True),
StructField('CallTypeGroup', StringType(), True),
StructField('NumberofAlarms', IntegerType(), True),
StructField('UnitType', StringType(), True),
StructField('Unitsequenceincalldispatch', IntegerType(), True),
StructField('FirePreventionDistrict', StringType(), True),
StructField('SupervisorDistrict', StringType(), True),
StructField('Neighborhooods-AnalysisBoundaries', StringType(), True),
StructField('Location', StringType(), True),
StructField('RowID', StringType(), True),
StructField('shape', StringType(), True),
StructField('SupervisorDistricts', IntegerType(), True),
StructField('FirePreventionDistricts', IntegerType(), True),
StructField('CurrentPoliceDistricts', IntegerType(), True),
StructField('Neighborhoods-AnalysisBoundaries', IntegerType(), True),
StructField('ZipCodes', IntegerType(), True),
StructField('Neighborhoods(old)', IntegerType(), True),
StructField('PoliceDistricts', IntegerType(), True),
StructField('CivicCenterHarmReductionProjectBoundary', IntegerType(), True),
StructField('HSOCZones', IntegerType(), True),
StructField('CentralMarket/TenderloinBoundaryPolygon-Updated', IntegerType(), True),
StructField('Neighborhoods', IntegerType(), True),
StructField('SFFindNeighborhoods', IntegerType(), True),
StructField('CurrentPoliceDistricts2', IntegerType(), True),
StructField('CurrentSupervisorDistricts', IntegerType(), True)
])

fireServiceCallsDF = spark.read.csv(path = '/content/drive/My\ Drive/Colab\ Datasets/Fire_Department_Calls_for_Service.csv', \
                               header=True, \
                               schema=fireSchema)

fireServiceCallsDF.printSchema()

fireServiceCallsDF.show(5, False)

#How many different types of calls were made to the Fire Department
fireServiceCallsDF.select('CallType').distinct().show(10, False)

#How many incidents of each call type were made to the Fire Department
fireServiceCallsDF.select('CallType').groupBy('CallType').count().orderBy('count',ascending = False).show(10,False)

from pyspark.sql.functions import *

# Change the Date Fields format to convert to Timestamp

from_pattern1 = 'MM/dd/yyyy'
# to_pattern1 = 'yyyy-MM-dd'

from_pattern2 = 'MM/dd/yyyy HH:mm:ss a'
# to_pattern2 = 'MM/dd/yyyy HH:mm:ss.SS'

fireServiceCallsTsDf = fireServiceCallsDF \
                      .withColumn('CallDateTS', unix_timestamp(fireServiceCallsDF['CallDate'],from_pattern1).cast("timestamp")) \
                      .drop('CallDate') \
                      .withColumn('WatchDateTS', unix_timestamp(fireServiceCallsDF['WatchDate'],from_pattern1).cast("timestamp")) \
                      .drop('WatchDate') \
                      .withColumn('ReceivedDtTmTS', unix_timestamp(fireServiceCallsDF['ReceivedDtTm'],from_pattern2).cast("timestamp")) \
                      .drop('ReceivedDtTm') \
                      .withColumn('EntryDtTmTS', unix_timestamp(fireServiceCallsDF['EntryDtTm'],from_pattern2).cast("timestamp")) \
                      .drop('EntryDtTm') \
                      .withColumn('DispatchDtTmTS', unix_timestamp(fireServiceCallsDF['DispatchDtTm'],from_pattern2).cast("timestamp")) \
                      .drop('DispatchDtTm') \
                      .withColumn('ResponseDtTmTS', unix_timestamp(fireServiceCallsDF['ResponseDtTm'],from_pattern2).cast("timestamp")) \
                      .drop('ResponseDtTm') \
                      .withColumn('OnSceneDtTmTS', unix_timestamp(fireServiceCallsDF['OnSceneDtTm'],from_pattern2).cast("timestamp")) \
                      .drop('OnSceneDtTm') \
                      .withColumn('TransportDtTmTS', unix_timestamp(fireServiceCallsDF['TransportDtTm'],from_pattern2).cast("timestamp")) \
                      .drop('TransportDtTm') \
                      .withColumn('HospitalDtTmTS', unix_timestamp(fireServiceCallsDF['HospitalDtTm'],from_pattern2).cast("timestamp")) \
                      .drop('HospitalDtTm') \
                      .withColumn('AvailableDtTmTS', unix_timestamp(fireServiceCallsDF['AvailableDtTm'],from_pattern2).cast("timestamp")) \
                      .drop('AvailableDtTm')

fireServiceCallsTsDf.printSchema()

fireServiceCallsTsDf.show()

#Distinct years in the dataset
fireServiceCallsTsDf.select(year('CallDateTS')).distinct().orderBy(year('CallDateTS')).alias('Year').show(25)

# No. of Service Calls that were logged in the past 7 days

fireServiceCallsTsDf.filter(year('CallDateTS') == '2020') \
                    .filter(dayofyear('CallDateTS') >= 282) \
                    .groupBy('CallType') \
                    .count() \
                    .orderBy('count', ascending = False) \
                    .show(20, False)

# Get the number of partitions
fireServiceCallsTsDf.rdd.getNumPartitions()

# create sql view for the dataframe
fireServiceCallsTsDf.createOrReplaceTempView("FireServiceView")

# spark.catalog.cacheTable("FireServiceView")

# Get the total number of records.
fire_sql = spark.sql("SELECT count(*) FROM FireServiceView")

fire_sql.show()

# Which neighborhood in SF generated the most calls last year
sf_calls_sql = spark.sql("SELECT `Neighborhooods-AnalysisBoundaries`, count(`Neighborhooods-AnalysisBoundaries`) as neighborhood_call_count \
                          FROM FireServiceView \
                          WHERE year(`CallDateTS`) = '2019' \
                          GROUP BY `Neighborhooods-AnalysisBoundaries` \
                          ORDER BY neighborhood_call_count DESC \
                          LIMIT 20 \
                          ")

sf_calls_sql.show(20,False)

sf_calls_sql = spark.sql("SELECT `Neighborhooods-AnalysisBoundaries`, count(`Neighborhooods-AnalysisBoundaries`) as neighborhood_call_count \
                          FROM FireServiceView \
                          WHERE year(`CallDateTS`) = '2020' \
                          GROUP BY `Neighborhooods-AnalysisBoundaries` \
                          ORDER BY neighborhood_call_count DESC \
                          LIMIT 20 \
                          ")

sf_calls_sql.show(20,False)

# Types of calls made from Tenderloin area during this year
sf_calls_sql = spark.sql("SELECT `CallType`, count(`CallType`) as call_type_count \
                          FROM FireServiceView \
                          WHERE year(`CallDateTS`) = '2020' \
                          AND `Neighborhooods-AnalysisBoundaries` = 'Tenderloin' \
                          GROUP BY `CallType` \
                          ORDER BY call_type_count DESC \
                          LIMIT 20 \
                          ")

sf_calls_sql.show(20,False)

# write results to a file.
# sf_calls_sql.write.parquet("/content/drive/My Drive/Colab Datasets/output/")

sf_calls_sql.write.csv("/content/drive/My Drive/Colab Datasets/output/sf_fire_dept_output.csv")
