"""
Contains all the Functions required for PySpark Operations
"""

import os
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

def create_spark(appName):
    """To Create Spark Session"""
    spark = SparkSession.builder.appName(appName).getOrCreate()
    return spark

def end_spark(spark):
    """To Stop Spark Session"""
    spark.stop()
    return "Spark Session Stopped"

def query(spark, df, query, name):
    """Execute Spark SQL Query and return the result"""
    df = df.createOrReplaceTempView(name)

    return spark.sql(query).show()

