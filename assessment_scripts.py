##################################################
#pizza_download_transactions_data job: pizza_download_transactions_data.py
##################################################
import os
import sys
import boto3
from tempfile import mkdtemp
import requests
import json
from awsglue.context import GlueContext
from pyspark.context import SparkContext, SparkConf
from pyspark.sql import SparkSession

conf = SparkConf()
sc = SparkContext.getOrCreate(conf)
glue_context = GlueContext(sc)
spark = glue_context.spark_session
logger = glue_context.get_logger()

def get_last_update():
	s3 = boto3.client('s3')
	last_updated_tempdir = mkdtemp()
	last_updated_tf = f"{last_updated_tempdir}/last_updated.txt"
	s3.download_file('pizza_bucket', 'last_updated.txt', last_updated_tf)
	
	#read file and return timestamp in UTC format as last_update variable

	return last_update

def rest_api_download(last_update):
	#connect to rest api
	api_url = "https://"
	response = requests.get(api_url)
	file = respond.json()

	#download json to temp directory
	s3 = boto3.client('s3')
	json_tempdir = mkdtemp()
	json_tf = f"{json_tempdir}/file.json"
	s3.download_file('pizza_bucket', file, last_updated_tf)

	#parse json
	file_df = pd.read_json(file)
	max_date = max(file_df[order_date])
	
	#if last_update is newer than max(order_date) from REST API JSON then job will be completed
	if last_update > max_date:
		#job completed
	#if last_update is older than max(order_date) from REST API JSON then job will upload all transactions/json to S3
	else:
		#download json to s3

def main():
    #get date from a text file that is updated each successful job run
    last_update = get_last_update()
    file_uploaded = rest_api_download(last_update)

    # If a file was uploaded then trigger the glue job
    if file_uploaded:
        glueclient = boto3.client('glue')
        logger.info(f"Starting glue job: pizza_source_transactions_data")
    	response = glueclient.start_trigger(Name='pizza_source_transactions_trigger')

if __name__ == "__main__":
    main()

##################################################
#pizza_source_customer_data job: pizza_source_customer_data.py
##################################################
from pyspark.context import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyodbc
import pandas as pd

conf = SparkConf()
sc = SparkContext.getOrCreate(conf)
glue_context = GlueContext(sc)
spark = glue_context.spark_session
glue_context.setConf('spark.sql.parquet.writeLegacyFormat', 'true')

def database_query():
	#connect to database; depending on database several ways to connect
	connect = pyodbc.connect('DRIVER={};SERVER=';DATABASE='';UID='';PWD='')
	cursor = connect.cursor()

	#query database
	query = "SELECT * FROM database.table"
	data = pd.read_sql(query, conn)

	#create spark dataframe
	data_spark = spark.createDataFrame(data)

	#write to table
	data_spark.write.option("path", "s3://pizza_bucket/tables/").mode("overwrite").saveAsTable(name="pizza_source.customer_table",format="parquet")


def main():

	database_query()

if __name__ == "__main__":
    main()

##################################################
#pizza_source_transactions_data job: pizza_source_transactions_data.py
##################################################
from pyspark.context import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pandas as pd

conf = SparkConf()
sc = SparkContext.getOrCreate(conf)
glue_context = GlueContext(sc)
spark = glue_context.spark_session
glue_context.setConf('spark.sql.parquet.writeLegacyFormat', 'true')


#this job will read the json file from s3
def get_json():
	s3 = boto3.client('s3')
	json_tempdir = mkdtemp()
	json_tf = f"{json_tempdir}/file.json"
	s3.download_file('pizza_bucket', 'file.json', json_tf)

	return file

#parse json and write to transactions_table
def json_parse(file):
	json_df = pd.read_json(file)
	json_spark = spark.createDataFrame(json_df)
	json_spark.write.option()("path", "s3://pizza_bucket/tables/").mode("append").saveAsTable(name="pizza_source.transactions_table"),format="parquet")

def main():

	file = get_json()
	json_df = json_parse(file)

if __name__ == "__main__":
    main()

##################################################
#pizza_source_population_data job: pizza_source_population_data.py
##################################################

#this job will connect to census.gov api
#pull 'state' and 'population' for last 12 months
#write to state_population_table

##################################################
#pizza_prepared_customer_pop_data job: pizza_prepared_customer_pop_data job.py
##################################################
from pyspark.context import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pandas as pd

conf = SparkConf()
sc = SparkContext.getOrCreate(conf)
glue_context = GlueContext(sc)
spark = glue_context.spark_session
glue_context.setConf('spark.sql.parquet.writeLegacyFormat', 'true')

#reads in customer_table data
customer = spark.sql("SELECT * FROM pizza_source.customer_table")
customer_df = customer.toPandas()

#reads in the state_population_table
state_pop = spark.sql("SELECT * FROM pizzs_source.state_population_table")
state_pop_df = state_pop.toPandas()

#join both tables based on state
combined = pd.merge(customer_df, state_pop_df, on='state', how='left')
combined_df = spark.createDataFrame(combined)

#write to table
combined_df.write.option("path", "s3://pizza_bucket/tables/").mode("overwrite").saveAsTable(name="pizza_prepared.customer_population_table",format="parquet")

##################################################
#pizza_data_warehouse job: pizza_data_warehouse job.py
##################################################

#this job will join transactions_table and customer_population_table by customer_id
#write to data_warehouse table