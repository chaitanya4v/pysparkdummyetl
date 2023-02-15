# Databricks notebook source
# import all the required functions

from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
import uuid

# COMMAND ----------

# Defining Generic functions

def read_parquet_data(path):
  ip_df = spark.read.parquet(path)
  return ip_df

# COMMAND ----------

# Defining the file paths
FilePath_Score_Region= "/Path_of_parquet_blob/MasterData/172.26.0.18/dw_repository/Scoring/score_data/scoring_regions.csv"

# Reading the CSV files
df_score_region_data=(spark.read                        # The DataFrameReader
   .option("header", "true")       # Use first line of all files as header
   .option("sep", ",")            
   .option("encoding", "UTF-8")
   .csv(FilePath_Score_Region)                   # Creates a DataFrame from CSV after reading in the file
)


account_path = '/Path_of_parquet_blob/883030-CRMDB/prod_crm_MSCRM/Merged/AccountBase.parquet'
account_df = read_parquet_data(account_path)

# cdp_dw_account_path = '/Path_of_parquet_blob/883030-CRMDB/prod_crm_MSCRM/Merged/AccountBase.parquet'
# cdp_dw_account_main_df = read_parquet_data(cdp_dw_account_path)
account_df=account_df.dropDuplicates()
account_df.createOrReplaceTempView("vw_account_df")


# COMMAND ----------

# Generate UUID for new column
# Create UDF function to create surrogate key

uuidUdf= udf(lambda : str(uuid.uuid4()),StringType())

# COMMAND ----------

# Drop duplicates
df_score_region_data=df_score_region_data.dropDuplicates()

# Add UTC param
UTCTIME = getArgument('UTCTIME')
# UTCTIME= '2021-12-07T13:10:23.7412073Z'

# Create a dataframe with required columns

df_score_region_data = df_score_region_data.select(
  df_score_region_data.org_id.alias("ACCOUNT_NUMBER"),
  df_score_region_data.region.alias("REGION")
).withColumn("DW_CREATE_DATE",lit(UTCTIME)).withColumn("DW_MODIFIED_DATE",lit(UTCTIME)).withColumn("SCORE_REGION_ID",uuidUdf())

df_score_region_data.createOrReplaceTempView("vw_score_region_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW vw_score_region_data_final AS(
# MAGIC select a.ACCOUNT_NUMBER,a.REGION,a.DW_CREATE_DATE,a.DW_MODIFIED_DATE,a.SCORE_REGION_ID,b.ACCOUNT_ID
# MAGIC from vw_score_region_data a
# MAGIC left join silver_dev.DIM_ACCOUNT b
# MAGIC where a.ACCOUNT_NUMBER=b.ACCOUNTNUMBER
# MAGIC 
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO silver_dev.SILVER_TB_SCORE_REGIONS AS target
# MAGIC 
# MAGIC USING vw_score_region_data_final AS source
# MAGIC ON target.ACCOUNT_NUMBER=source.ACCOUNT_NUMBER
# MAGIC 
# MAGIC WHEN MATCHED AND target.REGION != source.REGION THEN
# MAGIC 
# MAGIC UPDATE SET target.REGION = source.REGION, target.DW_MODIFIED_DATE = source.DW_MODIFIED_DATE
# MAGIC 
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (ACCOUNT_NUMBER,REGION,DW_CREATE_DATE,DW_MODIFIED_DATE,SCORE_REGION_ID,CRM_Account_ID) VALUES(ACCOUNT_NUMBER,REGION,DW_CREATE_DATE,DW_MODIFIED_DATE,SCORE_REGION_ID,ACCOUNT_ID)

# COMMAND ----------

# Read from Delta & Write to parquet

df_score_regions_delta = spark.read.format('delta').load("/Path_of_parquet_blobCRM/TB_SCORE_REGIONS")
df_regions_delta = spark.read.format('delta').load("/Path_of_parquet_blobCRM/DIM_REGIONS")

df_regions_delta=df_regions_delta.select(df_regions_delta.REGION_ID,df_regions_delta.REGION_NAME)

df_score_regions_Silver=df_score_regions_delta.join(
  df_regions_delta,
  df_score_regions_delta.REGION==df_regions_delta.REGION_NAME,
  'left')


df_score_regions_Silver=df_score_regions_Silver.select(df_score_regions_Silver.SCORE_REGION_ID,df_score_regions_Silver.ACCOUNT_NUMBER,df_score_regions_Silver.REGION,df_score_regions_Silver.REGION_ID,df_score_regions_Silver.DW_CREATE_DATE,df_score_regions_Silver.DW_MODIFIED_DATE,df_score_regions_Silver.CRM_Account_ID)

df_score_regions_Silver.write.mode("overwrite").format("parquet").save('/Path_of_parquet_blobPostDataset/SILVER_TB_SCORE_REGIONS')


# COMMAND ----------

# # Adding the watermark

# df_waterMark= spark.read.format('parquet').load('/Path_of_parquet_blob/Silver_waterMark/SILVER_TB_SCORE_REGIONS_WATERMARK')

# WaterMark=df_waterMark.collect()[0][0]

# df_score_regions_Silver_incremented = df_score_regions_Silver.filter(df_score_regions_Silver.DW_MODIFIED_DATE >= WaterMark)

# df_score_regions_Silver_incremented.write.mode("overwrite").format("parquet").save('/Path_of_parquet_blobPostDataset/SILVER_TB_SCORE_REGIONS')

# WaterMark_theme_max_modified_dt = df_score_regions_Silver.select(max(df_score_regions_Silver.DW_MODIFIED_DATE).alias('MaxModifiedOn'))
# WaterMark_theme_max_modified_dt.write.mode("overwrite").format("parquet").save('/Path_of_parquet_blob/Silver_waterMark/SILVER_TB_SCORE_REGIONS_WATERMARK')

# COMMAND ----------

# df_acc_authority_delta_read = spark.read.format('delta').load("/Path_of_parquet_blobMasterData/TB_SCORE_REGIONS")
# WaterMark_theme_max_modified_dt = df_acc_authority_delta_read.select(max(df_acc_authority_delta_read.DW_MODIFIED_DATE).alias('MaxModifiedOn'))
# WaterMark_theme_max_modified_dt.write.mode("overwrite").format("parquet").save('/Path_of_parquet_blob/Silver_waterMark/SILVER_TB_SCORE_REGIONS_WATERMARK')

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table silver_dev.SILVER_TB_SCORE_REGIONS
# MAGIC --select * from silver_dev.SILVER_TB_SCORE_REGIONS
# MAGIC -- CREATE TABLE silver_dev.SILVER_TB_SCORE_REGIONS
# MAGIC -- (
# MAGIC 
# MAGIC -- ACCOUNT_NUMBER string,
# MAGIC -- CRM_Account_ID string,
# MAGIC -- REGION string,
# MAGIC -- DW_CREATE_DATE timestamp,
# MAGIC -- DW_MODIFIED_DATE timestamp,
# MAGIC -- SCORE_REGION_ID string
# MAGIC -- )
# MAGIC -- using delta
# MAGIC -- location '/Path_of_parquet_blobCRM/TB_SCORE_REGIONS'

# COMMAND ----------

